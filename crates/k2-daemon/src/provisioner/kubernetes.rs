//! `Kubernetes`: the host pool (P4 T1, `docs/prd/progress/p4-plan.md`
//! section 1). Placement is [`Static`]'s; on top of it the pool scales the
//! StatefulSet `k2-host`:
//!
//! - **Scale up**: when no usable host has room under `k2.max_awake`, mint
//!   a one-time enroll token for the next ordinal (unless that ordinal is
//!   already enrolled: its key is on its volume), put it in the Secret
//!   `k2-host-enroll`, *then* raise the replicas, and answer `Wait`. The
//!   need is retried when the new host's signed hello arrives
//!   ([`Provisioner::on_hello`]), never when its pod reads Ready.
//! - **Limit**: at `K2_PROVISIONER_MAX_HOSTS` the answer is a refusal that
//!   names the limit.
//! - **Scale down**: StatefulSets shrink from the highest ordinal only. When
//!   that host's agents have fit in the others' spare room for
//!   `K2_PROVISIONER_IDLE_AFTER`, it is marked draining (never chosen), the
//!   controller releases its agents, and once it has none the replicas go
//!   down by one. Its volume and key stay.
//! - **Never** below `K2_PROVISIONER_MIN_HOSTS`, never a host that is away
//!   or draining on its own (SIGTERM, S8), and never a scale-up for a pinned
//!   agent (it waits for its own host).
//!
//! Kubernetes and enroll calls block and may take the daemon's shared DB
//! lock (minting): `RemoteHost` calls the provisioner off its own lock.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::static_::Static;
use super::{Choice, Ensure, HostView, IdleStep, Mode, Need, Provisioner, Refusal};
use crate::k8s;

/// `K2_PROVISIONER_*` and the pool's object names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolSettings {
    pub statefulset: String,
    pub enroll_secret: String,
    pub min_hosts: u32,
    pub max_hosts: u32,
    pub idle_after: Duration,
    /// How long a minted token is good for (the pod has to start in time).
    pub token_ttl_ms: i64,
}

impl Default for PoolSettings {
    fn default() -> Self {
        PoolSettings {
            statefulset: "k2-host".into(),
            enroll_secret: "k2-host-enroll".into(),
            min_hosts: 1,
            max_hosts: 3,
            idle_after: Duration::from_secs(15 * 60),
            token_ttl_ms: crate::hosts::enroll::TOKEN_TTL_MS,
        }
    }
}

impl PoolSettings {
    pub fn from_env(get: impl Fn(&str) -> Option<String>) -> Result<PoolSettings, String> {
        let d = PoolSettings::default();
        let num = |k: &str, def: u64| -> Result<u64, String> {
            match get(k).map(|v| v.trim().to_string()).filter(|v| !v.is_empty()) {
                None => Ok(def),
                Some(v) => v.parse::<u64>().map_err(|_| format!("{k}={v:?} is not a whole number")),
            }
        };
        let s = PoolSettings {
            statefulset: get("K2_PROVISIONER_HOST_STATEFULSET").filter(|v| !v.is_empty()).unwrap_or(d.statefulset),
            enroll_secret: get("K2_HOST_ENROLL_SECRET").filter(|v| !v.is_empty()).unwrap_or(d.enroll_secret),
            min_hosts: num("K2_PROVISIONER_MIN_HOSTS", d.min_hosts as u64)? as u32,
            max_hosts: num("K2_PROVISIONER_MAX_HOSTS", d.max_hosts as u64)? as u32,
            idle_after: Duration::from_secs(num("K2_PROVISIONER_IDLE_AFTER", d.idle_after.as_secs())?),
            token_ttl_ms: d.token_ttl_ms,
        };
        if s.min_hosts > s.max_hosts {
            return Err(format!("K2_PROVISIONER_MIN_HOSTS={} is above K2_PROVISIONER_MAX_HOSTS={}", s.min_hosts, s.max_hosts));
        }
        if s.max_hosts == 0 {
            return Err("K2_PROVISIONER_MAX_HOSTS=0 leaves no host for any agent".into());
        }
        Ok(s)
    }
}

/// The Kubernetes calls the pool makes.
pub trait PoolApi: Send + Sync {
    fn replicas(&self) -> Result<u32, String>;
    fn set_replicas(&self, n: u32) -> Result<(), String>;
    fn put_enroll_token(&self, host_name: &str, token: &str) -> Result<(), String>;
}

/// [`PoolApi`] on the real API: the StatefulSet's scale and the enroll Secret.
pub struct K8sPool {
    pub client: k8s::Client,
    pub statefulset: String,
    pub enroll_secret: String,
}

impl PoolApi for K8sPool {
    fn replicas(&self) -> Result<u32, String> {
        Ok(self.client.get_scale(&self.statefulset)?.spec)
    }
    fn set_replicas(&self, n: u32) -> Result<(), String> {
        self.client.set_replicas(&self.statefulset, n).map(|_| ()).map_err(String::from)
    }
    fn put_enroll_token(&self, host_name: &str, token: &str) -> Result<(), String> {
        Ok(self.client.put_secret_key(&self.enroll_secret, host_name, token.as_bytes())?)
    }
}

/// W5 enrollment as the pool needs it (D8).
pub trait Enroller: Send + Sync {
    /// `host_name` holds a key that isn't revoked: its pod relinks by key.
    fn enrolled(&self, host_name: &str) -> bool;
    /// A one-time token, from inside the controller (no HTTP).
    fn mint(&self, ttl_ms: i64) -> Result<String, String>;
}

impl Enroller for crate::hosts::enroll::Registry {
    fn enrolled(&self, host_name: &str) -> bool {
        self.keys().is_ok_and(|ks| ks.iter().any(|k| k.host_name == host_name && k.revoked_ms.is_none()))
    }
    fn mint(&self, ttl_ms: i64) -> Result<String, String> {
        Ok(self.mint_token(crate::hosts::enroll::now_ms(), ttl_ms)?.token)
    }
}

/// The controller's own registry, looked up at each call: it is reached
/// through `remote::global()`, which is still being built when the
/// provisioner is.
pub struct ControllerEnroll;

impl Enroller for ControllerEnroll {
    fn enrolled(&self, host_name: &str) -> bool {
        crate::hosts::enroll::controller_registry().is_ok_and(|r| Enroller::enrolled(r.as_ref(), host_name))
    }
    fn mint(&self, ttl_ms: i64) -> Result<String, String> {
        Enroller::mint(crate::hosts::enroll::controller_registry()?.as_ref(), ttl_ms)
    }
}

#[derive(Default)]
struct PoolState {
    /// The host the last scale-up waits for.
    scaling: Option<String>,
    /// Since when the highest ordinal's agents would fit elsewhere.
    spare_since: Option<Instant>,
    /// The host this pool is draining for a scale-down.
    draining: Option<String>,
}

pub struct Kubernetes {
    pub settings: PoolSettings,
    api: Box<dyn PoolApi>,
    enroll: Arc<dyn Enroller>,
    st: Mutex<PoolState>,
}

impl Kubernetes {
    pub fn new(settings: PoolSettings, api: Box<dyn PoolApi>, enroll: Arc<dyn Enroller>) -> Kubernetes {
        Kubernetes { settings, api, enroll, st: Mutex::default() }
    }

    /// In a pod: the ServiceAccount's client, `K2_PROVISIONER_*`, and the
    /// controller's enroll registry.
    pub fn in_cluster() -> Result<Kubernetes, String> {
        let settings = PoolSettings::from_env(|k| std::env::var(k).ok())?;
        let client = k8s::Client::new(k8s::Config::in_cluster()?)?;
        let api = K8sPool { client, statefulset: settings.statefulset.clone(), enroll_secret: settings.enroll_secret.clone() };
        Ok(Kubernetes::new(settings, Box::new(api), Arc::new(ControllerEnroll)))
    }

    /// `k2-host-N`: the pod's name, and its `K2_HOST_NAME`.
    pub fn host_name(&self, ordinal: u32) -> String {
        format!("{}-{ordinal}", self.settings.statefulset)
    }

    pub fn ordinal(&self, host_id: &str) -> Option<u32> {
        host_id.strip_prefix(&self.settings.statefulset)?.strip_prefix('-')?.parse().ok()
    }

    /// True for a host this pool is draining: never chosen.
    pub fn is_draining(&self, host_id: &str) -> bool {
        self.st.lock().draining.as_deref() == Some(host_id)
    }

    /// `hosts` with this pool's draining mark applied.
    fn marked(&self, hosts: &[HostView]) -> Vec<HostView> {
        let draining = self.st.lock().draining.clone();
        hosts
            .iter()
            .cloned()
            .map(|mut h| {
                if draining.as_deref() == Some(h.host_id.as_str()) {
                    h.draining = true;
                }
                h
            })
            .collect()
    }

    /// No usable host has room: scale up by one (or wait for a pod that has
    /// no hello yet), or refuse at the limit.
    fn scale_up(&self, hosts: &[HostView]) -> Ensure {
        let mut st = self.st.lock();
        if let Some(n) = &st.scaling {
            return Ensure::Wait(format!("scaling up: waiting for {n}'s signed hello"));
        }
        let replicas = match self.api.replicas() {
            Ok(n) => n,
            Err(e) => return Ensure::Refused(format!("can't read the host pool's size: {e}")),
        };
        // A pod that exists but hasn't said hello (after a controller restart,
        // or still starting): wait for it rather than add another.
        if let Some(missing) = (0..replicas).map(|i| self.host_name(i)).find(|n| !hosts.iter().any(|h| &h.host_id == n)) {
            st.scaling = Some(missing.clone());
            return Ensure::Wait(format!("{missing} is starting: waiting for its signed hello"));
        }
        if replicas >= self.settings.max_hosts {
            return Ensure::Refused(format!(
                "host pool full: K2_PROVISIONER_MAX_HOSTS={} reached ({replicas} host(s), none with room under k2.max_awake)",
                self.settings.max_hosts
            ));
        }
        let name = self.host_name(replicas);
        // The token first: the pod must find it when it starts.
        if !self.enroll.enrolled(&name) {
            let token = match self.enroll.mint(self.settings.token_ttl_ms) {
                Ok(t) => t,
                Err(e) => return Ensure::Refused(format!("can't mint an enroll token for {name}: {e}")),
            };
            if let Err(e) = self.api.put_enroll_token(&name, &token) {
                return Ensure::Refused(format!("can't put {name}'s enroll token in Secret {}: {e}", self.settings.enroll_secret));
            }
        }
        if let Err(e) = self.api.set_replicas(replicas + 1) {
            return Ensure::Refused(format!("can't scale {} to {}: {e}", self.settings.statefulset, replicas + 1));
        }
        k2_core::log_debug!("[provisioner] scaled {} {replicas} -> {}: waiting for {name}'s hello", self.settings.statefulset, replicas + 1);
        k2_core::trace::emit(|| k2_core::trace::Event::new("ctl.pool_scale").attr("host", name.as_str()).attr("replicas", replicas + 1));
        st.scaling = Some(name.clone());
        st.spare_since = None;
        Ensure::Wait(format!("scaling up: waiting for {name}'s signed hello"))
    }
}

impl Provisioner for Kubernetes {
    fn kind(&self) -> &'static str {
        "kubernetes"
    }

    fn choose(&self, need: &Need, hosts: &[HostView]) -> Result<Choice, Refusal> {
        Static.choose(need, &self.marked(hosts))
    }

    fn ensure_host(&self, need: &Need, hosts: &[HostView]) -> Ensure {
        let hosts = self.marked(hosts);
        match Static.choose(need, &hosts) {
            Ok(c) => Ensure::Ready(c.host),
            // Room in the pool is the answer only for a portable agent.
            Err(Refusal::NoHosts | Refusal::Full { .. }) if matches!(need.mode, Mode::Portable) => self.scale_up(&hosts),
            Err(r) => Ensure::Refused(r.message()),
        }
    }

    fn idle(&self, now: Instant, hosts: &[HostView]) -> IdleStep {
        let mut st = self.st.lock();
        if st.scaling.is_some() {
            st.spare_since = None;
            return IdleStep::Nothing;
        }
        let Ok(replicas) = self.api.replicas() else { return IdleStep::Nothing };
        if replicas <= self.settings.min_hosts {
            st.spare_since = None;
            st.draining = None;
            return IdleStep::Nothing;
        }
        let highest = self.host_name(replicas - 1);
        let Some(top) = hosts.iter().find(|h| h.host_id == highest) else {
            st.spare_since = None;
            return IdleStep::Nothing;
        };
        // Away, or draining on its own (SIGTERM): not ours to touch.
        if top.away || top.draining || !top.linked {
            st.spare_since = None;
            return IdleStep::Nothing;
        }
        if st.draining.as_deref() == Some(highest.as_str()) {
            if top.awake > 0 || top.saving > 0 {
                return IdleStep::Nothing; // its agents are being released, their homes saved
            }
            if let Err(e) = self.api.set_replicas(replicas - 1) {
                k2_core::log_debug!("[provisioner] scale {} down to {}: {e}", self.settings.statefulset, replicas - 1);
                return IdleStep::Nothing;
            }
            k2_core::log_debug!("[provisioner] scaled {} {replicas} -> {}: {highest} gone", self.settings.statefulset, replicas - 1);
            k2_core::trace::emit(|| k2_core::trace::Event::new("ctl.pool_scale").attr("host", highest.as_str()).attr("replicas", replicas - 1));
            st.draining = None;
            st.spare_since = None;
            return IdleStep::ScaledDown { host: highest, to: replicas - 1 };
        }
        let room_elsewhere: u64 = hosts
            .iter()
            .filter(|h| h.usable() && h.host_id != highest)
            .map(|h| h.room().map(u64::from).unwrap_or(u64::from(u32::MAX)))
            .sum();
        if u64::from(top.awake) > room_elsewhere {
            st.spare_since = None;
            return IdleStep::Nothing;
        }
        let since = *st.spare_since.get_or_insert(now);
        if now.saturating_duration_since(since) < self.settings.idle_after {
            return IdleStep::Nothing;
        }
        k2_core::log_debug!("[provisioner] {highest} idle for {:?}: draining it for a scale-down", self.settings.idle_after);
        st.draining = Some(highest.clone());
        IdleStep::Drain(highest)
    }

    fn on_hello(&self, host_id: &str) -> bool {
        let mut st = self.st.lock();
        if st.scaling.as_deref() == Some(host_id) {
            st.scaling = None;
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests;
