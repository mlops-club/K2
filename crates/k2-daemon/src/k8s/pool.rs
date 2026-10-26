//! The host pool's scaler (P4 T1, `docs/prd/progress/p4-plan.md` section
//! 1): the `ensure_host` and `idle` halves of P3's `Provisioner` for
//! `K2_PROVISIONER=kubernetes`. It moves behind P3's trait in
//! `provisioner/kubernetes.rs` once that exists; `choose` stays P3's.
//!
//! - **Scale up**: when no usable host has room under `k2.max_awake`, mint
//!   a one-time enroll token for the next ordinal (unless that ordinal is
//!   already enrolled: its key is on its volume), put it in the Secret
//!   `k2-host-enroll`, *then* raise the StatefulSet's replicas, and answer
//!   `Wait`. The need is retried when the new host's signed hello arrives
//!   ([`Pool::on_hello`]), never when its pod reads Ready.
//! - **Limit**: at `K2_PROVISIONER_MAX_HOSTS` the answer is a refusal that
//!   names the limit.
//! - **Scale down**: StatefulSets shrink from the highest ordinal only. When
//!   that host's agents have fit in the others' spare room for
//!   `K2_PROVISIONER_IDLE_AFTER`, it is marked draining (never chosen), its
//!   agents are released by the caller (P3 sleep, home snapshot), and once
//!   it has none the replicas go down by one. Its volume and key stay.
//! - **Never** below `K2_PROVISIONER_MIN_HOSTS`, and never a host that is
//!   away or draining on its own (SIGTERM, S8).

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

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
            statefulset: get("K2_PROVISIONER_STATEFULSET").filter(|v| !v.is_empty()).unwrap_or(d.statefulset),
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
    pub client: super::Client,
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

/// What the pool needs to know of a host the controller has seen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostView {
    pub host_id: String,
    /// Its link is up (it said hello).
    pub linked: bool,
    /// Its link is down; its agents keep their placement there (W3).
    pub away: bool,
    /// It said it is draining (SIGTERM, S8).
    pub draining: bool,
    pub awake: u32,
    /// `k2.max_awake` from its hello labels.
    pub max_awake: u32,
}

impl HostView {
    fn free(&self) -> u32 {
        self.max_awake.saturating_sub(self.awake)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ensure {
    /// A usable host with room: the most free one.
    Ready(String),
    /// A host is coming; retry the need when [`Pool::on_hello`] says so.
    Wait(String),
    Refused(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdleStep {
    Nothing,
    /// Release this host's agents (P3 sleep, home snapshot); it is draining
    /// and is never chosen again until it is gone.
    Drain(String),
    /// Replicas went down to `to`; `host` is gone (its volume and key stay).
    ScaledDown { host: String, to: u32 },
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

pub struct Pool {
    pub settings: PoolSettings,
    api: Box<dyn PoolApi>,
    enroll: Arc<dyn Enroller>,
    st: Mutex<PoolState>,
}

impl Pool {
    pub fn new(settings: PoolSettings, api: Box<dyn PoolApi>, enroll: Arc<dyn Enroller>) -> Pool {
        Pool { settings, api, enroll, st: Mutex::default() }
    }

    /// `k2-host-N`: the pod's name, and its `K2_HOST_NAME`.
    pub fn host_name(&self, ordinal: u32) -> String {
        format!("{}-{ordinal}", self.settings.statefulset)
    }

    pub fn ordinal(&self, host_id: &str) -> Option<u32> {
        host_id.strip_prefix(&self.settings.statefulset)?.strip_prefix('-')?.parse().ok()
    }

    /// True for a host this pool is draining: `choose` must skip it.
    pub fn is_draining(&self, host_id: &str) -> bool {
        self.st.lock().draining.as_deref() == Some(host_id)
    }

    /// Hosts a need can go to: linked, not away, not draining (on their own
    /// or for this pool's scale-down).
    fn usable<'a>(hosts: &'a [HostView], draining: Option<&str>) -> Vec<&'a HostView> {
        hosts.iter().filter(|h| h.linked && !h.away && !h.draining && Some(h.host_id.as_str()) != draining).collect()
    }

    /// A host for one more awake agent, or a scale-up and `Wait`, or the
    /// limit. Blocking (Kubernetes calls): run it on a blocking thread.
    pub fn ensure_host(&self, hosts: &[HostView]) -> Ensure {
        let mut st = self.st.lock();
        let best = Self::usable(hosts, st.draining.as_deref())
            .into_iter()
            .filter(|h| h.free() > 0)
            .max_by_key(|h| (h.free(), std::cmp::Reverse(h.host_id.clone())));
        if let Some(h) = best {
            return Ensure::Ready(h.host_id.clone());
        }
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
        k2_core::log_debug!("[k8s/pool] scaled {} {replicas} -> {}: waiting for {name}'s hello", self.settings.statefulset, replicas + 1);
        k2_core::trace::emit(|| k2_core::trace::Event::new("ctl.pool_scale").attr("host", name.as_str()).attr("replicas", replicas + 1));
        st.scaling = Some(name.clone());
        st.spare_since = None;
        Ensure::Wait(format!("scaling up: waiting for {name}'s signed hello"))
    }

    /// A host's signed hello. `true` when a scale-up was waiting for it:
    /// retry the needs that got `Wait`.
    pub fn on_hello(&self, host_id: &str) -> bool {
        let mut st = self.st.lock();
        if st.scaling.as_deref() == Some(host_id) {
            st.scaling = None;
            return true;
        }
        false
    }

    /// One idle tick at `now` over the hosts the controller sees.
    pub fn idle(&self, now: Instant, hosts: &[HostView]) -> IdleStep {
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
            if top.awake > 0 {
                return IdleStep::Nothing; // its agents are being released
            }
            if let Err(e) = self.api.set_replicas(replicas - 1) {
                k2_core::log_debug!("[k8s/pool] scale {} down to {}: {e}", self.settings.statefulset, replicas - 1);
                return IdleStep::Nothing;
            }
            k2_core::log_debug!("[k8s/pool] scaled {} {replicas} -> {}: {highest} gone", self.settings.statefulset, replicas - 1);
            k2_core::trace::emit(|| k2_core::trace::Event::new("ctl.pool_scale").attr("host", highest.as_str()).attr("replicas", replicas - 1));
            st.draining = None;
            st.spare_since = None;
            return IdleStep::ScaledDown { host: highest, to: replicas - 1 };
        }
        let room_elsewhere: u32 = Self::usable(hosts, None).into_iter().filter(|h| h.host_id != highest).map(HostView::free).sum();
        if top.awake > room_elsewhere {
            st.spare_since = None;
            return IdleStep::Nothing;
        }
        let since = *st.spare_since.get_or_insert(now);
        if now.saturating_duration_since(since) < self.settings.idle_after {
            return IdleStep::Nothing;
        }
        k2_core::log_debug!("[k8s/pool] {highest} idle for {:?}: draining it for a scale-down", self.settings.idle_after);
        st.draining = Some(highest.clone());
        IdleStep::Drain(highest)
    }
}

#[cfg(test)]
mod tests;
