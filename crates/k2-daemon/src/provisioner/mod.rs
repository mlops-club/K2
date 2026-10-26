//! Where an agent runs (PRD "K2 Distributed Runtime" P3, task T2 phase A;
//! design in `docs/prd/progress/p3-plan.md` section 2).
//!
//! A controller asks its provisioner for a host whenever an agent without a
//! live placement needs one: a spawn, a wake (`k2 msg --wake`, a heartbeat,
//! the app's ensure-pinned-chat), all through `RemoteHost`'s one hook.
//!
//! - [`Local`]: `K2_ROLE=all`, the in-process host. Nothing changes (I5).
//! - [`static_::Static`]: the hosts that enroll and link (VMs, laptops).
//! - Kubernetes (P4): the same rules plus scaling the host pool.
//!
//! Every new placement writes the agent's [`policy`] row: its mode (pinned
//! to a host, or portable) and its last host, so a wake goes back to the
//! host that holds its home (phase A: sticky wakes; home sync is T1).

pub mod policy;
pub mod static_;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::time::Instant;

/// Host labels with a meaning to placement, sent in `hello`.
pub const LABEL_MAX_AWAKE: &str = "k2.max_awake";
pub const LABEL_KIND: &str = "k2.kind";
/// `K2_HOST_MAX_AWAKE` on a host becomes its `k2.max_awake` label.
pub const MAX_AWAKE_VAR: &str = "K2_HOST_MAX_AWAKE";
pub const PROVISIONER_VAR: &str = "K2_PROVISIONER";

/// How an agent may be placed (D11: "pinned" here never means keep-awake
/// or a terminal size).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    /// Only on this host. When it is away, the agent waits for it.
    Pinned { host: String },
    /// Any host; back to its last one when it can.
    Portable,
}

/// What a host is, from its `k2.kind` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostKind {
    Server,
    Laptop,
}

/// One agent's need for a host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Need {
    pub agent: String,
    pub mode: Mode,
    pub last_host: Option<String>,
    pub home_seq: Option<u64>,
    /// Labels the host must carry (`key=value`, or a bare `key`).
    pub labels: Vec<String>,
}

impl Need {
    /// A new agent: portable, no history.
    pub fn new(agent: &str) -> Need {
        Need { agent: agent.to_string(), mode: Mode::Portable, last_host: None, home_seq: None, labels: vec![] }
    }
}

/// What placement knows of a host the controller has seen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostView {
    pub host_id: String,
    /// Its link is up.
    pub linked: bool,
    /// Known (enrolled, seen, or holding a placement) but its link is down.
    pub away: bool,
    /// It said it is draining (SIGTERM, S8), or a pool is draining it.
    pub draining: bool,
    /// Agents placed on it now.
    pub awake: u32,
    /// `k2.max_awake`; `None` = no limit.
    pub max_awake: Option<u32>,
    pub kind: HostKind,
    pub labels: BTreeMap<String, String>,
}

impl HostView {
    /// A view of a host from its `hello` labels.
    pub fn from_labels(host_id: &str, labels: &[String]) -> HostView {
        let labels = parse_labels(labels);
        let max_awake = labels.get(LABEL_MAX_AWAKE).and_then(|v| v.parse().ok());
        let kind = if labels.get(LABEL_KIND).map(String::as_str) == Some("laptop") { HostKind::Laptop } else { HostKind::Server };
        HostView { host_id: host_id.to_string(), linked: false, away: false, draining: false, awake: 0, max_awake, kind, labels }
    }

    /// Takes a new agent now: linked, not draining.
    pub fn usable(&self) -> bool {
        self.linked && !self.away && !self.draining
    }

    /// Room for `n` more agents under `k2.max_awake`.
    pub fn room(&self) -> Option<u32> {
        self.max_awake.map(|m| m.saturating_sub(self.awake))
    }

    pub fn has_room(&self) -> bool {
        self.room() != Some(0)
    }

    /// Carries every label in `want` (`key=value`, or `key` for any value).
    pub fn matches(&self, want: &[String]) -> bool {
        want.iter().all(|w| match w.split_once('=') {
            Some((k, v)) => self.labels.get(k.trim()).is_some_and(|have| have == v.trim()),
            None => self.labels.contains_key(w.trim()),
        })
    }
}

/// `k=v` and bare `k` labels as a map (a bare label maps to "").
pub fn parse_labels(labels: &[String]) -> BTreeMap<String, String> {
    labels
        .iter()
        .filter(|l| !l.trim().is_empty())
        .map(|l| match l.split_once('=') {
            Some((k, v)) => (k.trim().to_string(), v.trim().to_string()),
            None => (l.trim().to_string(), String::new()),
        })
        .collect()
}

/// Why a host was chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Why {
    Pinned,
    LastHost,
    MostRoom,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Choice {
    pub host: String,
    pub why: Why,
}

/// No host for the need now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// No host is linked at all.
    NoHosts,
    /// Pinned to a host that is away: the agent waits for it, never moves.
    PinnedAway { agent: String, host: String },
    /// Pinned to a host that is linked but at `k2.max_awake`.
    PinnedFull { agent: String, host: String, max_awake: u32 },
    /// Every usable host is at `k2.max_awake`.
    Full { hosts: usize },
    /// No usable host carries the labels.
    NoMatch { labels: Vec<String> },
}

impl Refusal {
    /// The HTTP status a spawn answers with.
    pub fn status(&self) -> &'static str {
        "503 Service Unavailable"
    }

    pub fn message(&self) -> String {
        match self {
            Refusal::NoHosts => crate::agent_host::remote::NO_HOSTS.to_string(),
            Refusal::PinnedAway { agent, host } => {
                format!("agent {agent} is pinned to host {host}, which is away; it waits for {host} to link again (it is never placed elsewhere)")
            }
            Refusal::PinnedFull { agent, host, max_awake } => format!(
                "agent {agent} is pinned to host {host}, which has {max_awake} agents awake (k2.max_awake, {MAX_AWAKE_VAR} on the host)"
            ),
            Refusal::Full { hosts } => format!(
                "every agent host is full: {hosts} linked host(s) at k2.max_awake ({MAX_AWAKE_VAR} on each host); let an agent sleep or add a host"
            ),
            Refusal::NoMatch { labels } => format!("no linked agent host carries the labels {labels:?}"),
        }
    }
}

/// `ensure_host`'s answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ensure {
    Ready(String),
    /// A host is coming (Kubernetes scale-up) or returning (a pinned
    /// agent's away host): retry when [`Provisioner::on_hello`] says so.
    Wait(String),
    Refused(String),
}

/// `idle`'s answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdleStep {
    Nothing,
    /// Release this host's agents (sleep); it is draining.
    Drain(String),
    /// The host is gone (scaled down).
    ScaledDown { host: String, to: u32 },
}

pub trait Provisioner: Send + Sync {
    /// `"local"`, `"static"` or `"kubernetes"`.
    fn kind(&self) -> &'static str;

    /// The host for `need` among `hosts`, or why there is none now.
    fn choose(&self, need: &Need, hosts: &[HostView]) -> Result<Choice, Refusal>;

    /// [`Self::choose`], or make a host appear (Kubernetes: scale up).
    fn ensure_host(&self, need: &Need, hosts: &[HostView]) -> Ensure {
        match self.choose(need, hosts) {
            Ok(c) => Ensure::Ready(c.host),
            Err(r @ Refusal::PinnedAway { .. }) => Ensure::Wait(r.message()),
            Err(r) => Ensure::Refused(r.message()),
        }
    }

    /// One idle tick (Static: nothing; Kubernetes: scale down).
    fn idle(&self, _now: Instant, _hosts: &[HostView]) -> IdleStep {
        IdleStep::Nothing
    }

    /// A host's hello: `true` when needs that got `Wait` should be retried.
    fn on_hello(&self, _host_id: &str) -> bool {
        false
    }
}

/// `K2_ROLE=all`: the in-process host is the only host.
pub struct Local;

pub const LOCAL_HOST: &str = "local";

impl Provisioner for Local {
    fn kind(&self) -> &'static str {
        "local"
    }
    fn choose(&self, _need: &Need, _hosts: &[HostView]) -> Result<Choice, Refusal> {
        Ok(Choice { host: LOCAL_HOST.to_string(), why: Why::MostRoom })
    }
}

/// The provisioner `K2_PROVISIONER` names for this role. Unset: `local`
/// under `K2_ROLE=all`, `static` on a controller. `kubernetes` is P4's;
/// until this build has it, a controller uses `static` and says so.
pub fn select(role: crate::runtime_role::Role, raw: Option<&str>) -> Result<Box<dyn Provisioner>, String> {
    use crate::runtime_role::Role;
    let v = raw.map(str::trim).filter(|v| !v.is_empty());
    match (role, v) {
        (Role::All, None | Some("local")) => Ok(Box::new(Local)),
        (Role::All, Some(other)) => Err(format!("{PROVISIONER_VAR}={other} needs K2_ROLE=controller (K2_ROLE=all runs agents in-process)")),
        (Role::Host, _) => Ok(Box::new(Local)),
        (Role::Controller, None | Some("static")) => Ok(Box::new(static_::Static)),
        (Role::Controller, Some("kubernetes")) => {
            k2_core::log_debug!("[provisioner] {PROVISIONER_VAR}=kubernetes: not in this build (P4); placing on the linked hosts (static)");
            Ok(Box::new(static_::Static))
        }
        (Role::Controller, Some("local")) => Err(format!("{PROVISIONER_VAR}=local: a controller runs no agents; use static or kubernetes")),
        (_, Some(other)) => Err(format!("{PROVISIONER_VAR}={other:?}: use local, static or kubernetes")),
    }
}
