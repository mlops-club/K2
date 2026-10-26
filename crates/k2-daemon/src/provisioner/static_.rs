//! `Static`: the hosts that enroll and link (VMs, laptops). P3 plan
//! section 2, in order:
//!
//! 1. A pinned agent goes only to its pinned host. If that host is away
//!    the answer is to wait for it, never another host.
//! 2. A portable agent goes back to its last host if that host is usable
//!    and under `k2.max_awake` (its home is there).
//! 3. Otherwise the usable host with the agent's labels and the most room
//!    (fewest awake when no host sets a limit; ties by name).
//! 4. No host with room: the refusal names `K2_HOST_MAX_AWAKE`.
//!
//! Usable = linked, not away, not draining.

use super::{Choice, HostView, Mode, Need, Provisioner, Refusal, Why};

pub struct Static;

impl Provisioner for Static {
    fn kind(&self) -> &'static str {
        "static"
    }

    fn choose(&self, need: &Need, hosts: &[HostView]) -> Result<Choice, Refusal> {
        if let Mode::Pinned { host } = &need.mode {
            return match hosts.iter().find(|h| &h.host_id == host).filter(|h| h.usable()) {
                Some(h) if h.has_room() => Ok(Choice { host: host.clone(), why: Why::Pinned }),
                Some(h) => Err(Refusal::PinnedFull { agent: need.agent.clone(), host: host.clone(), max_awake: h.max_awake.unwrap_or(0) }),
                None if hosts.iter().any(|h| &h.host_id == host && h.linked && h.draining) => {
                    Err(Refusal::PinnedDraining { agent: need.agent.clone(), host: host.clone() })
                }
                None => Err(Refusal::PinnedAway { agent: need.agent.clone(), host: host.clone() }),
            };
        }
        if let Some(last) = &need.last_host {
            if hosts.iter().any(|h| &h.host_id == last && h.usable() && h.has_room() && h.matches(&need.labels)) {
                return Ok(Choice { host: last.clone(), why: Why::LastHost });
            }
        }
        let usable: Vec<&HostView> = hosts.iter().filter(|h| h.usable()).collect();
        if usable.is_empty() {
            return Err(Refusal::NoHosts);
        }
        let matching: Vec<&HostView> = usable.iter().copied().filter(|h| h.matches(&need.labels)).collect();
        if matching.is_empty() {
            return Err(Refusal::NoMatch { labels: need.labels.clone() });
        }
        // Most room first: a host with no limit counts as unlimited room;
        // among those, the fewest awake. Ties by name.
        let best = matching
            .iter()
            .filter(|h| h.has_room())
            .min_by_key(|h| (std::cmp::Reverse(h.room().unwrap_or(u32::MAX)), h.awake, h.host_id.clone()));
        match best {
            Some(h) => Ok(Choice { host: h.host_id.clone(), why: Why::MostRoom }),
            None => Err(Refusal::Full { hosts: matching.len() }),
        }
    }
}
