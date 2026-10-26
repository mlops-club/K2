//! `K2_ROLE` (PRD K2 Distributed Runtime): which half of K2 this daemon
//! runs.
//!
//! - unset, empty or `all` (today): the controller plus the in-process
//!   agent host.
//! - `controller` (P2 first cut): the controller only. Agents run on
//!   agent hosts that dial in (`crate::hosts`); needs `K2_HOSTS=1` and
//!   `K2_HOST_BOOTSTRAP_TOKEN_FILE`.
//! - `host` (P2 first cut, until `k2-host` exists): today's daemon plus a
//!   link to the controller at `K2_CONTROLLER_URL`; needs
//!   `K2_HOST_ENROLL_TOKEN_FILE`.
//!
//! Anything else is a boot error naming the allowed values. Checked once,
//! before the runtime starts (`daemon_main::run`), which then records the
//! role for [`current`].

use std::sync::OnceLock;

pub const ENV_VAR: &str = "K2_ROLE";

/// What a booted daemon runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    All,
    Controller,
    Host,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::All => "all",
            Role::Controller => "controller",
            Role::Host => "host",
        }
    }
}

const ALLOWED: &str = "all, controller, host";

/// Pure decision behind [`from_env`] for the role name alone. `raw` is
/// the variable's value, if set. The role's own settings are checked by
/// [`check_settings`].
pub fn parse(raw: Option<&str>) -> Result<Role, String> {
    match raw.map(str::trim) {
        None | Some("") | Some("all") => Ok(Role::All),
        Some("controller") => Ok(Role::Controller),
        Some("host") => Ok(Role::Host),
        Some(other) => Err(format!("{ENV_VAR}={other:?} is not a role; allowed values: {ALLOWED}")),
    }
}

/// The settings `role` needs, read through `get`: nothing for `all`.
pub fn check_settings(role: Role, get: impl Fn(&str) -> Option<String>) -> Result<(), String> {
    match role {
        Role::All => Ok(()),
        Role::Controller => crate::hosts::controller_config(&get).and_then(|_| crate::hosts::enroll::auth_mode(&get)).map(drop),
        Role::Host => crate::hosts::host_config(&get).and_then(|_| crate::hosts::enroll::auth_mode(&get)).map(drop),
    }
}

/// [`parse`] and [`check_settings`] on the process environment. A
/// non-UTF-8 value is an error, never "unset".
pub fn from_env() -> Result<Role, String> {
    let role = match std::env::var(ENV_VAR) {
        Ok(v) => parse(Some(&v)),
        Err(std::env::VarError::NotPresent) => parse(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{ENV_VAR} is not valid UTF-8; allowed values: {ALLOWED}")),
    }?;
    check_settings(role, |k| std::env::var(k).ok())?;
    Ok(role)
}

static CURRENT: OnceLock<Role> = OnceLock::new();

/// Record the role boot validated. Once per process; later calls are
/// ignored.
pub fn set_current(role: Role) {
    let _ = CURRENT.set(role);
}

/// The role this daemon booted as. `All` until boot records one, so
/// tests and tools that never boot behave as today.
pub fn current() -> Role {
    CURRENT.get().copied().unwrap_or(Role::All)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k2_core::test_env::EnvVar;

    #[test]
    fn unset_empty_and_all_boot_as_today() {
        assert_eq!(parse(None), Ok(Role::All));
        assert_eq!(parse(Some("")), Ok(Role::All));
        assert_eq!(parse(Some("all")), Ok(Role::All));
        assert_eq!(parse(Some(" all\n")), Ok(Role::All));
        assert_eq!(Role::All.as_str(), "all");
    }

    #[test]
    fn controller_and_host_are_roles() {
        assert_eq!(parse(Some("controller")), Ok(Role::Controller));
        assert_eq!(parse(Some(" host\n")), Ok(Role::Host));
        assert_eq!(Role::Controller.as_str(), "controller");
        assert_eq!(Role::Host.as_str(), "host");
    }

    #[test]
    fn controller_without_its_settings_is_refused() {
        assert_eq!(
            check_settings(Role::Controller, |_| None),
            Err("K2_ROLE=controller runs agents only on agent hosts: set K2_HOSTS=1 (got unset)".to_string())
        );
        assert_eq!(
            check_settings(Role::Host, |_| None),
            Err("K2_ROLE=host needs K2_CONTROLLER_URL (for example ws://controller:7420)".to_string())
        );
        assert_eq!(check_settings(Role::All, |_| None), Ok(()));
    }

    #[test]
    fn current_is_all_until_boot_records_a_role() {
        // Tests never boot, so nothing records a role.
        assert_eq!(current(), Role::All);
    }

    #[test]
    fn any_other_value_names_the_allowed_values() {
        assert_eq!(
            parse(Some("hosts")),
            Err("K2_ROLE=\"hosts\" is not a role; allowed values: all, controller, host".to_string())
        );
        // Case matters: the documented values are lower case.
        assert_eq!(
            parse(Some("ALL")),
            Err("K2_ROLE=\"ALL\" is not a role; allowed values: all, controller, host".to_string())
        );
    }

    #[test]
    fn from_env_reads_the_variable() {
        {
            let _v = EnvVar::remove(ENV_VAR);
            assert_eq!(from_env(), Ok(Role::All));
        }
        {
            let _v = EnvVar::set(ENV_VAR, "controller");
            let _h = EnvVar::remove(crate::hosts::HOSTS_VAR);
            assert_eq!(
                from_env(),
                Err("K2_ROLE=controller runs agents only on agent hosts: set K2_HOSTS=1 (got unset)".to_string())
            );
        }
        {
            let _v = EnvVar::set(ENV_VAR, "both");
            assert_eq!(from_env(), Err("K2_ROLE=\"both\" is not a role; allowed values: all, controller, host".to_string()));
        }
    }
}
