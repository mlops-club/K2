//! `K2_ROLE` (PRD K2 Distributed Runtime, P0): which half of K2 this
//! daemon runs.
//!
//! - unset, empty or `all` (today): the controller plus the in-process
//!   agent host.
//! - `controller`: the controller only, agents on dialled-in hosts. Hosts
//!   don't exist until P2, so this build refuses it at boot.
//!
//! Anything else is a boot error naming the allowed values. Checked once,
//! before the runtime starts (`daemon_main::run`).

pub const ENV_VAR: &str = "K2_ROLE";

/// What a booted daemon runs. Only `All` boots in this build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    All,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::All => "all",
        }
    }
}

/// Exact message for `K2_ROLE=controller` (deploy/README.md; PRD P2).
pub const CONTROLLER_NOT_YET: &str = "K2_ROLE=controller needs agent hosts (PRD P2); not available yet";

/// Pure decision behind [`from_env`]. `raw` is the variable's value, if set.
pub fn parse(raw: Option<&str>) -> Result<Role, String> {
    match raw.map(str::trim) {
        None | Some("") | Some("all") => Ok(Role::All),
        Some("controller") => Err(CONTROLLER_NOT_YET.to_string()),
        Some(other) => Err(format!("{ENV_VAR}={other:?} is not a role; allowed values: all, controller")),
    }
}

/// [`parse`] on the process environment. A non-UTF-8 value is an error,
/// never "unset".
pub fn from_env() -> Result<Role, String> {
    match std::env::var(ENV_VAR) {
        Ok(v) => parse(Some(&v)),
        Err(std::env::VarError::NotPresent) => parse(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{ENV_VAR} is not valid UTF-8; allowed values: all, controller")),
    }
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
    fn controller_is_refused_with_the_documented_message() {
        assert_eq!(
            parse(Some("controller")),
            Err("K2_ROLE=controller needs agent hosts (PRD P2); not available yet".to_string())
        );
    }

    #[test]
    fn any_other_value_names_the_allowed_values() {
        assert_eq!(
            parse(Some("host")),
            Err("K2_ROLE=\"host\" is not a role; allowed values: all, controller".to_string())
        );
        // Case matters: the documented values are lower case.
        assert_eq!(
            parse(Some("ALL")),
            Err("K2_ROLE=\"ALL\" is not a role; allowed values: all, controller".to_string())
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
            assert_eq!(from_env(), Err(CONTROLLER_NOT_YET.to_string()));
        }
        {
            let _v = EnvVar::set(ENV_VAR, "both");
            assert_eq!(from_env(), Err("K2_ROLE=\"both\" is not a role; allowed values: all, controller".to_string()));
        }
    }
}
