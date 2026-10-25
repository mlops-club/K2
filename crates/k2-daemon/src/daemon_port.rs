//! `K2_DAEMON_PORT` (PRD K2 Distributed Runtime, P0): a fixed HTTP port
//! instead of the sticky one in `daemon.port` (`k2_core::port_claim`).
//!
//! Containers publish one known port, so a taken port is a boot error,
//! never a fall back to a random port. The daemon still writes
//! `daemon.port` (in `daemon_main`), so local clients find it the usual way.
//! Unset or empty: today's sticky claim.

use std::net::{Ipv4Addr, SocketAddr, TcpListener};

pub const ENV_VAR: &str = "K2_DAEMON_PORT";

/// Pure parse of the variable. `Ok(None)` = unset or empty (today).
pub fn parse(raw: Option<&str>) -> Result<Option<u16>, String> {
    let Some(v) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    match v.parse::<u16>() {
        Ok(0) | Err(_) => Err(format!("{ENV_VAR}={v:?} is not a port; use a number from 1 to 65535")),
        Ok(p) => Ok(Some(p)),
    }
}

/// [`parse`] on the process environment.
pub fn from_env() -> Result<Option<u16>, String> {
    match std::env::var(ENV_VAR) {
        Ok(v) => parse(Some(&v)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("{ENV_VAR} is not valid UTF-8")),
    }
}

/// Bind exactly `bind_ip:port`. The error names the port and the cause.
pub fn bind_fixed(bind_ip: Ipv4Addr, port: u16) -> Result<TcpListener, String> {
    let addr = SocketAddr::from((bind_ip, port));
    TcpListener::bind(addr).map_err(|e| format!("{ENV_VAR}={port}: can't listen on {addr}: {e} (no fallback to another port)"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use k2_core::test_env::EnvVar;

    #[test]
    fn unset_or_empty_keeps_the_sticky_port() {
        assert_eq!(parse(None), Ok(None));
        assert_eq!(parse(Some("")), Ok(None));
        assert_eq!(parse(Some("  ")), Ok(None));
    }

    #[test]
    fn a_number_is_the_fixed_port() {
        assert_eq!(parse(Some("7420")), Ok(Some(7420)));
        assert_eq!(parse(Some(" 65535\n")), Ok(Some(65535)));
        assert_eq!(parse(Some("1")), Ok(Some(1)));
    }

    #[test]
    fn anything_else_is_refused() {
        for bad in ["0", "65536", "-1", "abc", "7420x", "74 20"] {
            assert_eq!(
                parse(Some(bad)),
                Err(format!("K2_DAEMON_PORT={:?} is not a port; use a number from 1 to 65535", bad.trim())),
                "{bad}"
            );
        }
    }

    #[test]
    fn from_env_reads_the_variable() {
        {
            let _v = EnvVar::remove(ENV_VAR);
            assert_eq!(from_env(), Ok(None));
        }
        {
            let _v = EnvVar::set(ENV_VAR, "7420");
            assert_eq!(from_env(), Ok(Some(7420)));
        }
        {
            let _v = EnvVar::set(ENV_VAR, "nope");
            assert!(from_env().is_err());
        }
    }

    #[test]
    fn a_taken_port_fails_loudly_and_never_moves() {
        // Hold a real listener for the whole test: the port stays taken.
        let held = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind a port to hold");
        let port = held.local_addr().expect("addr").port();
        let err = bind_fixed(Ipv4Addr::LOCALHOST, port).expect_err("the held port must not bind");
        assert!(
            err.starts_with(&format!("K2_DAEMON_PORT={port}: can't listen on 127.0.0.1:{port}: ")),
            "{err}"
        );
        assert!(err.ends_with("(no fallback to another port)"), "{err}");
        drop(held);
    }
}
