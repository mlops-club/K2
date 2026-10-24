//! Capability detection at boot (PRD K2 Distributed Runtime, P0 and FR9).
//!
//! For each feature the root helper serves today (Postgres, mail, Sew, Apps,
//! front door) plus vault key storage, decide where it runs:
//!
//! - `host-native`: on this machine, the way K2 does it today (root helper,
//!   App engine on its socket, the vault's key store);
//! - `delegated`: somewhere else, named by a `K2_CAP_*` override
//!   (`external`, `engine`, `kubernetes`, `listen`);
//! - `off`: not available here (no root helper, as in a container), or
//!   `K2_CAP_*=off`.
//!
//! Override values are those of deploy/README.md. A value a feature doesn't
//! take is a boot error naming the ones it does (checked in
//! `daemon_main::run`, before anything starts). The report is computed once,
//! logged in one line and shown read-only on `/boot-status`
//! (`capabilities`). Nothing acts on it yet.

use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Feature {
    Postgres,
    Mail,
    Sew,
    Apps,
    Front,
    Vault,
}

impl Feature {
    pub const ALL: [Feature; 6] =
        [Feature::Postgres, Feature::Mail, Feature::Sew, Feature::Apps, Feature::Front, Feature::Vault];

    /// Key in the report and the log line.
    pub fn key(self) -> &'static str {
        match self {
            Feature::Postgres => "postgres",
            Feature::Mail => "mail",
            Feature::Sew => "sew",
            Feature::Apps => "apps",
            Feature::Front => "front",
            Feature::Vault => "vault",
        }
    }

    /// The override variable, if the feature has one. The vault's key
    /// source gets its own setting later (`K2_KEK_FILE`).
    pub fn env_var(self) -> Option<&'static str> {
        match self {
            Feature::Postgres => Some("K2_CAP_POSTGRES"),
            Feature::Mail => Some("K2_CAP_MAIL"),
            Feature::Sew => Some("K2_CAP_SEW"),
            Feature::Apps => Some("K2_CAP_APPS"),
            Feature::Front => Some("K2_CAP_FRONT"),
            Feature::Vault => None,
        }
    }

    /// Override values this feature takes (deploy/README.md; the
    /// per-feature split follows compose.yaml and controller.yaml).
    pub fn allowed_modes(self) -> &'static [&'static str] {
        match self {
            Feature::Postgres | Feature::Mail | Feature::Sew => &["host", "external", "kubernetes", "off"],
            Feature::Apps => &["host", "engine", "kubernetes", "off"],
            Feature::Front => &["host", "listen", "off"],
            Feature::Vault => &[],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    HostNative,
    Delegated,
    Off,
}

impl State {
    pub fn as_wire(self) -> &'static str {
        match self {
            State::HostNative => "host-native",
            State::Delegated => "delegated",
            State::Off => "off",
        }
    }
}

/// `K2_CAP_*` overrides, already checked against [`Feature::allowed_modes`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Overrides(Vec<(Feature, &'static str)>);

impl Overrides {
    pub fn get(&self, f: Feature) -> Option<&'static str> {
        self.0.iter().find(|(g, _)| *g == f).map(|(_, m)| *m)
    }
}

/// Parse the overrides through `get` (the environment at boot, a map in
/// tests). Unset or empty = no override.
pub fn parse_overrides(get: impl Fn(&str) -> Option<String>) -> Result<Overrides, String> {
    let mut out = Vec::new();
    for f in Feature::ALL {
        let Some(var) = f.env_var() else { continue };
        let Some(raw) = get(var) else { continue };
        let v = raw.trim();
        if v.is_empty() {
            continue;
        }
        match f.allowed_modes().iter().find(|m| **m == v) {
            Some(m) => out.push((f, *m)),
            None => return Err(format!("{var}={v:?} is not a mode; allowed values: {}", f.allowed_modes().join(", "))),
        }
    }
    Ok(Overrides(out))
}

/// [`parse_overrides`] on the process environment.
pub fn overrides_from_env() -> Result<Overrides, String> {
    parse_overrides(|k| std::env::var(k).ok())
}

/// What detection looks at. Gathered once at boot ([`Inputs::gather`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Inputs {
    pub overrides: Overrides,
    /// The root helper is installed and `sudo -n` may run it.
    pub root_helper: bool,
    /// The App engine's socket (`/run/k2app/engine.sock`) exists.
    pub app_engine_socket: bool,
    /// The open vault's key store (`KeyHome::as_str`), `None` if closed.
    pub vault_key_home: Option<&'static str>,
}

impl Inputs {
    /// Read the machine. Call after `vault::boot`.
    pub fn gather() -> Self {
        Inputs {
            overrides: overrides_from_env().unwrap_or_else(|e| panic!("capabilities: {e} (checked at boot)")),
            root_helper: crate::root_helper::state().kind == crate::root_helper::Kind::Root,
            app_engine_socket: std::path::Path::new(k2_root_proto::engine::SOCKET).exists(),
            vault_key_home: crate::vault::get().ok().map(|v| v.key_store().kind().as_str()),
        }
    }
}

/// One feature's line of the report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub feature: Feature,
    pub state: State,
    /// The mode in `K2_CAP_*` terms (`host`, `external`, …, `off`).
    pub mode: &'static str,
    /// Why, in a few words: the override, or what detection found.
    pub via: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report(pub Vec<Entry>);

/// Pure detection: same inputs, same report.
pub fn detect(inp: &Inputs) -> Report {
    let entry = |feature, state, mode, via: String| Entry { feature, state, mode, via };
    let mut out = Vec::new();
    for f in Feature::ALL {
        if let (Some(mode), Some(var)) = (inp.overrides.get(f), f.env_var()) {
            let state = match mode {
                "host" => State::HostNative,
                "off" => State::Off,
                _ => State::Delegated,
            };
            out.push(entry(f, state, mode, format!("{var}={mode}")));
            continue;
        }
        out.push(match f {
            Feature::Vault => match inp.vault_key_home {
                Some(home) => entry(f, State::HostNative, "host", format!("key store {home}")),
                None => entry(f, State::Off, "off", "vault not open".into()),
            },
            Feature::Apps if inp.app_engine_socket => entry(f, State::HostNative, "host", "App engine socket".into()),
            _ if inp.root_helper => entry(f, State::HostNative, "host", "root helper".into()),
            _ => entry(f, State::Off, "off", "no root helper".into()),
        });
    }
    Report(out)
}

impl Report {
    /// `/boot-status` `capabilities`: `{feature: {state, mode, via}}`.
    pub fn to_json(&self) -> serde_json::Value {
        let mut m = serde_json::Map::new();
        for e in &self.0 {
            m.insert(
                e.feature.key().into(),
                serde_json::json!({"state": e.state.as_wire(), "mode": e.mode, "via": e.via}),
            );
        }
        serde_json::Value::Object(m)
    }

    /// The one boot log line.
    pub fn log_line(&self) -> String {
        let parts: Vec<String> =
            self.0.iter().map(|e| format!("{}={} ({})", e.feature.key(), e.state.as_wire(), e.via)).collect();
        format!("[daemon] capabilities: {}", parts.join(", "))
    }
}

static REPORT: OnceLock<Report> = OnceLock::new();

/// This process's report, detected on first use (boot calls it after the
/// vault opens, so later readers see the same answer).
pub fn report() -> &'static Report {
    REPORT.get_or_init(|| detect(&Inputs::gather()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn overrides(pairs: &[(&str, &str)]) -> Result<Overrides, String> {
        let m: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        parse_overrides(|k| m.get(k).cloned())
    }

    fn states(r: &Report) -> Vec<(&'static str, &'static str, &'static str, &str)> {
        r.0.iter().map(|e| (e.feature.key(), e.state.as_wire(), e.mode, e.via.as_str())).collect()
    }

    #[test]
    fn a_container_with_nothing_set_reports_off_but_the_vault() {
        let r = detect(&Inputs { vault_key_home: Some("file"), ..Default::default() });
        assert_eq!(
            states(&r),
            vec![
                ("postgres", "off", "off", "no root helper"),
                ("mail", "off", "off", "no root helper"),
                ("sew", "off", "off", "no root helper"),
                ("apps", "off", "off", "no root helper"),
                ("front", "off", "off", "no root helper"),
                ("vault", "host-native", "host", "key store file"),
            ]
        );
    }

    #[test]
    fn a_linux_box_with_the_root_helper_is_host_native() {
        let r = detect(&Inputs { root_helper: true, vault_key_home: Some("keeper"), ..Default::default() });
        assert!(r.0.iter().all(|e| e.state == State::HostNative), "{r:?}");
        assert_eq!(r.0[0].via, "root helper");
        assert_eq!(r.0[5].via, "key store keeper");
    }

    #[test]
    fn the_app_engine_socket_alone_makes_apps_host_native() {
        let r = detect(&Inputs { app_engine_socket: true, ..Default::default() });
        assert_eq!(states(&r)[3], ("apps", "host-native", "host", "App engine socket"));
        assert_eq!(states(&r)[0], ("postgres", "off", "off", "no root helper"));
        assert_eq!(states(&r)[5], ("vault", "off", "off", "vault not open"));
    }

    #[test]
    fn overrides_win_over_detection() {
        let ov = overrides(&[
            ("K2_CAP_POSTGRES", "external"),
            ("K2_CAP_MAIL", "off"),
            ("K2_CAP_SEW", "kubernetes"),
            ("K2_CAP_APPS", "engine"),
            ("K2_CAP_FRONT", "listen"),
        ])
        .expect("valid overrides");
        let r = detect(&Inputs { overrides: ov, root_helper: true, vault_key_home: Some("file"), ..Default::default() });
        assert_eq!(
            states(&r),
            vec![
                ("postgres", "delegated", "external", "K2_CAP_POSTGRES=external"),
                ("mail", "off", "off", "K2_CAP_MAIL=off"),
                ("sew", "delegated", "kubernetes", "K2_CAP_SEW=kubernetes"),
                ("apps", "delegated", "engine", "K2_CAP_APPS=engine"),
                ("front", "delegated", "listen", "K2_CAP_FRONT=listen"),
                ("vault", "host-native", "host", "key store file"),
            ]
        );
    }

    #[test]
    fn host_override_is_host_native_without_a_helper() {
        let ov = overrides(&[("K2_CAP_POSTGRES", "host")]).expect("valid");
        let r = detect(&Inputs { overrides: ov, ..Default::default() });
        assert_eq!(states(&r)[0], ("postgres", "host-native", "host", "K2_CAP_POSTGRES=host"));
    }

    #[test]
    fn unset_and_empty_overrides_are_none() {
        assert_eq!(overrides(&[]), Ok(Overrides::default()));
        assert_eq!(overrides(&[("K2_CAP_MAIL", ""), ("K2_CAP_SEW", "  ")]), Ok(Overrides::default()));
        assert_eq!(overrides(&[("K2_CAP_MAIL", " external\n")]).expect("trimmed").get(Feature::Mail), Some("external"));
    }

    #[test]
    fn a_mode_the_feature_does_not_take_names_the_allowed_values() {
        assert_eq!(
            overrides(&[("K2_CAP_FRONT", "engine")]),
            Err("K2_CAP_FRONT=\"engine\" is not a mode; allowed values: host, listen, off".to_string())
        );
        assert_eq!(
            overrides(&[("K2_CAP_APPS", "external")]),
            Err("K2_CAP_APPS=\"external\" is not a mode; allowed values: host, engine, kubernetes, off".to_string())
        );
        assert_eq!(
            overrides(&[("K2_CAP_POSTGRES", "zalando")]),
            Err("K2_CAP_POSTGRES=\"zalando\" is not a mode; allowed values: host, external, kubernetes, off".to_string())
        );
    }

    #[test]
    fn overrides_from_env_reads_the_variables() {
        use k2_core::test_env::EnvVar;
        let _clear: Vec<EnvVar> = Feature::ALL.iter().filter_map(|f| f.env_var()).map(EnvVar::remove).collect();
        assert_eq!(overrides_from_env(), Ok(Overrides::default()));
        let _p = EnvVar::set("K2_CAP_POSTGRES", "kubernetes");
        assert_eq!(overrides_from_env().expect("valid").get(Feature::Postgres), Some("kubernetes"));
        let _f = EnvVar::set("K2_CAP_FRONT", "bogus");
        assert!(overrides_from_env().is_err());
    }

    #[test]
    fn json_and_log_line_shapes() {
        let r = detect(&Inputs { vault_key_home: Some("file"), ..Default::default() });
        assert_eq!(
            r.to_json(),
            serde_json::json!({
                "postgres": {"state": "off", "mode": "off", "via": "no root helper"},
                "mail": {"state": "off", "mode": "off", "via": "no root helper"},
                "sew": {"state": "off", "mode": "off", "via": "no root helper"},
                "apps": {"state": "off", "mode": "off", "via": "no root helper"},
                "front": {"state": "off", "mode": "off", "via": "no root helper"},
                "vault": {"state": "host-native", "mode": "host", "via": "key store file"},
            })
        );
        assert_eq!(
            r.log_line(),
            "[daemon] capabilities: postgres=off (no root helper), mail=off (no root helper), sew=off (no root helper), \
             apps=off (no root helper), front=off (no root helper), vault=host-native (key store file)"
        );
    }
}
