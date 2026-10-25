//! Where to look for an agent's transcripts before a `--resume`
//! (PRD "K2 Distributed Runtime", dist-link W1).
//!
//! The resume decision asks "does provider P hold conversation S for this
//! project?" (`ProviderResume::session_file_exists`,
//! `chat_history::claude_session_file_exists`, `sidecar::conversation_exists`)
//! and answers from `~/.claude`, `~/.grok`, … on this machine. On a
//! `K2_ROLE=controller` daemon the transcripts live on the agent's host, so
//! the controller installs a probe that asks the host. With no probe
//! installed (every other role, every test that doesn't install one) [`ask`]
//! returns `None` and those functions run exactly as before.

use parking_lot::RwLock;

/// `Some(found)` to answer, `None` to fall through to the local check.
pub type Probe = fn(provider: &str, session_id: &str, project_path: &str) -> Option<bool>;

static PROBE: RwLock<Option<Probe>> = RwLock::new(None);

/// Install (or with `None`, remove) the process-wide probe.
pub fn set(probe: Option<Probe>) {
    *PROBE.write() = probe;
}

/// The installed probe's answer, if one is installed and it answers.
pub fn ask(provider: &str, session_id: &str, project_path: &str) -> Option<bool> {
    let probe = *PROBE.read();
    probe.and_then(|p| p(provider, session_id, project_path))
}

#[cfg(test)]
mod tests {
    use super::*;

    // The probe is process-wide and other tests resolve transcripts in
    // parallel, so this one answers only for its own session id.
    const MINE: &str = "transcript-probe-test-session";

    fn probe(provider: &str, session_id: &str, _project: &str) -> Option<bool> {
        (session_id == MINE).then_some(provider == "claude")
    }

    #[test]
    fn the_probe_answers_first_and_falls_through_when_it_does_not() {
        assert_eq!(ask("claude", MINE, "/p"), None, "no probe: no answer");
        set(Some(probe));
        assert_eq!(ask("claude", MINE, "/p"), Some(true));
        assert_eq!(ask("grok", MINE, "/p"), Some(false));
        assert_eq!(ask("claude", "someone-else", "/p"), None);
        // Each lookup the resume decision uses consults it first …
        assert!(crate::chat_history::claude_session_file_exists(MINE, "/nonexistent/project"));
        assert!(crate::sidecar::conversation_exists("claude", MINE, "/nonexistent/project"));
        let claude = crate::workspace::provider_resume::provider_resume_for_provider("claude").unwrap();
        assert!(claude.session_file_exists(MINE, "/nonexistent/project"));
        let grok = crate::workspace::provider_resume::provider_resume_for_provider("grok").unwrap();
        assert!(!grok.session_file_exists(MINE, "/nonexistent/project"));
        // … and an id it doesn't answer for is the local check, as before.
        assert!(!crate::chat_history::claude_session_file_exists("someone-else", "/nonexistent/project"));
        set(None);
        assert!(!crate::chat_history::claude_session_file_exists(MINE, "/nonexistent/project"));
        assert_eq!(ask("claude", MINE, "/p"), None);
    }
}
