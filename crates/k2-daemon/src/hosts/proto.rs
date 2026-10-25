//! Frames on the host link (`/cli/hosts/attach`, protocol 1). One JSON
//! object per WebSocket text message, tagged by `t`. The full table, and
//! what is reserved for epochs, the outbox and replay, is in
//! `docs/prd/progress/dist-link.md` ("Wire protocol").
//!
//! Compatibility rules: unknown fields are ignored (serde's default), and
//! an unknown `t` decodes to [`Frame::Unknown`], which both ends log and
//! skip. Never `deny_unknown_fields` here.

use serde::{Deserialize, Serialize};

/// The protocol this build speaks.
pub const PROTOCOL: u32 = 1;

/// Path the host dials on the controller.
pub const ATTACH_PATH: &str = "/cli/hosts/attach";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t")]
pub enum Frame {
    // ── handshake ──────────────────────────────────────────────────────
    #[serde(rename = "hello")]
    Hello {
        protocol: u32,
        host_name: String,
        token: String,
        #[serde(default)]
        labels: Vec<String>,
    },
    #[serde(rename = "welcome")]
    Welcome {
        protocol: u32,
        host_id: String,
        controller: String,
    },
    #[serde(rename = "refused")]
    Refused { code: String, message: String },

    // ── requests (controller → host) and their reply ───────────────────
    #[serde(rename = "spawn")]
    Spawn {
        req: u64,
        agent_id: String,
        epoch: u64,
        body: serde_json::Value,
    },
    #[serde(rename = "close")]
    Close {
        req: u64,
        agent_id: String,
        epoch: u64,
        body: serde_json::Value,
    },
    /// `status` is the HTTP status line the host's handler returned
    /// (`"200 OK"`); `body` its JSON body as a string.
    #[serde(rename = "reply")]
    Reply { req: u64, status: String, body: String },

    // ── grid streams (both ways) ───────────────────────────────────────
    #[serde(rename = "grid.open")]
    GridOpen {
        stream: u64,
        session_id: String,
        epoch: u64,
        #[serde(default)]
        proto: Option<String>,
    },
    /// One text message of the app's `/cli/sessions/grid` protocol,
    /// verbatim, in either direction.
    #[serde(rename = "grid.data")]
    GridData { stream: u64, text: String },
    #[serde(rename = "grid.close")]
    GridClose {
        stream: u64,
        #[serde(default)]
        reason: String,
    },

    // ── events (host → controller) ─────────────────────────────────────
    /// A hook envelope that passed the owner check on the host (FR3).
    #[serde(rename = "hook")]
    Hook {
        pane: String,
        event: String,
        #[serde(default)]
        session_id: Option<String>,
        #[serde(default)]
        conversation: Option<String>,
        #[serde(default)]
        agent_pid: Option<i32>,
        at_ms: i64,
        #[serde(default)]
        epoch: Option<u64>,
    },
    #[serde(rename = "open_url")]
    OpenUrl { url: String, source: String },
    /// The session under `agent_id` went away on the host (closed, or the
    /// child exited).
    #[serde(rename = "session.gone")]
    SessionGone {
        agent_id: String,
        #[serde(default)]
        epoch: Option<u64>,
    },

    /// Any `t` this build doesn't know.
    #[serde(other)]
    Unknown,
}

impl Frame {
    pub fn encode(&self) -> String {
        // Every variant is plain data; serialising cannot fail.
        serde_json::to_string(self).expect("host link frame serialises")
    }

    pub fn decode(text: &str) -> Result<Frame, String> {
        serde_json::from_str(text).map_err(|e| format!("bad host link frame: {e}"))
    }

    /// The frame's `t`, for logs.
    pub fn kind(&self) -> &'static str {
        match self {
            Frame::Hello { .. } => "hello",
            Frame::Welcome { .. } => "welcome",
            Frame::Refused { .. } => "refused",
            Frame::Spawn { .. } => "spawn",
            Frame::Close { .. } => "close",
            Frame::Reply { .. } => "reply",
            Frame::GridOpen { .. } => "grid.open",
            Frame::GridData { .. } => "grid.data",
            Frame::GridClose { .. } => "grid.close",
            Frame::Hook { .. } => "hook",
            Frame::OpenUrl { .. } => "open_url",
            Frame::SessionGone { .. } => "session.gone",
            Frame::Unknown => "unknown",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_have_the_documented_wire_shape() {
        let f = Frame::Spawn {
            req: 7,
            agent_id: "a1".into(),
            epoch: 3,
            body: serde_json::json!({"agent_name": "a1"}),
        };
        assert_eq!(
            serde_json::to_value(&f).unwrap(),
            serde_json::json!({"t": "spawn", "req": 7, "agent_id": "a1", "epoch": 3, "body": {"agent_name": "a1"}})
        );
        let g = Frame::GridData { stream: 2, text: "{\"event\":\"delta\"}".into() };
        assert_eq!(g.encode(), r#"{"t":"grid.data","stream":2,"text":"{\"event\":\"delta\"}"}"#);
        assert_eq!(Frame::decode(&g.encode()), Ok(g));
    }

    #[test]
    fn unknown_types_and_fields_are_tolerated() {
        assert_eq!(Frame::decode(r#"{"t":"outbox.put","seq":1}"#), Ok(Frame::Unknown));
        // Reserved fields (epoch replay, outbox) on a known frame are ignored.
        assert_eq!(
            Frame::decode(r#"{"t":"grid.data","stream":1,"text":"x","ring_offset":99,"seq":4}"#),
            Ok(Frame::GridData { stream: 1, text: "x".into() })
        );
        assert!(Frame::decode("not json").is_err());
        assert!(Frame::decode(r#"{"no_t":1}"#).is_err());
    }
}
