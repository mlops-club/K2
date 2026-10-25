//! Frames on the host link (`/cli/hosts/attach`). One JSON object per
//! WebSocket text message, tagged by `t`. The tables, and what each
//! protocol version adds, are in `docs/prd/progress/dist-link.md`
//! ("Wire protocol", "Protocol 2").
//!
//! Compatibility rules:
//! - unknown fields are ignored (serde's default) and an unknown `t`
//!   decodes to [`Frame::Unknown`], which both ends log and skip. Never
//!   `deny_unknown_fields` here.
//! - every field added after protocol 1 is optional and left out of the
//!   encoding when empty, so a protocol-1 frame encodes byte for byte as
//!   it did in protocol 1.
//! - the controller accepts hosts speaking [`MIN_PROTOCOL`]..=[`PROTOCOL`]
//!   and answers `welcome` with the version both sides use (the host's).
//!   It never sends a protocol-2 frame on a protocol-1 link.
//!
//! Protocol 2 (wave 2) is defined here up front; the frames exist and
//! round-trip, but nothing sends them yet (`deliver`, `inject`, `release`,
//! `resume`, ring offsets).

use serde::{Deserialize, Serialize};

/// The newest protocol this build speaks.
pub const PROTOCOL: u32 = 2;
/// The oldest host protocol the controller still accepts.
pub const MIN_PROTOCOL: u32 = 1;

/// Path the host dials on the controller.
pub const ATTACH_PATH: &str = "/cli/hosts/attach";

/// The version a link uses, given the version the host sent in `hello`:
/// the host's, when this controller speaks it.
pub fn negotiate(host_protocol: u32) -> Option<u32> {
    (MIN_PROTOCOL..=PROTOCOL).contains(&host_protocol).then_some(host_protocol)
}

/// Trace event names for the protocol-2 flows (`K2_TRACE_FILE`), so the
/// wave-2 workers and the lab's flow files use the same strings.
pub mod trace_events {
    /// Controller sent `deliver` (attrs: msg_id, host, attempt).
    pub const CTL_DELIVER: &str = "ctl.deliver";
    /// Controller got `deliver.ack` (attrs: msg_id, host, status).
    pub const CTL_DELIVER_ACK: &str = "ctl.deliver_ack";
    /// Host got `deliver` (attrs: msg_id, duplicate).
    pub const HOST_DELIVER: &str = "host.deliver";
    /// Controller sent `inject` (attrs: req, host, steps).
    pub const CTL_INJECT: &str = "ctl.inject";
    /// Host answered `inject.result` (attrs: req, result).
    pub const HOST_INJECT_RESULT: &str = "host.inject_result";
    /// Host sent `resume` in `hello` (attrs: placements).
    pub const HOST_RESUME: &str = "host.resume";
    /// Controller decided on a resumed placement (attrs: agent, epoch, decision).
    pub const CTL_ADOPT: &str = "ctl.adopt";
    /// Controller sent `release` (attrs: agent, epoch, reason).
    pub const CTL_RELEASE: &str = "ctl.release";
    /// Host released a placement (attrs: agent, epoch, ring_end).
    pub const HOST_RELEASE: &str = "host.release";
    /// A grid stream opened from a ring offset (attrs: stream, from).
    pub const GRID_REPLAY: &str = "grid.replay";
    /// Controller issued a placement epoch (attrs: agent, epoch).
    pub const CTL_EPOCH: &str = "ctl.epoch";
}

/// One placement a host still runs, reported in `hello` (protocol 2) so a
/// restarted controller can re-adopt it (S9).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumePlacement {
    pub agent_id: String,
    pub session_id: String,
    /// The epoch the host was given for it.
    pub epoch: u64,
    /// The byte range of the session's output ring the host still holds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ring: Option<RingRange>,
    #[serde(default)]
    pub child_alive: bool,
}

/// Offsets into a session's output ring: `start` is the oldest byte the
/// host still holds, `end` the next byte it will write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RingRange {
    pub start: u64,
    pub end: u64,
}

/// One step of an inject flow on the wire (`k2_core::inject_flow::InjectAction`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum InjectStep {
    /// Bytes for the PTY, as text (lossless for UTF-8 input) …
    Write { text: String },
    /// … or base64 when the bytes aren't UTF-8.
    WriteB64 { b64: String },
    Sleep { ms: u64 },
    CheckAlive,
}

impl InjectStep {
    pub fn from_action(a: &k2_core::inject_flow::InjectAction) -> Self {
        use base64::Engine;
        use k2_core::inject_flow::InjectAction;
        match a {
            InjectAction::Write(bytes) => match std::str::from_utf8(bytes) {
                Ok(t) => InjectStep::Write { text: t.to_string() },
                Err(_) => InjectStep::WriteB64 { b64: base64::engine::general_purpose::STANDARD.encode(bytes) },
            },
            InjectAction::Sleep(ms) => InjectStep::Sleep { ms: *ms },
            InjectAction::CheckAlive => InjectStep::CheckAlive,
        }
    }

    pub fn to_action(&self) -> Result<k2_core::inject_flow::InjectAction, String> {
        use base64::Engine;
        use k2_core::inject_flow::InjectAction;
        Ok(match self {
            InjectStep::Write { text } => InjectAction::Write(text.as_bytes().to_vec()),
            InjectStep::WriteB64 { b64 } => InjectAction::Write(
                base64::engine::general_purpose::STANDARD.decode(b64).map_err(|e| format!("inject write_b64: {e}"))?,
            ),
            InjectStep::Sleep { ms } => InjectAction::Sleep(*ms),
            InjectStep::CheckAlive => InjectAction::CheckAlive,
        })
    }
}

/// How a host answered one `deliver`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliverStatus {
    /// Delivered to the agent now.
    Delivered,
    /// Already delivered under this `msg_id`; not delivered again.
    Duplicate,
    /// Not delivered and retrying won't help (no such agent, stale epoch);
    /// `reason` says why. Anything retryable is simply not acked.
    Rejected,
}

/// Result of an `inject` (`agent_host::InjectResult`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InjectOutcome {
    Delivered,
    ChildGone,
    /// No placement for the agent at that epoch on this host.
    NotPlaced,
}

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
        /// Protocol 2: placements this host still runs (S9 re-adoption).
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        resume: Vec<ResumePlacement>,
    },
    #[serde(rename = "welcome")]
    Welcome {
        /// The version this link uses.
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
        /// The Connect user the client signed in as; `None` for the owner
        /// token. The controller authorised the client and enforces the
        /// user's viewer/claimer rules; the host logs it.
        #[serde(default)]
        as_user: Option<String>,
        /// Protocol 2: replay the session's output from this ring offset
        /// before live frames (S9). `None` = a fresh snapshot, as in 1.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ring_offset: Option<u64>,
    },
    /// One text message of the app's `/cli/sessions/grid` protocol,
    /// verbatim, in either direction.
    #[serde(rename = "grid.data")]
    GridData {
        stream: u64,
        text: String,
        /// Protocol 2, host → controller: the ring offset this frame brings
        /// the client up to, so a reattach can ask for `ring_offset`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ring_offset: Option<u64>,
    },
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

    // ── protocol 2: messages (S3, FR4) ─────────────────────────────────
    /// A message for an agent, either direction: the controller delivers
    /// to the host that runs the recipient; a host hands one its agent sent
    /// to the controller. At least once: the sender resends until it gets
    /// `deliver.ack` for `msg_id`; the receiver dedupes on `msg_id`.
    #[serde(rename = "deliver")]
    Deliver {
        msg_id: String,
        /// The recipient's agent id.
        agent_id: String,
        /// The recipient's placement epoch, when the sender knows it (the
        /// controller always does); a host refuses a stale one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        epoch: Option<u64>,
        /// The sender, as K2 addresses it (`agent:…`, `user:…`).
        from: String,
        /// The message as the inbox stores it.
        body: serde_json::Value,
        /// 1 on the first send, +1 per resend (for traces; not for dedupe).
        #[serde(default = "first_attempt")]
        attempt: u32,
    },
    #[serde(rename = "deliver.ack")]
    DeliverAck {
        msg_id: String,
        status: DeliverStatus,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },

    // ── protocol 2: inject (the `AgentHost::inject` flow, remote) ──────
    /// Run an inject flow against the agent's PTY as one unit.
    #[serde(rename = "inject")]
    Inject {
        req: u64,
        agent_id: String,
        epoch: u64,
        steps: Vec<InjectStep>,
        /// The message this injection delivers, when it is one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        msg_id: Option<String>,
    },
    #[serde(rename = "inject.result")]
    InjectResult { req: u64, result: InjectOutcome },

    // ── protocol 2: release (sleep, move) ──────────────────────────────
    /// Give up the placement `agent_id` at `epoch`: stop the child after
    /// its turn, keep its home. The host answers `released`.
    #[serde(rename = "release")]
    Release {
        req: u64,
        agent_id: String,
        epoch: u64,
        /// `sleep`, `move`, `drain`, … (free text for logs and traces).
        reason: String,
    },
    #[serde(rename = "released")]
    Released {
        req: u64,
        agent_id: String,
        epoch: u64,
        /// Where the session's ring ended, for a later replay.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ring_end: Option<u64>,
    },

    /// Any `t` this build doesn't know.
    #[serde(other)]
    Unknown,
}

fn first_attempt() -> u32 {
    1
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
            Frame::Deliver { .. } => "deliver",
            Frame::DeliverAck { .. } => "deliver.ack",
            Frame::Inject { .. } => "inject",
            Frame::InjectResult { .. } => "inject.result",
            Frame::Release { .. } => "release",
            Frame::Released { .. } => "released",
            Frame::Unknown => "unknown",
        }
    }

    /// The oldest protocol a link must speak to carry this frame.
    pub fn min_protocol(&self) -> u32 {
        match self {
            Frame::Deliver { .. }
            | Frame::DeliverAck { .. }
            | Frame::Inject { .. }
            | Frame::InjectResult { .. }
            | Frame::Release { .. }
            | Frame::Released { .. } => 2,
            Frame::Hello { resume, .. } if !resume.is_empty() => 2,
            Frame::GridOpen { ring_offset: Some(_), .. } | Frame::GridData { ring_offset: Some(_), .. } => 2,
            _ => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip(f: &Frame) {
        assert_eq!(Frame::decode(&f.encode()).as_ref(), Ok(f), "round trip of {}", f.encode());
    }

    #[test]
    fn frames_have_the_documented_wire_shape() {
        let f = Frame::Spawn {
            req: 7,
            agent_id: "a1".into(),
            epoch: 3,
            body: json!({"agent_name": "a1"}),
        };
        assert_eq!(
            serde_json::to_value(&f).unwrap(),
            json!({"t": "spawn", "req": 7, "agent_id": "a1", "epoch": 3, "body": {"agent_name": "a1"}})
        );
        let g = Frame::GridData { stream: 2, text: "{\"event\":\"delta\"}".into(), ring_offset: None };
        assert_eq!(g.encode(), r#"{"t":"grid.data","stream":2,"text":"{\"event\":\"delta\"}"}"#);
        assert_eq!(Frame::decode(&g.encode()), Ok(g));
    }

    #[test]
    fn unknown_types_and_fields_are_tolerated() {
        assert_eq!(Frame::decode(r#"{"t":"outbox.put","seq":1}"#), Ok(Frame::Unknown));
        assert_eq!(
            Frame::decode(r#"{"t":"grid.data","stream":1,"text":"x","seq":4}"#),
            Ok(Frame::GridData { stream: 1, text: "x".into(), ring_offset: None })
        );
        assert!(Frame::decode("not json").is_err());
        assert!(Frame::decode(r#"{"no_t":1}"#).is_err());
    }

    #[test]
    fn protocol_1_frames_encode_exactly_as_protocol_1() {
        // What a protocol-1 peer sent and expects; the new optional fields
        // must not appear when unset.
        for (f, want) in [
            (
                Frame::Hello { protocol: 1, host_name: "h".into(), token: "t".into(), labels: vec![], resume: vec![] },
                r#"{"t":"hello","protocol":1,"host_name":"h","token":"t","labels":[]}"#,
            ),
            (
                Frame::GridOpen { stream: 1, session_id: "s".into(), epoch: 2, proto: None, as_user: None, ring_offset: None },
                r#"{"t":"grid.open","stream":1,"session_id":"s","epoch":2,"proto":null,"as_user":null}"#,
            ),
        ] {
            assert_eq!(f.encode(), want);
            assert_eq!(f.min_protocol(), 1);
            round_trip(&f);
        }
        // And a protocol-1 hello (no `resume`) decodes into protocol 2's.
        assert!(matches!(
            Frame::decode(r#"{"t":"hello","protocol":1,"host_name":"h","token":"t"}"#),
            Ok(Frame::Hello { resume, .. }) if resume.is_empty()
        ));
    }

    #[test]
    fn protocol_2_frames_round_trip_with_their_wire_shape() {
        let resume = ResumePlacement {
            agent_id: "a1".into(),
            session_id: "s1".into(),
            epoch: 41,
            ring: Some(RingRange { start: 100, end: 4196 }),
            child_alive: true,
        };
        let cases: Vec<(Frame, serde_json::Value)> = vec![
            (
                Frame::Hello { protocol: 2, host_name: "h".into(), token: "t".into(), labels: vec![], resume: vec![resume] },
                json!({"t": "hello", "protocol": 2, "host_name": "h", "token": "t", "labels": [],
                       "resume": [{"agent_id": "a1", "session_id": "s1", "epoch": 41,
                                   "ring": {"start": 100, "end": 4196}, "child_alive": true}]}),
            ),
            (
                Frame::Deliver {
                    msg_id: "m-1".into(),
                    agent_id: "a1".into(),
                    epoch: Some(41),
                    from: "agent:a2".into(),
                    body: json!({"text": "hi"}),
                    attempt: 2,
                },
                json!({"t": "deliver", "msg_id": "m-1", "agent_id": "a1", "epoch": 41, "from": "agent:a2",
                       "body": {"text": "hi"}, "attempt": 2}),
            ),
            (
                Frame::DeliverAck { msg_id: "m-1".into(), status: DeliverStatus::Duplicate, reason: None },
                json!({"t": "deliver.ack", "msg_id": "m-1", "status": "duplicate"}),
            ),
            (
                Frame::DeliverAck { msg_id: "m-2".into(), status: DeliverStatus::Rejected, reason: Some("stale epoch".into()) },
                json!({"t": "deliver.ack", "msg_id": "m-2", "status": "rejected", "reason": "stale epoch"}),
            ),
            (
                Frame::Inject {
                    req: 9,
                    agent_id: "a1".into(),
                    epoch: 41,
                    steps: vec![
                        InjectStep::Write { text: "hello".into() },
                        InjectStep::Sleep { ms: 150 },
                        InjectStep::Write { text: "\r".into() },
                        InjectStep::CheckAlive,
                    ],
                    msg_id: Some("m-1".into()),
                },
                json!({"t": "inject", "req": 9, "agent_id": "a1", "epoch": 41, "msg_id": "m-1", "steps": [
                    {"op": "write", "text": "hello"}, {"op": "sleep", "ms": 150},
                    {"op": "write", "text": "\r"}, {"op": "check_alive"}]}),
            ),
            (
                Frame::InjectResult { req: 9, result: InjectOutcome::ChildGone },
                json!({"t": "inject.result", "req": 9, "result": "child_gone"}),
            ),
            (
                Frame::Release { req: 10, agent_id: "a1".into(), epoch: 41, reason: "sleep".into() },
                json!({"t": "release", "req": 10, "agent_id": "a1", "epoch": 41, "reason": "sleep"}),
            ),
            (
                Frame::Released { req: 10, agent_id: "a1".into(), epoch: 41, ring_end: Some(4196) },
                json!({"t": "released", "req": 10, "agent_id": "a1", "epoch": 41, "ring_end": 4196}),
            ),
            (
                Frame::GridOpen {
                    stream: 3,
                    session_id: "s1".into(),
                    epoch: 41,
                    proto: None,
                    as_user: Some("owner".into()),
                    ring_offset: Some(100),
                },
                json!({"t": "grid.open", "stream": 3, "session_id": "s1", "epoch": 41, "proto": null,
                       "as_user": "owner", "ring_offset": 100}),
            ),
            (
                Frame::GridData { stream: 3, text: "{}".into(), ring_offset: Some(4196) },
                json!({"t": "grid.data", "stream": 3, "text": "{}", "ring_offset": 4196}),
            ),
        ];
        for (f, want) in cases {
            assert_eq!(serde_json::to_value(&f).unwrap(), want, "{}", f.kind());
            assert_eq!(f.min_protocol(), 2, "{} needs protocol 2", f.kind());
            round_trip(&f);
        }
        // `attempt` defaults to the first send.
        assert!(matches!(
            Frame::decode(r#"{"t":"deliver","msg_id":"m","agent_id":"a","from":"user:o","body":{}}"#),
            Ok(Frame::Deliver { attempt: 1, epoch: None, .. })
        ));
    }

    #[test]
    fn inject_steps_carry_any_bytes() {
        use k2_core::inject_flow::InjectAction;
        let actions = vec![
            InjectAction::Write(b"paste \xe2\x9c\x93".to_vec()),
            InjectAction::Write(vec![0xff, 0x00, 0x1b]),
            InjectAction::Sleep(250),
            InjectAction::CheckAlive,
        ];
        let steps: Vec<InjectStep> = actions.iter().map(InjectStep::from_action).collect();
        assert_eq!(steps[0], InjectStep::Write { text: "paste ✓".into() });
        assert!(matches!(steps[1], InjectStep::WriteB64 { .. }));
        let back: Vec<InjectAction> = steps.iter().map(|s| s.to_action().unwrap()).collect();
        assert_eq!(back, actions);
    }

    #[test]
    fn controller_accepts_protocols_1_and_2() {
        assert_eq!(negotiate(1), Some(1));
        assert_eq!(negotiate(2), Some(2));
        assert_eq!(negotiate(0), None);
        assert_eq!(negotiate(3), None);
    }
}
