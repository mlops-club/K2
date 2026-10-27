use rusqlite::Connection;

use super::outbox::{self, Enqueued, Hop, NewMessage, State};
use super::{ensure_schema, new_msg_id, seen, Policy};

const POLICY: Policy = Policy {
    backoff_base_ms: 1_000,
    backoff_max_ms: 8_000,
    max_attempts: 3,
    ttl_ms: 100_000,
    keep_finished_ms: 1_000_000,
    seen_cap: 4,
};

fn db() -> Connection {
    let c = Connection::open_in_memory().unwrap();
    ensure_schema(&c).unwrap();
    c
}

fn msg(id: &str, from: &str, to: &str) -> NewMessage {
    NewMessage { msg_id: id.into(), sender: from.into(), recipient: to.into(), body: format!(r#"{{"text":"{id}"}}"#) }
}

fn due_ids(c: &Connection, now: i64) -> Vec<String> {
    outbox::due(c, Hop::Down, now).unwrap().into_iter().map(|r| r.msg_id).collect()
}

#[test]
fn msg_ids_are_time_prefixed_and_unique() {
    let a = new_msg_id(1_700_000_000_123);
    let b = new_msg_id(1_700_000_000_123);
    assert!(a.starts_with("m_1700000000123_"), "{a}");
    assert_eq!(a.len(), "m_1700000000123_".len() + 16);
    assert_ne!(a, b);
}

#[test]
fn enqueue_dedupes_by_msg_id_per_hop() {
    let c = db();
    assert_eq!(outbox::enqueue(&c, Hop::Down, &msg("m1", "agent:a", "b"), 10).unwrap(), Enqueued::New);
    assert_eq!(outbox::enqueue(&c, Hop::Down, &msg("m1", "agent:a", "b"), 11).unwrap(), Enqueued::Exists(State::Queued));
    // The same id on the other hop is a different row (a host's up hop).
    assert_eq!(outbox::enqueue(&c, Hop::Up, &msg("m1", "agent:a", "b"), 12).unwrap(), Enqueued::New);
    assert_eq!(outbox::counts(&c, Hop::Down).unwrap(), (1, 0, 0, 0));
    // A resend after the message was delivered still changes nothing.
    outbox::mark_sent(&c, Hop::Down, "m1", 20, &POLICY).unwrap();
    assert!(outbox::mark_delivered(&c, Hop::Down, "m1", 21).unwrap());
    assert_eq!(outbox::enqueue(&c, Hop::Down, &msg("m1", "agent:a", "b"), 22).unwrap(), Enqueued::Exists(State::Delivered));
    assert_eq!(outbox::counts(&c, Hop::Down).unwrap(), (0, 0, 1, 0));
}

#[test]
fn seen_answers_a_redelivery_and_is_bounded() {
    let c = db();
    assert_eq!(seen::status(&c, "m1").unwrap(), None);
    seen::record(&c, "m1", "delivered", 1, POLICY.seen_cap).unwrap();
    seen::record(&c, "m1", "delivered", 2, POLICY.seen_cap).unwrap();
    assert_eq!(seen::status(&c, "m1").unwrap().as_deref(), Some("delivered"));
    // A later answer replaces an earlier one (the controller's `handling`).
    seen::record(&c, "h1", "handling", 1, POLICY.seen_cap).unwrap();
    seen::record(&c, "h1", "rejected: dormant_no_wake", 1, POLICY.seen_cap).unwrap();
    assert_eq!(seen::status(&c, "h1").unwrap().as_deref(), Some("rejected: dormant_no_wake"));
    seen::record(&c, "h1", "delivered", 1, POLICY.seen_cap).unwrap();
    for i in 2..=5 {
        seen::record(&c, &format!("m{i}"), "delivered", i, POLICY.seen_cap).unwrap();
    }
    assert_eq!(seen::status(&c, "m1").unwrap(), None, "oldest id dropped past the cap");
    assert_eq!(seen::status(&c, "m5").unwrap().as_deref(), Some("delivered"));
    let n: i64 = c.query_row("SELECT COUNT(*) FROM delivery_seen", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 4);
}

#[test]
fn recent_delivered_lists_what_may_have_reached_the_agent_and_forget_drops_a_write_ahead() {
    let c = db();
    seen::record(&c, "old", "delivered", 10, 100).unwrap();
    seen::record(&c, "d1", "delivered", 100, 100).unwrap();
    seen::record(&c, "w1", "delivering", 110, 100).unwrap();
    seen::record(&c, "r1", "rejected", 120, 100).unwrap();
    seen::record(&c, "w2", "delivering", 130, 100).unwrap();
    // Newest first; rejected and older than `since` left out; bounded.
    assert_eq!(
        seen::recent_delivered(&c, 50, 10).unwrap(),
        vec![("w2".into(), "delivering".into()), ("w1".into(), "delivering".into()), ("d1".into(), "delivered".into())]
    );
    assert_eq!(seen::recent_delivered(&c, 50, 1).unwrap().len(), 1);
    // A write-ahead whose delivery didn't happen is forgotten: a resend delivers it.
    seen::forget(&c, "w2").unwrap();
    assert_eq!(seen::status(&c, "w2").unwrap(), None);
}

#[test]
fn only_the_head_of_each_pair_is_due_so_pairs_stay_in_order() {
    let c = db();
    outbox::enqueue(&c, Hop::Down, &msg("a1", "agent:alice", "bob"), 1).unwrap();
    outbox::enqueue(&c, Hop::Down, &msg("c1", "agent:carol", "bob"), 2).unwrap();
    outbox::enqueue(&c, Hop::Down, &msg("a2", "agent:alice", "bob"), 3).unwrap();
    outbox::enqueue(&c, Hop::Down, &msg("a3", "agent:alice", "dave"), 4).unwrap();
    // One head per pair: (alice,bob) a1, (carol,bob) c1, (alice,dave) a3.
    assert_eq!(due_ids(&c, 10), ["a1", "c1", "a3"]);

    // a1 in flight: its pair has nothing due until the ack or the wait ends.
    outbox::mark_sent(&c, Hop::Down, "a1", 10, &POLICY).unwrap();
    assert_eq!(due_ids(&c, 10), ["c1", "a3"]);
    // Delivered: a2 is next for the pair.
    outbox::mark_delivered(&c, Hop::Down, "a1", 11).unwrap();
    assert_eq!(due_ids(&c, 11), ["c1", "a2", "a3"]);
    // A failed head also lets the pair move on.
    outbox::mark_failed(&c, Hop::Down, "a2", "rejected: test", 12).unwrap();
    assert_eq!(due_ids(&c, 12), ["c1", "a3"]);
}

#[test]
fn an_unacked_send_is_due_again_after_a_doubling_backoff() {
    assert_eq!((1..=5).map(|a| POLICY.backoff_ms(a)).collect::<Vec<_>>(), [1_000, 2_000, 4_000, 8_000, 8_000]);
    let c = db();
    outbox::enqueue(&c, Hop::Down, &msg("m1", "agent:a", "b"), 0).unwrap();
    assert_eq!(outbox::mark_sent(&c, Hop::Down, "m1", 100, &POLICY).unwrap(), 1);
    assert_eq!(outbox::next_wakeup_ms(&c, Hop::Down).unwrap(), Some(1_100));
    assert!(due_ids(&c, 1_099).is_empty());
    assert_eq!(due_ids(&c, 1_100), ["m1"]);
    assert_eq!(outbox::mark_sent(&c, Hop::Down, "m1", 1_100, &POLICY).unwrap(), 2);
    assert!(due_ids(&c, 3_099).is_empty());
    assert_eq!(due_ids(&c, 3_100), ["m1"]);
    let r = outbox::get(&c, Hop::Down, "m1").unwrap().unwrap();
    assert_eq!((r.state, r.attempts, r.next_at_ms), (State::Sent, 2, 3_100));
}

#[test]
fn no_ack_after_max_attempts_dead_letters_with_a_reason() {
    let c = db();
    outbox::enqueue(&c, Hop::Down, &msg("m1", "agent:a", "b"), 0).unwrap();
    let mut now = 0;
    for attempt in 1..=POLICY.max_attempts {
        assert_eq!(outbox::sweep(&c, Hop::Down, now, &POLICY).unwrap(), vec![]);
        assert_eq!(due_ids(&c, now), ["m1"]);
        assert_eq!(outbox::mark_sent(&c, Hop::Down, "m1", now, &POLICY).unwrap(), attempt);
        now += POLICY.backoff_ms(attempt);
    }
    let failed = outbox::sweep(&c, Hop::Down, now, &POLICY).unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].state, State::Failed);
    assert_eq!(failed[0].reason.as_deref(), Some("no_ack: 3 attempts without an ack"));
    assert!(due_ids(&c, now).is_empty());
    // A late ack can't resurrect it.
    assert!(!outbox::mark_delivered(&c, Hop::Down, "m1", now).unwrap());
}

#[test]
fn a_rejected_ack_and_the_ttl_both_end_in_failed() {
    let c = db();
    outbox::enqueue(&c, Hop::Down, &msg("rej", "agent:a", "b"), 0).unwrap();
    outbox::enqueue(&c, Hop::Down, &msg("old", "agent:a", "nobody"), 0).unwrap();
    outbox::mark_sent(&c, Hop::Down, "rej", 1, &POLICY).unwrap();
    assert!(outbox::mark_failed(&c, Hop::Down, "rej", "rejected: no agent b on host-2", 2).unwrap());
    // `old` never had a placement: queued, no attempts, until the TTL.
    assert_eq!(outbox::sweep(&c, Hop::Down, POLICY.ttl_ms - 1, &POLICY).unwrap(), vec![]);
    let failed = outbox::sweep(&c, Hop::Down, POLICY.ttl_ms, &POLICY).unwrap();
    assert_eq!(failed.iter().map(|r| r.msg_id.as_str()).collect::<Vec<_>>(), ["old"]);
    assert_eq!(failed[0].reason.as_deref(), Some("expired: not delivered within 100 s (0 attempts)"));
    let rej = outbox::get(&c, Hop::Down, "rej").unwrap().unwrap();
    assert_eq!((rej.state, rej.reason.as_deref()), (State::Failed, Some("rejected: no agent b on host-2")));
    assert_eq!(outbox::counts(&c, Hop::Down).unwrap(), (0, 0, 0, 2));
}

#[test]
fn finished_rows_are_pruned_after_the_keep_window() {
    let c = db();
    outbox::enqueue(&c, Hop::Down, &msg("m1", "agent:a", "b"), 0).unwrap();
    outbox::mark_delivered(&c, Hop::Down, "m1", 5).unwrap();
    outbox::sweep(&c, Hop::Down, 5 + POLICY.keep_finished_ms, &POLICY).unwrap();
    assert!(outbox::get(&c, Hop::Down, "m1").unwrap().is_some());
    outbox::sweep(&c, Hop::Down, 6 + POLICY.keep_finished_ms, &POLICY).unwrap();
    assert_eq!(outbox::get(&c, Hop::Down, "m1").unwrap(), None);
}

#[test]
fn a_restart_mid_queue_keeps_every_message_and_its_state() {
    let path = crate::test_env::unique_temp_path("delivery-restart.db");
    {
        let c = Connection::open(&path).unwrap();
        ensure_schema(&c).unwrap();
        outbox::enqueue(&c, Hop::Down, &msg("m1", "agent:a", "b"), 1).unwrap();
        outbox::enqueue(&c, Hop::Down, &msg("m2", "agent:a", "b"), 2).unwrap();
        outbox::enqueue(&c, Hop::Down, &msg("m3", "agent:a", "c"), 3).unwrap();
        outbox::mark_sent(&c, Hop::Down, "m1", 10, &POLICY).unwrap();
        outbox::mark_sent(&c, Hop::Down, "m3", 10, &POLICY).unwrap();
        outbox::mark_delivered(&c, Hop::Down, "m3", 11).unwrap();
        seen::record(&c, "x1", "delivered", 11, POLICY.seen_cap).unwrap();
    } // the controller dies here
    let c = Connection::open(&path).unwrap();
    ensure_schema(&c).unwrap();
    let states: Vec<(String, State, u32)> = {
        let mut v: Vec<_> = outbox::list(&c, Hop::Down, 10).unwrap().into_iter().map(|r| (r.msg_id, r.state, r.attempts)).collect();
        v.reverse();
        v
    };
    assert_eq!(
        states,
        [("m1".to_string(), State::Sent, 1), ("m2".to_string(), State::Queued, 0), ("m3".to_string(), State::Delivered, 1)]
    );
    // m1 is resent once its wait ends; m2 still waits behind it.
    assert_eq!(due_ids(&c, 10 + POLICY.backoff_ms(1)), ["m1"]);
    assert_eq!(seen::status(&c, "x1").unwrap().as_deref(), Some("delivered"));
    drop(c);
    std::fs::remove_file(&path).unwrap();
}

/// K2-d, K2-f.3: a cut turn's message ends `failed`, delivered or still
/// `sent` (its ack died with the host), and a late ack can't revive it. A
/// message never sent is not touched.
#[test]
fn an_interrupted_message_ends_failed_and_a_late_ack_changes_nothing() {
    let c = db();
    for id in ["m_queued", "m_sent", "m_delivered"] {
        outbox::enqueue(&c, Hop::Down, &msg(id, "agent:a", "b"), 10).unwrap();
    }
    outbox::mark_sent(&c, Hop::Down, "m_sent", 11, &POLICY).unwrap();
    outbox::mark_sent(&c, Hop::Down, "m_delivered", 11, &POLICY).unwrap();
    outbox::mark_delivered(&c, Hop::Down, "m_delivered", 12).unwrap();
    let why = "interrupted: b crashed mid-turn before it answered";
    assert!(!outbox::mark_interrupted(&c, Hop::Down, "m_queued", why, 13).unwrap(), "never sent");
    assert!(outbox::mark_interrupted(&c, Hop::Down, "m_sent", why, 13).unwrap());
    assert!(outbox::mark_interrupted(&c, Hop::Down, "m_delivered", why, 13).unwrap());
    assert!(!outbox::mark_delivered(&c, Hop::Down, "m_sent", 14).unwrap(), "the late ack");
    for id in ["m_sent", "m_delivered"] {
        let r = outbox::get(&c, Hop::Down, id).unwrap().unwrap();
        assert_eq!((r.state, r.reason.as_deref()), (State::Failed, Some(why)), "{id}");
    }
    assert_eq!(outbox::get(&c, Hop::Down, "m_queued").unwrap().unwrap().state, State::Queued);
}
