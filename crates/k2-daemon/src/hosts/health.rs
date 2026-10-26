//! `k2-daemon --host-health` (P4 T1, decision D10): an agent host pod's
//! readiness probe, with no curl in the image. Exit 0 only when this
//! machine's daemon is a `K2_ROLE=host` whose link to the controller is up;
//! 1 otherwise, with the reason on stderr. It asks the local daemon's
//! unauthenticated `/cli/hosts/status` and never boots a daemon itself.

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::time::Duration;

pub const FLAG: &str = "--host-health";

/// A probe must answer well inside a kubelet period (15 s in `hosts.yaml`).
const TIMEOUT: Duration = Duration::from_secs(3);

/// The process's exit code: [`check`] on the daemon this process would be.
pub fn run() -> i32 {
    let port = match crate::daemon_port::from_env() {
        Ok(Some(p)) => Some(p),
        Ok(None) => k2_core::port_claim::read_port_file(&k2_core::paths::daemon_port_file()),
        Err(e) => {
            eprintln!("host-health: {e}");
            return 1;
        }
    };
    let (code, line) = check(port);
    if code == 0 {
        println!("host-health: {line}");
    } else {
        eprintln!("host-health: {line}");
    }
    code
}

/// `(exit code, one line)` for the daemon on `port`.
pub fn check(port: Option<u16>) -> (i32, String) {
    let Some(port) = port else {
        return (1, "no daemon port (K2_DAEMON_PORT unset and no daemon.port file)".into());
    };
    match status_body(port).and_then(|b| verdict(&b)) {
        Ok(line) => (0, line),
        Err(e) => (1, e),
    }
}

/// Healthy only as a linked host. The reason names what is wrong.
pub fn verdict(body: &str) -> Result<String, String> {
    let v: serde_json::Value = serde_json::from_str(body).map_err(|e| format!("bad /cli/hosts/status answer: {e}"))?;
    if v["role"] != "host" {
        return Err(format!("this daemon is not an agent host (role {})", v["role"]));
    }
    let id = v["host_id"].as_str().unwrap_or("?");
    if v["linked"] == true {
        Ok(format!("linked as {id} to {}", v["controller"].as_str().unwrap_or("the controller")))
    } else {
        let why = v["last_error"].as_str().unwrap_or("never linked yet");
        Err(format!("not linked to the controller ({} attempt(s); last: {why})", v["attempts"]))
    }
}

fn status_body(port: u16) -> Result<String, String> {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let mut s = TcpStream::connect_timeout(&addr, TIMEOUT).map_err(|e| format!("no daemon on {addr}: {e}"))?;
    let _ = s.set_read_timeout(Some(TIMEOUT));
    let _ = s.set_write_timeout(Some(TIMEOUT));
    let req = format!("GET {} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n", super::gate::STATUS_PATH);
    s.write_all(req.as_bytes()).map_err(|e| format!("send to {addr}: {e}"))?;
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).map_err(|e| format!("read from {addr}: {e}"))?;
    let raw = String::from_utf8_lossy(&raw);
    let (head, body) = raw.split_once("\r\n\r\n").ok_or_else(|| format!("bad HTTP answer from {addr}"))?;
    let status = head.lines().next().unwrap_or("");
    if !status.contains(" 200 ") {
        return Err(format!("{} answered {status:?} (a K2_ROLE=all daemon has no host status)", super::gate::STATUS_PATH));
    }
    Ok(body.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    /// A one-shot daemon on loopback answering `status` and `body`.
    fn daemon(status: &'static str, body: String) -> u16 {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((mut s, _)) = l.accept() {
                let mut buf = [0u8; 1024];
                let _ = s.read(&mut buf);
                let _ = write!(s, "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            }
        });
        port
    }

    #[test]
    fn exits_0_only_for_a_linked_host() {
        let linked = serde_json::json!({"role": "host", "linked": true, "host_id": "k2-host-1", "controller": "ws://ctl/cli/hosts/attach", "attempts": 1});
        let (code, line) = check(Some(daemon("200 OK", linked.to_string())));
        assert_eq!(code, 0, "{line}");
        assert!(line.contains("k2-host-1"), "{line}");

        let down = serde_json::json!({"role": "host", "linked": false, "host_id": null, "last_error": "connect: refused", "attempts": 4});
        let (code, line) = check(Some(daemon("200 OK", down.to_string())));
        assert_eq!(code, 1);
        assert!(line.contains("not linked") && line.contains("connect: refused"), "{line}");

        let ctl = serde_json::json!({"role": "controller", "hosts": []});
        let (code, line) = check(Some(daemon("200 OK", ctl.to_string())));
        assert_eq!(code, 1);
        assert!(line.contains("not an agent host"), "{line}");
    }

    #[test]
    fn no_daemon_or_no_host_status_is_unhealthy() {
        // K2_ROLE=all: the route falls through to the dispatcher.
        let (code, line) = check(Some(daemon("404 Not Found", "{}".into())));
        assert_eq!(code, 1);
        assert!(line.contains("404"), "{line}");
        // Nothing listening.
        let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
        let (code, line) = check(Some(port));
        assert_eq!(code, 1);
        assert!(line.contains("no daemon"), "{line}");
        assert_eq!(check(None).0, 1);
        assert!(verdict("not json").is_err());
    }
}
