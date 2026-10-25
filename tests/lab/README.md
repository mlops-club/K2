# The K2 lab kit

The pieces `scripts/lab/prove.sh` uses (PRD "K2 Distributed Runtime",
section 3). Python 3 standard library only; every entry point runs with
`python3 -I`.

| Path | What |
|---|---|
| `lab.py` | Command line for prove.sh: `s1`, `flows`, `report`, `baseline`, `fleet`, `g2`, `collect`, `invariants`, `chaos` |
| `labkit/s1.py` | Scenario S1 (spawn, attach, type, see output) over the app's routes |
| `labkit/ws.py` | Minimal WebSocket client (RFC 6455) |
| `labkit/fleet.py` | G2 process manager: up, down, kill, start, monitor (`procs` and `compose` backends; layouts `fleet`, `solo`) |
| `labkit/g2.py` | Scenarios S2 to S9 against the controller's URL; message ledger; rounds under `--chaos` |
| `labkit/w1.py` | Chat overlay, message wake (S4), heartbeat fire (S5), across every host's home |
| `labkit/chaos.py` | `--chaos seed=N`: seeded kills of a CLI, a host or the controller; `chaos.log` |
| `labkit/invariants.py` | I1 to I4 from the run's evidence (traces, monitor samples, outboxes, ledger) |
| `labkit/flows.py` | Flow validation and the trace checker |
| `labkit/report.py` | Verdict, invariants, `report.json` / `report.html` / `invariants.txt` |
| `extract_flows.py` | PRD `FLOWS` JS data → `docs/prd/flows/<id>.json` (`--check` to verify) |
| `flows.schema.json` | The flow file shape |
| `baselines/<gate>-<topology>.json` | I5 baselines, written by `prove.sh --record-baseline` |
| `test_*.py`, `run_tests.sh` | The lab's own tests (plus `cargo test -p k2-fake-agent`) |

The fake agent is a Rust crate: `tests/fakes/k2-fake-agent`. The trace format
the checker reads is in `docs/prd/progress/lab.md`, "Trace format".

```sh
tests/lab/run_tests.sh                                  # the lab's tests
scripts/lab/prove.sh G0 --topology solo                 # a gate run
scripts/lab/prove.sh G0 --topology solo --parity none   # iterate on scenarios
scripts/lab/prove.sh G2 --topology fleet --chaos seed=4117 --parity none   # G2 (docs/prd/progress/lab-g2.md)
```
