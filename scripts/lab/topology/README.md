# Topology adapters

`prove.sh --topology <name>` sources `scripts/lab/topology/<name>.sh`. An
adapter defines four functions; everything else in the runner (parity,
scenarios, invariants, report) is the same for every topology.

| Function | Does | Must export / write |
|---|---|---|
| `topo_up <run-dir>` | Start K2 for this topology from the built binaries, with `k2-fake-agent` as every agent CLI. Return non-zero, with the reason on stderr, if it can't. | `LAB_PORT`, `LAB_TOKEN` (client route access), `LAB_HOME` (the agent home root: `$HOME` of whatever runs agents), `LAB_WORKSPACE` (a git dir agents start in), `LAB_FAKE_TRACE` (where the fake agents append trace events) |
| `topo_describe <dir>` | Record what is running. | `<dir>/versions.txt`, `<dir>/boot-status.json`, `<dir>/env.txt` (secrets redacted); for containers, `compose ps` / `kubectl get -o yaml` |
| `topo_metrics` | Current resource use. | Lines `metric,value,unit,note` on stdout (at least `daemon_rss`) |
| `topo_down` | Stop everything `topo_up` started and remove its temp dirs. Safe to call twice. | |

Wave 1 ships `solo` (today's daemon on this machine, in-process agent host).
`container` (from the `image` task), `fleet` (Compose: controller plus two
hosts) and `kind` plug in here as `container.sh`, `fleet.sh`, `kind.sh`;
until then `prove.sh` exits 2 for them. The `kind` adapter must only ever use
the `k2lab` cluster through an explicit kubeconfig
(`kind get kubeconfig --name k2lab`), never the current context.
