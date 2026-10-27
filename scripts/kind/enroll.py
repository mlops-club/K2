"""Signed host enrollment on kind (K2_HOST_AUTH=enroll; W5, docs/prd/progress/
host-enroll.md): the owner side, for scripts/kind/up.sh, fleet-g2.sh and
enroll-check.sh.

    enroll.py mint   --kubeconfig K --namespace NS --url URL --hosts h0,h1 [--ttl S] [--renew h1]
    enroll.py keys   --url URL
    enroll.py revoke --url URL --host NAME [--reason R]

Every command reads the controller's owner token (its daemon.token) as the
first line of stdin, so it is never on an argv or in a shell variable.

mint: for each host name with no key on the controller (neither enrolled
nor revoked), POST /cli/hosts/enroll-tokens mints a one-time token, and the
Secret k2-host-enroll gets it under the host's name. Each host pod reads
/run/secrets/k2-enroll/<its name>. A host with an enrolled key needs no new
token (it links by its key; the file only has to exist). A revoked host
gets none, so it stays refused, unless it is named in --renew: then it
gets a new token, for a NEW key (a revoked key can't enroll again; the
caller deletes the host's key file first). The Secret is written with `kubectl
create` or `replace` from stdin (not apply, which would copy the data into
an annotation). Tokens are never printed; only host names and key
fingerprints are.
"""

import argparse
import base64
import json
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

SECRET = "k2-host-enroll"


def call(url, owner, method, path, body=None):
    q = "?" + urllib.parse.urlencode({"token": owner})
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url + path + q, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except ValueError:
            return e.code, {}


def keys(url, owner):
    st, d = call(url, owner, "GET", "/cli/hosts/keys")
    if st != 200:
        sys.exit("enroll.py: GET /cli/hosts/keys -> HTTP %s %s" % (st, json.dumps(d)[:200]))
    return d


def kubectl(a, *args, data=None):
    return subprocess.run(["kubectl", "--kubeconfig", a.kubeconfig, "-n", a.namespace] + list(args),
                          input=data, capture_output=True, text=True)


def cmd_mint(a, owner):
    doc = keys(a.url, owner)
    if doc.get("mode") != "enroll":
        sys.exit("enroll.py: the controller runs K2_HOST_AUTH=%s, not enroll" % doc.get("mode"))
    known = {k.get("host_name"): k for k in doc.get("keys", [])}
    r = kubectl(a, "get", "secret", SECRET, "-o", "json")
    exists = r.returncode == 0
    data = json.loads(r.stdout).get("data", {}) if exists else {}
    minted = []
    for h in a.hosts:
        k = known.get(h)
        if k and k.get("revoked_ms") and h not in a.renew:
            print("enroll: %s: key %s is REVOKED; no token minted (it stays refused)" % (h, k.get("fingerprint", "")[:16]))
        elif k and not k.get("revoked_ms"):
            print("enroll: %s: enrolled, key %s" % (h, k.get("fingerprint", "")[:16]))
            data.setdefault(h, base64.b64encode(b"enrolled\n").decode())   # the file must exist at boot
        else:
            st, d = call(a.url, owner, "POST", "/cli/hosts/enroll-tokens", {"ttl_secs": a.ttl})
            if st != 200 or not d.get("token"):
                sys.exit("enroll.py: minting a token for %s -> HTTP %s" % (h, st))
            data[h] = base64.b64encode((d["token"] + "\n").encode()).decode()
            minted.append(h)
            print("enroll: %s: one-time enrollment token minted (ttl %d s, in Secret %s; not shown)" % (h, a.ttl, SECRET))
    if not minted and exists:
        return 0
    secret = {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
              "metadata": {"name": SECRET, "namespace": a.namespace,
                           "labels": {"k2.dev/deploy": a.deploy_label}},
              "data": data}
    r = kubectl(a, "replace" if exists else "create", "-f", "-", data=json.dumps(secret))
    if r.returncode != 0:
        sys.exit("enroll.py: writing Secret %s: %s" % (SECRET, r.stderr.strip()[-300:]))
    return 0


def cmd_keys(a, owner):
    doc = keys(a.url, owner)
    print("mode %s" % doc.get("mode"))
    for k in doc.get("keys", []):
        print("%-12s %s enrolled_ms=%s%s" % (k.get("host_name"), k.get("fingerprint", "")[:16], k.get("enrolled_ms"),
                                             " REVOKED (%s)" % k.get("revoked_reason") if k.get("revoked_ms") else ""))
    return 0


def cmd_revoke(a, owner):
    st, d = call(a.url, owner, "POST", "/cli/hosts/keys/revoke", {"host_name": a.host, "reason": a.reason})
    if st != 200:
        sys.exit("enroll.py: revoke %s -> HTTP %s %s" % (a.host, st, json.dumps(d)[:200]))
    rv = d.get("revoked") or {}
    print("enroll: revoked %s (key %s), live link cut: %s" % (a.host, rv.get("fingerprint", "")[:16], d.get("disconnected")))
    return 0


def main(argv):
    p = argparse.ArgumentParser(prog="enroll.py")
    p.add_argument("cmd", choices=["mint", "keys", "revoke"])
    p.add_argument("--url", required=True)
    p.add_argument("--kubeconfig")
    p.add_argument("--namespace")
    p.add_argument("--hosts", type=lambda s: [h for h in s.split(",") if h])
    p.add_argument("--ttl", type=int, default=3600)
    p.add_argument("--renew", type=lambda s: [h for h in s.split(",") if h], default=[])
    p.add_argument("--deploy-label", default="kind-dist-fake")
    p.add_argument("--host")
    p.add_argument("--reason", default="kind enroll-check")
    a = p.parse_args(argv)
    owner = sys.stdin.readline().strip()
    if not owner:
        sys.exit("enroll.py: no owner token on stdin")
    return {"mint": cmd_mint, "keys": cmd_keys, "revoke": cmd_revoke}[a.cmd](a, owner)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
