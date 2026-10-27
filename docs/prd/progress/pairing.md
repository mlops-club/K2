# pairing: a laptop enrolls once the owner confirms its code (P3 D7)

Branch `k2-dist/p3-pairing`, from `k2-dist/wave3` 94f1ea6dc.

## Why

W5 enrolls a host with a one-time token (`host-enroll.md`). Its known
limit: on `ws://` the token, the nonce and the hello cross the network in
clear. The signatures stop replay and impersonation, but not a man in the
middle who relays a live enrollment: he can enroll his own key with the
stolen token.

Decision D7: for interactive enrollment from a laptop, add k2-node's short
authentication string. Both ends show six digits and the owner confirms on
the controller. Tokens for kind, compose and VMs stay as they are.

## The construction

This is k2-node's CN1 (`k2_node_proto::pairing`) under its own domain,
`k2-host-sas-v1`:

1. **The token names the controller.** A token minted with `confirm` (or
   `laptop`) is `k2e_<random>.<fp16>`, where `<fp16>` is the first 16 hex
   characters of the controller's pairing key fingerprint. The key is
   P-256, made on first use at `<K2 home>/host-pairing-key.pem`
   (`K2_HOST_PAIR_KEY_FILE`). A man in the middle would need a key with a
   chosen 64-bit fingerprint prefix.
2. **Commit, then nonce.** The host's first `POST /cli/hosts/enroll` (key
   and signature as in W5) is recorded, not enrolled. Only now does the
   controller pick a fresh 16-byte nonce. It answers `202 {"pending":
   {nonce, controller_key_pem, sig}}`, where `sig` is the controller's
   signature over (host key fingerprint, nonce, token hash).
3. **The host checks** that the controller's key fingerprint starts with
   the token's `<fp16>` and that the signature verifies. Otherwise it
   refuses to pair.
4. **Both ends derive the code:** six digits from SHA-256 over (controller
   fingerprint, host fingerprint, nonce, token hash).
   - The host logs the code and shows it in `GET /cli/hosts/status`
     (`pairing_code`).
   - It keeps retrying its enroll call; every call gets the same pairing.
5. **The owner confirms.** `GET /cli/hosts/enroll/pending` lists the
   waiting hosts without their codes: the owner reads the code on the
   laptop. `POST /cli/hosts/enroll/confirm {"host_name", "code"}`:
   - **The code the host shows** (spaces allowed): the key is enrolled and
     the token spent. The host's next call answers enrolled, and it links
     with signed hellos as in W5.
   - **Any other code:** `403 sas_mismatch`. The pairing is dropped and the
     token burned; a new pairing needs a new token.
6. **One host per token.** A second key on a token that is pairing gets
   `409 token_in_use`.

**Laptops.** `POST /cli/hosts/enroll-tokens {"laptop": true}` mints a
confirm token whose key gets `k2.kind=laptop`. The controller merges an
enrolled key's labels into its hello labels; a hello label wins for the
same key. So a laptop paired this way is a laptop to placement (pinned by
D5) without setting `K2_HOST_LABELS`.

**What doesn't change:**
- a token minted without `confirm` enrolls at once, as before;
- `K2_HOST_AUTH=token`;
- the wire (the hello is unchanged; enroll answers `202` with `pending`).

## Routes

| Route | Who | |
|---|---|---|
| `POST /cli/hosts/enroll-tokens` | owner | new body fields: `confirm`, `laptop` |
| `POST /cli/hosts/enroll` | public (the token is the credential) | `202 {"pending": …}` for a confirm token not yet confirmed |
| `GET /cli/hosts/enroll/pending` | owner | the waiting pairings: host name, key fingerprint, labels, created, token expiry; no code |
| `POST /cli/hosts/enroll/confirm` | owner | `{"host_name", "code"}`; 200 enrolled, 403 `sas_mismatch` (token burned), 404 `no_pairing` |

## Hooks in other workers' files

| File | Hook |
|---|---|
| `hosts/enroll.rs` (W5) | confirm tokens, the `host_pairings` table and two token columns (added to an older table), `enroll_or_pair`, `confirm`, `pending`, `key_labels`, `merge_labels`, `hello_labels`, the two routes, the host's check (`pairing_code_for`) and its `pairing_code` |
| `hosts/hub.rs` | the hello labels with the key's (`hello_labels`) |
| `hosts/link.rs` | `pairing_code` in the host's status (keyed by host name) |
| `routes/route_policy.rs` | rows for `POST /cli/hosts/enroll/confirm` and `GET /cli/hosts/enroll/pending` (owner token) |

## Results

| Check | Result |
|---|---|
| `cargo test -p k2-daemon --lib -- hosts::enroll` | **23 passed**, 5 runs in a row. New: `a_confirm_token_enrolls_only_when_the_owner_types_the_hosts_code` (a wrong code refuses and burns the token, after which even the right code finds no pairing; the same pairing on every call; another key gets `token_in_use`; no link before the confirm; pending without the code; the right code, with spaces, enrolls with `k2.kind=laptop`; a hello label wins; a plain token still enrolls at once); `the_host_pairs_only_with_the_controller_its_token_names` (a man in the middle's key, a signature from another key, a nonce it didn't sign, a token naming no key: all refused); `a_laptop_pairs_over_http_when_the_owner_confirms_its_code` (the host's own code over HTTP and the real hub: it waits showing the code, the owner confirms, it links as kind laptop); `every_enrollment_route_has_a_route_policy_row`. Changed: `owner_routes_need_the_owner_token` covers the two new routes |
| Mutations | confirm accepts any code → the confirm and HTTP tests fail; the host skips the key-prefix check → the man-in-the-middle test fails |
| `scripts/lab/topology/laptop-pairing.sh` (controller, laptop and rogue host, all `K2_HOST_AUTH=enroll`), runs `20261027T160433Z`, `20261027T160448Z` | **4/4 PASS, twice**. (1) The laptop waits showing its code in its status; the controller lists it pending without the code. (2) A wrong code for a rogue host: `403 sas_mismatch`, its token burned, its retries refused `token_invalid`, it never links. (3) The laptop's own code: it links, kind `laptop` from its token (no `K2_HOST_LABELS`). (4) An agent first placed there is pinned to it. The first runs found `POST …/confirm` answered `405` by the real dispatcher: its route-policy rows were missing (the unit tests call the handler directly), so the rows and a test were added |
| `two-process-enroll.sh` `20261027T150901Z` | **5/5 PASS** (W5's enrollment unchanged) |
| `cargo test -p k2-daemon --no-fail-fast -- --test-threads=3` (throwaway HOME, after the route rows) | **5482 passed, 0 failed**, 6 ignored, 162 binaries |

## Follow-ups

- **`k2` CLI verbs:** mint a laptop token, list pending pairings, confirm
  one. Today these are owner-token HTTP calls, like W5's.
- **The laptop's side for a person:** the code is in the host's log and its
  `/cli/hosts/status`; the app could show it.
