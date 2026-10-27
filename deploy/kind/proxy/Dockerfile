# k2-kind-proxy:dev — the Mac's door to K2 on kind k2lab: a TCP relay from
# 127.0.0.1:<port> on the Mac to the K2 Service's NodePort, inside Docker's
# "kind" network. It replaces `kubectl port-forward`, which drops the whole
# forward when one stream breaks (seen 2026-10-24: "lost connection to pod").
# socat forks one child per connection, so a broken stream ends only itself.
#
#   docker build -t k2-kind-proxy:dev deploy/kind/proxy     # scripts/kind/up.sh does this
#   docker run -d --name k2-kind-fake-proxy --restart unless-stopped \
#     --memory 64m --memory-swap 64m --cpus 0.5 --network kind \
#     -p 127.0.0.1:7441:7441 k2-kind-proxy:dev \
#     TCP-LISTEN:7441,fork,reuseaddr TCP:k2lab-control-plane:30741
FROM alpine:3.20.10
RUN apk add --no-cache socat=1.8.0.0-r0
USER 65534:65534
ENTRYPOINT ["socat"]
