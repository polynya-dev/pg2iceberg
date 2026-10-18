#!/usr/bin/env bash
# Network round trips from pg2iceberg, by destination, on one machine —
# the network side of a cloud deployment (the object store's and
# catalog's own latency per request is their proxies', proxy/envoy.yaml):
#
#   NET_DELAY  to the object store and catalog proxies  default 1ms (another AZ)
#   PG_DELAY   to Postgres (source and coord)           default 1ms
#
# netem delays pg2iceberg's outgoing packets, so each round trip pays the
# delay once. The rules live in pg2iceberg's network namespace: they last
# as long as its container. Delays are fixed: jitter would reorder packets.
set -euo pipefail
cd "$(dirname "$0")"
NET_DELAY="${NET_DELAY:-1ms}"
PG_DELAY="${PG_DELAY:-1ms}"

docker run --rm --net container:pg2iceberg-tpcc-pg2iceberg-1 --cap-add NET_ADMIN \
  -e NET_DELAY="$NET_DELAY" -e PG_DELAY="$PG_DELAY" \
  nicolaka/netshoot:v0.14 sh -euc '
    ip_of() { getent hosts "$1" | cut -d" " -f1; }
    dev=eth0
    tc qdisc del dev $dev root 2>/dev/null || true
    # Three bands, everything unmatched in the last (no delay).
    tc qdisc add dev $dev root handle 1: prio bands 3 priomap 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2
    tc qdisc add dev $dev parent 1:1 handle 10: netem delay "$NET_DELAY"
    tc qdisc add dev $dev parent 1:2 handle 20: netem delay "$PG_DELAY"
    for host in s3-proxy catalog-proxy; do
      tc filter add dev $dev parent 1: protocol ip prio 1 u32 match ip dst "$(ip_of $host)/32" flowid 1:1
    done
    tc filter add dev $dev parent 1: protocol ip prio 1 u32 match ip dst "$(ip_of postgres)/32" flowid 1:2
    t() { curl -s -o /dev/null -w "%{time_starttransfer}" "$1"; }
    echo "network: proxies +$NET_DELAY, postgres +$PG_DELAY"
    echo "  first byte: s3-proxy $(t http://s3-proxy:9000/minio/health/live)s, catalog-proxy $(t http://catalog-proxy:8181/v1/config)s"
  '
