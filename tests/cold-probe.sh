#!/usr/bin/env bash
# Cold start alone: N distinct functions, each asked once, back to back (the bench's cold probe)
# and then again with a gap between requests, so a spare has had time to boot. The difference
# between the two lines is what booting spares costs the requests they compete with.
#
#   bash functions/tests/cold-probe.sh <binary> [functions] [VAR=value ...]
#
# Run it under `taskset -c 0,1` to see a 2-core host on a larger one; PROBE_LABEL names the lines, and PROBE_LOG keeps the server's log (with SNOUT_FUNCTIONS_DEBUG=1, each claim's timings).
set -uo pipefail
bin="${1:?usage: cold-probe.sh <binary> [functions] [VAR=value ...]}"
n="${2:-30}"
shift $(( $# >= 2 ? 2 : 1 ))
root="$(mktemp -d)"
ref=coldprobe0001
port=19110

functions=""
for i in $(seq 1 $(( n * 2 ))); do
	digest=$(printf 'cold-%s' "$i" | sha256sum | cut -c1-64)
	mkdir -p "$root/bundles/$digest"
	printf '%s\n' "Deno.serve((req) => Response.json({ n: $i, path: new URL(req.url).pathname }));" > "$root/bundles/$digest/index.ts"
	functions="$functions{ \"name\": \"f$i\", \"digest\": \"$digest\" },"
done
mkdir -p "$root/projects"
cat > "$root/projects/$ref.json" <<JSON
{ "ref": "$ref", "functions": [ ${functions%,} ], "limits": { "memoryMb": 128, "wallMs": 30000, "cpuMs": 2000 }, "env": {} }
JSON

env "$@" "$bin" start --main-service /ignored --port "$port" --root "$root" --sockets "$root/sockets" > "$root/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null; wait $server 2>/dev/null; [ -n "${PROBE_LOG:-}" ] && cp "$root/server.log" "$PROBE_LOG"; rm -rf "$root"' EXIT
for _ in $(seq 1 200); do curl -sf "http://127.0.0.1:$port/_snoutpod/health" >/dev/null && break; sleep 0.05; done
sleep 2

ask() { curl -sf -o /dev/null -w '%{time_total}\n' -H "x-snoutdata-ref: $ref" "http://127.0.0.1:$port/f$1" || echo "f$1 failed" >&2; }
summary() { sort -n | awk -v what="$1" '{ a[NR] = $1 * 1000 } END { printf "%-40s cold p50 / p90 / p99 %5.1f / %5.1f / %5.1f ms\n", what, a[int((NR + 1) / 2)], a[int(NR * 0.9 + 0.5)], a[int(NR * 0.99 + 0.5)] }'; }

label="${PROBE_LABEL:-${*:-default}}"
for i in $(seq 1 "$n"); do ask "$i"; done | summary "$label, back to back"
sleep 12
for i in $(seq $(( n + 1 )) $(( n * 2 ))); do ask "$i"; sleep 0.3; done | summary "$label, 300 ms apart"
