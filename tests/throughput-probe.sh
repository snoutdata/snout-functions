#!/usr/bin/env bash
# Warm throughput of one small function, 10 in flight, as the bench's warm-conc probe loads it,
# so a variant of the environment (V8 flags, replicas) can be compared on one box.
#
#   bash functions/tests/throughput-probe.sh <binary> [requests] [VAR=value ...]
set -uo pipefail
bin="${1:?usage: throughput-probe.sh <binary> [requests] [VAR=value ...]}"
requests="${2:-4000}"
shift $(( $# >= 2 ? 2 : 1 ))
root="$(mktemp -d)"; ref=throughput001; port=19400
digest=$(printf 'throughput' | sha256sum | cut -c1-64)
mkdir -p "$root/bundles/$digest" "$root/projects"
printf '%s\n' 'Deno.serve(async (req) => Response.json({ method: req.method, path: new URL(req.url).pathname, body: await req.text() }));' > "$root/bundles/$digest/index.ts"
echo "{ \"ref\": \"$ref\", \"functions\": [ { \"name\": \"echo\", \"digest\": \"$digest\" } ], \"limits\": { \"memoryMb\": 128, \"wallMs\": 30000, \"cpuMs\": 2000 }, \"env\": {} }" > "$root/projects/$ref.json"
env "$@" "$bin" start --main-service /ignored --port "$port" --root "$root" --sockets "$root/sockets" > "$root/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null; wait $server 2>/dev/null; rm -rf "$root"' EXIT
for _ in $(seq 1 200); do curl -sf "http://127.0.0.1:$port/_snoutpod/health" >/dev/null && break; sleep 0.05; done
url="http://127.0.0.1:$port/echo"
for _ in $(seq 1 200); do curl -sf -o /dev/null -H "x-snoutdata-ref: $ref" -d x "$url"; done
# One curl per 100 requests (--parallel reuses nothing across processes, so this is 10 lanes of
# keep-alive connections, as the bench's client holds them).
started=$(date +%s%3N)
seq 1 10 | xargs -P10 -I{} sh -c "for i in \$(seq 1 $((requests / 10))); do printf 'url = \"%s\"\noutput = \"/dev/null\"\n' '$url'; done > /tmp/tp.{}; curl -s -H 'x-snoutdata-ref: $ref' -d x -K /tmp/tp.{}; rm -f /tmp/tp.{}"
elapsed=$(( $(date +%s%3N) - started ))
workers=$(ls "$root/sockets" | wc -l)
awk -v what="${*:-default}" -v n="$requests" -v ms="$elapsed" -v w="$workers" \
	'BEGIN { printf "%-60s %6.0f requests a second  (%d in %d ms, %d worker sockets)\n", what, n * 1000 / ms, n, ms, w }'
