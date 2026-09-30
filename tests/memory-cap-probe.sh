#!/usr/bin/env bash
# The memory cap: more functions than fit under it, each asked twice, one after another. Every
# request should be answered (a new worker stops an idle one to make room) and the process should
# stay under the cap; without it, the workers would simply pile up.
#
#   bash functions/tests/memory-cap-probe.sh <binary> [cap MB, default 80] [functions, default 40]
set -uo pipefail
bin="${1:?usage: memory-cap-probe.sh <binary> [cap MB] [functions]}"
cap="${2:-80}"
n="${3:-40}"
root="$(mktemp -d)"
ref=memorycap0001
port=19120

functions=""
for i in $(seq 1 "$n"); do
	digest=$(printf 'cap-%s' "$i" | sha256sum | cut -c1-64)
	mkdir -p "$root/bundles/$digest"
	printf '%s\n' "const keep = new Uint8Array(2 << 20).fill($i); Deno.serve(() => Response.json({ n: $i, kept: keep.length }));" > "$root/bundles/$digest/index.ts"
	functions="$functions{ \"name\": \"f$i\", \"digest\": \"$digest\" },"
done
mkdir -p "$root/projects"
cat > "$root/projects/$ref.json" <<JSON
{ "ref": "$ref", "functions": [ ${functions%,} ], "limits": { "memoryMb": 128, "wallMs": 30000, "cpuMs": 2000 }, "env": {} }
JSON

SNOUT_FUNCTIONS_DEBUG=1 SNOUT_FUNCTIONS_MEMORY_MB="$cap" "$bin" start --main-service /ignored --port "$port" --root "$root" --sockets "$root/sockets" > "$root/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null; wait $server 2>/dev/null; rm -rf "$root"' EXIT
for _ in $(seq 1 200); do curl -sf "http://127.0.0.1:$port/_snoutpod/health" >/dev/null && break; sleep 0.05; done

anon() { awk '/^RssAnon/ { print int($2 / 1024) }' "/proc/$server/status"; }
peak=0; codes=""
for round in 1 2; do
	for i in $(seq 1 "$n"); do
		codes="$codes $(curl -s -o /dev/null -w '%{http_code}' -H "x-snoutdata-ref: $ref" "http://127.0.0.1:$port/f$i")"
		now=$(anon); [ "$now" -gt "$peak" ] && peak=$now
		sleep 0.12
	done
done
ok=$(printf '%s\n' $codes | grep -c '^200$')
full=$(printf '%s\n' $codes | grep -c '^503$')
head -1 "$root/server.log"
echo "cap $cap MB, $n functions twice: $ok answered, $full refused (503), peak $peak MB anonymous, $(grep -c 'stop requested (evicted to make room)' "$root/server.log") workers stopped to make room"
