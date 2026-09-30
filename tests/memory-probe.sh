#!/usr/bin/env bash
# What one warm worker costs: start the runtime, warm N distinct functions, and read the
# process's anonymous memory before and after. Each run is one line, so variants of the
# environment (V8 flags, the allocator) can be compared on one box.
#
#   bash functions/tests/memory-probe.sh <binary> [workers] [VAR=value ...]
#
# Anonymous memory (RssAnon) is what the bench and the fleet count; file-backed pages of the
# binary are shared and dropped under pressure.
set -uo pipefail
bin="${1:?usage: memory-probe.sh <binary> [workers] [VAR=value ...]}"
workers="${2:-30}"
shift $(( $# >= 2 ? 2 : 1 ))
root="$(mktemp -d)"
ref=memoryprobe01
port=19100

functions=""
for i in $(seq 1 "$workers"); do
	digest=$(printf 'probe-%s' "$i" | sha256sum | cut -c1-64)
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
trap 'kill $server 2>/dev/null; wait $server 2>/dev/null; rm -rf "$root"' EXIT
for _ in $(seq 1 200); do curl -sf "http://127.0.0.1:$port/_snoutpod/health" >/dev/null && break; sleep 0.05; done

anon() { awk '/^RssAnon/ { print $2 }' "/proc/$server/status"; }
sleep 3
idle=$(anon)
# The first round is $workers cold requests back to back, as the bench's cold probe is.
cold=()
for i in $(seq 1 "$workers"); do
	cold+=("$(curl -sf -o /dev/null -w '%{time_total}' -H "x-snoutdata-ref: $ref" "http://127.0.0.1:$port/f$i" || echo "f$i failed" >&2)")
done
for i in $(seq 1 "$workers"); do
	curl -sf -o /dev/null -H "x-snoutdata-ref: $ref" "http://127.0.0.1:$port/f$i" || echo "f$i failed" >&2
done
sleep 3
warm=$(anon)
coldms=$(printf '%s\n' "${cold[@]}" | sort -n | awk '{ a[NR] = $1 * 1000 } END { printf "%.1f / %.1f", a[int((NR + 1) / 2)], a[int(NR * 0.99 + 0.5)] }')
awk -v what="${*:-default}" -v idle="$idle" -v warm="$warm" -v n="$workers" -v cold="$coldms" \
	'BEGIN { printf "%-50s idle %5.1f MB  warm %6.1f MB  per worker %5.2f MB  cold p50 / p99 %s ms\n", what, idle / 1024, warm / 1024, (warm - idle) / 1024 / n, cold }'
