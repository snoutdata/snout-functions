#!/usr/bin/env bash
# The memory guard: one function grows by 8 MB a call under a container cap smaller than its own
# limit, beside a function that does nothing. The grower should be stopped by the guard, near the
# cap and before its own limit, with a sentence saying why; the other should go on answering, and
# the process should stay under the cap.
#
#   bash functions/tests/memory-guard-probe.sh <binary> [cap MB, default 120]
set -uo pipefail
bin="${1:?usage: memory-guard-probe.sh <binary> [cap MB]}"
cap="${2:-120}"
root="$(mktemp -d)"
ref=memoryguard01
port=19130

d_grow=$(printf 'guard-grow' | sha256sum | cut -c1-64)
d_calm=$(printf 'guard-calm' | sha256sum | cut -c1-64)
mkdir -p "$root/bundles/$d_grow" "$root/bundles/$d_calm" "$root/projects"
printf '%s\n' 'const kept = []; Deno.serve(() => { kept.push(new Uint8Array(8 << 20).fill(kept.length + 1)); return new Response(String(kept.length * 8)); });' > "$root/bundles/$d_grow/index.ts"
printf '%s\n' 'Deno.serve(() => new Response("calm"));' > "$root/bundles/$d_calm/index.ts"
cat > "$root/projects/$ref.json" <<JSON
{ "ref": "$ref", "functions": [ { "name": "grow", "digest": "$d_grow" }, { "name": "calm", "digest": "$d_calm" } ],
  "limits": { "memoryMb": 256, "wallMs": 30000, "cpuMs": 2000 }, "env": {} }
JSON

# The image's allocator settings (Containerfile).
MALLOC_ARENA_MAX=2 MALLOC_MMAP_THRESHOLD_=1048576 SNOUT_FUNCTIONS_DEBUG="${PROBE_DEBUG:-}" SNOUT_FUNCTIONS_MEMORY_MB="$cap" "$bin" start --main-service /ignored --port "$port" --root "$root" --sockets "$root/sockets" > "$root/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null; wait $server 2>/dev/null; rm -rf "$root"' EXIT
for _ in $(seq 1 200); do curl -sf "http://127.0.0.1:$port/_snoutpod/health" >/dev/null && break; sleep 0.05; done

call() { curl -s -m 30 -H "x-snoutdata-ref: $ref" "http://127.0.0.1:$port/$1"; }
# The server and every process it started (a process per project since 0.2.0).
anon() { for p in $server $(pgrep -P "$server"); do cat "/proc/$p/status" 2>/dev/null; done | awk '/^RssAnon/ { kb += $2 } END { print int(kb / 1024) }'; }
call calm > /dev/null
peak=0; last=""; stopped=""
for i in $(seq 1 40); do
	out="$(call grow)"; [ -n "${PROBE_DEBUG:-}" ] && echo "call $i: [$out] anon $(anon) MB"
	now=$(anon); [ "$now" -gt "$peak" ] && peak=$now
	# The guard may stop the grower between two calls, and then the next call meets a new worker
	# whose count starts again: that is the stop, as the function sees it.
	case "$out" in
		[0-9]*) if [ -n "$last" ] && [ "$out" -lt "$last" ]; then stopped="restarted, its count back to $out MB"; break; fi; last="$out" ;;
		*) stopped="$out"; break ;;
	esac
	sleep 0.4
done
sleep 1.5
echo "cap $cap MB: the grower reached $last MB, then: ${stopped:-never stopped}"
echo "peak $peak MB anonymous, $(anon) MB a moment after; the calm function after: $(call calm)"
grep 'memory guard' "$root/server.log" | head -2; [ -n "${PROBE_DEBUG:-}" ] && grep -E "grow" "$root/server.log" | tail -25
