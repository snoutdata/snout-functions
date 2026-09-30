#!/usr/bin/env bash
# One worker's V8 heap, by space, as the worker itself reads it (node:v8), after one request.
#   bash functions/tests/heap-spaces.sh <binary> [VAR=value ...]
set -uo pipefail
bin="${1:?usage: heap-spaces.sh <binary> [VAR=value ...]}"; shift
root="$(mktemp -d)"; ref=heapspaces001; port=19200
digest=$(printf 'heap-spaces' | sha256sum | cut -c1-64)
mkdir -p "$root/bundles/$digest" "$root/projects"
cat > "$root/bundles/$digest/index.ts" <<'JS'
import v8 from "node:v8";
Deno.serve(() => {
	const kb = (n: number) => Math.round(n / 1024);
	const spaces = v8.getHeapSpaceStatistics().map((s) => `${s.space_name}: size ${kb(s.space_size)} KB, used ${kb(s.space_used_size)} KB, physical ${kb(s.physical_space_size)} KB`);
	const h = v8.getHeapStatistics();
	return new Response([...spaces, `total physical ${kb(h.total_physical_size)} KB, external ${kb(h.external_memory)} KB, malloced ${kb(h.malloced_memory)} KB (peak ${kb(h.peak_malloced_memory)} KB)`].join("\n") + "\n");
});
JS
echo "{ \"ref\": \"$ref\", \"functions\": [ { \"name\": \"h\", \"digest\": \"$digest\" } ], \"limits\": { \"memoryMb\": 128, \"wallMs\": 30000, \"cpuMs\": 2000 }, \"env\": {} }" > "$root/projects/$ref.json"
env "$@" "$bin" start --main-service /ignored --port "$port" --root "$root" --sockets "$root/sockets" > "$root/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null; wait $server 2>/dev/null; rm -rf "$root"' EXIT
for _ in $(seq 1 200); do curl -sf "http://127.0.0.1:$port/_snoutpod/health" >/dev/null && break; sleep 0.05; done
curl -s -H "x-snoutdata-ref: $ref" "http://127.0.0.1:$port/h" >/dev/null
curl -s -H "x-snoutdata-ref: $ref" "http://127.0.0.1:$port/h"
