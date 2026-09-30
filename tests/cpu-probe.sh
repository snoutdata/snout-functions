#!/usr/bin/env bash
# How fast is CPU-bound JavaScript here, against Deno's own CLI? The same loop, timed INSIDE the
# isolate, under ours at two heap limits and under the Deno CLI itself.
#
#   bash functions/tests/cpu-probe.sh <snout-functions binary>      (on a build host, as root)
set -uo pipefail
bin="${1:?usage: cpu-probe.sh <binary>}"
root="$(mktemp -d)"
src='function work(n) { let x = 0; for (let i = 0; i < n; i += 1) { x += Math.sqrt(i) * Math.sin(i); } return x; }
function once() { const t = performance.now(); work(2_000_000); return performance.now() - t; }
const warm = () => { for (let i = 0; i < 10; i++) once(); const r = []; for (let i = 0; i < 20; i++) r.push(once()); r.sort((a, b) => a - b); return r[10].toFixed(1); };'
digest=$(printf 'cpu' | sha256sum | cut -c1-64)
mkdir -p "$root/bundles/$digest/src" "$root/projects"
printf '%s\nDeno.serve(() => new Response(warm()));\n' "$src" > "$root/bundles/$digest/src/index.ts"
printf 'import "./src/index.ts";\n' > "$root/bundles/$digest/index.ts"
for mb in 128 1024; do
	printf '{ "functions": [{ "name": "cpu", "digest": "%s" }], "limits": { "memoryMb": %s, "wallMs": 60000, "cpuMs": 30000 }, "env": {} }\n' "$digest" "$mb" > "$root/projects/cpuprobe$mb.json"
done
"$bin" start --port 19200 --root "$root" --sockets "$root/sockets" > "$root/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null; rm -rf "$root"' EXIT
for _ in $(seq 1 100); do curl -sf http://127.0.0.1:19200/_snoutpod/health >/dev/null && break; sleep 0.05; done
for mb in 128 1024; do
	echo "ours, heap limit ${mb} MB: $(curl -s -m 60 -H "x-snoutdata-ref: cpuprobe$mb" http://127.0.0.1:19200/cpu) ms per work()"
done
printf '%s\nconsole.log(warm());\n' "$src" > "$root/plain.js"
echo "deno 2.9.7 run: $(podman run --rm -v "$root:/w:Z" docker.io/denoland/deno:distroless-2.9.7 run /w/plain.js) ms per work()" # pins-allow: the Deno CLI of the release deno_runtime 0.267 is built from, the thing compared against
node -e "$src; console.log('node ' + process.version + ': ' + warm() + ' ms per work()')" 2>/dev/null || true
