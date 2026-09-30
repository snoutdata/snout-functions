#!/usr/bin/env bash
# Start the runtime against a functions root laid out the way the host agent writes one, and
# drive it with curl: a hello, a TypeScript module with a relative import and an env var, a
# changed secret, the limits, and 100 requests held at once.
#
#   bash functions/tests/smoke.sh <path to the snout-functions binary>
set -uo pipefail
bin="${1:?usage: smoke.sh <binary>}"
root="$(mktemp -d)"
ref=smoketestref01
pass=0; fail=0
check() { # name, expected, actual
	if [ "$2" = "$3" ]; then pass=$((pass + 1)); echo "ok   $1"; else fail=$((fail + 1)); echo "FAIL $1: expected [$2] got [$3]"; fi
}

bundle() { # digest, file=content...
	local dir="$root/bundles/$1"; shift
	mkdir -p "$dir/src"
	printf '%s\n' 'import "./src/index.ts";' > "$dir/index.ts"
	while [ $# -gt 0 ]; do printf '%s\n' "${1#*=}" > "$dir/src/${1%%=*}"; shift; done
}
d_hello=$(printf 'hello' | sha256sum | cut -c1-64)
d_ts=$(printf 'ts' | sha256sum | cut -c1-64)
d_mem=$(printf 'mem' | sha256sum | cut -c1-64)
d_cpu=$(printf 'cpu' | sha256sum | cut -c1-64)
d_sleep=$(printf 'sleep' | sha256sum | cut -c1-64)
d_work=$(printf 'work' | sha256sum | cut -c1-64)
d_url=$(printf 'url' | sha256sum | cut -c1-64)
bundle "$d_hello" 'index.ts=Deno.serve(() => new Response("ok"));'
bundle "$d_ts" 'index.ts=import { greet } from "./greet.ts";
Deno.serve(async (req: Request): Promise<Response> => new Response(greet(Deno.env.get("WHO") ?? "nobody")));' \
	'greet.ts=export function greet(who: string): string { return `hello ${who}`; }'
bundle "$d_mem" 'index.ts=Deno.serve((req) => { const keep = []; const kind = new URL(req.url).searchParams.get("kind");
for (let i = 0; i < 256 * 16; i++) keep.push(kind === "heap" ? Array.from({ length: 4096 }, (_, j) => ({ j })) : new Uint8Array(65536).fill(1));
return new Response("held " + keep.length); });'
bundle "$d_cpu" 'index.ts=Deno.serve(() => { for (;;) {} });'
bundle "$d_work" 'index.ts=Deno.serve(() => { let x = 0; for (let i = 0; i < 3000000; i++) x += Math.sqrt(i); return new Response(String(x > 0)); });'
bundle "$d_sleep" 'index.ts=Deno.serve(async (req) => { const ms = Number(new URL(req.url).searchParams.get("ms") ?? 1000); await new Promise((r) => setTimeout(r, ms)); return new Response("slept " + ms); });'
d_loop=$(printf 'loop' | sha256sum | cut -c1-64)
bundle "$d_loop" 'index.ts=Deno.serve(async () => { const out = {}; for (const base of ["127.0.0.1", "localhost", "127.0.0.2", "[::ffff:127.0.0.1]", "localtest.me", "2130706433", "127.1"]) { const u = "http://" + base + ":19000/hello"; try { out[base] = (await fetch(u, { headers: { "x-snoutdata-ref": "smoketestref01" } })).status; } catch (e) { out[base] = e.name; } } return Response.json(out); });'
d_peer=$(printf 'peer' | sha256sum | cut -c1-64)
bundle "$d_peer" 'index.ts=Deno.serve(async (req) => { if (req.headers.get("x-peer-probe")) return new Response("self");
const dir = new URL(req.url).searchParams.get("dir"); const out = { connected: [], self: [] };
try { out.list = [...Deno.readDirSync(dir)].length; } catch (e) { out.list = e.name; }
for (let i = 0; i < 40; i++) { try { const c = await Deno.connect({ transport: "unix", path: dir + "/w" + i + "/s.sock" });
await c.write(new TextEncoder().encode("GET / HTTP/1.1\r\nhost: x\r\nx-peer-probe: 1\r\nconnection: close\r\n\r\n")); const buf = new Uint8Array(4096); const n = await c.read(buf) ?? 0;
(new TextDecoder().decode(buf.subarray(0, n)).endsWith("self") ? out.self : out.connected).push(i); c.close(); } catch (e) { out.error = e.name; } }
return Response.json(out); });'
bundle "$d_url" 'index.ts=Deno.serve((req) => Response.json({ url: req.url, host: req.headers.get("host"), env: Deno.env.toObject(), processEnvReadable: (() => { try { return typeof Deno.env.get("PATH"); } catch (e) { return "refused"; } })() }));'

manifest() { # env json
	mkdir -p "$root/projects"
	cat > "$root/projects/$ref.json.tmp" <<JSON
{ "ref": "$ref", "functions": [
	{ "name": "hello", "digest": "$d_hello" }, { "name": "ts", "digest": "$d_ts" },
	{ "name": "mem", "digest": "$d_mem" }, { "name": "cpu", "digest": "$d_cpu" },
	{ "name": "sleep", "digest": "$d_sleep" }, { "name": "work", "digest": "$d_work" }, { "name": "loop", "digest": "$d_loop" }, { "name": "url", "digest": "$d_url" }, { "name": "peer", "digest": "$d_peer" } ],
  "limits": { "memoryMb": 128, "wallMs": 30000, "cpuMs": 2000, "hardCpuMs": 3500 }, "env": $1 }
JSON
	mv "$root/projects/$ref.json.tmp" "$root/projects/$ref.json"
}
manifest '{"WHO":"world"}'

door=smoke-door-secret-0123456789
SNOUT_FUNCTIONS_DEBUG=1 SNOUT_FUNCTIONS_DOOR_SECRET="$door" "$bin" start --main-service /ignored --port 19000 --root "$root" --sockets "$root/sockets" > "$root/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null; rm -rf "$root"' EXIT
for _ in $(seq 1 100); do curl -sf http://127.0.0.1:19000/_snoutpod/health >/dev/null && break; sleep 0.05; done

call() { curl -s -m 60 -H "x-snoutdata-door: $door" -H "x-snoutdata-ref: $ref" "http://127.0.0.1:19000$1"; }
ms() { curl -s -m 60 -o /dev/null -w '%{time_total}' -H "x-snoutdata-door: $door" -H "x-snoutdata-ref: $ref" "http://127.0.0.1:19000$1" | awk '{printf "%.1f", $1 * 1000}'; }

check "health" "ok" "$(curl -s http://127.0.0.1:19000/_snoutpod/health)"
check "no door secret is refused" "403" "$(curl -s -o /dev/null -w '%{http_code}' -H "x-snoutdata-ref: $ref" http://127.0.0.1:19000/hello)"
check "a wrong door secret is refused" "403" "$(curl -s -o /dev/null -w '%{http_code}' -H "x-snoutdata-door: ${door}x" -H "x-snoutdata-ref: $ref" http://127.0.0.1:19000/hello)"
check "no ref is refused" "400" "$(curl -s -o /dev/null -w '%{http_code}' -H "x-snoutdata-door: $door" http://127.0.0.1:19000/hello)"
check "unknown function" "404" "$(curl -s -o /dev/null -w '%{http_code}' -H "x-snoutdata-door: $door" -H "x-snoutdata-ref: $ref" http://127.0.0.1:19000/nope)"
echo "cold hello: $(ms /hello) ms"
check "hello" "ok" "$(call /hello)"
warm=(); for _ in $(seq 1 200); do warm+=("$(ms /hello)"); done
echo "warm hello over 200 (curl, a new connection each): p50 $(printf '%s\n' "${warm[@]}" | sort -n | sed -n 100p) ms, p99 $(printf '%s\n' "${warm[@]}" | sort -n | sed -n 198p) ms"
check "typescript + relative import + env" "hello world" "$(call /ts)"
manifest '{"WHO":"changed"}'
check "a changed secret reaches the next request" "hello changed" "$(call /ts)"
echo "url as the function sees it: $(call /url)"
check "a JSON answer is gzipped when asked" "1" "$(curl -s -m 30 -o /dev/null -D - -H 'accept-encoding: gzip, deflate' -H "x-snoutdata-door: $door" -H "x-snoutdata-ref: $ref" http://127.0.0.1:19000/url | grep -ci '^content-encoding: gzip')"
check "the url is http, not http+unix" "1" "$(call /url | grep -c '"url":"http://127.0.0.1:19000/url"')"
check "the process environment is not the worker's" "undefined" "$(call /url | sed -n 's/.*"processEnvReadable":"\([a-z]*\)".*/\1/p')"
started=$(date +%s%3N)
pids=(); for i in $(seq 1 100); do call '/sleep?ms=5000' > "$root/held.$i" & pids+=($!); done; wait "${pids[@]}"
held_ok=$(cat "$root"/held.* | grep -o 'slept 5000' | wc -l)
echo "100 held requests of 5 s: $held_ok answered in $(( $(date +%s%3N) - started )) ms"
check "100 held requests answered" "100" "$held_ok"
wpids=(); for w in 1 2 3 4; do ( for _ in $(seq 1 60); do call /work; echo; done > "$root/work.$w" ) & wpids+=($!); done; wait "${wpids[@]}"
check "steady CPU-bound traffic, 4 in flight, is never stopped" "240" "$(cat "$root"/work.* | grep -c '^true$')"
# The same 240 again, timed against one at a time: a function busy on CPU gets another worker, so
# 4 in flight on a box with 4 cores or more takes well under 4 times as long as 1 in flight.
one=$(date +%s%3N); for _ in $(seq 1 20); do call /work > /dev/null; done; one=$(( ($(date +%s%3N) - one) / 20 ))
four=$(date +%s%3N); wpids=(); for w in 1 2 3 4; do ( for _ in $(seq 1 20); do call /work > /dev/null; done ) & wpids+=($!); done; wait "${wpids[@]}"; four=$(( ($(date +%s%3N) - four) / 20 ))
echo "CPU-bound work: $one ms a request alone, $four ms a round of 4 in flight"
check "4 CPU-bound requests in flight run side by side" "1" "$(( nproc_=$(nproc), nproc_ < 4 || four * 10 < one * 25 ))"
out="$(call /loop)"; echo "a worker fetching the runtime on loopback: $out"
# By name (127.0.0.1, localhost) the worker's permissions refuse it; every other address of this
# container (127.0.0.2 is loopback too) reaches the port, and the door's secret, which no worker
# holds, refuses it there.
check "no worker calls a function through this container's own address" "0" "$(printf '%s' "$out" | grep -c ':200')"
check "and the secret, not the address, is what refuses 127.0.0.2" "1" "$(printf '%s' "$out" | grep -c '"127.0.0.2":403')"
call /hello > /dev/null
out="$(call "/peer?dir=$root/sockets")"; echo "a worker listing the sockets and connecting to its neighbours: $out"
check "no worker reaches another worker's socket" "1" "$(printf '%s' "$out" | grep -cF '"connected":[]')"
check "nor lists the sockets" "0" "$(printf '%s' "$out" | grep -c '"list":[0-9]')"
out="$(call '/mem?kind=buffers')"; echo "256 MB of buffers: $out"
check "buffer memory limit is named" "1" "$(printf '%s' "$out" | grep -c 'memory limit')"
out="$(call '/mem?kind=heap')"; echo "256 MB of heap: $out"
check "memory limit is named" "1" "$(printf '%s' "$out" | grep -c 'memory limit')"
started=$(date +%s%3N); out="$(call /cpu)"; echo "busy loop: $out in $(( $(date +%s%3N) - started )) ms"
check "CPU limit is named" "1" "$(printf '%s' "$out" | grep -c 'CPU limit')"
check "hello still answers after another worker was stopped" "ok" "$(call /hello)"
echo "anon MB now: $(awk '/RssAnon/{printf "%.1f", $2/1024}' /proc/$server/status)"
echo "--- server log"; tail -20 "$root/server.log"
echo "passed $pass, failed $fail"
[ "$fail" = 0 ]
