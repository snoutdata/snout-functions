#!/usr/bin/env bash
# What a process per project costs: memory per project, the first request to a project (which
# finds the spare process waiting, or not), and a warm request through the extra hop. One line
# per measure, for the layout named by SNOUT_FUNCTIONS_PROCESSES (unset: a process per project;
# `one`: every project in one process), so the two are run back to back and compared.
#
#   bash functions/tests/project-cost-probe.sh <binary> [projects, default 20]
set -uo pipefail
bin="${1:?usage: project-cost-probe.sh <binary> [projects]}"
n="${2:-20}"
root="$(mktemp -d)"
port=19160
mkdir -p "$root/projects"
for i in $(seq 1 $((n + 10))); do
	ref=$(printf 'costprobe%04d' "$i")
	d=$(printf '%s' "$ref" | sha256sum | cut -c1-64)
	mkdir -p "$root/bundles/$d"
	printf '%s\n' 'Deno.serve(() => new Response("ok"));' > "$root/bundles/$d/index.ts"
	printf '{ "functions": [ { "name": "hi", "digest": "%s" } ] }\n' "$d" > "$root/projects/$ref.json"
done

"$bin" start --port "$port" --root "$root" --sockets "$root/sockets" > "$root/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null; wait $server 2>/dev/null; rm -rf "$root"' EXIT
for _ in $(seq 1 200); do curl -sf "http://127.0.0.1:$port/_snoutpod/health" >/dev/null && break; sleep 0.05; done
sleep 1.5

# Anonymous memory of the server and every process it started, in MB.
anon() {
	local total=0 kb
	for pid in $server $(pgrep -P "$server"); do
		kb=$(awk '/^RssAnon/ { print $2 }' "/proc/$pid/status" 2>/dev/null)
		total=$((total + ${kb:-0}))
	done
	echo $((total / 1024))
}
ms() { curl -s -o /dev/null -m 20 -w '%{time_total}' -H "x-snoutdata-ref: $1" "http://127.0.0.1:$port/hi" | awk '{ printf "%.1f", $1 * 1000 }'; }
pct() { sort -n | awk -v p="$1" '{ v[NR] = $1 } END { i = int(NR * p / 100); if (i < 1) i = 1; print v[i] }'; }

before=$(anon)
first=(); warm=()
for i in $(seq 1 "$n"); do
	ref=$(printf 'costprobe%04d' "$i")
	first+=("$(ms "$ref")")
	warm+=("$(ms "$ref")")
	sleep 0.3
done
after=$(anon)
burst=()
for i in $(seq $((n + 1)) $((n + 10))); do
	burst+=("$(ms "$(printf 'costprobe%04d' "$i")")")
done
layout="${SNOUT_FUNCTIONS_PROCESSES:-a process per project}"
echo "layout: $layout"
echo "memory: $before MB with nothing called, $after MB with $n projects warm: $(( (after - before) * 10 / n )) tenths of a MB per project"
echo "first request to a project, spaced 300 ms: p50 $(printf '%s\n' "${first[@]}" | pct 50) ms, p95 $(printf '%s\n' "${first[@]}" | pct 95) ms"
echo "first request to a project, 10 back to back: p50 $(printf '%s\n' "${burst[@]}" | pct 50) ms, max $(printf '%s\n' "${burst[@]}" | pct 100) ms"
echo "a warm request: p50 $(printf '%s\n' "${warm[@]}" | pct 50) ms, p95 $(printf '%s\n' "${warm[@]}" | pct 95) ms"
