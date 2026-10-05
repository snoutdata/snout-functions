#!/usr/bin/env bash
# A process per project, checked from OUTSIDE each project's process: whatever its code managed,
# even out of its isolate, is bounded by what the kernel says of the process. Two projects, one
# function each; then, for each project's process, from /proc:
#
#   * its user is its own, not root and not the other project's, and it holds no capability;
#   * no_new_privs is set, so no exec can gain any;
#   * its filesystem root is its own directory, holding its own bundle and not the other's, and
#     no manifest of anybody's;
#   * the front door's secret is not in its environment (read on the spare, which is started the
#     same way: a confined process's environment is not readable from outside, by design);
#   * and a process that dies is replaced on the next request.
#
# Run as root (in a container, as the fleet does); as anyone else the processes are separate but
# not confined, which this says and then checks only the separation.
#
#   bash functions/tests/isolation-probe.sh <binary>
set -uo pipefail
bin="${1:?usage: isolation-probe.sh <binary>}"
root="$(mktemp -d)"
port=19140
door=isolation-door-secret-0123456789
pass=0; fail=0
check() { # name, expected, actual
	if [ "$2" = "$3" ]; then pass=$((pass + 1)); echo "ok   $1"; else fail=$((fail + 1)); echo "FAIL $1: expected [$2] got [$3]"; fi
}

project() { # ref, text the function answers
	local digest
	digest=$(printf '%s' "$1" | sha256sum | cut -c1-64)
	mkdir -p "$root/bundles/$digest" "$root/projects"
	# /hi/exec asks Deno.execPath(), which reads /proc/self/exe: there is no /proc in a project's root.
	printf '%s\n' "Deno.serve((req) => new Response(new URL(req.url).pathname.endsWith(\"/exec\") ? Deno.execPath() : \"$2\"));" > "$root/bundles/$digest/index.ts"
	printf '{ "functions": [ { "name": "hi", "digest": "%s" } ], "env": { "SECRET": "%s-secret" } }\n' "$digest" "$1" > "$root/projects/$1.json"
	echo "$digest"
}
d_a=$(project isolationa01 "from a")
d_b=$(project isolationb01 "from b")

owner=$(stat -c %u "$bin")
SNOUT_FUNCTIONS_DOOR_SECRET="$door" "$bin" start --port "$port" --root "$root" > "$root/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null; wait $server 2>/dev/null; rm -rf "$root"' EXIT
for _ in $(seq 1 200); do curl -sf "http://127.0.0.1:$port/_snoutpod/health" >/dev/null && break; sleep 0.05; done

call() { curl -s -m 20 -H "x-snoutdata-door: $door" -H "x-snoutdata-ref: $1" "http://127.0.0.1:$port/hi"; }
check "a answers" "from a" "$(call isolationa01)"
check "b answers" "from b" "$(call isolationb01)"
check "Deno.execPath() answers in a confined process" "yes" "$(curl -s -m 20 -H "x-snoutdata-door: $door" -H "x-snoutdata-ref: isolationa01" "http://127.0.0.1:$port/hi/exec" | grep -q '^/' && echo yes || echo no)"
check "and the process is still there after it" "from a" "$(call isolationa01)"
check "no door, no answer" "403" "$(curl -s -o /dev/null -w '%{http_code}' -H "x-snoutdata-ref: isolationa01" "http://127.0.0.1:$port/hi")"

confined=0
[ "$(id -u)" = 0 ] && confined=1
[ $confined = 1 ] || echo "note: not root, so the processes are separate but not confined; only that is checked"

# Each project's process, found by what /proc lets anyone read of it, its status: confined, it is a
# user of its own, and this script (root without ptrace, like the front process) may read neither
# its root nor its environment, which is itself the point. The router gives uids in the order
# projects are first called: a, then b.
views="${TMPDIR:-/tmp}/snout-functions-projects"
field() { awk -v k="$2:" '$1 == k { print $2 }' "/proc/$1/status"; }
pid_by_uid() { for pid in $(pgrep -P "$server"); do [ "$(field "$pid" Uid)" = "$1" ] && { echo "$pid"; return; }; done; }
spare=""; for pid in $(pgrep -P "$server"); do [ "$(field "$pid" Uid)" = 0 ] && spare=$pid; done
check "an unassigned spare is waiting" "yes" "$([ -n "$spare" ] && echo yes || echo no)"
# Every project's process is started as the spare is, so the spare's environment is theirs.
[ -n "$spare" ] && check "a project's process is not given the door's secret" "0" "$(tr '\0' '\n' < "/proc/$spare/environ" | grep -c SNOUT_FUNCTIONS_DOOR_SECRET)"
if [ $confined = 1 ]; then
	pid_a=$(pid_by_uid 20000); pid_b=$(pid_by_uid 20001)
	check "each project has a process of its own" "yes" "$([ -n "$pid_a" ] && [ -n "$pid_b" ] && echo yes || echo "no: a=$pid_a b=$pid_b")"
	for side in a b; do
		pid=$([ $side = a ] && echo "$pid_a" || echo "$pid_b")
		[ -n "$pid" ] || continue
		check "$side: no capability" "0000000000000000" "$(field "$pid" CapEff)"
		check "$side: nothing it can gain later" "0000000000000000" "$(field "$pid" CapPrm)"
		check "$side: no_new_privs" "1" "$(field "$pid" NoNewPrivs)"
		check "$side: its root and environment are not readable by another user" "denied" "$(readlink "/proc/$pid/root" >/dev/null 2>&1 && echo readable || echo denied)"
	done
	# Bound inside its own root, a socket's name is /sockets/front.sock: a path that exists nowhere
	# outside a project's directory, so two of them are two processes each shut in its own.
	check "both sockets were bound inside their own roots" "2" "$(awk '$8 == "/sockets/front.sock"' /proc/net/unix | wc -l)"
	check "no such path outside them" "no" "$([ -e /sockets ] && echo yes || echo no)"
	check "a's root holds its own bundle and not b's" "$d_a" "$(ls "$views/isolationa01/bundles")"
	check "b's root holds its own bundle and not a's" "$d_b" "$(ls "$views/isolationb01/bundles")"
	check "a's root holds no manifest" "bundles etc proc sockets tmp" "$(ls "$views/isolationa01" | tr '\n' ' ' | sed 's/ $//')"
	check "the binary its proc/self/exe names is still its owner's" "$owner" "$(stat -c %u "$bin")"
	# Audit 5-C: root writes into a project's root while it runs, so the project owns only its
	# /tmp and its socket's directory, and reads the rest through its group.
	check "a's bundles are root's, readable by a's group alone" "0:20000:750" "$(stat -c %u:%g:%a "$views/isolationa01/bundles")"
	check "a's root itself is root's" "0:20000:750" "$(stat -c %u:%g:%a "$views/isolationa01")"
	check "a's /etc is root's" "0:20000:750" "$(stat -c %u:%g:%a "$views/isolationa01/etc")"
	check "a owns its /tmp and its sockets' directory" "20000:20000 20000:20000" "$(stat -c %u:%g "$views/isolationa01/tmp" "$views/isolationa01/sockets" | tr '\n' ' ' | sed 's/ $//')"
else
	pid_a=""
	check "each project has a process of its own" "yes" "$([ "$(pgrep -P "$server" | wc -l)" -ge 3 ] && echo yes || echo no)"
fi

# A process that dies is replaced on the next request, and the other project does not notice.
[ -n "$pid_a" ] && kill -9 "$pid_a" && sleep 0.3
check "a answers again after its process died" "from a" "$(call isolationa01)"
check "b was not touched" "from b" "$(call isolationb01)"

echo "$pass passed, $fail failed"
[ $fail = 0 ] || { echo "--- server log"; tail -30 "$root/server.log"; exit 1; }
