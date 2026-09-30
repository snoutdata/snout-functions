#!/usr/bin/env bash
# Does `deno bundle` turn the functions customers actually write into one file our runtime can
# run? Each case is bundled with the pinned Deno image, then served by snout-functions.
#
#   bash functions/tests/bundle-probe.sh <snout-functions binary>      (on a build host, as root)
set -uo pipefail
bin="${1:?usage: bundle-probe.sh <binary>}"
deno_image="${DENO_IMAGE:-docker.io/denoland/deno:2.9.7}" # pins-allow: the Deno release deno_runtime 0.267 is built from, with a shell; not the builder pin
root="$(mktemp -d)"
ref=bundleprobe01
functions=()

case_() { # name, source
	local digest; digest=$(printf '%s' "$1" | sha256sum | cut -c1-64)
	local dir="$root/bundles/$digest"
	mkdir -p "$dir/src"
	printf '%s\n' "$2" > "$dir/src/index.ts"
	printf '%s\n' 'import "./src/index.ts";' > "$dir/index.ts"
	local started; started=$(date +%s%3N)
	if podman run --rm --network host -v "$dir:/work:Z" -w /work -e DENO_DIR=/tmp/deno "$deno_image" \
		bundle --quiet --platform deno --format esm -o /work/.built/main.js /work/index.ts > "$root/$1.bundle.log" 2>&1; then
		echo "bundled $1 in $(( $(date +%s%3N) - started )) ms, $(wc -c < "$dir/.built/main.js") bytes"
	else
		echo "BUNDLE FAILED $1: $(tail -3 "$root/$1.bundle.log" | tr '\n' ' ')"
	fi
	functions+=("{ \"name\": \"$1\", \"digest\": \"$digest\" }")
}

case_ zod 'import { z } from "npm:zod@3.23.8";
Deno.serve(() => new Response(JSON.stringify(z.object({ a: z.number() }).parse({ a: 1 }))));'
case_ jsr 'import { encodeHex } from "jsr:@std/encoding@1/hex";
Deno.serve(() => new Response(encodeHex(new TextEncoder().encode("ok"))));'
case_ esmsh 'import { nanoid } from "https://esm.sh/nanoid@5.0.7";
Deno.serve(() => new Response(nanoid().length.toString()));'
case_ stdserve 'import { serve } from "https://deno.land/std@0.168.0/http/server.ts";
serve(() => new Response("std serve"));'
case_ client 'import { createClient } from "npm:@snoutdata/client@0.1";
const client = createClient("http://127.0.0.1:1", "key");
Deno.serve(() => new Response(typeof client.from));'
case_ nodebuiltins 'import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
Deno.serve(() => new Response(Buffer.from("hi").toString("base64") + " " + createHash("sha256").update("x").digest("hex").slice(0, 8) + " " + typeof EventEmitter));'
case_ stripe 'import Stripe from "npm:stripe@17";
const stripe = new Stripe("sk_test_x", { httpClient: Stripe.createFetchHttpClient() });
Deno.serve(() => new Response(typeof stripe.customers.create));'
case_ processenv 'import process from "node:process";
Deno.serve(() => new Response(String(process.env.WHO)));'

mkdir -p "$root/projects"
( IFS=,; printf '{ "functions": [%s], "limits": { "memoryMb": 256, "wallMs": 20000, "cpuMs": 4000 }, "env": { "WHO": "env-ok" } }\n' "${functions[*]}" ) > "$root/projects/$ref.json"

SNOUT_FUNCTIONS_DEBUG=1 "$bin" start --port 19100 --root "$root" --sockets "$root/sockets" > "$root/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null; rm -rf "$root"' EXIT
for _ in $(seq 1 100); do curl -sf http://127.0.0.1:19100/_snoutpod/health >/dev/null && break; sleep 0.05; done
for name in zod jsr esmsh stdserve client nodebuiltins stripe processenv; do
	printf '%-13s %s\n' "$name" "$(curl -s -m 30 -w ' [%{http_code} %{time_total}s]' -H "x-snoutdata-ref: $ref" "http://127.0.0.1:19100/$name" | cut -c1-220)"
done
echo "--- server log"; grep -v "in flight" "$root/server.log" | tail -30
