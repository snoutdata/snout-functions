# snout-functions

The runtime for Snout Functions: TypeScript and JavaScript on Deno's web platform, with Node
compatibility for npm packages, answering HTTP. One process serves every project's functions on a
host; each function runs in V8 isolates of its own, on threads of their own, with the memory, CPU
and wall-clock limits its project's plan sets. Written in Rust on `deno_runtime` and `deno_core`
(MIT), built to run every project on a SnoutData Cloud host.

> **Status: in production.** Every SnoutData Cloud project's functions, and the control plane's
> own, run on it since 2026-09-30 (image 0.1.0: 94.6 MB, 39 MB compressed).

- **Isolated by permissions, not by trust.** A worker may read its own bundle and read and write
  its own socket directory, and reach the network; nothing else. No environment of the process, no
  subprocess, no FFI, no other file, and never this container's own loopback, where the runtime's
  port answers. `Deno.env` is the project's variables and only those.
- **Proves where a request came from.** Every request must carry the front door's secret
  (`x-snoutdata-door`), compared in constant time before the project is read and removed before a
  worker sees the request. A function cannot call another project's functions by naming it.
- **Limits that say which one was hit.** Memory is stopped at the allocation that crosses it
  (ArrayBuffers are counted by the runtime's own allocator) or at V8's heap limit; CPU is the
  longest stretch a request holds its thread without yielding; the wall clock belongs to the
  request, all of it. The caller is told `it reached its memory limit`, `CPU limit` or
  `wall clock limit`, not a generic cancellation.
- **Many requests, few workers.** One worker serves any number of requests that wait on I/O (100
  held for 20 s at once: all 100 answered, 2.5 MB more memory). A function gets another worker
  only while every worker it has is busy on CPU, up to one a core and its own concurrency.
- **Fast to start.** Isolates are booted ahead of need from Deno's snapshot, and a spare left
  unclaimed for 100 ms serves one request to itself, so the code a first request runs is already
  compiled: a cold start that finds a spare waiting is about 3 ms. Bursts grow the pool, which
  shrinks back after 10 s.
- **Small.** About 10 MB idle, about 7 MB for each warm worker before the function's own objects.
  On a 2-CPU arm64 machine, one function answers 2,100 requests a second with 10 in flight (p99
  15 ms).
- **Imports resolved before a request.** A bundle's `npm:`, `jsr:` and URL imports are resolved
  into one file when the host prepares it, never on a caller's request; TypeScript is stripped of
  its types when a module loads (no type check).
- **Weighed, not only counted.** Every project on the host shares one container and its memory cap, and a container over its cap is killed whole. Past 85% of the cap a new worker starts only by stopping an idle one (40 functions under an 80 MB cap, each called twice: all 80 answered, peak 79 MB; uncapped, 392 MB).
- **Replies compressed where the client asks** (gzip, brotli), by the same rules for every
  function.
- **Stops gracefully.** SIGTERM stops accepting and lets requests in flight finish.

## Running it

```sh
podman build -f functions/Containerfile -t snout-functions .   # from packages/stack
podman run -v /srv/snoutfn:/snoutfn:ro -p 9000:9000 -e SNOUT_FUNCTIONS_DOOR_SECRET=... snout-functions
```

```
snout-functions start [--port 9000] [--root /snoutfn] [--sockets <dir>] [--main-service <ignored>]
```

| Flag | Default | What |
|---|---|---|
| `--port` | `9000` | The one HTTP port: functions and the health path |
| `--root` | `/snoutfn` | The functions mount (below); read only |
| `--sockets` | `$TMPDIR/snout-functions` | Where each worker's Unix socket lives; emptied at start |
| `--main-service` | | Accepted and ignored, for command lines written for the runtime this replaces |

| Variable | Default | Secret | What |
|---|---|---|---|
| `SNOUT_FUNCTIONS_DOOR_SECRET` | none | yes | The front door's secret. Unset, no request is asked for one (logged as a warning at start) |
| `SNOUT_FUNCTIONS_IDLE_MS` | `60000` | no | How long a worker with nothing to do is kept |
| `SNOUT_FUNCTIONS_MAX_WORKERS` | `256` | no | Workers on the host at once; past it, `503` with `Retry-After: 1` |
| `SNOUT_FUNCTIONS_MEMORY_MB` | the container's cap | no | The memory every worker together may use, counted against this process. Unset, the container's own cgroup cap is read; past 85% of it a new worker first stops an idle one, and with none idle the request waits on its function's busy worker or gets `503` |
| `SNOUT_FUNCTIONS_MAX_REPLICAS` | the cores | no | The most workers one function gets when each is busy on CPU (a function's own concurrency may lower it) |
| `SNOUT_FUNCTIONS_SPARES` | `1` | no | Isolates kept booted per memory limit in use; `0` boots one per cold request |
| `SNOUT_FUNCTIONS_REHEARSE` | on | no | `0` stops spares serving a request to themselves before they are claimed |
| `SNOUT_FUNCTIONS_DRAIN_MS` | `25000` | no | How long SIGTERM waits for requests in flight |
| `SNOUT_FUNCTIONS_V8_FLAGS` | none | no | Space-separated V8 flags, after the runtime's own (`--minor-ms --optimize-for-size`), which they override |
| `SNOUT_FUNCTIONS_DEBUG` | off | no | Set to anything: a line on stderr per worker start, claim (with its timings), stop and limit decision |
| `MALLOC_ARENA_MAX` | `2` in the image | no | glibc's arenas; more costs about 1.5 MB a warm worker |

## What it reads

The functions mount is written by the host agent and never by this process:

- `projects/<ref>.json`, one per project, replaced whole (by rename):
  `{ "functions": [{ "name", "digest", "memoryMb"?, "concurrency"? }], "limits": { "memoryMb", "wallMs", "cpuMs", "hardCpuMs"? }, "env": { ... } }`.
  Read again whenever its modification time changes, so a changed secret or limit reaches the next
  request. A function's `memoryMb` replaces the project's for its workers; `concurrency` caps its
  workers.
- `bundles/<digest>/`: the function's files under `src/`, a one-line `index.ts` importing its
  entrypoint there, and `.built/main.js`
  when the host has resolved its imports (`.built/error.txt` when it could not, which the caller
  is then told).

## What a caller sees

A request is `/<name>/<anything>` with the project in `x-snoutdata-ref`, both written by the
front door. The function gets `http://<host>/<name>/<anything>` as its `req.url`. What the runtime
answers itself is JSON, `{ "message", "hint" }`:

| Status | When |
|---|---|
| `403` | No front-door secret, or the wrong one |
| `400` | No project named, or not a valid one |
| `404` | No function of that name in the project |
| `500` | The function could not load, threw, returned something that is not a `Response`, or hit a limit (the hint names which) |
| `503` | The host is running as many workers as it may (`Retry-After: 1`) |

## Operating it

- **Health.** `GET /_snoutpod/health` answers `ok` once the port listens; it needs no secret.
- **Metrics.** None of its own. Invocations are counted at the front door.
- **Logs.** Standard error: a function's own `console` output, a line per worker that could not
  start or stopped on a limit, and with `SNOUT_FUNCTIONS_DEBUG` every worker's life with timings.
- **Stopping.** SIGTERM: no new connection is accepted, requests in flight get
  `SNOUT_FUNCTIONS_DRAIN_MS` to finish, then it exits.
- **Upgrading.** Replace the image. Nothing is stored: workers are rebuilt from the mount on
  demand, and a worker is keyed by bundle digest, variables and limits, so nothing stale is reused.
- **Rotating the door secret.** Give the front doors the new secret first, then this process:
  a runtime with a secret refuses every request that lacks it.

## Development

Built and tested on an arm64 Linux machine (V8 is linked in; building it under emulation takes
hours). `tests/smoke.sh <binary>` drives a running binary with curl through every behaviour
above; `tests/cold-probe.sh`, `memory-probe.sh`, `throughput-probe.sh` and `cpu-probe.sh` measure
cold start, memory per worker, warm throughput and CPU-bound speed, one line per variant of the
environment.

## Licence

[Apache License 2.0](./LICENSE). Security reports: [SECURITY.md](./SECURITY.md).
