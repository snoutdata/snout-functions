# snout-functions' image: one binary (V8 and the runtime linked in) and the four libraries it loads.
#
#   podman build -t snout-functions .                             # in this repository
#   podman build -f functions/Containerfile -t snout-functions .  # from the stack's workspace
#
# Build it natively on the architecture it is for: V8 under emulation takes hours. The build stage
# wants clang, cmake and protoc for the Deno crates' build scripts.
#
# Root inside the container, on purpose: the host agent writes each project's manifest 0600 as
# the user rootless podman maps to the container's root, and the front process must read them;
# and it is root that may shut each project's process into its own directory as a user of its own
# (src/confine.rs: chroot, setuid, setgid, which a container's default capabilities allow). On a
# host that root is the unprivileged `snoutpod` user. Customer code never runs as it.
FROM docker.io/library/rust:1.98.1-bookworm AS build
RUN apt-get update -q \
	&& apt-get install -y -q --no-install-recommends clang libclang-dev cmake protobuf-compiler pkg-config \
	&& rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN if [ -d functions ]; then cd functions; fi \
	&& cargo build --locked --release && cp target/release/snout-functions /snout-functions

# The whole filesystem the runtime needs: the binary, the four libraries it loads (libc, libm,
# libgcc_s and the loader; V8 and everything else is linked in), and a /tmp for the workers'
# sockets. Nothing else: no shell, no package manager, no CA bundle (Deno's fetch carries its own
# roots), no timezone files (ICU carries its own). The libraries come from the same Debian release
# the build stage links against. An image can also be built from this stage on, over a binary
# built and tested elsewhere (`COPY snout-functions` in place of the stage copy below).
FROM docker.io/library/debian:bookworm-slim AS rootfs
COPY --from=build /snout-functions /rootfs/snout-functions
RUN ldd /rootfs/snout-functions | grep -o '/[^ ]*' | xargs -I{} cp --parents -L {} /rootfs \
	&& mkdir -m 1777 /rootfs/tmp

FROM scratch
# glibc gives each thread that allocates its own arena, and every worker is a thread: two arenas
# for the process took 1.5 MB off each warm worker (tests/memory-probe.sh).
ENV MALLOC_ARENA_MAX=2
# And allocations of 1 MB or more come from the system and go back to it when freed, rather than
# glibc raising that threshold as it sees them freed and then keeping what a stopped worker held
# (tests/memory-guard-probe.sh: without it, a process stayed at 99% of its cap after the worker
# holding the memory was gone).
ENV MALLOC_MMAP_THRESHOLD_=1048576
COPY --from=rootfs /rootfs /
EXPOSE 9000
# How SnoutData Studio's "Find databases" knows this container is part of the SnoutData stack:
# by label, never by guessing from the image name. Only the
# `postgres` component is offered as a database; the rest are recognised and left out.
LABEL com.snoutdata.stack="1" com.snoutdata.component="functions"
ENTRYPOINT ["/snout-functions"]
CMD ["start", "--port", "9000"]
