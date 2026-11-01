# One Jepsen DB node. `aspect jepsen` starts five of these, n1 to n5, on one
# Docker network, and Jepsen drives them with `docker exec` and `docker cp`.
#
# The image holds only what Jepsen and krabka need on a node:
#
# - `iptables` for network partitions and `iproute2` (`tc`) for packet faults;
# - `procps` and `psmisc` for Jepsen's pause, resume and kill of the broker;
# - `start-stop-daemon` (from `dpkg`, already in the base) for the daemon;
# - `libfaketime`, which gives each broker a wall clock of its own for clock
#   faults: containers share the host's clock, so it cannot be stepped;
# - `sudo`, which Jepsen wraps every privileged command in;
# - `tar` and `bzip2` to archive data directories into the test store.
#
# The broker binaries are not in the image: each test uploads the ones it
# runs, so one image serves every build. Trixie's glibc is new enough for a
# broker built on any current distribution.
FROM debian:trixie-slim@sha256:f6b08b09a05725f9431502d3b5714f5c4576e82c1a84fa1debe0c24a7e0c3ec4

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bash bzip2 ca-certificates iproute2 iptables libfaketime procps psmisc sudo tar \
 && rm -rf /var/lib/apt/lists/*

CMD ["sleep", "infinity"]
