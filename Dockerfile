# The deployment artifact: the Knowledge binary, built for the machine it runs on.
#
# `ratatoskr-workspace/docs/DEPLOYMENT_TARGET.md` fixes the target as one Raspberry Pi 5: `aarch64`,
# Debian 12, glibc 2.36. A binary linked against a newer glibc does not start against an older one,
# so the builder is Debian 12 (`bookworm`) as well, and the runtime stage is the same release.
#
# Three stages, in this order: `builder`, `export` and `runtime`. A plain `docker build` builds the
# LAST stage, so `runtime` stays last; CI's arm64 smoke test runs the binary in it. `export` holds
# only the binary that `deploy/systemd/ratatoskr-knowledge.service` starts, and is what
# `ratatoskr-workspace`'s reusable-release-artifact workflow publishes for each commit on main:
#
#     docker buildx build --platform linux/arm64 --target export --output type=local,dest=out .
#
# Both base images are pinned by digest as well as by tag, so a re-pushed tag cannot change the
# artifact without a commit here.

# ---------------------------------------------------------------------------------------------
# builder
# ---------------------------------------------------------------------------------------------
# The same toolchain as `rust-toolchain.toml`, so cargo does not download a second one.
FROM rust:1.97.0-slim-bookworm@sha256:6d220bf85c74e842a79da63997af8d2e74455c0b8847d8bb3a5888572334991d AS builder

# `git` because the contracts crates are git dependencies pinned to a SHA; a C compiler is already in
# the image for `ring`.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates git pkg-config \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# The commit this artifact was built from. The binary does not read it yet; the argument keeps the
# build command the same as in the other repositories.
ARG RATATOSKR_GIT_SHA=unknown
ENV RATATOSKR_GIT_SHA=${RATATOSKR_GIT_SHA}

COPY . .

# `--locked`, as the gate builds. Only the deployed binary: the release-artifact check refuses a
# binary that no unit starts.
#
# The registry, the git checkouts and `target/` are BuildKit cache mounts, so a rebuild compiles only
# what changed. A cache mount is not part of the layer, so the binary is copied to `/out` in the same
# step.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked -p ratatoskr-knowledge-service --bin ratatoskr-knowledge-service \
 && mkdir /out \
 && cp target/release/ratatoskr-knowledge-service /out/

# ---------------------------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------------------------
# Only the binary: the local exporter writes this stage's whole filesystem, and all of it is published.
FROM scratch AS export
COPY --from=builder /out/ /

# ---------------------------------------------------------------------------------------------
# runtime
# ---------------------------------------------------------------------------------------------
FROM debian:12-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS runtime

# The trust store that the provider HTTPS calls verify against.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --no-create-home --shell /usr/sbin/nologin ratatoskr

COPY --from=builder /out/ratatoskr-knowledge-service /usr/local/bin/

USER ratatoskr
ENTRYPOINT ["/usr/local/bin/ratatoskr-knowledge-service"]
