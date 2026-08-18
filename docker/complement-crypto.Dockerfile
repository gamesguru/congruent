# syntax = docker/dockerfile:1

# Complement-Crypto runner image.
#
# Complement-Crypto is the Matrix E2EE end-to-end test suite (from
# matrix-org). It runs `go test` against a *homeserver* image, spawning that
# homeserver (and a mitmproxy sidecar) in docker containers as the tests run.
#
# This image is the *tester*: it carries the complement-crypto source, a
# built matrix-js-sdk Chrome bundle, the Go toolchain needed to run the suite,
# and a system Chromium binary (chromedp drives a real browser for the JS SDK
# client).
#
# The *homeserver under test* is NOT built here. It is provided at runtime via
# the COMPLEMENT_BASE_IMAGE environment variable, which CI points at the
# already-existing continuwuity complement image (see `make complement/docker`).
#
# Only the JS SDK flavour is built (client matrix `jj`, `-tags=jssdk`).
# To also build the Rust SDK FFI flavour, this image would additionally need a
# matrix-rust-sdk checkout and uniffi-bindgen-go.

# Build base: Go toolchain + Node (corepack provides yarn) + git to install
# and build matrix-js-sdk into the Chrome bundle directory.
FROM golang:1.26-bookworm AS builder

ENV NODE_MAJOR=22
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg git \
    && curl -fsSL https://deb.nodesource.com/setup_${NODE_MAJOR}.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && corepack enable \
    && node --version && yarn --version \
    && rm -rf /var/lib/apt/lists/*

# Version of matrix-js-sdk to test against: a branch, commit or tag, fed
# straight into `yarn add matrix-js-sdk@<source>`. Defaults to the JS SDK
# "develop" branch (the same default upstream CI uses). Override at build time
# with:  --build-arg MATRIX_JS_SDK_SOURCE=<branch|commit|tag>
ARG MATRIX_JS_SDK_SOURCE=develop

WORKDIR /usr/src/complement-crypto
COPY complement-crypto-src/ .

# The source is a git submodule; its `.git` file points at a gitdir
# (`../.git/modules/complement-crypto-src`) that does not exist inside the
# build context. `yarn add` shells out to git to resolve the matrix-js-sdk
# URL, and that stale pointer breaks git repo discovery. Strip it so the
# copied directory is treated as a plain tree.
RUN rm -rf .git

# Build the JS SDK and place the bundle where the `//go:embed dist` directive
# in internal/api/js/chrome expects it.
RUN set -eux; \
    corepack enable; \
    ./rebuild_js_sdk.sh "matrix-js-sdk@https://github.com/matrix-org/matrix-js-sdk#${MATRIX_JS_SDK_SOURCE}"

# Fetch Go module deps so they are cached in the image (network at build time).
RUN set -eux; \
    go mod download

# === Runtime image ===
FROM golang:1.26-bookworm AS runtime
LABEL org.opencontainers.image.description="Complement-Crypto tester for continuwuity (JS SDK flavour)"

# System Chromium for chromedp, docker CLI to reach the host docker daemon
# (bind-mounted at runtime) so `go test` can spawn the homeserver/mitmproxy
# containers, and helpers for result normalisation.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        docker.io \
        chromium \
        xvfb \
        fonts-liberation \
        libnss3 libnspr4 libasound2 libatk1.0-0 libatk-bridge2.0-0 \
        libcups2 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 \
        libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 \
        libatspi2.0-0 libxshmfence1 \
        jq \
    && rm -rf /var/lib/apt/lists/*

# Bring in the built source (with embedded JS bundle + go modules).
WORKDIR /usr/src/complement-crypto
COPY --from=builder /usr/src/complement-crypto .

# Entrypoint: runs the suite and normalises output into jsonl.
COPY complement/complement-crypto-entrypoint.sh /usr/local/bin/complement-crypto-entrypoint.sh
RUN chmod a+x /usr/local/bin/complement-crypto-entrypoint.sh

# chromedp opts-in to a system chrome when CHROME_BIN is set (fallback to the
# chromium package binary otherwise; chromium's ELF triggers no sandbox issues
# under the default Docker seccomp profile when run with --no-sandbox).
ENV COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX=jj
ENV COMPLEMENT_ENABLE_DIRTY_RUNS=1
ENV TESTCONTAINERS_RYUK_DISABLED=true
ENV COMPLEMENT_HOSTNAME_RUNNING_COMPLEMENT=host.docker.internal
ENV COMPLEMENT_HOST_MOUNTS=/var/run/docker.sock:/var/run/docker.sock

ENTRYPOINT ["/usr/local/bin/complement-crypto-entrypoint.sh"]
