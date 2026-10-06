# List available commands
_help:
    @just --list

PREFIX := env_var_or_default("PREFIX", "/usr/local")

# --- Pre-building C/C++ Libraries ---
# Note: Building these from source avoids Cargo constantly recompiling them
# and trashing your target/ directory. After building, use the install commands
# to make them available to Cargo, RustRover, and your system.

# Central metadata for C/C++ dependencies
CSV := ".github/ellis_link_deps.csv"

# Initialize the global build directory
init-prebuild:
    @echo "Creating {{ PREFIX }}/build and assigning ownership to $USER... (Requires sudo)"
    sudo mkdir -p {{ PREFIX }}/build {{ PREFIX }}/lib {{ PREFIX }}/include {{ PREFIX }}/bin
    sudo chown -R $USER:$USER {{ PREFIX }}/build {{ PREFIX }}/lib {{ PREFIX }}/include {{ PREFIX }}/bin
    @echo "Done. You can now run prebuild commands."

# Pre-build all C/C++ dependencies
prebuild-all: init-prebuild prebuild-jemalloc prebuild-lz4 prebuild-snappy prebuild-zstd prebuild-rocksdb prebuild-aws-lc

# Install all pre-built C/C++ dependencies
install-all: install-jemalloc install-lz4 install-snappy install-zstd install-rocksdb install-aws-lc

# Builds liburing
prebuild-liburing:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^liburing," {{ CSV }} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^liburing," {{ CSV }} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{ PREFIX }}/build && sudo chown -R $USER:$USER {{ PREFIX }}/build
    echo "Cloning and building liburing $TAG..."
    [ ! -d "{{ PREFIX }}/build/liburing" ] && git clone $REPO {{ PREFIX }}/build/liburing || true
    cd {{ PREFIX }}/build/liburing
    git fetch --all --tags
    git checkout $TAG
    ./configure --prefix={{ PREFIX }}
    make -j$(nproc)

# Installs liburing
install-liburing:
    @echo "Installing liburing (requires sudo)..."
    cd {{ PREFIX }}/build/liburing && sudo make install
    @echo "Done! You might need to run 'sudo ldconfig' to update library cache."

# Builds bzip2
prebuild-bzip2:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^bzip2," {{ CSV }} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^bzip2," {{ CSV }} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{ PREFIX }}/build && sudo chown -R $USER:$USER {{ PREFIX }}/build
    echo "Cloning and building bzip2 $TAG..."
    [ ! -d "{{ PREFIX }}/build/bzip2" ] && git clone $REPO {{ PREFIX }}/build/bzip2 || true
    cd {{ PREFIX }}/build/bzip2
    git fetch --all --tags
    git checkout $TAG
    make -f Makefile-libbz2_so
    make

# Installs bzip2
install-bzip2:
    @echo "Installing bzip2 (requires sudo)..."
    cd {{ PREFIX }}/build/bzip2 && sudo make install PREFIX={{ PREFIX }}
    cd {{ PREFIX }}/build/bzip2 && sudo cp -f libbz2.so.1.0.* {{ PREFIX }}/lib/
    cd {{ PREFIX }}/build/bzip2 && sudo ln -sf {{ PREFIX }}/lib/libbz2.so.1.0.* {{ PREFIX }}/lib/libbz2.so
    sudo ldconfig
    @echo "Done! Installed libbz2.so to {{ PREFIX }}/lib"

# Pre-build jemalloc
prebuild-jemalloc:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^jemalloc," {{ CSV }} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^jemalloc," {{ CSV }} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{ PREFIX }}/build && sudo chown -R $USER:$USER {{ PREFIX }}/build
    echo "Cloning jemalloc $TAG..."
    [ ! -d "{{ PREFIX }}/build/jemalloc" ] && git clone $REPO {{ PREFIX }}/build/jemalloc || true
    echo "Building jemalloc..."
    cd {{ PREFIX }}/build/jemalloc
    git fetch --all --tags
    git checkout $TAG
    [ -f configure ] || ./autogen.sh
    [ -f Makefile ] || ./configure --prefix={{ PREFIX }}
    make -j$(nproc)

# Install jemalloc globally (requires sudo)
install-jemalloc:
    @echo "Installing jemalloc to {{ PREFIX }}... (Requires sudo)"
    cd {{ PREFIX }}/build/jemalloc && sudo make install_lib_static install_lib_shared install_include
    sudo ldconfig

# Pre-build lz4
prebuild-lz4:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^lz4," {{ CSV }} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^lz4," {{ CSV }} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{ PREFIX }}/build && sudo chown -R $USER:$USER {{ PREFIX }}/build
    echo "Cloning lz4 $TAG..."
    [ ! -d "{{ PREFIX }}/build/lz4" ] && git clone $REPO {{ PREFIX }}/build/lz4 || true
    echo "Building lz4..."
    cd {{ PREFIX }}/build/lz4
    git fetch --all --tags
    git checkout $TAG
    make lib -j$(nproc)

# Install lz4 globally (requires sudo)
install-lz4:
    @echo "Installing lz4 to {{ PREFIX }}... (Requires sudo)"
    cd {{ PREFIX }}/build/lz4 && sudo make install PREFIX={{ PREFIX }}
    sudo ldconfig

# Pre-build RocksDB shared and statically
prebuild-rocksdb:
    #!/usr/bin/env bash
    set -e
    # satisfy build_detect_platform if hostname is missing
    if ! command -v hostname >/dev/null 2>&1; then
        hostname() { uname -n; }
        export -f hostname
    fi
    TAG=$(grep "^rocksdb," {{ CSV }} | cut -d',' -f4 | tr -d '\r' || true)
    if [ -z "$TAG" ]; then
        TAG="v10.5.1"
    fi
    REPO=$(grep "^rocksdb," {{ CSV }} | cut -d',' -f3 | tr -d '\r' || true)
    if [ -z "$REPO" ]; then
        REPO="https://github.com/facebook/rocksdb.git"
    fi
    sudo mkdir -p {{ PREFIX }}/build && sudo chown -R $USER:$USER {{ PREFIX }}/build
    echo "Cloning rocksdb $TAG..."
    if [ ! -d "{{ PREFIX }}/build/rocksdb" ]; then
        git clone --recursive "$REPO" {{ PREFIX }}/build/rocksdb
    else
        (cd {{ PREFIX }}/build/rocksdb && git remote set-url origin "$REPO")
    fi
    echo "Building RocksDB..."
    cd {{ PREFIX }}/build/rocksdb

    # Use --all --tags to support arbitrary commit hashes from the CSV
    git fetch --all --tags
    git reset --hard "$TAG"

    # Disable ccache auto-detection ONLY if we are already using sccache
    if [[ "$CC" == *"sccache"* ]]; then
        export USE_CCACHE=0
    fi

    # Clean build directory to avoid issues with stale dependency files
    # make clean

    # Build core libraries explicitly WITHOUT RTTI
    env ROCKSDB_NO_FBCODE=1 ROCKSDB_DISABLE_BENCHMARK=1 DISABLE_JEMALLOC=1 EXTRA_CXXFLAGS="${EXTRA_CXXFLAGS:-} -I{{ PREFIX }}/include -Wno-error=unused-parameter -Wno-error=maybe-uninitialized" EXTRA_LDFLAGS="-L{{ PREFIX }}/lib" PORTABLE=0 USE_RTTI=1 make shared_lib static_lib -j$(nproc)

    # Build ldb (statically linked to avoid shared library RTTI/ABI mismatches)
    env DISABLE_WARNING_AS_ERROR=1 DEBUG_LEVEL=0 USE_RTTI=1 make ldb
    g++ -o ldb_static tools/ldb.o tools/ldb_cmd.o tools/ldb_tool.o tools/sst_dump_tool.o utilities/blob_db/blob_dump_tool.o librocksdb.a -lpthread -lrt -ldl -lsnappy -lz -lbz2 -llz4 -lzstd -luring -ljemalloc -lstdc++ -lm
    mv ldb_static ldb

# Install RocksDB globally (requires sudo)
install-rocksdb:
    @echo "Installing RocksDB to {{ PREFIX }}... (Requires sudo)"
    cd {{ PREFIX }}/build/rocksdb && sudo make install-shared PREFIX={{ PREFIX }}
    cd {{ PREFIX }}/build/rocksdb && sudo make install-static PREFIX={{ PREFIX }}
    sudo install -m 755 {{ PREFIX }}/build/rocksdb/ldb {{ PREFIX }}/bin/ldb
    sudo ldconfig
    @echo "Remember to set ROCKSDB_LIB_DIR={{ PREFIX }}/lib if Cargo doesn't see it."

# Pre-build snappy
prebuild-snappy:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^snappy," {{ CSV }} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^snappy," {{ CSV }} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{ PREFIX }}/build && sudo chown -R $USER:$USER {{ PREFIX }}/build
    echo "Cloning snappy $TAG..."
    if [ ! -d "{{ PREFIX }}/build/snappy" ]; then
        git clone $REPO {{ PREFIX }}/build/snappy
    fi
    echo "Building snappy..."
    cd {{ PREFIX }}/build/snappy
    git fetch origin
    git checkout $TAG
    sed -i 's/cmake_minimum_required(VERSION 3.1)/cmake_minimum_required(VERSION 3.10)/' CMakeLists.txt
    # Use sccache compiler launcher if available
    if command -v sccache >/dev/null 2>&1; then
        export CMAKE_C_COMPILER_LAUNCHER=sccache
        export CMAKE_CXX_COMPILER_LAUNCHER=sccache
        # Use explicit base compilers to avoid double-wrapping with sccache
        export CC=cc
        export CXX=c++
    fi

    mkdir -p build_static && cd build_static
    # rm -f CMakeCache.txt
    cmake -DCMAKE_INSTALL_PREFIX={{ PREFIX }} -DBUILD_SHARED_LIBS=OFF -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF ..
    make -j$(nproc)
    cd ..
    mkdir -p build_shared && cd build_shared
    # rm -f CMakeCache.txt
    cmake -DCMAKE_INSTALL_PREFIX={{ PREFIX }} -DBUILD_SHARED_LIBS=ON -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF ..
    make -j$(nproc)

# Install snappy globally (requires sudo)
install-snappy:
    @echo "Installing snappy to {{ PREFIX }}... (Requires sudo)"
    cd {{ PREFIX }}/build/snappy/build_static && sudo make install
    cd {{ PREFIX }}/build/snappy/build_shared && sudo make install
    sudo ldconfig

# Pre-build zstd
prebuild-zstd:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^zstd," {{ CSV }} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^zstd," {{ CSV }} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{ PREFIX }}/build && sudo chown -R $USER:$USER {{ PREFIX }}/build
    echo "Cloning zstd $TAG..."
    [ ! -d "{{ PREFIX }}/build/zstd" ] && git clone $REPO {{ PREFIX }}/build/zstd || true
    echo "Building zstd..."
    cd {{ PREFIX }}/build/zstd
    git fetch --all --tags
    git checkout $TAG
    make lib-release -j$(nproc)

# Install zstd globally (requires sudo)
install-zstd:
    @echo "Installing zstd to {{ PREFIX }}... (Requires sudo)"
    cd {{ PREFIX }}/build/zstd && sudo make install -C lib PREFIX={{ PREFIX }}
    sudo ldconfig

# Pre-build aws-lc
prebuild-aws-lc:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^aws-lc," {{ CSV }} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^aws-lc," {{ CSV }} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{ PREFIX }}/build && sudo chown -R $USER:$USER {{ PREFIX }}/build
    echo "Cloning aws-lc $TAG..."
    [ ! -d "{{ PREFIX }}/build/aws-lc" ] && git clone $REPO {{ PREFIX }}/build/aws-lc || true
    echo "Building aws-lc..."
    cd {{ PREFIX }}/build/aws-lc
    git fetch --all --tags
    git checkout $TAG
    # aws-lc (boringssl) has issues with sccache wrapping during CMake checks
    export NO_SCCACHE=1
    unset CC
    unset CXX
    unset CMAKE_C_COMPILER_LAUNCHER
    unset CMAKE_CXX_COMPILER_LAUNCHER

    mkdir -p build && cd build
    # rm -f CMakeCache.txt
    cmake -DCMAKE_INSTALL_PREFIX={{ PREFIX }} -DBUILD_TESTING=OFF -DBUILD_LIBSSL=ON -DGENERATE_RUST_BINDINGS=ON ..
    make -j$(nproc)

# Install aws-lc globally (requires sudo)
install-aws-lc:
    @echo "Installing aws-lc to {{ PREFIX }}... (Requires sudo)"
    cd {{ PREFIX }}/build/aws-lc/build && sudo make install
    sudo ldconfig

# --- CPU Profiling ---

# Run CPU flamegraph profiling on release build (requires sudo for perf)
profile-runtime-cpu *args:
    cargo flamegraph --root --features local_profiling --bin conduwuit -- {{ args }}
    @echo "Flamegraph saved to flamegraph.svg"

# Run CPU flamegraph profiling on dev build (requires sudo for perf)
profile-runtime-cpu-dev *args:
    cargo flamegraph --root --dev --features local_profiling --bin conduwuit -- {{ args }}
    @echo "Flamegraph saved to flamegraph.svg"

# --- Async & I/O Profiling ---

# Run with tokio-console instrumentation active
profile-runtime-async *args:
    @echo "Run 'tokio-console' in a separate terminal"
    env RUSTFLAGS="--cfg tokio_unstable ${RUSTFLAGS:-}" cargo run --features local_profiling --bin conduwuit -- {{ args }}

# --- Memory Profiling (jemalloc) ---

# Run release build and dump jemalloc heap profiles
profile-runtime-mem *args:
    cargo build --release --features local_profiling --bin conduwuit
    @echo "Starting with jemalloc profiling..."
    env MALLOC_CONF="prof:true,lg_prof_interval:24,prof_prefix:jeprof.out" ./target/release/conduwuit {{ args }}

# Generate heap_profile.svg from collected jemalloc dumps
profile-runtime-mem-analyze:
    jeprof --svg ./target/release/conduwuit jeprof.out.*
    @echo "Saved heap_profile.svg"

# Clean up jemalloc dump files
profile-runtime-mem-clean:
    rm -f jeprof.out.* heap_profile.svg

# --- Compile-time Profiling ---

# Profile cargo build times
profile-build-times:
    cargo build --profile ${PROFILE:-release} --timings
    @echo "Report saved to target/cargo-timings/"

# Analyze binary size by crates
profile-build-bloat-crates:
    cargo bloat --profile ${PROFILE:-release} -p conduwuit --crates

# Analyze binary size by functions
profile-build-bloat-functions:
    cargo bloat --profile ${PROFILE:-release} -p conduwuit --bin conduwuit -n 50

# Analyze generic instantiation (Monomorphization)
profile-build-llvm-lines:
    cargo llvm-lines --profile ${PROFILE:-release} -p conduwuit --lib

# --- Build targets ---

# Build dev (default,console,url_preview)
build-dev:
    cargo build --profile dev --features default,console,url_preview

# --- Cross Compilation ---

# Cross-compile using cargo-zigbuild for specific glibc versions
# Usage: just build-cross-compile <target-glibc-version> <cpu-arch>
# Example: just build-cross-compile 2.36 skylake
build-cross-compile glibc_version="2.36" cpu_arch="skylake":
    @echo "Building for glibc {{ glibc_version }} with CPU target {{ cpu_arch }} using cargo-zigbuild..."
    @if ! command -v cargo-zigbuild >/dev/null 2>&1; then \
        echo "Error: cargo-zigbuild is not installed. Run: cargo install cargo-zigbuild"; \
        exit 1; \
    fi
    @if ! command -v zig >/dev/null 2>&1; then \
        echo "Error: zig is not installed. Run: sudo pacman -S zig (or your package manager's equivalent)"; \
        exit 1; \
    fi
    rustup target add x86_64-unknown-linux-gnu
    env RUSTFLAGS="-C target-cpu={{ cpu_arch }}" cargo zigbuild --release --target x86_64-unknown-linux-gnu.{{ glibc_version }}

# Extracts the workspace version from Cargo.toml
version := "$(grep -m1 '^version = ' Cargo.toml | cut -d \" -f 2)"

# Start gdbserver for lightweight remote debugging (POC)
# Usage: just remote-debug-poc /path/to/conduwuit.toml
remote-debug-poc config="conduwuit-example.toml":
    @echo "Starting gdbserver on :1234 using config: {{ config }}"
    sudo -u conduwuit gdbserver :1234 ./target/debug/continuwuity --config {{ config }}

# Run Complement tests (requires complement-src)
# Usage: just complement TestName
complement args=".":
    #!/usr/bin/env bash
    set -euo pipefail
    COMPLEMENT_IMAGE="${COMPLEMENT_IMAGE:-continuwuity:complement-$( (git branch --show-current 2>/dev/null || git rev-parse --short HEAD 2>/dev/null || echo detached) | tr '[:upper:]/:@ ' '[:lower:]----' | tr -cs 'a-z0-9_.-' '-' | sed 's/^-//;s/-$//' | cut -c1-96 )}"
    HOST_LIBS=$(ldd target/latest/conduwuit | awk '/=> \/usr\/lib\// {print $3}' | grep -vE 'libc\.so|libm\.so|libgcc_s\.so|libstdc\+\+\.so|libdl\.so|libpthread\.so|librt\.so' | awk '{print $1":"$1":ro"}' | paste -sd ';' - || true)
    MOUNTS="{{ PREFIX }}/lib:{{ PREFIX }}/lib:ro"
    if [ -n "$HOST_LIBS" ]; then MOUNTS="$MOUNTS;$HOST_LIBS"; fi
    env COMPLEMENT_ALWAYS_PRINT_SERVER_LOGS=1 RESULTS_DIR="{{ env_var_or_default("COMPLEMENT_RESULTS_DIR", "tests/complement") }}" COMPLEMENT_BASE_IMAGE="$COMPLEMENT_IMAGE" COMPLEMENT_HOST_MOUNTS="$MOUNTS" COMPLEMENT_RUN="{{ args }}" ./bin/complement ./complement-src

# Run Complement-Crypto (E2EE) tests (requires complement-crypto-src).
# Reuses the complement homeserver image; builds the tester image on first use.
# Usage: just e2ee TestNameRegex
e2ee args=".*":
    #!/usr/bin/env bash
    set -euo pipefail
    # Mirrors the `complement` recipe: run complement-crypto's `go test` directly
    # on the host (no tester docker image), against the already-built
    # complement-crypto-src submodule. Results/logs are written as the invoking
    # user (shane) straight into tests/crypto.
    #
    # Prerequisite: the generated artifacts must be built once (the JS-SDK bundle
    # into complement-crypto-src/internal/api/js/chrome/dist, and, for rust
    # matrices, the matrix_sdk_ffi Go bindings). Build both with:
    #   just bootstrap-crypto
    # Sources are configurable via LOCAL_JS_SDK / MATRIX_JS_SDK_SOURCE and
    # COMPLEMENT_CRYPTO_RUST_SDK_DIR; see the complement-crypto FAQ. To instead
    # copy the JS bundle out of an existing tester image:
    #   c=$(docker create continuwuity:complement-crypto-...); docker cp $c:/usr/src/complement-crypto/internal/api/js/chrome/dist complement-crypto-src/internal/api/js/chrome/dist; docker rm $c
    COMPLEMENT_SRC="${COMPLEMENT_CRYPTO_SRC:-$(pwd)/complement-crypto-src}"
    COMPLEMENT_BASE_IMAGE="${COMPLEMENT_IMAGE:-continuwuity:complement-$( (git branch --show-current 2>/dev/null || git rev-parse --short HEAD 2>/dev/null || echo detached) | tr '[:upper:]/:@ ' '[:lower:]----' | tr -cs 'a-z0-9_.-' '-' | sed 's/^-//;s/-$//' | cut -c1-96 )}"

    # Required before go test: the homeserver image (JS/rust bundle prerequisites
    # are checked later, after the client matrix is known).
    docker image inspect "$COMPLEMENT_BASE_IMAGE" >/dev/null 2>&1 || { echo "ERROR: $COMPLEMENT_BASE_IMAGE not present. Build it with: make complement/docker"; exit 1; }

    # Host-mount the run-runtime libraries into the spawned homeservers, exactly
    # as `just complement` does.
    HOST_LIBS=$(ldd target/latest/conduwuit | awk '/=> \/usr\/lib\// {print $3}' | grep -vE 'libc\.so|libm\.so|libgcc_s\.so|libstdc\+\+\.so|libdl\.so|libpthread\.so|librt\.so' | awk '{print $1":"$1":ro"}' | paste -sd ';' - || true)
    MOUNTS="{{ PREFIX }}/lib:{{ PREFIX }}/lib:ro"
    if [ -n "$HOST_LIBS" ]; then MOUNTS="$MOUNTS;$HOST_LIBS"; fi

    RESULTS_FILE_STAGING="{{ env_var_or_default("COMPLEMENT_CRYPTO_RESULTS_DIR", "$(git rev-parse --show-toplevel)/tests/crypto") }}"
    MAIN_RESULTS_FILE="$RESULTS_FILE_STAGING/results.jsonl"
    # Match bin/complement's naming: a full/`.` run is called `all`, otherwise
    # slugify the requested test pattern (so `.*` doesn't leave a bare `__`).
    run_suffix="$(printf '%s' "{{ args }}" | sed 's/[^a-zA-Z0-9]/_/g; s/^_*//; s/_*$//; s/__*/_/g' | cut -c 1-32)"
    if [ -z "$run_suffix" ] || [ "$run_suffix" = "_" ]; then run_suffix="all"; fi
    run_stamp="$(date +%s%N)"
    test_start_seconds=$SECONDS
    # Centralization: ALL complement-crypto output (raw per-shard logs, merged
    # logs, staged results, and the tracked results.jsonl ledger) lives under
    # tests/crypto. There is no separate .tmp staging dir.
    STAGING_DIR="$RESULTS_FILE_STAGING"
    mkdir -p "$STAGING_DIR"
    RESULTS_FILE="$STAGING_DIR/test_results.${run_suffix}.${run_stamp}.jsonl"
    LOG_FILE="$STAGING_DIR/logs.${run_suffix}.${run_stamp}.jsonl"

    echo ""
    echo "running go test with:"
    echo "\$COMPLEMENT_SRC: $COMPLEMENT_SRC"
    echo "\$COMPLEMENT_BASE_IMAGE: $COMPLEMENT_BASE_IMAGE"
    echo "\$RESULTS_FILE (staging): $RESULTS_FILE"
    echo "\$MAIN_RESULTS_FILE: $MAIN_RESULTS_FILE"
    echo "\$LOG_FILE: $LOG_FILE"
    echo ""

    COMPLEMENT_ENABLE_DIRTY_RUNS="${COMPLEMENT_ENABLE_DIRTY_RUNS:-0}"
    # Keep exclusions opt-in: every security and interoperability test runs by
    # default. Users can still pass a Go regular expression when diagnosing an
    # independently-known flaky upstream test.
    COMPLEMENT_CRYPTO_SKIP="${COMPLEMENT_CRYPTO_SKIP:-}"
    GO_TEST_SKIP_ARGS=()
    if [ -n "$COMPLEMENT_CRYPTO_SKIP" ]; then
        GO_TEST_SKIP_ARGS=(-skip "$COMPLEMENT_CRYPTO_SKIP")
    fi
    # The client test matrix controls which SDKs are compiled in and used, and
    # therefore which Go build tags apply. Values are two-letter permutations of
    # `r`(ust)/`j`(s) on hs1 and `R`/`J` on hs2 (see complement-crypto
    # internal/config/config.go). The `-tags` flag must match the languages the
    # matrix references or the unregistered language panics at init.
    #
    #   COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX=jj          -> tags=jssdk (JS only)
    #   COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX=rr          -> tags=rust (Rust only)
    #   COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX=jj,jr,rj,rr -> tags=jssdk,rust (both)
    #
    # Only JS/federation `J` needs the JS bundle; only rust `r`/`R` needs the
    # generated matrix_sdk_ffi Go bindings plus the shared library on
    # LIBRARY_PATH/LD_LIBRARY_PATH (supply COMPLEMENT_CRYPTO_RUST_SDK_DIR).
    CRYPTO_MATRIX="${COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX:-}"
    if [ -z "$CRYPTO_MATRIX" ]; then
        if [ -n "${COMPLEMENT_CRYPTO_RUST_SDK_DIR:-}" ]; then
            CRYPTO_MATRIX="jj,jr,rj,rr"
        else
            CRYPTO_MATRIX="jj"
        fi
    fi
    CRYPTO_TAGS=""
    case "$CRYPTO_MATRIX" in
        *[rR]*) CRYPTO_TAGS="${CRYPTO_TAGS:+$CRYPTO_TAGS,}rust" ;;
    esac
    case "$CRYPTO_MATRIX" in
        *[jJ]*) CRYPTO_TAGS="${CRYPTO_TAGS:+$CRYPTO_TAGS,}jssdk" ;;
    esac
    : "${CRYPTO_TAGS:?matrix must reference at least one of r/R (rust) or j/J (js)}"

    # Prerequisites depend on the resolved matrix: JS needs the bundled SDK dist;
    # rust needs the generated matrix_sdk_ffi Go bindings plus the shared library.
    case "$CRYPTO_MATRIX" in
        *[jJ]*)
            if [ ! -f "$COMPLEMENT_SRC/internal/api/js/chrome/dist/index.html" ]; then
                echo "ERROR: JS SDK bundle missing in $COMPLEMENT_SRC/internal/api/js/chrome/dist."
                echo "Build it first: just bootstrap-crypto"
                exit 1
            fi
            ;;
    esac
    case "$CRYPTO_MATRIX" in
        *[rR]*)
            if [ ! -f "$COMPLEMENT_SRC/internal/api/rust/matrix_sdk_ffi/matrix_sdk_ffi.go" ]; then
                echo "ERROR: matrix-sdk-ffi Go bindings missing in $COMPLEMENT_SRC/internal/api/rust."
                echo "Generate them with: COMPLEMENT_CRYPTO_RUST_SDK_DIR=<matrix-rust-sdk> just bootstrap-crypto"
                exit 1
            fi
            if [ -z "${COMPLEMENT_CRYPTO_RUST_SDK_DIR:-}" ]; then
                echo "ERROR: COMPLEMENT_CRYPTO_RUST_SDK_DIR must point at a matrix-rust-sdk checkout (for libmatrix_sdk_ffi)."
                echo "Example: COMPLEMENT_CRYPTO_RUST_SDK_DIR=/path/to/matrix-rust-sdk just e2ee ..."
                exit 1
            fi
            ;;
    esac

    # For rust clients, the cgo LDFLAGS (see uniffi.toml) pull
    # libmatrix_sdk_ffi from `target/debug` of the rust-sdk checkout, so that
    # directory must be on LIBRARY_PATH (link) and LD_LIBRARY_PATH (runtime).
    # Gated on the matrix (not just the env var being set) because CI sets
    # COMPLEMENT_CRYPTO_RUST_SDK_DIR unconditionally but only populates
    # target/debug when the resolved matrix actually references r/R.
    case "$CRYPTO_MATRIX" in
        *[rR]*)
            RUST_LIBDIR="$(realpath "$COMPLEMENT_CRYPTO_RUST_SDK_DIR/target/debug")"
            LIBRARY_PATH="${LIBRARY_PATH:+$LIBRARY_PATH:}$RUST_LIBDIR"
            LD_LIBRARY_PATH="${LD_LIBRARY_PATH:+$LD_LIBRARY_PATH:}$RUST_LIBDIR"
            export LIBRARY_PATH LD_LIBRARY_PATH
            ;;
    esac

    # Multiprocess tests (client restart/SIGKILL scenarios) need a standalone
    # cmd/rpc binary; without COMPLEMENT_CRYPTO_RPC_BINARY set they silently
    # SKIP instead of running (internal/cc/test_context.go). Build it
    # automatically whenever rust is in the matrix, rather than requiring
    # every caller to remember a separate build step and env var - rebuild
    # only when missing or older than its sources, so this is a no-op on a
    # repeat run.
    case "$CRYPTO_MATRIX" in
        *[rR]*)
            RPC_BIN="$COMPLEMENT_SRC/rpc"
            if [ ! -x "$RPC_BIN" ] || [ -n "$(find "$COMPLEMENT_SRC/cmd/rpc" "$COMPLEMENT_SRC/internal/deploy/rpc" "$COMPLEMENT_SRC/internal/api" "$COMPLEMENT_SRC/go.mod" "$COMPLEMENT_SRC/go.sum" -newer "$RPC_BIN" -print -quit 2>/dev/null)" ]; then
                echo "Building complement-crypto's cmd/rpc binary (multiprocess tests)..."
                (cd "$COMPLEMENT_SRC" && go build -tags="$CRYPTO_TAGS" -o rpc ./cmd/rpc)
            fi
            export COMPLEMENT_CRYPTO_RPC_BINARY="$RPC_BIN"
            ;;
    esac

    # This suite is fundamentally serial: the tests live in a single package and
    # none of them call t.Parallel(), so go test's `-parallel`/`-p` flags have no
    # work to overlap within one process. The only real way to run tests in
    # parallel is to shard them across N *separate* `go test` processes: each
    # process runs its own TestMain -> its own complement deployment on its own
    # randomly-mapped host ports (testcontainers allocates free ports), so
    # concurrent shards don't collide. `-parallel` is ignored; we shard instead.
    NUM_SHARDS="${COMPLEMENT_CRYPTO_PARALLEL:-4}"
    if [ -z "$NUM_SHARDS" ] || [ "$NUM_SHARDS" -lt 1 ]; then NUM_SHARDS=1; fi

    # Enumerate the top-level tests once, sorted, so sharding is deterministic.
    readarray -t ALL_TESTS < <(cd "$COMPLEMENT_SRC" && grep -hoE '^func (Test[A-Za-z0-9_]+)\(' tests/*_test.go | sed -E 's/^func (Test[A-Za-z0-9_]+)\(.*/\1/' | grep -v '^TestMain$' | sort -u)

    # Build the per-shard anchored `-run` regexes. Top-level tests only, anchored
    # with ^...$ so `TestRoomKeyIsCycledAfterEnoughMessages` doesn't sweep up its
    # later-in-alpha sibling. Targeted runs (args != `.*`) run as a single shard,
    # and `args` is a regex (the default is `.*`), so pass it through unchanged --
    # don't rewrite its metacharacters. The targeted pattern is only start-anchored
    # so a plain prefix (e.g. `TestRoomKeyIsCycledAfterEnough`) matches every test
    # beginning with it.
    SHARD_PATTERNS=()
    if [ "$run_suffix" = "all" ]; then
        total=${#ALL_TESTS[@]}
        if [ "$total" -eq 0 ]; then
            echo "ERROR: no top-level tests found in $COMPLEMENT_SRC/tests" >&2
            exit 1
        fi
        # ceil so every test is covered even when NUM_SHARDS > total.
        num_groups=$(( (total + NUM_SHARDS - 1) / NUM_SHARDS ))
        if [ "$num_groups" -lt 1 ]; then num_groups=1; fi
        for ((i = 0; i < total; i += num_groups)); do
            group=("${ALL_TESTS[@]:i:num_groups}")
            printf -v joined '%s|' "${group[@]}"
            joined="${joined%|}"
            SHARD_PATTERNS+=("^(${joined})$")
        done
    else
        SHARD_PATTERNS+=("^({{ args }})")
    fi
    num_shards=${#SHARD_PATTERNS[@]}

    echo "Sharding into $num_shards concurrent go test process(es):"
    for ((i = 0; i < num_shards; i++)); do
        echo "  shard $((i + 1))/$num_shards: $COMPLEMENT_SRC/tests -run '${SHARD_PATTERNS[$i]}'"
    done
    echo ""

    # One staging results/log file per shard; concatenated at the end.
    : >"$RESULTS_FILE"
    : >"$LOG_FILE"
    shard_pids=()
    # Each concurrent shard must get its own complement `PackageNamespace` so its
    # deployed docker network/containers (`complement_<ns>.<blueprint>.hs1`) don't
    # collide with the other shards' (the namespace is unique per `go test` via
    # COMPLEMENT_CRYPTO_NAMESPACE, read in complement-crypto-src/tests/main_test.go).
    set +e
    for ((s = 0; s < num_shards; s++)); do
        shard_results="$STAGING_DIR/test_results.${run_suffix}.${run_stamp}.s$((s + 1)).jsonl"
        shard_log="$STAGING_DIR/logs.${run_suffix}.${run_stamp}.s$((s + 1)).jsonl"
        : >"$shard_results"
        : >"$shard_log"
        (
            # shellcheck disable=SC2016
            env \
                -C "$COMPLEMENT_SRC" \
                COMPLEMENT_BASE_IMAGE="$COMPLEMENT_BASE_IMAGE" \
                COMPLEMENT_HOST_MOUNTS="$MOUNTS" \
                COMPLEMENT_ENABLE_DIRTY_RUNS="$COMPLEMENT_ENABLE_DIRTY_RUNS" \
                COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX="$CRYPTO_MATRIX" \
                COMPLEMENT_CRYPTO_NAMESPACE="crypto$((s + 1))" \
                ${COMPLEMENT_CRYPTO_MITMDUMP:+COMPLEMENT_CRYPTO_MITMDUMP="$COMPLEMENT_CRYPTO_MITMDUMP"} \
                go test -tags "$CRYPTO_TAGS" -json \
                -timeout "{{ env_var_or_default("COMPLEMENT_CRYPTO_TIMEOUT", "30m") }}" \
                -count=1 \
                "${GO_TEST_SKIP_ARGS[@]}" \
                -run "${SHARD_PATTERNS[$s]}" \
                ./tests |
                tee -a "$shard_log" |
                jq --unbuffered -r 'select((.Action == "pass" or .Action == "fail" or .Action == "skip") and .Test != null) | (.Elapsed // 0) as $elapsed | [.Action, .Test, (if $elapsed == 0 then "0s" else (((($elapsed * 100) | round) / 100) | tostring) + "s" end)] | @tsv' |
                while IFS=$'\t' read -r action test_name elapsed; do
                    [ -n "$action" ] || continue
                    jq -nc --arg Action "$action" --arg Test "$test_name" '{Action: $Action, Test: $Test}' >>"$shard_results"
                    printf 'shard %d\t%s\t%s %s\n' "$((s + 1))" "${action^^}" "$test_name" "$elapsed"
                done
        ) &
        shard_pids+=($!)
    done

    # Wait for every shard; preserve the first non-zero exit as the overall code.
    go_test_exit=0
    for pid in "${shard_pids[@]}"; do
        wait "$pid" || [ "$go_test_exit" -ne 0 ] || go_test_exit=$?
    done
    set -e

    # Combine per-shard staged results and logs into the single aggregate files.
    for ((s = 0; s < num_shards; s++)); do
        shard_results="$STAGING_DIR/test_results.${run_suffix}.${run_stamp}.s$((s + 1)).jsonl"
        shard_log="$STAGING_DIR/logs.${run_suffix}.${run_stamp}.s$((s + 1)).jsonl"
        [ -f "$shard_results" ] && cat "$shard_results" >>"$RESULTS_FILE"
        [ -f "$shard_log" ] && cat "$shard_log" >>"$LOG_FILE"
    done

    # A compile/setup failure produces no pass/fail rows, so without this the
    # run just reports "0 pass / 0 fail" and hides the real error (for example
    # generated bindings that do not compile). go test -json emits
    # `build-output` lines plus a `fail` action carrying `FailedBuild`; surface
    # both so the cause is the first thing printed.
    if [ "$go_test_exit" -ne 0 ]; then
        # A failed build sets `FailedBuild`; `build-output` alone can be benign
        # linker noise (e.g. DT_TEXTREL warnings from the cgo/PIE link), so only
        # treat it as a build failure when a package actually failed to build.
        failed_build="$(jq -r 'select(.FailedBuild) | .FailedBuild' "$LOG_FILE" 2>/dev/null | sort -u || true)"
        if [ -n "$failed_build" ]; then
            build_err="$(jq -r 'select(.Action == "build-output") | .Output' "$LOG_FILE" 2>/dev/null || true)"
            echo ""
            echo "==================== BUILD FAILURE ===================="
            echo "failed package(s): $(printf '%s ' $failed_build)"
            if [ -n "$build_err" ]; then
                printf '%s' "$build_err"
            fi
            echo "======================================================"
            echo ""
        fi
    fi

    toplevel="$(git rev-parse --show-toplevel)"
    if [ -s "$RESULTS_FILE" ]; then
        # Dedupe/sort the staged rows, then merge them into the persistent
        # ledger. This preserves results from tests not covered by the current
        # matrix or test-pattern run (for example, a JS-only crypto run).
        python3 "$toplevel/bin/merge_complement_results.py" --dedupe-in-place "$RESULTS_FILE" \
            || echo "WARN: dedupe of staged results failed ($RESULTS_FILE); keeping raw rows" >&2
        python3 "$toplevel/bin/merge_complement_results.py" --sort-in-place "$RESULTS_FILE" \
            || echo "WARN: sort of staged results failed ($RESULTS_FILE); keeping arrival order" >&2
        tmp_results="$MAIN_RESULTS_FILE.tmp"
        if python3 "$toplevel/bin/merge_complement_results.py" "$MAIN_RESULTS_FILE" "$RESULTS_FILE" "$tmp_results"; then
            mv -f "$tmp_results" "$MAIN_RESULTS_FILE" \
                || { echo "MERGE FAILED: moving merged results into $MAIN_RESULTS_FILE" >&2; exit 1; }
            echo "merged $(wc -l <"$RESULTS_FILE") staged results into $MAIN_RESULTS_FILE"
        else
            # Merge failed (e.g. under load); append the staged results so
            # the new pass/fail rows are recorded rather than lost.
            echo "WARN: merge into $MAIN_RESULTS_FILE failed; appending staged results" >&2
            cat "$RESULTS_FILE" >>"$MAIN_RESULTS_FILE"
            rm -f "$tmp_results"
        fi
    else
        echo "Warning: $RESULTS_FILE is missing or empty. No results processed."
        [ "$go_test_exit" -eq 0 ] && go_test_exit=1
    fi

    # Centralization: expose the SDK/server runtime logs (written by the
    # complement-crypto TestMain into $COMPLEMENT_SRC/tests/logs) under the results
    # dir too, so every artifact of a run lives in tests/crypto.
    if [ -d "$COMPLEMENT_SRC/tests/logs" ] && [ "$(ls -A "$COMPLEMENT_SRC/tests/logs")" ]; then
        mkdir -p "$RESULTS_FILE_STAGING/logs"
        for f in "$COMPLEMENT_SRC/tests/logs"/*; do
            b="$(basename -- "$f")"
            ln -sfn "$f" "$RESULTS_FILE_STAGING/logs/$b"
        done
        echo "linked complement-crypto runtime logs -> $RESULTS_FILE_STAGING/logs"
    fi

    _pass=$(jq -s '[.[] | select(.Action == "pass")] | length' "$RESULTS_FILE" 2>/dev/null || true)
    _fail=$(jq -s '[.[] | select(.Action == "fail")] | length' "$RESULTS_FILE" 2>/dev/null || true)
    _skip=$(jq -s '[.[] | select(.Action == "skip")] | length' "$RESULTS_FILE" 2>/dev/null || true)
    test_duration_seconds=$((SECONDS - test_start_seconds))

    echo ""
    echo "RESULTS: ${_pass:-0} pass / ${_fail:-0} fail / ${_skip:-0} skip"
    echo "TIME: $(printf '%d:%02d' $((test_duration_seconds / 60)) $((test_duration_seconds % 60))) min"
    echo ""
    echo "complement logs saved at $LOG_FILE"
    echo "complement results staged at $RESULTS_FILE"
    echo "complement results merged into $MAIN_RESULTS_FILE"
    echo ""

    exit "$go_test_exit"

# Named aliases for common client matrices. They delegate to `e2ee` (single
# source of truth for the logic); the matrix env var is all they vary. Rust
# targets still need COMPLEMENT_CRYPTO_RUST_SDK_DIR pointing at a
# matrix-rust-sdk checkout (see the e2ee prerequisite errors).
# Usage: just crypto-rs TestNameRegex   (also: crypto-js, crypto-jsrs)
#
# The test recipes do not rebuild the generated artifacts (the JS bundle and
# the Rust Go bindings), so a stale or missing one silently tests an old SDK
# or fails the prerequisite checks. `bootstrap-crypto` builds both from
# configurable sources and is idempotent.
bootstrap-crypto:
    #!/usr/bin/env bash
    set -euo pipefail
    # Delegate to the authoritative recipes in complement-crypto-src, which own
    # the build logic and configuration (LOCAL_JS_SDK,
    # COMPLEMENT_CRYPTO_RUST_SDK_DIR). LOCAL_JS_SDK precedence:
    #   1. LOCAL_JS_SDK env/.env entry (a full matrix-js-sdk spec)
    #   2. MATRIX_JS_SDK_SOURCE env/.env entry (url#sha, kept for compatibility)
    #   3. the pinned GitLab fork commit
    sdk="{{ LOCAL_JS_SDK }}"
    (cd complement-crypto-src && LOCAL_JS_SDK="$sdk" just bootstrap)

# Rebuild just the JS-SDK bundle complement-crypto embeds (a subset of
# bootstrap-crypto); run it after changing the SDK pin.
crypto-js-bundle:
    #!/usr/bin/env bash
    set -euo pipefail
    (cd complement-crypto-src && LOCAL_JS_SDK="{{ LOCAL_JS_SDK }}" just rebuild-js-sdk)

crypto-js pattern=".*":
    # matrix-js-sdk#4291: JS does not update its crypto membership from a
    # completed /invite until the corresponding /sync is processed. This test
    # intentionally delays that /sync, so skip only the known JS limitation.
    COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX=jj {{ just_executable() }} e2ee "{{ pattern }}"

crypto-rs pattern=".*":
    COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX=rr {{ just_executable() }} e2ee "{{ pattern }}"

crypto-jsrs pattern=".*":
    COMPLEMENT_CRYPTO_TEST_CLIENT_MATRIX=jj,jr,rj,rr {{ just_executable() }} e2ee "{{ pattern }}"

# -----------------------------------------------------------------------------
# Complement CI
# -----------------------------------------------------------------------------

PROFILE := env_var_or_default("PROFILE", "release")

# matrix-js-sdk source that the Complement-Crypto tester image embeds. Keep the
# default pinned for reproducible local bundles; override it when needed.
MATRIX_JS_SDK_SOURCE := env_var_or_default("MATRIX_JS_SDK_SOURCE", "https://gitlab.com/Wombat-Foundation/matrix-js-sdk#1ea51700dd8e4899ba2bfc69255ac0f7f0e4e3af")

# Full matrix-js-sdk spec consumed by complement-crypto's build recipes: a
# `matrix-js-sdk@<url>#<sha>` or `matrix-js-sdk@file:/abs/path`. Defaults to
# MATRIX_JS_SDK_SOURCE (which carries no package prefix) and is overridden
# directly by the LOCAL_JS_SDK environment variable / .env entry.
LOCAL_JS_SDK := env_var_or_default("LOCAL_JS_SDK", "matrix-js-sdk@" + MATRIX_JS_SDK_SOURCE)

# Aggregates test results generated by complement
ci-complement-stats:
    #!/usr/bin/env bash
    set -euo pipefail

    RESULTS_DIR="{{ env_var_or_default("COMPLEMENT_RESULTS_DIR", "tests/complement") }}"
    RESULTS="$RESULTS_DIR/results.jsonl"
    if [ ! -f "$RESULTS" ]; then
        echo "ERROR: $RESULTS does not exist"
        exit 1
    fi

    echo "Parsing Complement test results..."
    PASS=$(jq -s '[.[] | select(.Action == "pass")] | length' "$RESULTS")
    FAIL=$(jq -s '[.[] | select(.Action == "fail")] | length' "$RESULTS")
    SKIP=$(jq -s '[.[] | select(.Action == "skip")] | length' "$RESULTS")
    TOTAL=$((PASS + FAIL + SKIP))

    echo ""
    if [ "$FAIL" -gt 0 ] && [ "${VERBOSE:-0}" = "1" ]; then
        echo "Failed Tests:"
        jq -r 'select(.Action == "fail") | .Test' "$RESULTS" | sort -u
        echo ""
    fi

    echo "=== Complement Test Stats ==="
    echo "✓ Passed:  $PASS"
    echo "✗ Failed:  $FAIL"
    echo "⚠ Skipped: $SKIP"
    echo "Overall:   $TOTAL tests"

    echo ""
    echo "Last modified by (this branch):"
    git log -5 --format="%an (%ad) %H" -- tests/complement/results.jsonl

# -----------------------------------------------------------------------------
# CI Database Queries
# -----------------------------------------------------------------------------

# Query the CI run regressions view via DB shell.
# Usage:
# just ci-query-failures limit=100 order=run_date asc like=branch_name baseline=123
ci-query-failures +args="":
    #!/usr/bin/env bash
    ./.github/actions/postgres/ci-query-failures.py {{ args }}
