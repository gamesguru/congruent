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
    @echo "Creating {{PREFIX}}/build and assigning ownership to $USER... (Requires sudo)"
    sudo mkdir -p {{PREFIX}}/build {{PREFIX}}/lib {{PREFIX}}/include {{PREFIX}}/bin
    sudo chown -R $USER:$USER {{PREFIX}}/build {{PREFIX}}/lib {{PREFIX}}/include {{PREFIX}}/bin
    @echo "Done. You can now run prebuild commands."

# Pre-build all C/C++ dependencies
prebuild-all: init-prebuild prebuild-jemalloc prebuild-lz4 prebuild-snappy prebuild-zstd prebuild-rocksdb prebuild-aws-lc

# Install all pre-built C/C++ dependencies
install-all: install-jemalloc install-lz4 install-snappy install-zstd install-rocksdb install-aws-lc

# Builds liburing
prebuild-liburing:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^liburing," {{CSV}} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^liburing," {{CSV}} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{PREFIX}}/build && sudo chown -R $USER:$USER {{PREFIX}}/build
    echo "Cloning and building liburing $TAG..."
    [ ! -d "{{PREFIX}}/build/liburing" ] && git clone $REPO {{PREFIX}}/build/liburing || true
    cd {{PREFIX}}/build/liburing
    git fetch --all --tags
    git checkout $TAG
    ./configure --prefix={{PREFIX}}
    make -j$(nproc)

# Installs liburing
install-liburing:
    @echo "Installing liburing (requires sudo)..."
    cd {{PREFIX}}/build/liburing && sudo make install
    @echo "Done! You might need to run 'sudo ldconfig' to update library cache."

# Builds bzip2
prebuild-bzip2:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^bzip2," {{CSV}} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^bzip2," {{CSV}} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{PREFIX}}/build && sudo chown -R $USER:$USER {{PREFIX}}/build
    echo "Cloning and building bzip2 $TAG..."
    [ ! -d "{{PREFIX}}/build/bzip2" ] && git clone $REPO {{PREFIX}}/build/bzip2 || true
    cd {{PREFIX}}/build/bzip2
    git fetch --all --tags
    git checkout $TAG
    make -f Makefile-libbz2_so
    make

# Installs bzip2
install-bzip2:
    @echo "Installing bzip2 (requires sudo)..."
    cd {{PREFIX}}/build/bzip2 && sudo make install PREFIX={{PREFIX}}
    cd {{PREFIX}}/build/bzip2 && sudo cp -f libbz2.so.1.0.* {{PREFIX}}/lib/
    cd {{PREFIX}}/build/bzip2 && sudo ln -sf {{PREFIX}}/lib/libbz2.so.1.0.* {{PREFIX}}/lib/libbz2.so
    sudo ldconfig
    @echo "Done! Installed libbz2.so to {{PREFIX}}/lib"

# Pre-build jemalloc
prebuild-jemalloc:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^jemalloc," {{CSV}} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^jemalloc," {{CSV}} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{PREFIX}}/build && sudo chown -R $USER:$USER {{PREFIX}}/build
    echo "Cloning jemalloc $TAG..."
    [ ! -d "{{PREFIX}}/build/jemalloc" ] && git clone $REPO {{PREFIX}}/build/jemalloc || true
    echo "Building jemalloc..."
    cd {{PREFIX}}/build/jemalloc
    git fetch --all --tags
    git checkout $TAG
    [ -f configure ] || ./autogen.sh
    [ -f Makefile ] || ./configure --prefix={{PREFIX}}
    make -j$(nproc)

# Install jemalloc globally (requires sudo)
install-jemalloc:
    @echo "Installing jemalloc to {{PREFIX}}... (Requires sudo)"
    cd {{PREFIX}}/build/jemalloc && sudo make install_lib_static install_lib_shared install_include
    sudo ldconfig

# Pre-build lz4
prebuild-lz4:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^lz4," {{CSV}} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^lz4," {{CSV}} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{PREFIX}}/build && sudo chown -R $USER:$USER {{PREFIX}}/build
    echo "Cloning lz4 $TAG..."
    [ ! -d "{{PREFIX}}/build/lz4" ] && git clone $REPO {{PREFIX}}/build/lz4 || true
    echo "Building lz4..."
    cd {{PREFIX}}/build/lz4
    git fetch --all --tags
    git checkout $TAG
    make lib -j$(nproc)

# Install lz4 globally (requires sudo)
install-lz4:
    @echo "Installing lz4 to {{PREFIX}}... (Requires sudo)"
    cd {{PREFIX}}/build/lz4 && sudo make install PREFIX={{PREFIX}}
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
    TAG=$(grep "^rocksdb," {{CSV}} | cut -d',' -f4 | tr -d '\r' || true)
    if [ -z "$TAG" ]; then
        TAG="v10.5.1"
    fi
    REPO=$(grep "^rocksdb," {{CSV}} | cut -d',' -f3 | tr -d '\r' || true)
    if [ -z "$REPO" ]; then
        REPO="https://github.com/facebook/rocksdb.git"
    fi
    sudo mkdir -p {{PREFIX}}/build && sudo chown -R $USER:$USER {{PREFIX}}/build
    echo "Cloning rocksdb $TAG..."
    if [ ! -d "{{PREFIX}}/build/rocksdb" ]; then
        git clone --recursive "$REPO" {{PREFIX}}/build/rocksdb
    else
        (cd {{PREFIX}}/build/rocksdb && git remote set-url origin "$REPO")
    fi
    echo "Building RocksDB..."
    cd {{PREFIX}}/build/rocksdb

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
    env ROCKSDB_NO_FBCODE=1 ROCKSDB_DISABLE_BENCHMARK=1 DISABLE_JEMALLOC=1 EXTRA_CXXFLAGS="${EXTRA_CXXFLAGS:-} -I{{PREFIX}}/include -Wno-error=unused-parameter -Wno-error=maybe-uninitialized" EXTRA_LDFLAGS="-L{{PREFIX}}/lib" PORTABLE=0 USE_RTTI=1 make shared_lib static_lib -j$(nproc)

    # Build ldb (statically linked to avoid shared library RTTI/ABI mismatches)
    env DISABLE_WARNING_AS_ERROR=1 DEBUG_LEVEL=0 USE_RTTI=1 make ldb
    g++ -o ldb_static tools/ldb.o tools/ldb_cmd.o tools/ldb_tool.o tools/sst_dump_tool.o utilities/blob_db/blob_dump_tool.o librocksdb.a -lpthread -lrt -ldl -lsnappy -lz -lbz2 -llz4 -lzstd -luring -ljemalloc -lstdc++ -lm
    mv ldb_static ldb

# Install RocksDB globally (requires sudo)
install-rocksdb:
    @echo "Installing RocksDB to {{PREFIX}}... (Requires sudo)"
    cd {{PREFIX}}/build/rocksdb && sudo make install-shared PREFIX={{PREFIX}}
    cd {{PREFIX}}/build/rocksdb && sudo make install-static PREFIX={{PREFIX}}
    sudo install -m 755 {{PREFIX}}/build/rocksdb/ldb {{PREFIX}}/bin/ldb
    sudo ldconfig
    @echo "Remember to set ROCKSDB_LIB_DIR={{PREFIX}}/lib if Cargo doesn't see it."

# Pre-build snappy
prebuild-snappy:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^snappy," {{CSV}} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^snappy," {{CSV}} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{PREFIX}}/build && sudo chown -R $USER:$USER {{PREFIX}}/build
    echo "Cloning snappy $TAG..."
    if [ ! -d "{{PREFIX}}/build/snappy" ]; then
        git clone $REPO {{PREFIX}}/build/snappy
    fi
    echo "Building snappy..."
    cd {{PREFIX}}/build/snappy
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
    cmake -DCMAKE_INSTALL_PREFIX={{PREFIX}} -DBUILD_SHARED_LIBS=OFF -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF ..
    make -j$(nproc)
    cd ..
    mkdir -p build_shared && cd build_shared
    # rm -f CMakeCache.txt
    cmake -DCMAKE_INSTALL_PREFIX={{PREFIX}} -DBUILD_SHARED_LIBS=ON -DSNAPPY_BUILD_TESTS=OFF -DSNAPPY_BUILD_BENCHMARKS=OFF ..
    make -j$(nproc)

# Install snappy globally (requires sudo)
install-snappy:
    @echo "Installing snappy to {{PREFIX}}... (Requires sudo)"
    cd {{PREFIX}}/build/snappy/build_static && sudo make install
    cd {{PREFIX}}/build/snappy/build_shared && sudo make install
    sudo ldconfig

# Pre-build zstd
prebuild-zstd:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^zstd," {{CSV}} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^zstd," {{CSV}} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{PREFIX}}/build && sudo chown -R $USER:$USER {{PREFIX}}/build
    echo "Cloning zstd $TAG..."
    [ ! -d "{{PREFIX}}/build/zstd" ] && git clone $REPO {{PREFIX}}/build/zstd || true
    echo "Building zstd..."
    cd {{PREFIX}}/build/zstd
    git fetch --all --tags
    git checkout $TAG
    make lib-release -j$(nproc)

# Install zstd globally (requires sudo)
install-zstd:
    @echo "Installing zstd to {{PREFIX}}... (Requires sudo)"
    cd {{PREFIX}}/build/zstd && sudo make install -C lib PREFIX={{PREFIX}}
    sudo ldconfig

# Pre-build aws-lc
prebuild-aws-lc:
    #!/usr/bin/env bash
    set -e
    TAG=$(grep "^aws-lc," {{CSV}} | cut -d',' -f4 | tr -d '\r')
    REPO=$(grep "^aws-lc," {{CSV}} | cut -d',' -f3 | tr -d '\r')
    sudo mkdir -p {{PREFIX}}/build && sudo chown -R $USER:$USER {{PREFIX}}/build
    echo "Cloning aws-lc $TAG..."
    [ ! -d "{{PREFIX}}/build/aws-lc" ] && git clone $REPO {{PREFIX}}/build/aws-lc || true
    echo "Building aws-lc..."
    cd {{PREFIX}}/build/aws-lc
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
    cmake -DCMAKE_INSTALL_PREFIX={{PREFIX}} -DBUILD_TESTING=OFF -DBUILD_LIBSSL=ON ..
    make -j$(nproc)

# Install aws-lc globally (requires sudo)
install-aws-lc:
    @echo "Installing aws-lc to {{PREFIX}}... (Requires sudo)"
    cd {{PREFIX}}/build/aws-lc/build && sudo make install
    sudo ldconfig

# --- CPU Profiling ---

# Run CPU flamegraph profiling on release build (requires sudo for perf)
profile-runtime-cpu *args:
    cargo flamegraph --root --features local_profiling --bin conduwuit -- {{args}}
    @echo "Flamegraph saved to flamegraph.svg"

# Run CPU flamegraph profiling on dev build (requires sudo for perf)
profile-runtime-cpu-dev *args:
    cargo flamegraph --root --dev --features local_profiling --bin conduwuit -- {{args}}
    @echo "Flamegraph saved to flamegraph.svg"

# --- Async & I/O Profiling ---

# Run with tokio-console instrumentation active
profile-runtime-async *args:
    @echo "Run 'tokio-console' in a separate terminal"
    env RUSTFLAGS="--cfg tokio_unstable ${RUSTFLAGS:-}" cargo run --features local_profiling --bin conduwuit -- {{args}}

# --- Memory Profiling (jemalloc) ---

# Run release build and dump jemalloc heap profiles
profile-runtime-mem *args:
    cargo build --release --features local_profiling --bin conduwuit
    @echo "Starting with jemalloc profiling..."
    env MALLOC_CONF="prof:true,lg_prof_interval:24,prof_prefix:jeprof.out" ./target/release/conduwuit {{args}}

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
    @echo "Building for glibc {{glibc_version}} with CPU target {{cpu_arch}} using cargo-zigbuild..."
    @if ! command -v cargo-zigbuild >/dev/null 2>&1; then \
        echo "Error: cargo-zigbuild is not installed. Run: cargo install cargo-zigbuild"; \
        exit 1; \
    fi
    @if ! command -v zig >/dev/null 2>&1; then \
        echo "Error: zig is not installed. Run: sudo pacman -S zig (or your package manager's equivalent)"; \
        exit 1; \
    fi
    rustup target add x86_64-unknown-linux-gnu
    env RUSTFLAGS="-C target-cpu={{cpu_arch}}" cargo zigbuild --release --target x86_64-unknown-linux-gnu.{{glibc_version}}

# Extracts the workspace version from Cargo.toml
version := "$(grep -m1 '^version = ' Cargo.toml | cut -d \" -f 2)"

# Start gdbserver for lightweight remote debugging (POC)
# Usage: just remote-debug-poc /path/to/conduwuit.toml
remote-debug-poc config="conduwuit-example.toml":
    @echo "Starting gdbserver on :1234 using config: {{config}}"
    sudo -u conduwuit gdbserver :1234 ./target/debug/continuwuity --config {{config}}

# Run Complement tests (requires complement-src)
# Usage: just complement TestName
complement args=".":
    #!/usr/bin/env bash
    set -euo pipefail
    COMPLEMENT_IMAGE="${COMPLEMENT_IMAGE:-continuwuity:complement-$( (git branch --show-current 2>/dev/null || git rev-parse --short HEAD 2>/dev/null || echo detached) | tr '[:upper:]/:@ ' '[:lower:]----' | tr -cs 'a-z0-9_.-' '-' | sed 's/^-//;s/-$//' | cut -c1-96 )}"
    HOST_LIBS=$(ldd target/latest/conduwuit | awk '/=> \/usr\/lib\// {print $3}' | grep -vE 'libc\.so|libm\.so|libgcc_s\.so|libstdc\+\+\.so|libdl\.so|libpthread\.so|librt\.so' | awk '{print $1":"$1":ro"}' | paste -sd ';' - || true)
    MOUNTS="{{PREFIX}}/lib:{{PREFIX}}/lib:ro"
    if [ -n "$HOST_LIBS" ]; then MOUNTS="$MOUNTS;$HOST_LIBS"; fi
    env COMPLEMENT_ALWAYS_PRINT_SERVER_LOGS=1 RESULTS_DIR="{{env_var_or_default("COMPLEMENT_RESULTS_DIR", "tests/test_results/complement-gg")}}" COMPLEMENT_BASE_IMAGE="$COMPLEMENT_IMAGE" COMPLEMENT_HOST_MOUNTS="$MOUNTS" COMPLEMENT_RUN="{{args}}" ./bin/complement ./complement-src

# -----------------------------------------------------------------------------
# Complement CI
# -----------------------------------------------------------------------------

PROFILE := env_var_or_default("PROFILE", "release")

# Aggregates test results generated by complement
ci-complement-stats:
    #!/usr/bin/env bash
    set -euo pipefail

    RESULTS_DIR="{{env_var_or_default("COMPLEMENT_RESULTS_DIR", "tests/test_results/complement")}}"
    RESULTS="$RESULTS_DIR/test_results.jsonl"
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
    echo "Last modified by:"
    git log -5 --format="%an (%ad) %H" origin/main -- tests/test_results/complement-gg/test_results.jsonl

# -----------------------------------------------------------------------------
# CI Database Queries
# -----------------------------------------------------------------------------

# Query the CI run regressions view via DB shell.
# Usage:
#   just ci-query-failures limit=100 order=run_date asc like=branch_name baseline=123
ci-query-failures +args="":
    #!/usr/bin/env bash
    ./.github/actions/postgres/ci-query-failures.py {{args}}
