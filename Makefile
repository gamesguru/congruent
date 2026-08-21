SHELL=/bin/bash
.DEFAULT_GOAL=_help

# [CONFIG] Suppresses annoying "make[1]: Entering directory" messages
MAKEFLAGS += --no-print-directory

# [CONFIG] source .env if it exists
ifneq (,$(wildcard ./.env))
	include .env
	export
	# Strip double quotes from .env values (annoying disagreement between direnv, dotenv)
	RUSTFLAGS := $(subst ",,$(RUSTFLAGS))
endif

# Example .env:
#!/bin/bash
# export PREFIX=/usr/local
# export ROCKSDB_INCLUDE_DIR=${PREFIX}/include
# export ROCKSDB_LIB_DIR=${PREFIX}/lib
# export LD_LIBRARY_PATH=${ROCKSDB_LIB_DIR}:${LD_LIBRARY_PATH}
# #export CPU_TARGET=skylake
# export OS_VERSION=ubuntu-24.04
# export GH_REPO=.../continuwuity
# export SKIP_CONFIRM=1
# export PROFILE=dev-quick
# # export PROFILE=release-high-perf
# # export RUSTFLAGS="-C target-cpu=native"
# # export RUSTFLAGS="-C target-cpu=skylake"
# export COMPLEMENT_DIR="/.../complement-suite"
# export CONDUWUIT_CONFIG="/<...>/etc/conduwuit/conduwuit.toml"
# export CONDUWUIT_DATABASE_PATH=/var/lib/conduwuit-v18



# [CONFIG] Auto-discover vars defined in Makefiles (not env-inherited)
VARS = $(sort $(foreach v,$(.VARIABLES),$(if $(filter file override command,$(origin $v)),$v)))

# [ENUM] Styling / Colors
STYLE_CYAN := $(shell tput setaf 6 2>/dev/null || echo -e "\033[36m")
STYLE_RESET := $(shell tput sgr0 2>/dev/null || echo -e "\033[0m")


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Meta/help commands
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.PHONY: doctor
doctor: ##H Output version info for required tools
	@echo "Sanity check — not comprehensive. Requirements may be missing or out of date."
	@echo "See rust-toolchain.toml for authoritative versions."
	cargo --version
	rustup --version
	cargo +nightly fmt --version
	cargo fmt --version
	cargo +nightly clippy --version
	cargo clippy --version
	pre-commit --version
	pkg-config --version
	pkg-config --libs --cflags liburing
	@echo "OK."
	@echo "Checking for newer tags [DRY RUN]..."
	git fetch --all --tags --dry-run

.PHONY: cpu-info
cpu-info: ##H Print CPU info relevant to native target-cpu
	@echo "=== CPU Model ==="
	@grep -m1 'model name' /proc/cpuinfo 2>/dev/null || sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown"
	@echo "=== Architecture ==="
	@uname -a
	@echo "=== rustc Host Target ==="
	@rustc -vV | grep host
	@echo "=== rustc Native CPU ==="
	@rustc --print=cfg -C target-cpu=native 2>/dev/null | grep target_feature | sort
	@echo "=== CPU Flags [from /proc/cpuinfo] ==="
	@grep -m1 'flags' /proc/cpuinfo 2>/dev/null | tr ' ' '\n' | grep -E 'avx|sse|aes|bmi|fma|popcnt|lzcnt|sha|pclmul' | sort

.PHONY: vars
vars: ##H Print debug info
	@$(foreach v, $(VARS), printf "$(STYLE_CYAN)%-25s$(STYLE_RESET) %s\n" "$(v)" "$($(v))";)
	@echo "... computing version."
	@ROCKSDB_INCLUDE_DIR=$(ROCKSDB_INCLUDE_DIR) \
		ROCKSDB_LIB_DIR=$(ROCKSDB_LIB_DIR) \
		LD_LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LD_LIBRARY_PATH \
		printf "$(STYLE_CYAN)%-25s$(STYLE_RESET) %s\n" "VERSION" \
		"$$(cargo run $(CARGO_FLAGS) -p conduwuit_build_metadata --bin version --quiet)"


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Development commands
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

PROFILE ?=
p ?=
CRATE ?=
CARGO_SCOPE ?= $(if $(p),-p $(p),$(if $(CRATE),-p $(CRATE),--workspace))
CARGO_FLAGS ?= $(if $(PROFILE),--profile $(PROFILE),) --config Cargo.custom.toml


.PHONY: cargo/lock-init
cargo/lock-init:        ##H Init or fully upgrade the lockfile (wipes it)
	ROCKSDB_INCLUDE_DIR=$(ROCKSDB_INCLUDE_DIR) \
		ROCKSDB_LIB_DIR=$(ROCKSDB_LIB_DIR) \
		LD_LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LD_LIBRARY_PATH \
	ROCKSDB_LIB_DIR=$(ROCKSDB_LIB_DIR) cargo generate-lockfile
	@echo "OK."


.PHONY: profiles
profiles: ##H List available cargo profiles
# NOTE: not authoritative — see Cargo.toml for definitive profiles.
	@grep "^\[profile\." Cargo.toml Cargo.custom.toml 2>/dev/null \
		| sed 's/.*\[profile\.//;s/\]//' \
		| grep -v 'package' \
		| grep -v 'build-override' \
		| sort

.PHONY: features
features: ##H List available cargo features
	@awk '/^\[features\]/{flag=1; next} /^\[.*\]/{flag=0} flag && /^[^ #\t=]+ =/ {gsub(/ =.*/, ""); print}' src/main/Cargo.toml | sort


# For native, highly-optimized builds that work only for you cpu: -C target-cpu=native
RUSTFLAGS ?=

# Display crate compilation progress [X/Y] in nohup or no-tty environment.
# Override or unset in .env to disable.
export CARGO_TERM_PROGRESS_WHEN ?= auto
export CARGO_TERM_PROGRESS_WIDTH ?= 200

# To suppress the confirmation prompt, add to your .env: SKIP_CONFIRM=1
SKIP_CONFIRM ?=

# Target to prompt for confirmation before proceeding (slow tasks, cleaning builds, etc)
.PHONY: _confirm
_confirm:
	# Verifying required variables are set...
	@test "$(PROFILE)" || (echo "ERROR: PROFILE is not set" && exit 1)
	# Confirming you wish to proceed...
	@if [ -t 0 ] && [ -z "$(SKIP_CONFIRM)" ]; then read -p "Press [Enter] to continue or Ctrl+C to cancel..." _; fi


.PHONY: format
format: ##H Run pre-commit hooks/formatters
	pre-commit run --all-files
	-pg_format -i -L .github/actions/postgres/*.sql
#	ROCKSDB_INCLUDE_DIR=$(ROCKSDB_INCLUDE_DIR) \
		ROCKSDB_LIB_DIR=$(ROCKSDB_LIB_DIR) \
		LD_LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LD_LIBRARY_PATH \
		cargo fix $(CARGO_SCOPE) $(CARGO_FLAGS) --features default --allow-dirty --allow-no-vcs

.PHONY: lint
lint:   ##H Lint code
	@echo "Lint code? PROFILE='$(PROFILE)'"
	@$(MAKE) _confirm
	ROCKSDB_INCLUDE_DIR=$(ROCKSDB_INCLUDE_DIR) \
		ROCKSDB_LIB_DIR=$(ROCKSDB_LIB_DIR) \
		LD_LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LD_LIBRARY_PATH \
		AWS_LC_SYS_LDFLAGS="-L$(PREFIX)/lib -lssl -lcrypto" \
		AWS_LC_SYS_INCLUDES="$(PREFIX)/include" \
		AWS_LC_RS_NO_BUNDLE=1 \
		AWS_LC_RS_PREBUILT_PATH=$(PREFIX) \
		cargo clippy $(CARGO_SCOPE) --locked --no-deps $(CARGO_FLAGS) $$(if [ -n "$$CI" ]; then echo "-- -D warnings"; fi)

.PHONY: test
test:   ##H Run tests
	@echo "Run tests? PROFILE='$(PROFILE)'"
	@$(MAKE) _confirm
	ROCKSDB_INCLUDE_DIR=$(ROCKSDB_INCLUDE_DIR) \
		ROCKSDB_LIB_DIR=$(ROCKSDB_LIB_DIR) \
		LD_LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LD_LIBRARY_PATH \
		AWS_LC_SYS_LDFLAGS="-L$(PREFIX)/lib -lssl -lcrypto" \
		AWS_LC_SYS_INCLUDES="$(PREFIX)/include" \
		AWS_LC_RS_NO_BUNDLE=1 \
		AWS_LC_RS_PREBUILT_PATH=$(PREFIX) \
		cargo test $(CARGO_SCOPE) --locked --all-targets --timings $(CARGO_FLAGS)


PREFIX ?= /usr/local
ROCKSDB_LIB_DIR ?= $(PREFIX)/lib
ROCKSDB_INCLUDE_DIR ?= $(PREFIX)/include

# Default features to use for the build
# We use bindgen-runtime by default to use the system libclang.so for building.
# Bundling RocksDB statically can be enabled via features.
FEATURES ?= console,url_preview

.PHONY: build
build:  ##H Build with selected profile
	# NOTE: for a build that works best and ONLY for your CPU: export RUSTFLAGS=-C target-cpu=native
	@echo "Build this profile? PROFILE='$(PROFILE)' FEATURES='$(FEATURES)'"
	@$(MAKE) _confirm
	ROCKSDB_INCLUDE_DIR=$(ROCKSDB_INCLUDE_DIR) \
		ROCKSDB_LIB_DIR=$(ROCKSDB_LIB_DIR) \
		LD_LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LD_LIBRARY_PATH \
		LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LIBRARY_PATH \
		AWS_LC_SYS_LDFLAGS="-L$(PREFIX)/lib -lssl -lcrypto" \
		AWS_LC_SYS_INCLUDES="$(PREFIX)/include" \
		AWS_LC_RS_NO_BUNDLE=1 \
		AWS_LC_RS_PREBUILT_PATH=$(PREFIX) \
		ROCKSDB_STATIC=$(ROCKSDB_STATIC) \
		ROCKSDB_LIB_STATIC=$(ROCKSDB_LIB_STATIC) \
# 		RUSTFLAGS="-L $(ROCKSDB_LIB_DIR) -l z -l bz2 -l lz4 -l snappy -l zstd -l uring -l stdc++ $$RUSTFLAGS" \
		cargo build --features $(FEATURES) --locked $(CARGO_FLAGS)
	@echo "Build finished! Hard-linking '$(PROFILE)' binary to target/latest/"
	mkdir -p target/latest target/debug
	-ln -f target/$(if $(CARGO_BUILD_TARGET),$(CARGO_BUILD_TARGET)/)$(if $(filter $(PROFILE),dev test),debug,$(PROFILE))/conduwuit target/latest/conduwuit
	-ln -f target/$(if $(CARGO_BUILD_TARGET),$(CARGO_BUILD_TARGET)/)$(if $(filter $(PROFILE),dev test),debug,$(PROFILE))/conduwuit target/debug/conduwuit


.PHONY: build-bundled
build-bundled: ##H Build a bundled binary (Static RocksDB)
	ROCKSDB_INCLUDE_DIR=$(ROCKSDB_INCLUDE_DIR) \
	ROCKSDB_LIB_DIR=$(ROCKSDB_LIB_DIR) \
	LD_LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LD_LIBRARY_PATH \
	LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LIBRARY_PATH \
	ROCKSDB_STATIC=1 \
	ROCKSDB_LIB_STATIC=1 \
	$(MAKE) build FEATURES="$(FEATURES),conduwuit-database/bindgen-static"


GLIBC_VERSION ?= 2.35
CPU_TARGET ?= skylake

.PHONY: build-cross
build-cross: ##H Cross-compile for specific glibc and CPU (uses cargo-zigbuild)
	@echo "Cross-compiling with PROFILE='$(PROFILE)' CPU_TARGET='$(CPU_TARGET)' GLIBC_VERSION='$(GLIBC_VERSION)'"
	@$(MAKE) _confirm
	ROCKSDB_INCLUDE_DIR=$(ROCKSDB_INCLUDE_DIR) \
		ROCKSDB_LIB_DIR=$(ROCKSDB_LIB_DIR) \
		LD_LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LD_LIBRARY_PATH \
		LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LIBRARY_PATH \
		AWS_LC_SYS_LDFLAGS="-L$(PREFIX)/lib -lssl -lcrypto" \
		AWS_LC_SYS_INCLUDES="$(PREFIX)/include" \
		AWS_LC_RS_NO_BUNDLE=1 \
		AWS_LC_RS_PREBUILT_PATH=$(PREFIX) \
		RUSTFLAGS="-C target-cpu=$(CPU_TARGET) -C link-arg=-L/usr/lib -C link-arg=-L/usr/local/lib $$RUSTFLAGS" \
		cargo zigbuild --target x86_64-unknown-linux-gnu.$(GLIBC_VERSION) --features $(FEATURES) --locked $(CARGO_FLAGS)


.PHONY: build-dynamic
build-dynamic: ##H Build with shared library (requires librocksdb.so at runtime)
	ROCKSDB_INCLUDE_DIR=$(ROCKSDB_INCLUDE_DIR) \
	ROCKSDB_LIB_DIR=$(ROCKSDB_LIB_DIR) \
	LD_LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LD_LIBRARY_PATH \
	$(MAKE) build FEATURES="$(FEATURES),conduwuit-database/bindgen-runtime"


.PHONY: release
release: ##H Build a production-ready bundled binary (High-performance, Static RocksDB)
	ROCKSDB_INCLUDE_DIR=$(ROCKSDB_INCLUDE_DIR) \
	ROCKSDB_LIB_DIR=$(ROCKSDB_LIB_DIR) \
	LD_LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LD_LIBRARY_PATH \
	LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LIBRARY_PATH \
	ROCKSDB_STATIC=1 \
	ROCKSDB_LIB_STATIC=1 \
	$(MAKE) build PROFILE=release-max-perf FEATURES="$(FEATURES),conduwuit-database/bindgen-static"


.PHONY: clean
clean:  ##H Clean build directory
	@echo "Clean everything?"
	@$(MAKE) _confirm
	cargo clean
#	-rm -rf target/latest target/debug
# Old logic, wipes it out too much, results in slow builds
#       cargo clean --features default --profile $(PROFILE)
#       @echo "Also remove debian build?"
#       @$(MAKE) _confirm
#       rm -rf target/debian


.PHONY: build-docs
build-docs:     ##H Regenerate docs (admin commands, etc.)
	@echo "Regenerate docs with PROFILE='$(PROFILE)'?"
	@$(MAKE) _confirm
	ROCKSDB_INCLUDE_DIR=$(ROCKSDB_INCLUDE_DIR) \
		ROCKSDB_LIB_DIR=$(ROCKSDB_LIB_DIR) \
		LD_LIBRARY_PATH=$(ROCKSDB_LIB_DIR):$$LD_LIBRARY_PATH \
		AWS_LC_SYS_LDFLAGS="-L$(PREFIX)/lib -lssl -lcrypto" \
		AWS_LC_SYS_INCLUDES="$(PREFIX)/include" \
		AWS_LC_RS_NO_BUNDLE=1 \
		AWS_LC_RS_PREBUILT_PATH=$(PREFIX) \
		cargo run -p xtask $(CARGO_FLAGS) -- generate-docs


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Complement (build docker, stats, run... also used by CI)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

COMPLEMENT_DIR ?=
COMPLEMENT_IMAGE ?= continuwuity:complement
COMPLEMENT_BASE_IMAGE ?= ubuntu:latest

.PHONY: complement/build
complement/build: ##H Build conduwuit w direct_tls
	@echo "Building conduwuit binary with direct_tls feature for Complement..."
	@$(MAKE) _confirm
	$(MAKE) build PROFILE=$(PROFILE) CARGO_FLAGS="--config Cargo.custom.toml --profile $(PROFILE) --features direct_tls"

.PHONY: complement/docker
complement/docker: ##H Build docker image from existing binary
	@echo "Building Complement Docker image using base image: $(COMPLEMENT_BASE_IMAGE)..."
	DOCKER_BUILDKIT=1 docker buildx build \
		--build-arg BASE_IMAGE=$(COMPLEMENT_BASE_IMAGE) \
		--build-arg BINARY_PATH=target/latest/conduwuit \
		--build-arg UID=$(shell id -u) \
		--build-arg GID=$(shell id -g) \
		-t $(COMPLEMENT_IMAGE) \
		-f ./docker/complement.Dockerfile \
		--load .

HOST_LIBS_MOUNTS = $(shell LD_LIBRARY_PATH="$(ROCKSDB_LIB_DIR):$(LD_LIBRARY_PATH)" ldd target/latest/conduwuit | grep -E '=> /' | awk '{print $$3}' | grep -vE '^/usr/local/|libc\.so|libm\.so|libgcc_s\.so|libstdc\+\+\.so|libdl\.so|libpthread\.so|librt\.so|ld-linux' | awk '{print $$1":"$$1":ro"}' | paste -sd ';' - || true)

.PHONY: complement/run
complement/run: ##H Run Complement docker tests locally (requires COMPLEMENT_DIR)
	@test -d "$(COMPLEMENT_DIR)" || (echo "ERROR: COMPLEMENT_DIR ($(COMPLEMENT_DIR)) does not exist" && exit 1)
	@echo "Running Complement tests from $(COMPLEMENT_DIR)..."
	COMPLEMENT_BASE_IMAGE="$(COMPLEMENT_IMAGE)" COMPLEMENT_HOST_MOUNTS="$(PREFIX)/lib:$(PREFIX)/lib:ro$(if $(HOST_LIBS_MOUNTS),;$(HOST_LIBS_MOUNTS))" ./bin/complement $(COMPLEMENT_DIR)


.PHONY: complement/clean
complement/clean: ##H Force-remove all Complement Docker containers and networks
	@echo "Cleaning up Complement docker resources..."
	@docker ps -aq --filter "name=complement_" | xargs -r docker rm -f
	@docker network ls -q --filter "name=complement_" | xargs -r docker network rm
	@echo "Done."

.PHONY: complement/stats
complement/stats: ##H Check local test stats
	@test -f "tests/test_results/complement/test_results.jsonl" || (echo "ERROR: tests/test_results/complement/test_results.jsonl does not exist" && exit 1)
	@echo "Parsing Complement test results..."
	@PASS=$$(jq -s '[.[] | select(.Action == "pass")] | length' tests/test_results/complement/test_results.jsonl); \
	FAIL=$$(jq -s '[.[] | select(.Action == "fail")] | length' tests/test_results/complement/test_results.jsonl); \
	SKIP=$$(jq -s '[.[] | select(.Action == "skip")] | length' tests/test_results/complement/test_results.jsonl); \
	TOTAL=$$((PASS + FAIL + SKIP)); \
	echo ""; \
	if [ "$$FAIL" -gt 0 ] && [ "$$VERBOSE" = "1" ]; then \
		echo "Failed Tests:"; \
		jq -r 'select(.Action == "fail") | .Test' tests/test_results/complement/test_results.jsonl | sort -u; \
		echo ""; \
	fi; \
	echo "=== Complement Test Stats ==="; \
	echo "✓ Passed:  $$PASS"; \
	echo "✗ Failed:  $$FAIL"; \
	echo "○ Skipped: $$SKIP"; \
	echo "---------------------------"; \
	echo "Σ Total:   $$TOTAL"; \
	echo ""; \
	echo "JSON file (on main) last modified by: "; \
	git log -1 --format="%an (%ad) %H" origin/main -- tests/test_results/complement/test_results.jsonl

.PHONY: complement/diff
complement/diff: ##H Diff local results against branch baseline (requires REF=git-ref)
	@test -n "$(REF)" || { echo "ERROR: REF variable is required (e.g. make complement/diff REF=HEAD~1)"; exit 1; }
	@SHA=$$(git rev-parse $(REF)) && \
	git show-ref --quiet origin/_metadata/badges || { echo "ERROR: branch origin/_metadata/badges missing. Run: git fetch origin _metadata/badges"; exit 1; } && \
	FILE=$$(git ls-tree -r origin/_metadata/badges --name-only | grep -E "runs_data/($${SHA:0:8}|$$SHA).*\.jsonl" | head -n 1) && \
	if [ -z "$$FILE" ]; then \
		echo "ERROR: could not find results for $$SHA (or $${SHA:0:8}) in origin/_metadata/badges"; \
		exit 1; \
	fi; \
	diff -u --color tests/test_results/complement/test_results.jsonl <(git show origin/_metadata/badges:$$FILE)



.PHONY: complement/logs
complement/logs: ##H Tail logs for all running and future Complement containers
	@echo "Tailing logs for Complement containers (Press Ctrl+C to stop)..."
	@docker ps -q --filter "name=complement_" | xargs -r -n 1 docker logs -f & \
	docker events --filter 'event=start' --format '{{.Actor.Attributes.name}}' | grep --line-buffered "^complement_" | xargs -r -I {} sh -c 'echo "--- Tailing {} ---"; docker logs -f {} &' ; wait



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# GitHub CI/build related targets
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

GH_REPO ?=

GH_CACHE_KEY ?=

PREBUILT_TAG ?= prebuilts-v0.5

.PHONY: download/prebuilts
download/prebuilts: ##H Download prebuilt libraries from GitHub Release
	@test "$(GH_REPO)" || (echo "ERROR: GH_REPO is not set. Add GH_REPO=owner/repo to .env" && exit 1)
	@test "$(CPU_TARGET)" || (echo "ERROR: CPU_TARGET is not set (e.g. x86-64-v3). Add CPU_TARGET=... to .env" && exit 1)
	@test "$(OS_VERSION)" || (echo "ERROR: OS_VERSION is not set (e.g. ubuntu-24.04). Add OS_VERSION=... to .env" && exit 1)
	@echo "Downloading prebuilts for $(CPU_TARGET) on $(OS_VERSION) from $(PREBUILT_TAG)..."
	@mkdir -p .tmp/prebuilts && rm -rf .tmp/prebuilts/*
	@if gh release download $(PREBUILT_TAG) -R $(GH_REPO) -p "*-$(CPU_TARGET)-$(OS_VERSION).tar.gz" -D .tmp/prebuilts --clobber; then \
		for f in .tmp/prebuilts/*.tar.gz; do \
			echo "Extracting $$f to $(PREFIX)..."; \
			sudo tar -xzvf "$$f" -C $(PREFIX); \
		done; \
		sudo ldconfig; \
		echo "Prebuilts installed."; \
	else \
		echo "ERROR: Failed to download prebuilts. Check tag, repo, and pattern."; \
		exit 1; \
	fi
	@rm -rf .tmp/prebuilts

.PHONY: download/clear-cache
download/clear-cache: ##H Delete GitHub Actions caches
	# Testing you have explicitly set GH_CACHE_KEY
	@if [ -z "$(GH_CACHE_KEY)" ]; then \
		gh cache list -R "$(GH_REPO)" --limit 100; \
		echo ""; \
		echo "Hint: Use GH_CACHE_KEY=prefix to delete caches."; \
	else \
		echo "Deleting GitHub Actions caches matching prefix: $(GH_CACHE_KEY)..."; \
		gh cache list -R "$(GH_REPO)" --key "$(GH_CACHE_KEY)" --limit 1000 | awk '{print $$1}' \
			| grep -v 'ID' | grep -v 'KEYS' \
			| xargs -r -I {} sh -c 'echo "Deleting cache: {}" && gh cache delete -R "$(GH_REPO)" {}'; \
	fi


CPU_TARGET ?=
OS_VERSION ?=

GH_ARTIFACT_NAME ?= conduwuit
GH_ARTIFACT_SUFFIX ?= $(CPU_TARGET)-$(OS_VERSION)
ARTIFACT ?= $(GH_ARTIFACT_NAME)$(GH_ARTIFACT_SUFFIX)

RUN ?=
N_RUNS ?= 6

.PHONY: download
download:	##H Download CI binary (set RUN to a specific RunID)
	# Testing whether GH_REPO is set
	test "$(GH_REPO)"
	# Testing whether ARTIFACT related vars are set
	(test "$(OS_VERSION)" && test "$(CPU_TARGET)" ) || test "$(ARTIFACT)"
	@mkdir -p target/ci
	# Checking version of old binary, if it exists
	@-./target/ci/conduwuit -V
	@rm -rf target/ci/*
	gh run download $(RUN) -R $(GH_REPO) -n $(ARTIFACT) -D target/ci
	@if [ -f "target/ci/$(ARTIFACT).tar.gz" ]; then \
		tar -xzf "target/ci/$(ARTIFACT).tar.gz" -C target/ci; \
	fi
	@if [ -f "target/ci/bin/conduwuit" ]; then \
		mv target/ci/bin/conduwuit target/ci/conduwuit; \
	fi
	@if [ ! -f "target/ci/conduwuit" ]; then \
		echo "ERROR: Expected binary target/ci/conduwuit not found after download/extract."; \
		exit 1; \
	fi
	@chmod +x target/ci/conduwuit
	@echo "Downloaded to target/ci/conduwuit"
	@./target/ci/conduwuit -V
	@ln -sfn ci target/latest

.PHONY: download/hash
download/hash:	##H Download CI binary by Git commit hash (set HASH=)
	@test "$(GH_REPO)" || (echo "ERROR: GH_REPO is not set. Add GH_REPO=owner/repo to .env" && exit 1)
	@test "$(HASH)" || (echo "ERROR: HASH is not set (e.g., make download/hash HASH=bdbb016)" && exit 1)
	@RUN_ID=$$(gh run list -R "$(GH_REPO)" --commit "$(HASH)" --json databaseId -q '.[0].databaseId') && \
	if [ -z "$$RUN_ID" ] || [ "$$RUN_ID" = "null" ]; then \
		echo "ERROR: Could not find any CI runs for commit $(HASH)"; \
		exit 1; \
	else \
		echo "Found Run ID $$RUN_ID for commit $(HASH). Executing standard download..."; \
		$(MAKE) download RUN="$$RUN_ID" ARTIFACT="$(ARTIFACT)"; \
	fi

.PHONY: download/list
download/list:	##H List recent CI runs
	@test "$(GH_REPO)" || (echo "ERROR: GH_REPO is not set. Add GH_REPO=owner/repo to .env" && exit 1)
	mkdir -p .tmp && echo '**' > .tmp/.gitignore
	# gh run list -R $(GH_REPO) --limit $(N_RUNS)
	@echo "Fetching latest $(N_RUNS) runs and their artifacts in parallel..."
	@echo ""
	@gh run list -R "$(GH_REPO)" --limit $(N_RUNS) --json databaseId,workflowName,headBranch,headSha,event,conclusion,status > .tmp/runs.json
	@bash -c ' \
	for ID in $$(jq -r ".[].databaseId" .tmp/runs.json); do \
		gh api "repos/$(GH_REPO)/actions/runs/$$ID/artifacts" --jq ".artifacts[].name" > ".tmp/artifacts_$$ID.txt" & \
	done; \
	wait; \
	jq -c ".[]" .tmp/runs.json | while read -r run; do \
		ID=$$(echo "$$run" | jq -r ".databaseId"); \
		STATUS=$$(echo "$$run" | jq -r "if .status == \"completed\" then .conclusion else .status end"); \
		WORKFLOW=$$(echo "$$run" | jq -r ".workflowName"); \
		BRANCH=$$(echo "$$run" | jq -r ".headBranch"); \
		SHA=$$(echo "$$run" | jq -r ".headSha" | cut -c 1-7); \
		ICON="-"; \
		if [ "$$STATUS" = "success" ]; then ICON="✓"; fi; \
		if [ "$$STATUS" = "failure" ]; then ICON="X"; fi; \
		if [ "$$STATUS" = "in_progress" ]; then ICON="*"; fi; \
		printf "%-2s %-20s %-30s %-15s (ID: %s)\n" "$$ICON" "$$WORKFLOW" "$$BRANCH ($$SHA)" "$$STATUS" "$$ID"; \
		if [ -s ".tmp/artifacts_$$ID.txt" ]; then \
			while read -r artifact; do \
				echo "    └─ $$artifact"; \
			done < ".tmp/artifacts_$$ID.txt"; \
		else \
			echo "    └─ (No artifacts)"; \
		fi; \
		echo ""; \
		rm -f ".tmp/artifacts_$$ID.txt"; \
	done'
	@rm -f .tmp/runs.json

# Binary name
CONTINUWUITY ?= conduwuit
# systemctl service name
C10Y_SERV ?= conduwuit.service

# Configure these in .env if alternate path(s) are desired
BUILD_BIN_DIR ?= target/latest
DEPLOY_BIN_DIR ?= $(PREFIX)/bin

BUILD_BIN ?= $(BUILD_BIN_DIR)/$(CONTINUWUITY)
DEPLOY_BIN ?= $(DEPLOY_BIN_DIR)/$(CONTINUWUITY)

.PHONY: install
install:	##H Install (executed on VPS)
	@echo "Install $(CONTINUWUITY) to $(DEPLOY_BIN)?"
	@$(MAKE) _confirm
	# You may need to run with sudo or adjust REMOTE_BIN_DIR if this fails
	install -b -p -m 755 $(BUILD_BIN) $(DEPLOY_BIN)
	@echo "Installation complete."


.PHONY: restart
restart:    ##H Restart service (using systemctl)
	sudo systemctl restart $(C10Y_SERV)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Help command (messes up vim/sublime syntax, so stuck at end)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.PHONY: _help
_help: ##H Show this help, list available targets
	@grep -hE '^[a-zA-Z0-9_\/-]+:.*?##H .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?##H "}; {printf "$(STYLE_CYAN)%-20s$(STYLE_RESET) %s\n", $$1, $$2}'
