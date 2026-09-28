.PHONY: help lint fmt fmt-check clippy ensure-nightly \
        ch ch-query ch-migrate ch-migrations \
        run-tests run-tests-integration

# ---- config ----------------------------------------------------------------
CARGO              ?= cargo
NIGHTLY            ?= +nightly
NIGHTLY_TOOLCHAIN  ?= nightly

CLIPPY_FLAGS       ?= --workspace --all-targets --all-features
CLIPPY_DENY        ?= -D warnings

MIGRATION_DIR      ?= ./migration
CLICKHOUSE_HOST    ?= localhost:9000
CLICKHOUSE_DATABASE?= monitoring
CLICKHOUSE_USERNAME?= default
CLICKHOUSE_PASSWORD?= password
CLICKHOUSE_CONTAINER ?= solana-sentinel-clickhouse-1

RUST_LOG           ?= info

# ---- targets ---------------------------------------------------------------

help: ## Show this help message
	@printf "Available targets:\n\n"
	@printf "Rust:\n"
	@printf "  %-14s %s\n" "fmt"           "Format the workspace with nightly rustfmt"
	@printf "  %-14s %s\n" "fmt-check"     "Check formatting (no writes, CI-friendly)"
	@printf "  %-14s %s\n" "clippy"        "Run clippy on the workspace, deny warnings"
	@printf "  %-14s %s\n" "lint"          "Run fmt-check + clippy (use in CI / pre-push)"
	@printf "\nClickHouse:\n"
	@printf "  %-14s %s\n" "ch"            "Open clickhouse-client shell in the container"
	@printf "  %-14s %s\n" "ch-query"      "Run a one-off query (Q=\"...\")"
	@printf "  %-14s %s\n" "ch-migrate"    "Apply pending ClickHouse migrations"
	@printf "  %-14s %s\n" "ch-migrations" "Show applied migrations"
	@printf "
Tests:
"
	@printf "  %-20s %s\n" "run-tests"             "Run the workspace test suite (no Docker needed)"
	@printf "  %-20s %s\n" "run-tests-integration" "Run the Redis-backed alerting tests (needs Docker)"
	@printf "\nVariables:\n"
	@printf "  %-22s %s\n" "CARGO"                "$(CARGO)"
	@printf "  %-22s %s\n" "NIGHTLY_TOOLCHAIN"    "$(NIGHTLY_TOOLCHAIN)"
	@printf "  %-22s %s\n" "CLICKHOUSE_HOST"      "$(CLICKHOUSE_HOST)"
	@printf "  %-22s %s\n" "CLICKHOUSE_DATABASE"  "$(CLICKHOUSE_DATABASE)"
	@printf "  %-22s %s\n" "CLICKHOUSE_CONTAINER" "$(CLICKHOUSE_CONTAINER)"
	@printf "  %-22s %s\n" "RUST_LOG"             "$(RUST_LOG)"


ensure-nightly:
	@if ! rustup run $(NIGHTLY_TOOLCHAIN) rustfmt --version >/dev/null 2>&1; then \
		printf "Installing %s toolchain with rustfmt...\n" "$(NIGHTLY_TOOLCHAIN)"; \
		rustup toolchain install $(NIGHTLY_TOOLCHAIN) --profile minimal --component rustfmt --no-self-update; \
		printf "Nightly toolchain ready\n"; \
	fi

fmt: ensure-nightly ## Format with nightly rustfmt
	@printf "Formatting workspace...\n"
	@$(CARGO) $(NIGHTLY) fmt --all
	@printf "Formatted\n"

fmt-check: ensure-nightly ## Check formatting without writing
	@printf "Checking formatting...\n"
	@$(CARGO) $(NIGHTLY) fmt --all -- --check && \
		printf "Formatting OK\n" || \
		(printf "Formatting issues found\n"; exit 1)

clippy: ## Run clippy with -D warnings
	@printf "Running clippy...\n"
	@$(CARGO) clippy $(CLIPPY_FLAGS) -- $(CLIPPY_DENY) && \
		printf "Clippy clean\n" || \
		(printf "Clippy reported issues\n"; exit 1)

lint: fmt-check clippy ## Run fmt-check + clippy
	@printf "All checks passed\n"

# ---- clickhouse ------------------------------------------------------------

ch: ## Open clickhouse-client shell in the container
	@printf "Opening clickhouse-client in %s...\n" "$(CLICKHOUSE_CONTAINER)"
	@docker exec -it $(CLICKHOUSE_CONTAINER) \
		clickhouse-client \
		--user $(CLICKHOUSE_USERNAME) \
		--password $(CLICKHOUSE_PASSWORD) \
		--database $(CLICKHOUSE_DATABASE)

ch-query: ## Run a one-off query: make ch-query Q="SELECT count() FROM alerts"
	@if [ -z "$(Q)" ]; then \
		printf "Usage: make ch-query Q=\"<sql>\"\n"; \
		exit 1; \
	fi
	@docker exec $(CLICKHOUSE_CONTAINER) \
		clickhouse-client \
		--user $(CLICKHOUSE_USERNAME) \
		--password $(CLICKHOUSE_PASSWORD) \
		--database $(CLICKHOUSE_DATABASE) \
		--query "$(Q)"

ch-migrate: ## Apply pending ClickHouse migrations
	@printf "Running migrations against %s...\n" "$(CLICKHOUSE_HOST)"
	@cd $(MIGRATION_DIR) && \
		CLICKHOUSE_HOST=$(CLICKHOUSE_HOST) \
		CLICKHOUSE_DATABASE=$(CLICKHOUSE_DATABASE) \
		CLICKHOUSE_USERNAME=$(CLICKHOUSE_USERNAME) \
		CLICKHOUSE_PASSWORD=$(CLICKHOUSE_PASSWORD) \
		RUST_LOG=$(RUST_LOG) cargo run
	@printf "Migrations completed\n"

ch-migrations: ## Show applied migrations
	@printf "Applied migrations:\n"
	@docker exec $(CLICKHOUSE_CONTAINER) \
		clickhouse-client \
		--user $(CLICKHOUSE_USERNAME) \
		--password $(CLICKHOUSE_PASSWORD) \
		--database $(CLICKHOUSE_DATABASE) \
		--query "SELECT version, name, applied_on FROM refinery_schema_history ORDER BY version FORMAT PrettyCompact"

run-tests: ## Run the workspace test suite
	@printf "Running tests...\n"
	@$(CARGO) test --workspace
	@printf "Tests completed\n"

run-tests-integration: ## Run the Redis-backed alerting tests (needs Docker)
	@$(CARGO) test -p alerting -- --ignored --nocapture
