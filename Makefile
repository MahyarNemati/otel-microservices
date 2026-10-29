# Convenience targets. CGO_ENABLED=0 is set for local builds only: the macOS
# SDK on the development host cannot link cgo. CI must not set it, because
# `go test -race` requires cgo.
SHELL := /usr/bin/env bash
export CGO_ENABLED ?= 0

GO_PKGS := ./...
NODE_DIR := services/fulfillment

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the three Go binaries into bin/
	@mkdir -p bin
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/orders ./cmd/orders
	go build -o bin/inventory ./cmd/inventory

.PHONY: test
test: ## Run the Go and Node test suites
	go test -count=1 $(GO_PKGS)
	cd $(NODE_DIR) && npm test

.PHONY: test-verbose
test-verbose: ## Run both suites verbosely, so you can see what actually ran
	go test -count=1 -v $(GO_PKGS)
	cd $(NODE_DIR) && npm test -- --reporter=verbose

.PHONY: lint
lint: ## gofmt, go vet, tsc and shellcheck
	@unformatted="$$(gofmt -l .)"; \
	 if [ -n "$$unformatted" ]; then echo "not gofmt'd:"; echo "$$unformatted"; exit 1; fi
	go vet $(GO_PKGS)
	cd $(NODE_DIR) && npm run typecheck
	shellcheck -S warning scripts/*.sh

.PHONY: validate
validate: ## Validate every YAML config, rejecting duplicate keys
	python3 scripts/validate-yaml.py

.PHONY: up
up: ## Bring the whole stack up and wait for it
	docker compose up -d --build --wait --wait-timeout 420

.PHONY: down
down: ## Tear the stack down, including volumes
	docker compose down -v

.PHONY: verify
verify: ## Assert the running stack produces correlated telemetry
	./scripts/verify-stack.sh

.PHONY: traffic
traffic: ## Drive 20 requests through the gateway
	./scripts/traffic.sh 20

.PHONY: chaos
chaos: ## Inject latency and faults, then replay traffic
	./scripts/chaos.sh --latency 40 --faults 15 --count 100

.PHONY: logs
logs: ## Tail the application logs
	docker compose logs -f gateway orders inventory fulfillment

.PHONY: clean
clean: ## Remove build output
	rm -rf bin $(NODE_DIR)/dist coverage.out
