
PROJECT := xWAL

OK := "✅"
NOK := "❌"


.DEFAULT := help

.PHONY: help
help:  ## Displays help message
	@echo "Makefile to control tasks for $(PROJECT) project"
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: protobuf
protobuf: ./scripts/compile_protocol_buffers.sh ## Compile Protocol Buffers files
	@. $^

.PHONY: tidy
tidy: go.mod ## Runs go mod tidy on the project
	$(info Tidying project dependencies..)
	@go mod tidy && echo $(OK) || echo $(NOK)

.PHONY: run
run: cmd/xwal/main.go ## Runs the library executable
	$(info Running library binary..)
	@go run $^

.PHONY: test
test: ## Runs the library tests
	$(info Running project tests..)
	@go clean -testcache
	@go test -race ./... && echo $(OK) || echo $(NOK)

.PHONY: lint
lint: ## Runs the Golang Linter
	$(info Linting source code..)
	@golangci-lint run && echo $(OK) || echo $(NOK)

.PHONY: setup
setup: ./scripts/local_setup.sh ## Sets up the local machine with OS level tools and dependencies
	$(info Setting up local development environment..)
	@. $^ && echo $(OK) || echo $(NOK)


.PHONY: build
build: cmd/xwal/main.go ## Builds a library binary
	$(info Building xWAL project..)
	@go build -race -o bin/xwal $^ && echo $(OK) || echo $(NOK)
