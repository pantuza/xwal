
PROJECT := xWAL

OK := ✅
NOK := ❌
SKIP := 🔕

SED := $(shell which sed)

GOTEST_COLORIZE := | $(SED) 's/PASS/$(OK) PASS/g' | $(SED) 's/FAIL/$(NOK) FAIL/g' | $(SED) 's/SKIP/$(SKIP) SKIP/g'

# ANSI Colors definitions
CYAN := \033[0;36m
BROWN := \033[0;33m
RED := \033[0;31m
GREEN := \033[0;32m
BLUE := \033[0;34m
CLRRST := \033[0m

# List of directories containing examples of the library usage. Used for compiling each example code.
EXAMPLES_DIRS := $(wildcard examples/*)

# Title function is used to print a pretty message explaining what the target being executed is about
define title
@echo "\n$(CYAN) • $(BROWN)$(1)$(CLRRST)"
endef

.DEFAULT := help


.PHONY: help
help:  ## Displays help message
	@echo "Makefile to control tasks for $(BLUE)$(PROJECT)$(CLRRST) project"
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)


.PHONY: protobuf
protobuf: ./scripts/compile_protocol_buffers.sh ## Compile Protocol Buffers files
	$(call title, Compiling protocol buffer files..)
	@. $^


.PHONY: tidy
tidy: go.mod ## Runs go mod tidy on the project
	$(call title, Tidying project dependencies..)
	@go mod tidy && echo $(OK) || echo $(NOK)


.PHONY: run
run: cmd/xwal/main.go ## Runs the library executable
	$(call title, Running library binary..)
	@go run $^


.PHONY: test
test: ## Runs the library tests
	$(call title, Running project tests..)
	@go clean -testcache
	@go test ./... -race -test.v $(GOTEST_COLORIZE)


.PHONY: bench
bench: ## Runs the library benchmarks
	$(call title, Running project benchmarks..)
	@go clean -testcache
	@go test ./benchmark/ -race -bench . -count 5 $(GOTEST_COLORIZE)


.PHONY: profile
profile: ## Generates CPU and Memory Profiles from Benchmarks
	$(call title, Running project benchmarks..)
	@go clean -testcache
	@go test ./benchmark/ -race -bench . -count 5 \
		-benchmem -memprofile profiles/mem-profile.out \
		-cpuprofile profiles/cpu-profile.out \
		$(GOTEST_COLORIZE)


.PHONY: clean_profile
clean_profile: ## Cleans profiles data from profiles directory
	$(call title, Cleaning profiling data..)
	@rm -vf profiles/*.out


.PHONY: lint
lint: ## Runs the Golang Linter
	$(call title, Linting source code..)
	@golangci-lint run && echo $(OK) || echo $(NOK)


.PHONY: setup
setup: ./scripts/local_setup.sh ## Sets up the local machine with OS level tools and dependencies
	$(call title, Setting up local development environment..)
	@. $^ && echo $(OK) || echo $(NOK)


.PHONY: build
build: cmd/xwal/main.go ## Builds a library binary
	$(call title, Building xWAL project..)
	@go build -race -o bin/xwal $^ && echo $(OK) || echo $(NOK)


.PHONY: check
check: _clrscr tidy lint test bench build run ## Runs all checks before sending code remote repository
	@echo "$(BROWN)--\n$(OK) $(GREEN)All checks have passed$(CLRRST)"


.PHONY: run_localfs_example
run_localfs_example: ./examples/localfs/main.go _clrscr ## Runs xWAL with LocalFS backend example
	$(call title, Running xWAL with LocalFS backend example..)
	@go run $<


.PHONY: _clrscr
_clrscr:
	@clear
