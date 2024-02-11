

.DEFAULT := help

.PHONY: help
help:  ## Displays help message
	@echo "Makefile to control tasks for credit card bill"
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: protobuf
protobuf: ./scripts/compile_protocol_buffers.sh ## Compile Protocol Buffers files
	@. $^

.PHONY: tidy
tidy: go.mod ## Runs go mod tidy on the project
	@go mod tidy

.PHONY: run
run: cmd/xwal/main.go ## Runs the library executable
	@go run $^
