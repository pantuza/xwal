# Contributing to xwal

Thanks for your interest in improving xwal.

## Before you open a PR

1. **Branch:** Work on a feature branch off `main`.
2. **Verify:** Run `make check` locally. It runs `go mod tidy`, lint, tests (with the race detector), benchmarks, builds all examples, and runs the simple example.
3. **CI:** Ensure GitHub Actions passes on your branch (Linux, macOS, Windows).
4. **Changelog:** For user-visible or API changes, add an entry under `## [Unreleased]` in [CHANGELOG.md](CHANGELOG.md) (Keep a Changelog style).

## Tooling

- **Go:** Version pinned in [go.mod](go.mod); use the same toolchain for local work and golangci-lint.
- **Lint:** [.golangci.yml](.golangci.yml) targets **golangci-lint v2** (same as CI). Install v2, for example:
  `GOTOOLCHAIN=go1.26.0 go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.4`
  or run `make setup`. The Makefile prefers `$(go env GOPATH)/bin/golangci-lint`. Older golangci-lint v1 releases may not load this config correctly.

## Commits

Keep commits small and focused. Conventional-style prefixes help (`feat:`, `fix:`, `docs:`, etc.).

## Maintainer notes

See [AGENTS.md](AGENTS.md) for release tagging and CI details.
