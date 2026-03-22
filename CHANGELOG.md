# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - 2026-03-22

### Fixed

- **CI:** Use `golangci-lint-action` **v7** so the requested **golangci-lint v2.11.4** installs correctly (v6 only supports golangci-lint v1).

[0.3.1]: https://github.com/pantuza/xwal/releases/tag/v0.3.1

## [0.3.0] - 2026-03-22

### Changed

- **Go:** Require Go 1.26 (module and CI updated from 1.22).
- **Lint:** CI and `make setup` use golangci-lint **v2.11.4**, built with Go 1.26 so analysis matches the module version; the Makefile prefers `$(go env GOPATH)/bin/golangci-lint` when present.

[0.3.0]: https://github.com/pantuza/xwal/releases/tag/v0.3.0

## [0.2.2] - 2026-03-22

### Fixed

- **LocalFS (Windows):** Close the active segment file before renaming it to garbage during replay; resolve path identity with `os.SameFile` when short vs long paths differ.
- **LocalFS:** Close the temporary read handle in `getLastLogSequencyNumber` (leaked handle blocked renames on Windows).
- **xWAL:** On backend replay error, close the replay channel and wait for the callback goroutine to avoid races.
- **Tests:** Close file handles in LocalFS tests before rename/delete; AWS S3 integration tests skip when Docker/Localstack is unavailable (e.g. macOS CI).

### Changed

- **CI:** Git checkout for releases uses full history (`fetch-depth: 0`) so GoReleaser changelogs include all commits.

[0.2.2]: https://github.com/pantuza/xwal/releases/tag/v0.2.2
