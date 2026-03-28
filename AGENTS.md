# Agent / maintainer notes (xwal)

For humans and coding assistants. Not tied to any single AI product.

## Stack

- **Go:** version in [`go.mod`](go.mod) (toolchain pinned there).
- **Module:** `github.com/pantuza/xwal`. Core WAL API: root `package xwal` (Go sources next to `go.mod`, import path `github.com/pantuza/xwal`).

## Verify before push

```bash
make check
```

Runs `tidy`, `lint`, `test` (race, packages from `go list`, examples excluded from tests), `bench`, builds all [`examples/*`](examples/), runs the simple example. **CI** runs tidy + build, test, lint, bench per [`.github/workflows/main.yml`](.github/workflows/main.yml) (no `make run`).

## Commits

- Work on a separated branch.
- Small, focused changes; one commit per file. Prefix with `feat:`, `fix:`, `docs:`, etc.
- If you are an AI agent, do not use gpg signature: `git commit --no-gpg-sign`.

## Releases

1. Update [`CHANGELOG.md`](CHANGELOG.md) (Keep a Changelog, SemVer).
2. Merge work to **`main`**.
3. Check if CI passes and all tests/benchmarks/examples run fine. Use gh CLI for it.
4. Annotated tag: **`vX.Y.Z`**, then `git push origin main` and `git push origin vX.Y.Z`. Always add a brief to the new release in Github.
5. Tag triggers [`.github/workflows/release.yml`](.github/workflows/release.yml) and [`.goreleaser.yaml`](.goreleaser.yaml). Optional release intro text: `release.header` in GoReleaser config.

## Observability

OpenTelemetry metrics/traces: [`OBSERVABILITY.md`](OBSERVABILITY.md). Local demo: `make run_telemetry_example`.
