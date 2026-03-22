#!/usr/bin/env bash
# Run a command with progressive terminal output when possible.
#
# Problem: piping `go test` into sed removes the TTY, so Go block-buffers and
# output appears only at the end. Fix: run under a PTY (script) when it works,
# else try stdbuf -oL, else fall back to plain execution. Downstream we always
# use line-buffered Perl (not multi-stage sed) for PASS/FAIL/SKIP labels.
#
# Usage: scripts/colorize_stream.sh <command> [args...]
# Exit status: exit status of <command> (not the colorizer).

set -u

pretty_stream() {
	perl -pe 'BEGIN{$|=1} s/\bPASS\b/✅ PASS/g; s/\bFAIL\b/❌ FAIL/g; s/\bSKIP\b/🔕 SKIP/g;'
}

# Run "$@" and pipe through pretty_stream; set global _last_rc to first stage exit.
_pipe_pretty() {
	set +e
	"$@" 2>&1 | pretty_stream
	_last_rc=${PIPESTATUS[0]:-1}
	set -e
}

try_stdbuf_or_plain() {
	if command -v stdbuf >/dev/null 2>&1; then
		_pipe_pretty stdbuf -oL -eL "$@"
	else
		_pipe_pretty "$@"
	fi
}

main() {
	if [[ $# -lt 1 ]]; then
		echo "usage: $0 command [args...]" >&2
		exit 2
	fi

	local _last_rc
	local os
	os=$(uname -s)

	case "$os" in
	Linux)
		if script -V 2>&1 | grep -qi util-linux; then
			local quoted
			quoted=$(printf '%q ' "$@")
			# -q quiet, -e child exit status, -c command (util-linux script)
			# shellcheck disable=SC2086
			set +e
			script -qec "${quoted}" /dev/null 2>&1 | pretty_stream
			_last_rc=${PIPESTATUS[0]:-1}
			set -e
		else
			try_stdbuf_or_plain "$@"
		fi
		;;
	Darwin | FreeBSD | OpenBSD)
		# Probe: some environments (CI, nested shells) cannot allocate a PTY.
		# Do not use /bin/true — on macOS `true` lives in /usr/bin; /bin/true may be missing.
		if script -q /dev/null sh -c 'exit 0' >/dev/null 2>&1; then
			set +e
			script -q /dev/null "$@" 2>&1 | pretty_stream
			_last_rc=${PIPESTATUS[0]:-1}
			set -e
		else
			try_stdbuf_or_plain "$@"
		fi
		;;
	*)
		try_stdbuf_or_plain "$@"
		;;
	esac

	exit "$_last_rc"
}

main "$@"
