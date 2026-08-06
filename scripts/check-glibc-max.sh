#!/usr/bin/env bash
# check-glibc-max.sh — Enforce a glibc symbol-version ceiling on ELF binaries.
#
# Release workflows build Linux artifacts inside the manylinux_2_28 container
# (AlmaLinux 8, glibc 2.28). This guard fails loudly if an artifact ever links
# against newer GLIBC_* symbol versions — e.g. after an accidental move back to
# a bare ubuntu-latest runner — so the portability floor (RHEL/Alma/Rocky 8,
# Debian 10+, Ubuntu 20.04+) cannot regress silently.
#
# Usage:
#   ./scripts/check-glibc-max.sh <max-glibc-version> <file> [<file>...]
#   ./scripts/check-glibc-max.sh 2.28 target/release/xbbg-mcp
#
# Exit codes:
#   0  All files reference only GLIBC_<= max-glibc-version symbols.
#   1  At least one file requires a newer glibc.
#   2  Usage or tooling error (missing file, not a dynamic ELF, no objdump).
set -euo pipefail

if [ "$#" -lt 2 ]; then
    echo "usage: $0 <max-glibc-version> <file> [<file>...]" >&2
    exit 2
fi

MAX="$1"
shift

# Override the binutils tool for cross builds/tests, e.g. OBJDUMP=llvm-objdump.
OBJDUMP="${OBJDUMP:-objdump}"
command -v "$OBJDUMP" >/dev/null 2>&1 || { echo "error: $OBJDUMP not found" >&2; exit 2; }

status=0
for file in "$@"; do
    if [ ! -f "$file" ]; then
        echo "error: no such file: $file" >&2
        exit 2
    fi
    if ! "$OBJDUMP" -T "$file" >/dev/null 2>&1; then
        echo "error: not a dynamic ELF object: $file" >&2
        exit 2
    fi

    newest=$("$OBJDUMP" -T "$file" \
        | grep -oE 'GLIBC_[0-9]+(\.[0-9]+)+' \
        | sed 's/^GLIBC_//' \
        | sort -uV \
        | tail -n 1 || true)

    if [ -z "$newest" ]; then
        echo ":: $file — no GLIBC symbol-version references (OK)"
        continue
    fi

    highest=$(printf '%s\n%s\n' "$newest" "$MAX" | sort -V | tail -n 1)
    if [ "$highest" = "$MAX" ]; then
        echo ":: $file — max GLIBC_$newest <= GLIBC_$MAX (OK)"
    else
        echo "FAIL: $file requires GLIBC_$newest > GLIBC_$MAX" >&2
        echo "Offending symbols:" >&2
        "$OBJDUMP" -T "$file" | grep "GLIBC_$newest" | head -n 20 >&2 || true
        status=1
    fi
done

exit "$status"
