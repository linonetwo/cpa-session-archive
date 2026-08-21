#!/usr/bin/env bash
set -euo pipefail
: "${ARGV_OUTPUT:?ARGV_OUTPUT is required}"
printf '%s\n' "$@" > "${ARGV_OUTPUT}"
