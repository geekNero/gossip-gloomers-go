#!/usr/bin/env bash
set -euo pipefail

TREES_DIR="$(dirname "$0")/trees"

find "$TREES_DIR" -name '*.dot' | while read -r dot; do
    neato -Tsvg "$dot" -o "${dot%.dot}.svg"
done
