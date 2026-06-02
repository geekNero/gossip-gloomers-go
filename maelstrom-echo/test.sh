#!/usr/bin/env bash
# https://fly.io/dist-sys/1/
set -euo pipefail

go build -o maelstrom-echo .
maelstrom test -w echo --bin ./maelstrom-echo --node-count 1 --time-limit 10 "$@"
