#!/usr/bin/env bash
# https://fly.io/dist-sys/4/
set -euo pipefail

go build -o maelstrom-counter .
maelstrom test -w g-counter --bin ./maelstrom-counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition "$@"
