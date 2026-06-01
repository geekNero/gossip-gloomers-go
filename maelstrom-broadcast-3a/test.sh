#!/usr/bin/env bash
# https://fly.io/dist-sys/3a/
set -euo pipefail

go build -o maelstrom-broadcast .
maelstrom test -w broadcast --bin ./maelstrom-broadcast --node-count 1 --time-limit 20 --rate 10
