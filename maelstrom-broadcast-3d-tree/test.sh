#!/usr/bin/env bash
# https://fly.io/dist-sys/3d/
set -euo pipefail

go build -o maelstrom-broadcast .
maelstrom test -w broadcast --bin ./maelstrom-broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
