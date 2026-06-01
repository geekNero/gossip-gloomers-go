#!/usr/bin/env bash
# https://fly.io/dist-sys/3c/
set -euo pipefail

go build -o maelstrom-broadcast .
maelstrom test -w broadcast --bin ./maelstrom-broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition
