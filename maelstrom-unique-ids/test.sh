#!/usr/bin/env bash
# https://fly.io/dist-sys/2/
set -euo pipefail

go build -o maelstrom-unique-ids .
maelstrom test -w unique-ids --bin ./maelstrom-unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition "$@"
