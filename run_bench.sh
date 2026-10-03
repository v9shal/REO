#!/usr/bin/env bash
# Run from the REO repo root (where main.go / store.go live).
# Copies of data.log go to a temp dir so your repo stays clean.
#
#   ./run_bench.sh                      # defaults
#   CONNS=100 DUR=15s KEYS=500000 ./run_bench.sh
set -euo pipefail

CONNS=${CONNS:-50}
DUR=${DUR:-10s}
KEYS=${KEYS:-100000}
SIZE=${SIZE:-64}
TIERED_CAP=${TIERED_CAP:-$((KEYS / 10))}   # 10% of keyspace stays in RAM

if ! grep -q REO_MAX_RAM_KEYS store.go; then
  echo "WARNING: store.go has no REO_MAX_RAM_KEYS support (apply store-fixes.patch first)."
  echo "         Tiered scenarios will not be meaningful."
fi

TMP=$(mktemp -d)
SP=
cleanup() { if [ -n "$SP" ]; then kill "$SP" 2>/dev/null || true; fi; rm -rf "$TMP"; }
trap cleanup EXIT
go build -o "$TMP/reo" .
go build -o "$TMP/bench" ./bench

echo "=== Environment (report this next to your numbers) ==="
echo "go:    $(go version)"
echo "os:    $(uname -sr)"
if [ -r /proc/cpuinfo ]; then
  echo "cpu:   $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)  x$(nproc) threads"
else
  echo "cpu:   $(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)  x$(sysctl -n hw.ncpu 2>/dev/null || echo ?)"
fi
echo "load generator and server run on the SAME machine"
echo

run() { # label cap dist
  local label=$1 cap=$2 dist=$3
  local dir="$TMP/run-$RANDOM"; mkdir -p "$dir"; cd "$dir"
  REO_MAX_RAM_KEYS=$cap "$TMP/reo" >/dev/null 2>&1 &
  SP=$!
  sleep 1
  echo "################ $label  (RAM cap=$cap keys, keyspace=$KEYS, dist=$dist)"
  "$TMP/bench" -c "$CONNS" -d "$DUR" -keys "$KEYS" -size "$SIZE" -dist "$dist" | sed -n '/CORRECTNESS/,$p'
  local rss; rss=$(ps -o rss= -p "$SP" | awk '{printf "%.0f", $1/1024}')
  echo "server RSS: ${rss} MB   data.log: $(du -h data.log | cut -f1)"
  echo
  kill "$SP"; wait "$SP" 2>/dev/null || true; SP=
  cd - >/dev/null
}

run "1. RAM-hot (everything fits in memory)" 100000000 uniform
run "2. Tiered, zipf (realistic hot/cold skew)" "$TIERED_CAP" zipf
run "3. Tiered, uniform (worst case: constant disk hits)" "$TIERED_CAP" uniform