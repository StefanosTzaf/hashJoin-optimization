#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <engine: base|robin|hopscotch|cuckoo> <plans.json or args...>" >&2
  exit 1
fi

engine="$1"; shift

bin_dir="$(dirname "$0")/build"
case "$engine" in
  base)
    exe="${bin_dir}/run";
    ;;
  cuckoo)
    exe="${bin_dir}/run_cuckoo";
    ;;
  robin)
    exe="${bin_dir}/run_robin";
    ;;
  *)
    echo "Unknown engine: $engine (expected base|cuckoo|robin)" >&2
    exit 1
    ;;
 esac

if [[ ! -x "$exe" ]]; then
  echo "Executable not found: $exe. Did you build it?" >&2
  echo "Hint: cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -- -j $(nproc) run run_cuckoo run_robin" >&2
  exit 1
fi

exec "$exe" "$@"
