#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"

for name in order notification retry payment; do
  stop_proc "$name"
  echo "$name berhenti"
done
