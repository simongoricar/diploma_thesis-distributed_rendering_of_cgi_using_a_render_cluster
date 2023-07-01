#!/usr/bin/env bash
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Script resides in $SCRIPT_DIR."

"$SCRIPT_DIR/queue-job.sh" \
  --jobFile="blender-projects/01_simple-animation/01-simple-animation_measuring_600f-5w_naive-fine.toml" \
  --runName="01-simple_600f-5w_naive-fine" \
  --runPort="9910" \
  --runTimeLimitMinutes=900 \
  --numWorkers=5
