#!/usr/bin/env bash
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Script resides in $SCRIPT_DIR."

"$SCRIPT_DIR/queue-job.sh" \
  --jobFile="blender-projects/01_simple-animation/01-simple-animation_measuring_600f-1w.toml" \
  --runName="01-simple_600f-1w" \
  --runPort="9905" \
  --runTimeLimitMinutes=3000 \
  --numWorkers=1
