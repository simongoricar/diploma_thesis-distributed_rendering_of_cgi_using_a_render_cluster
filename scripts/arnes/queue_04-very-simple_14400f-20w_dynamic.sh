#!/usr/bin/env bash
set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
echo "Script resides in $SCRIPT_DIR."

"$SCRIPT_DIR/queue-job.sh" \
  --jobFile="blender-projects/04_very-simple/04_very-simple_measuring_14400f-20w_dynamic.toml" \
  --resultsDirectory="blender-projects/04_very-simple/results" \
  --runName="04-very-simple_14400f-20w_dynamic" \
  --runPort="9932" \
  --runTimeLimitMinutes=400 \
  --numWorkers=20
