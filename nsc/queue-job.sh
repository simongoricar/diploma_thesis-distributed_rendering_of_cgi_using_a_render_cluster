#!/usr/bin/env bash
set -e

# Parse arguments
while [ $# -gt 0 ]; do
  case "$1" in
    --jobFile=*)
      JOB_FILE_RELATIVE_TO_BASE="${1#*=}"
      ;;
    --runName=*)
      RUN_NAME="${1#*=}"
      ;;
    --runPort=*)
      RUN_PORT="${1#*=}"
      ;;
    --runTimeLimitMinutes=*)
      TIME_LIMIT_MINUTES="${1#*=}"
      ;;
    --numWorkers=*)
      NUM_WORKERS="${1#*=}"
      ;;
    *)
      printf "* Error: Invalid argument *\n"
      exit 1
  esac
  shift
done

if [[ -z "$JOB_FILE_RELATIVE_TO_BASE" ]]; then
  echo "Missing --jobFile!"
  exit 2
fi

if [[ -z "$RUN_NAME" ]]; then
  echo "Missing --runName!"
  exit 2
fi

if [[ -z "$RUN_PORT" ]]; then
  echo "Missing --runPort!"
  exit 2
fi

if [[ -z "$TIME_LIMIT_MINUTES" ]]; then
  echo "Missing --runTimeLimitMinutes!"
  exit 2
fi

if [[ -z "$NUM_WORKERS" ]]; then
  echo "Missing --numWorkers!"
  exit 2
fi


# Run configuration
RUN_HOST="nsc-login1"
RUN_BASE_DIRECTORY="$HOME/diploma/distributed-rendering-diploma"
RUN_RESULTS_DIRECTORY="$RUN_BASE_DIRECTORY/blender-projects/01_simple-animation/results"

JOB_FILE_ABSOLUTE_PATH="$RUN_BASE_DIRECTORY/$JOB_FILE_RELATIVE_TO_BASE"

# Worker limits
TIME_LIMIT_MINUTES="${TIME_LIMIT_MINUTES:-300}"
MEMORY_LIMIT="8G"
NUM_CPU_PER_TASK=4
NUM_THREADS_PER_CORE=1


echo "Job file: \"$JOB_FILE_ABSOLUTE_PATH\"."
echo "About to queue job $RUN_NAME on $RUN_HOST:$RUN_PORT with $NUM_WORKERS workers."
echo "Each worker has $MEMORY_LIMIT memory, $NUM_THREADS_PER_CORE threads on $NUM_CPU_PER_TASK CPUs."
echo "The job has a time limit of $TIME_LIMIT_MINUTES minutes."


read -r -p "Press ENTER to confirm and continue." </dev/tty

cd "$RUN_BASE_DIRECTORY"


echo "Starting master on login instance..."
RUST_LOG="debug" screen -d -m -t "cm_$RUN_NAME" -S "cm_$RUN_NAME" -L -Logfile "cm_$RUN_NAME.log" -- "$RUN_BASE_DIRECTORY/target/release/master" --host 0.0.0.0 --port "$RUN_PORT" run-job --resultsDirectory "$RUN_RESULTS_DIRECTORY" "$JOB_FILE_ABSOLUTE_PATH"


echo "Queueing workers on via srun..."
screen -d -m -t "cw_$RUN_NAME" -S "cw_$RUN_NAME" -L -Logfile "cw_$RUN_NAME.log" -- srun --job-name="cw_$RUN_NAME" --ntasks="$NUM_WORKERS" --output="cm_$RUN_NAME.workers.log" --time="$TIME_LIMIT_MINUTES" --mem=$MEMORY_LIMIT --cpus-per-task=$NUM_CPU_PER_TASK --threads-per-core=$NUM_THREADS_PER_CORE --constraint=zen3 -- singularity exec --bind "$RUN_BASE_DIRECTORY/blender-projects" --env RUST_LOG="debug" "$HOME/diploma/blender-docker.sif" "$RUN_BASE_DIRECTORY/target/release/worker" --masterServerHost "$RUN_HOST" --masterServerPort "$RUN_PORT" --baseDirectory "$RUN_BASE_DIRECTORY" --blenderBinary /usr/bin/blender

echo "Queued!"
