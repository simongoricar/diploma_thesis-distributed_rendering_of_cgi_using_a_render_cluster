#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

# Parse arguments
while [ $# -gt 0 ]; do
  case "$1" in
    --jobFile=*)
      JOB_FILE_RELATIVE_TO_BASE="${1#*=}"
      ;;
    --resultsDirectory=*)
      RUN_RESULTS_DIRECTORY="${1#*=}"
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
    --exclusive)
      EXCLUSIVE=1
      ;;
    *)
      echo -e "* Error: Invalid argument: \"$1\" *\n"
      exit 1
  esac
  shift
done

if [[ -z "$JOB_FILE_RELATIVE_TO_BASE" ]]; then
  echo "Missing --jobFile!"
  exit 2
fi

if [[ -z "$RUN_RESULTS_DIRECTORY" ]]; then
  echo "Missing --resultsDirectory!"
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

FORMATTED_CURRENT_DATE_TIME=$(date +%Y-%m-%d_%H-%M-%S)

# Run configuration
RUN_HOST="hpc-login1"
RUN_BASE_DIRECTORY=$(realpath "$SCRIPT_DIR/../..")
REAL_RUN_RESULTS_DIRECTORY="$RUN_BASE_DIRECTORY/$RUN_RESULTS_DIRECTORY"

JOB_FILE_ABSOLUTE_PATH="$RUN_BASE_DIRECTORY/$JOB_FILE_RELATIVE_TO_BASE"

# Worker limits
TIME_LIMIT_MINUTES="${TIME_LIMIT_MINUTES:-300}"
EXCLUSIVE="${EXCLUSIVE:-0})"
MEMORY_LIMIT="8G"
NUM_CPU_PER_TASK=4
NUM_THREADS_PER_CORE=1


echo "Job file: \"$JOB_FILE_ABSOLUTE_PATH\"."
echo "About to queue job $RUN_NAME on $RUN_HOST:$RUN_PORT with $NUM_WORKERS workers."
echo "Each worker has $MEMORY_LIMIT memory, $NUM_THREADS_PER_CORE threads on $NUM_CPU_PER_TASK CPUs."
echo "The job has a time limit of $TIME_LIMIT_MINUTES minutes."

if [ "$EXCLUSIVE" = 1 ]; then
  echo "The job will use --exclusive."
else
  echo "The job will NOT use --exclusive."
fi


read -r -p "Press ENTER to confirm and continue." </dev/tty

(

cd "$RUN_BASE_DIRECTORY"
mkdir -p logs


echo "Starting master on login instance..."
RUST_LOG="debug" screen -d -m -t "cm_${RUN_NAME}_$FORMATTED_CURRENT_DATE_TIME" -S "cm_${RUN_NAME}_$FORMATTED_CURRENT_DATE_TIME" -L -Logfile "logs/${FORMATTED_CURRENT_DATE_TIME}_cm_$RUN_NAME.log" -- "$RUN_BASE_DIRECTORY/target/release/master" --host "0.0.0.0" --port "$RUN_PORT" run-job --resultsDirectory "$REAL_RUN_RESULTS_DIRECTORY" "$JOB_FILE_ABSOLUTE_PATH"


echo "Queueing workers on via srun..."
if [ "$EXCLUSIVE" = 1 ]; then
  screen -d -m -t "cw_${RUN_NAME}_$FORMATTED_CURRENT_DATE_TIME" -S "cw_${RUN_NAME}_$FORMATTED_CURRENT_DATE_TIME" -L -Logfile "logs/${FORMATTED_CURRENT_DATE_TIME}_cw_$RUN_NAME.log" -- srun --exclusive --job-name="cw_${RUN_NAME}_$FORMATTED_CURRENT_DATE_TIME" --ntasks="$NUM_WORKERS" --output="$RUN_BASE_DIRECTORY/logs/${FORMATTED_CURRENT_DATE_TIME}_cm_$RUN_NAME.workers.log" --time="$TIME_LIMIT_MINUTES" --mem=$MEMORY_LIMIT --cpus-per-task=$NUM_CPU_PER_TASK --threads-per-core=$NUM_THREADS_PER_CORE --constraint="amd&rome" --dependency="singleton" --exclude="wn[201-224]" -- singularity exec --bind "$RUN_BASE_DIRECTORY/blender-projects" --env RUST_LOG="debug" "$HOME/diploma/distributed-rendering-diploma/blender-3.6.0.sif" "$RUN_BASE_DIRECTORY/target/release/worker" --masterServerHost "$RUN_HOST" --masterServerPort "$RUN_PORT" --baseDirectory "$RUN_BASE_DIRECTORY" --blenderBinary /usr/bin/blender
else
  screen -d -m -t "cw_${RUN_NAME}_$FORMATTED_CURRENT_DATE_TIME" -S "cw_${RUN_NAME}_$FORMATTED_CURRENT_DATE_TIME" -L -Logfile "logs/${FORMATTED_CURRENT_DATE_TIME}_cw_$RUN_NAME.log" -- srun --job-name="cw_${RUN_NAME}_$FORMATTED_CURRENT_DATE_TIME" --ntasks="$NUM_WORKERS" --output="$RUN_BASE_DIRECTORY/logs/${FORMATTED_CURRENT_DATE_TIME}_cm_$RUN_NAME.workers.log" --time="$TIME_LIMIT_MINUTES" --mem=$MEMORY_LIMIT --cpus-per-task=$NUM_CPU_PER_TASK --threads-per-core=$NUM_THREADS_PER_CORE --constraint="amd&rome" --dependency="singleton" --exclude="wn[201-224]" -- singularity exec --bind "$RUN_BASE_DIRECTORY/blender-projects" --env RUST_LOG="debug" "$HOME/diploma/distributed-rendering-diploma/blender-3.6.0.sif" "$RUN_BASE_DIRECTORY/target/release/worker" --masterServerHost "$RUN_HOST" --masterServerPort "$RUN_PORT" --baseDirectory "$RUN_BASE_DIRECTORY" --blenderBinary /usr/bin/blender
fi


echo "Queued!"

)
