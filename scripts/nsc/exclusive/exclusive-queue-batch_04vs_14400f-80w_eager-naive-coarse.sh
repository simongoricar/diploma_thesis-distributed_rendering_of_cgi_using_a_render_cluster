#!/usr/bin/env bash
#SBATCH --job-name=exc-qb_04vs_14400f-80w_eager-naive-coarse
#SBATCH --ntasks=81
#SBATCH --time=120
#SBATCH --output=/ceph/grid/home/sg7710/diploma/distributed-rendering-diploma/logs/%A.sbatch.exc-qb_04vs_14400f-80w_eager-naive-coarse.log
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=2G
#SBATCH --ntasks-per-core=1
#SBATCH --wait-all-nodes=1
#SBATCH --constraint=zen3
#SBATCH --dependency=singleton
#SBATCH --exclusive

set -e

#####
# Configuration
###
RUN_BASE_DIRECTORY="$HOME/diploma/distributed-rendering-diploma"
LOGS_DIRECTORY="$RUN_BASE_DIRECTORY/logs"

LOG_NAME="exc-qb_04vs_14400f-80w_eager-naive-coarse"
BLENDER_PROJECT_DIRECTORY="$RUN_BASE_DIRECTORY/blender-projects/04_very-simple"
JOB_FILE_PATH="$BLENDER_PROJECT_DIRECTORY/nsc-jobs/04_very-simple_measuring_14400f-80w_eager-naive-coarse.toml"
RESULTS_DIRECTORY="$BLENDER_PROJECT_DIRECTORY/results-exclusive"
SERVER_PORT=9851
###
# END of Configuration
#####


FORMATTED_CURRENT_DATE_TIME=$(date +%Y-%m-%d_%H-%M-%S)
JOB_LOG_DIRECTORY_PATH="$LOGS_DIRECTORY/$FORMATTED_CURRENT_DATE_TIME.$LOG_NAME"
mkdir -p "$JOB_LOG_DIRECTORY_PATH"


cd ~/diploma/distributed-rendering-diploma
export RUST_LOG=debug

# SLURM runs the sbatch script on the first allocated node.
# We'll run the master server on this node, so we can simply call hostname here.
# (see https://slurm.schedmd.com/sbatch.html)
SERVER_NODE_HOSTNAME=$(hostname -s)
echo "[Batching] Hostname of server will be $SERVER_NODE_HOSTNAME."


## Start master server
echo "[Batching] Starting master server on $SERVER_NODE_HOSTNAME."
srun --job-name="master" --nodelist="$SERVER_NODE_HOSTNAME" --ntasks=1 --nodes=1 --output="$JOB_LOG_DIRECTORY_PATH/$LOG_NAME.slurm.master.log" --cpu-bind=cores --exact "$RUN_BASE_DIRECTORY/target/release/master" --host "0.0.0.0" --port "$SERVER_PORT" --logFilePath "$JOB_LOG_DIRECTORY_PATH/$LOG_NAME.master.log" run-job --resultsDirectory "$RESULTS_DIRECTORY" "$JOB_FILE_PATH" &

sleep 4


## Start all workers
echo "[Batching] Starting workers (80 tasks)."
for i in {1..80}; do
  echo "[Batching] Starting worker $i."

  srun --job-name="worker.$i" --ntasks=1 --nodes=1 --output="$JOB_LOG_DIRECTORY_PATH/$LOG_NAME.slurm.worker.$i.log" --cpu-bind=cores --exact singularity exec --bind "$RUN_BASE_DIRECTORY/blender-projects" --env RUST_LOG="debug" "$RUN_BASE_DIRECTORY/blender-3.6.0.sif" "$RUN_BASE_DIRECTORY/target/release/worker" --logFilePath "$JOB_LOG_DIRECTORY_PATH/$LOG_NAME.worker.$i.log" --masterServerHost "$SERVER_NODE_HOSTNAME" --masterServerPort "$SERVER_PORT" --baseDirectory "$RUN_BASE_DIRECTORY" --blenderBinary "/usr/bin/blender" &

  sleep 1
done

echo "[Batching] All workers started, waiting for all steps to complete."

wait
