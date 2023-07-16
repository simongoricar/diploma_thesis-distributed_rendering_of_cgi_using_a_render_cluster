#!/usr/bin/env bash
#SBATCH --job-name=qb_04vs_14400f-1w_var2
#SBATCH --ntasks=2
#SBATCH --time=6000
#SBATCH --output=/d/hpc/projects/FRI/sg7710/distributed-rendering-logs/%A.sbatch.qb_04vs_14400f-1w_var2.log
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=2G
#SBATCH --ntasks-per-core=1
#SBATCH --wait-all-nodes=1
#SBATCH --constraint=amd&rome
#SBATCH --dependency=singleton
#SBATCH --exclude=wn[201-224]
#SBATCH --partition=long

set -e

#####
# Configuration
###
RUN_BASE_DIRECTORY="$HOME/diploma/distributed-rendering-diploma"
LOGS_DIRECTORY="/d/hpc/projects/FRI/sg7710/distributed-rendering-logs/logs"

LOG_NAME="qb_04vs_14400f-1w"
BLENDER_PROJECT_DIRECTORY="$RUN_BASE_DIRECTORY/blender-projects/04_very-simple"
JOB_FILE_PATH="$BLENDER_PROJECT_DIRECTORY/04_very-simple_measuring_14400f-1w_variant2.toml"
RESULTS_DIRECTORY="$BLENDER_PROJECT_DIRECTORY/results"
SERVER_PORT=9901
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
echo "[Batching] Starting worker (1 task)."

echo "[Batching] Starting worker 0."
srun --job-name="worker.0" --ntasks=1 --nodes=1 --output="$JOB_LOG_DIRECTORY_PATH/$LOG_NAME.slurm.worker.0.log" --cpu-bind=cores --exact singularity exec --bind "$RUN_BASE_DIRECTORY/blender-projects" --env RUST_LOG="debug" "$RUN_BASE_DIRECTORY/blender-3.6.0.sif" "$RUN_BASE_DIRECTORY/target/release/worker" --logFilePath "$JOB_LOG_DIRECTORY_PATH/$LOG_NAME.worker.$i.log" --masterServerHost "$SERVER_NODE_HOSTNAME" --masterServerPort "$SERVER_PORT" --baseDirectory "$RUN_BASE_DIRECTORY" --blenderBinary "/usr/bin/blender" &

echo "[Batching] All workers started, waiting for all steps to complete."

wait
