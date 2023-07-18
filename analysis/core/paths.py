from pathlib import Path

SCRIPT_DIR = Path(__file__).parent

BLENDER_PROJECT_DIR = SCRIPT_DIR / "../../blender-projects/04_very-simple"
RUN_RESULTS_DIR = BLENDER_PROJECT_DIR / "results/arnes-results"

# Plot outputs
PLOT_OUTPUT_DIR = SCRIPT_DIR / "../plots"
if not PLOT_OUTPUT_DIR.is_dir():
    PLOT_OUTPUT_DIR.mkdir(parents=True)

JOB_TAIL_DELAY_OUTPUT_DIRECTORY: Path = PLOT_OUTPUT_DIR / "job-tail-delay"
if not JOB_TAIL_DELAY_OUTPUT_DIRECTORY.is_dir():
    JOB_TAIL_DELAY_OUTPUT_DIRECTORY.mkdir(parents=True)

WORKER_UTILIZATION_OUTPUT_DIRECTORY: Path = PLOT_OUTPUT_DIR / "worker-utilization"
if not WORKER_UTILIZATION_OUTPUT_DIRECTORY.is_dir():
    WORKER_UTILIZATION_OUTPUT_DIRECTORY.mkdir(parents=True)
