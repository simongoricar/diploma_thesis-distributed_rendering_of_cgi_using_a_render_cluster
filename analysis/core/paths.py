from pathlib import Path

SCRIPT_DIR = Path(__file__).parent

BLENDER_PROJECT_DIR = SCRIPT_DIR / "../../blender-projects/04_very-simple"
RUN_RESULTS_DIR = BLENDER_PROJECT_DIR / "results/arnes-results"

# Caching
CACHING_DIR = SCRIPT_DIR / "../cache"
if not CACHING_DIR.is_dir():
    CACHING_DIR.mkdir(parents=True)

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

JOB_DURATION_OUTPUT_DIRECTORY: Path = PLOT_OUTPUT_DIR / "job-duration"
if not JOB_DURATION_OUTPUT_DIRECTORY.is_dir():
    JOB_DURATION_OUTPUT_DIRECTORY.mkdir(parents=True)

WORKER_LATENCY_OUTPUT_DIRECTORY: Path = PLOT_OUTPUT_DIR / "worker-latency"
if not WORKER_LATENCY_OUTPUT_DIRECTORY.is_dir():
    WORKER_LATENCY_OUTPUT_DIRECTORY.mkdir(parents=True)

SPEEDUP_OUTPUT_DIRECTORY: Path = PLOT_OUTPUT_DIR / "speedup"
if not SPEEDUP_OUTPUT_DIRECTORY.is_dir():
    SPEEDUP_OUTPUT_DIRECTORY.mkdir(parents=True)

EFFICIENCY_OUTPUT_DIRECTORY: Path = PLOT_OUTPUT_DIR / "efficiency"
if not EFFICIENCY_OUTPUT_DIRECTORY.is_dir():
    EFFICIENCY_OUTPUT_DIRECTORY.mkdir(parents=True)

READING_RENDERING_WRITING_OUTPUT_DIRECTORY: Path = PLOT_OUTPUT_DIR / "reading-rendering-writing"
if not READING_RENDERING_WRITING_OUTPUT_DIRECTORY.is_dir():
    READING_RENDERING_WRITING_OUTPUT_DIRECTORY.mkdir(parents=True)
