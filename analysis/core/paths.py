from pathlib import Path

SCRIPT_DIR = Path(__file__).parent

BLENDER_PROJECT_DIR = SCRIPT_DIR / "../../blender-projects/04_very-simple"
RUN_RESULTS_DIR = BLENDER_PROJECT_DIR / "results/arnes-results"

PLOT_OUTPUT_DIR = SCRIPT_DIR / "../plots"
if not PLOT_OUTPUT_DIR.is_dir():
    PLOT_OUTPUT_DIR.mkdir(parents=True)
