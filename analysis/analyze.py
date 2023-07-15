import time
from pathlib import Path
from typing import List

import matplotlib as mpl
import matplotlib.pyplot as plot

from core.analysis.amount_of_results import show_amount_of_results
from core.analysis.render_job_duration import analyze_duration
from core.analysis.render_tail_delay import analyze_tail_delay
from core.analysis.worker_utilization import analyze_utilization
from core.models import FullTrace

PROJECT_DIR = Path("../blender-projects/04_very-simple")
PROJECT_RESULTS_DIR = PROJECT_DIR / "results/arnes-results"


def load_traces(results_directory: Path) -> List[FullTrace]:
    # List of paths to the raw trace (JSON) files.
    print("Scanning results directory for traces...")

    trace_paths: List[Path] = list(results_directory.glob("*_raw-trace.json"))

    print(f"Found {len(trace_paths)} traces, loading...")

    time_trace_load_begin = time.time()
    full_traces: List[FullTrace] = [
        FullTrace.load_from_trace_file(trace_file_path)
        for trace_file_path in trace_paths
    ]
    time_trace_load_end = time.time()

    time_trace_load_delta = round(time_trace_load_end - time_trace_load_begin, 2)
    time_trace_load_average_delta_per_trace = round(time_trace_load_delta / len(full_traces), 2)
    print(
        f"Parsed {len(trace_paths)} traces in {time_trace_load_delta} seconds "
        f"({time_trace_load_average_delta_per_trace} seconds per trace on average)."
        f"\n"
    )

    return full_traces




if __name__ == '__main__':
    traces = load_traces(PROJECT_RESULTS_DIR)

    show_amount_of_results(traces)

    print("====")

    analyze_utilization(traces)
    analyze_duration(traces)
    analyze_tail_delay(traces)
