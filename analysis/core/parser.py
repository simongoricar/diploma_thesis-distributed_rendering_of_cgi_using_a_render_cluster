import time
from pathlib import Path
from typing import List

from core.models import JobTrace
from core.paths import RUN_RESULTS_DIR


def load_traces(results_directory: Path) -> List[JobTrace]:
    # List of paths to the raw trace (JSON) files.
    print("Scanning results directory for traces...")

    trace_paths: List[Path] = list(results_directory.glob("*_raw-trace.json"))

    print(f"Found {len(trace_paths)} traces, loading...")

    time_trace_load_begin = time.time()
    full_traces: List[JobTrace] = [
        JobTrace.load_from_trace_file(trace_file_path)
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


def load_traces_from_default_path() -> List[JobTrace]:
    return load_traces(RUN_RESULTS_DIR)
