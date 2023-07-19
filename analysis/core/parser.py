import time
from pathlib import Path
import dill
import concurrent.futures as cf
from typing import List

from core.models import JobTrace
from core.paths import RUN_RESULTS_DIR, CACHING_DIR


def load_traces_sequentially(results_directory: Path) -> List[JobTrace]:
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


def load_traces_in_parallel(
    results_directory: Path,
    max_processes: int,
) -> List[JobTrace]:
    # List of paths to the raw trace (JSON) files.
    print("Scanning results directory for traces...")
    trace_paths: List[Path] = list(results_directory.glob("*_raw-trace.json"))
    print(f"Found {len(trace_paths)} traces, loading...")


    full_traces: List[JobTrace] = []

    time_trace_load_begin = time.time()

    with cf.ProcessPoolExecutor(
        max_workers=max_processes,
        max_tasks_per_child=None,
    ) as executor:
        trace_futures = [
            executor.submit(JobTrace.load_from_trace_file, trace_path) for trace_path in trace_paths
        ]

        for parsed_trace in cf.as_completed(trace_futures, timeout=None):
            full_traces.append(parsed_trace.result())

    time_trace_load_end = time.time()

    time_trace_load_delta = time_trace_load_end - time_trace_load_begin
    time_trace_load_average_delta_per_trace = time_trace_load_delta / len(full_traces)
    print(
        f"Parsed {len(trace_paths)} traces in {round(time_trace_load_delta, 2)} seconds "
        f"({round(time_trace_load_average_delta_per_trace, 2)} seconds per trace on average)."
        f"\n"
    )

    return full_traces


def _load_cached(cache_file_path: Path) -> JobTrace:
    with cache_file_path.open(mode="rb") as file:
        return dill.load(file)

def _load_from_raw_file_and_cache(
    trace_file_path: Path,
    cache_file_path: Path,
) -> JobTrace:
    trace = JobTrace.load_from_trace_file(trace_file_path)

    with cache_file_path.open(mode="wb") as file:
        dill.dump(trace, file)

    return trace

def load_traces_in_parallel_with_caching(
    results_directory: Path,
    max_processes: int,
) -> List[JobTrace]:
    # List of paths to the raw trace (JSON) files.
    print("Scanning results directory for traces...")
    trace_paths: List[Path] = list(results_directory.glob("*_raw-trace.json"))
    print(f"Found {len(trace_paths)} traces, loading...")

    full_traces: List[JobTrace] = []

    time_trace_load_begin = time.time()

    with cf.ThreadPoolExecutor(
        max_workers=max_processes,
    ) as executor:
        trace_futures = []
        for trace_path in trace_paths:
            potential_cached_file_path = (CACHING_DIR / trace_path.name).with_suffix(".ct")

            if potential_cached_file_path.is_file():
                # If cached, load that instead
                trace_futures.append(
                    executor.submit(_load_cached, potential_cached_file_path)
                )
            else:
                # Otherwise, load from JSON file, parse, then save into cache.
                trace_futures.append(
                    executor.submit(_load_from_raw_file_and_cache, trace_path, potential_cached_file_path)
                )


        for parsed_trace in cf.as_completed(trace_futures, timeout=None):
            full_traces.append(parsed_trace.result())

    time_trace_load_end = time.time()

    time_trace_load_delta = time_trace_load_end - time_trace_load_begin
    time_trace_load_average_delta_per_trace = time_trace_load_delta / len(full_traces)
    print(
        f"Parsed {len(trace_paths)} traces in {round(time_trace_load_delta, 2)} seconds "
        f"({round(time_trace_load_average_delta_per_trace, 2)} seconds per trace on average)."
        f"\n"
    )

    return full_traces


def load_traces_from_default_path() -> List[JobTrace]:
    return load_traces_in_parallel_with_caching(RUN_RESULTS_DIR, 4)
