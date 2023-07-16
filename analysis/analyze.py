import argparse
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import matplotlib as mpl
import matplotlib.pyplot as plot


from core.analysis.amount_of_results import show_amount_of_results
from core.analysis.job_duration import analyze_duration
from core.analysis.job_tail_delay import analyze_tail_delay
from core.analysis.worker_utilization import analyze_utilization
from core.models import FullTrace

PROJECT_DIR = Path("../blender-projects/04_very-simple")
PROJECT_RESULTS_DIR = PROJECT_DIR / "results/arnes-results"

@dataclass(slots=True, frozen=True)
class CLIArguments:
    profile_memory: bool
    analysis_to_run: str

def parse_cli_arguments() -> CLIArguments:
    parser = argparse.ArgumentParser(
        prog="result-analysis",
        description="Render farm result analysis scripts."
    )

    parser.add_argument(
        "-m", "--profileMemory",
        dest="profile_memory",
        help="Whether to profile the used memory with pympler (will add significant overhead).",
        action="store_true",
    )

    parser.add_argument(
        "analysis",
        help="What analysis to run.",
        choices=[
            "all", "amount-of-results",
            "job-duration", "job-tail-delay",
            "worker-utilization"
        ]
    )

    args = parser.parse_args()

    return CLIArguments(
        profile_memory=args.profile_memory,
        analysis_to_run=args.analysis,
    )


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


def main():
    args = parse_cli_arguments()

    # Load traces (optionally with memory profiling)
    tracker: Optional["ClassTracker"] = None
    if args.profile_memory:
        from pympler.classtracker import ClassTracker
        tracker = ClassTracker()
        tracker.track_class(FullTrace)
        tracker.create_snapshot(description="Before load")

    traces = load_traces(PROJECT_RESULTS_DIR)

    if args.profile_memory:
        tracker.create_snapshot(description="After load")
        tracker.stats.print_summary()
        print()
        print()


    # Run requested analysis action.
    if args.analysis_to_run == "all":
        show_amount_of_results(traces)

        print("=====================")

        print("==== Utilization ====")
        analyze_utilization(traces)
        print()

        print("==== Job Duration ====")
        analyze_duration(traces)
        print()

        print("==== Job Tail Delay ====")
        analyze_tail_delay(traces)
        print()
    elif args.analysis_to_run == "amount-of-results":
        show_amount_of_results(traces)
    elif args.analysis_to_run == "job-duration":
        analyze_duration(traces)
    elif args.analysis_to_run == "job-tail-delay":
        analyze_tail_delay(traces)
    elif args.analysis_to_run == "worker-utilization":
        analyze_utilization(traces)
    else:
        raise RuntimeError("Invalid analysis action.")


if __name__ == '__main__':
    main()
