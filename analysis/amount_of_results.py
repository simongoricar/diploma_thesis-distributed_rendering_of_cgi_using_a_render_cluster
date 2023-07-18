import argparse
from dataclasses import dataclass
from typing import List, Optional

from core.models import JobTrace, FrameDistributionStrategy
from core.parser import load_traces_from_default_path


@dataclass(slots=True, frozen=True)
class CLIArguments:
    profile_memory: bool

def parse_cli_arguments() -> CLIArguments:
    parser = argparse.ArgumentParser(
        prog="amount-of-results"
    )

    parser.add_argument(
        "-m", "--profileMemory",
        dest="profile_memory",
        help="Whether to profile the used memory with pympler (will add significant overhead).",
        action="store_true",
    )

    args = parser.parse_args()

    return CLIArguments(
        profile_memory=args.profile_memory,
    )



def show_amount_of_results(traces: List[JobTrace]):
    VALID_CLUSTER_SIZES: List[int] = [1, 5, 10, 20, 40, 80]

    for size in VALID_CLUSTER_SIZES:
        print(f"-- {size} {'worker' if size == 1 else 'workers'} --")

        num_naive_fine = len(list(
            run
            for run in traces
            if run.job.wait_for_number_of_workers == size
            and run.job.frame_distribution_strategy == FrameDistributionStrategy.NAIVE_FINE
        ))

        num_eager_naive_coarse = len(list(
            run
            for run in traces
            if run.job.wait_for_number_of_workers == size
            and run.job.frame_distribution_strategy == FrameDistributionStrategy.EAGER_NAIVE_COARSE
        ))

        num_dynamic = len(list(
            run
            for run in traces
            if run.job.wait_for_number_of_workers == size
            and run.job.frame_distribution_strategy == FrameDistributionStrategy.DYNAMIC
        ))

        print(f"   naive fine:          {num_naive_fine}")
        print(f"   eager naive coarse:  {num_eager_naive_coarse}")
        print(f"   dynamic:             {num_dynamic}")

        print()


def main():
    args = parse_cli_arguments()

    # Load traces (optionally with memory profiling)
    tracker: Optional["ClassTracker"] = None
    if args.profile_memory:
        from pympler.classtracker import ClassTracker
        tracker = ClassTracker()
        tracker.track_class(JobTrace)
        tracker.create_snapshot(description="Before load")

    traces = load_traces_from_default_path()

    if args.profile_memory:
        tracker.create_snapshot(description="After load")
        tracker.stats.print_summary()
        print()
        print()

    show_amount_of_results(traces)


if __name__ == '__main__':
    main()
