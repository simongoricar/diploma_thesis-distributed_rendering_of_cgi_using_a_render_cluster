from typing import List

from core.models import FullTrace, FrameDistributionStrategy


def show_amount_of_results(traces: List[FullTrace]):
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
