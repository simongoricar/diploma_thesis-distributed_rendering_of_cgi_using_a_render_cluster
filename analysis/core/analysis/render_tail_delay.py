from typing import List

from core.models import FullTrace, FrameDistributionStrategy


def analyze_tail_delay(traces: List[FullTrace]):
    for cluster_size in [1, 5, 10, 20, 40, 80]:
        print(f"Cluster size: {cluster_size}")
        naive_fine_results = [
            run
            for run in traces
            if run.job.frame_distribution_strategy == FrameDistributionStrategy.NAIVE_FINE
            and run.job.wait_for_number_of_workers == cluster_size
        ]
        naive_coarse_results = [
            run
            for run in traces
            if run.job.frame_distribution_strategy == FrameDistributionStrategy.EAGER_NAIVE_COARSE
            and run.job.wait_for_number_of_workers == cluster_size
        ]
        dynamic_results = [
            run
            for run in traces
            if run.job.frame_distribution_strategy == FrameDistributionStrategy.DYNAMIC
            and run.job.wait_for_number_of_workers == cluster_size
        ]

        if len(naive_fine_results) == 0 or len(naive_coarse_results) == 0 or len(dynamic_results) == 0:
            print("  insufficient tests")
            continue
            
        # naive fine
        naive_fine_max_tail_delay = sum([
            max([
                trace.get_tail_delay()
                for trace in run.worker_traces.values()
            ])
            for run in naive_fine_results
        ]) / len(naive_fine_results)
        print(f"  naive fine maximum tail delay:   {naive_fine_max_tail_delay}")

        # naive coarse
        naive_coarse_max_tail_delay = sum([
            max([
                trace.get_tail_delay()
                for trace in run.worker_traces.values()
            ])
            for run in naive_coarse_results
        ]) / len(naive_coarse_results)
        print(f"  naive coarse maximum tail delay: {naive_coarse_max_tail_delay}")

        # dynamic
        dynamic_max_tail_delay = sum([
            max([
                trace.get_tail_delay()
                for trace in run.worker_traces.values()
            ])
            for run in dynamic_results
        ]) / len(dynamic_results)
        print(f"  dynamic maximum tail delay:     {dynamic_max_tail_delay}")
