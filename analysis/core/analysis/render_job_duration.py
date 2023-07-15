from typing import List

from core.models import FullTrace, FrameDistributionStrategy


def analyze_duration(traces: List[FullTrace]):
    for cluster_size in [5, 10, 20]:
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

        # Naive fine
        naive_fine_average_duration = sum([
            (run.get_job_finished_at() - run.get_job_started_at()).total_seconds()
            for run in naive_fine_results
        ]) / len(naive_fine_results)
        print(f"  naive fine average duration:   {naive_fine_average_duration}")

        # naive coarse
        naive_coarse_average_duration = sum([
            (run.get_job_finished_at() - run.get_job_started_at()).total_seconds()
            for run in naive_coarse_results
        ]) / len(naive_coarse_results)
        print(f"  naive coarse average duration: {naive_coarse_average_duration}")

        # dynamic
        dynamic_average_duration = sum([
            (run.get_job_finished_at() - run.get_job_started_at()).total_seconds()
            for run in dynamic_results
        ]) / len(dynamic_results)
        print(f"  dynamic average duration:      {dynamic_average_duration}")
