from typing import List

from core.models import JobTrace, FrameDistributionStrategy
from core.parser import load_traces_from_default_path


def analyze_duration(traces: List[JobTrace]):
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


def main():
    traces = load_traces_from_default_path()
    analyze_duration(traces)


if __name__ == '__main__':
    main()
