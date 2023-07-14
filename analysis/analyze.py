from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Tuple, Self, Dict, Any

import matplotlib as mpl
import matplotlib.pyplot as plot

from core.trace import FullTrace, WorkerTrace, WorkerFrameTrace, FrameDistributionStrategy

RAW_DATA_DIR = Path("./raw-data")
SIMPLE_PROJECT_DIR = Path("../blender-projects/01_simple-animation")



# List of tuples containing two paths:
#  - path to the raw trace (JSON) file
#  - path to the associated job definition (TOML) file that was used in the run
TRACE_PATHS: List[Tuple[Path, Path]] = [
    # 01 simple animation (600 frames)

    ## 1 worker
    ### naive coarse (all frames at once)
    #### TODO

    ## 5 workers
    ### naive fine
    (
        RAW_DATA_DIR / "2023-06-13_13-23-30_job-01_simple-animation_measuring_600f-5w_naive-fine_raw-trace.json",
        SIMPLE_PROJECT_DIR / "01-simple-animation_measuring_600f-5w_naive-fine.toml"
    ),
    ### naive coarse
    (
        RAW_DATA_DIR / "2023-06-15_12-17-07_job-01_simple-animation_measuring_600f-5w_naive-coarse_raw-trace.json",
        SIMPLE_PROJECT_DIR / "01-simple-animation_measuring_600f-5w_naive-coarse.toml"
    ),
    ### dynamic
    (
        RAW_DATA_DIR / "2023-06-13_13-23-52_job-01_simple-animation_measuring_600f-5w_dynamic_raw-trace.json",
        SIMPLE_PROJECT_DIR / "01-simple-animation_measuring_600f-5w_dynamic.toml"
    ),

    ## 10 workers
    ### naive fine
    (
        RAW_DATA_DIR / "2023-06-13_13-24-22_job-01_simple-animation_measuring_600f-10w_naive-fine_raw-trace.json",
        SIMPLE_PROJECT_DIR / "01-simple-animation_measuring_600f-10w_naive-fine.toml"
    ),
    ### naive coarse
    (
        RAW_DATA_DIR / "2023-06-14_11-48-21_job-01_simple-animation_measuring_600f-10w_naive-coarse_raw-trace.json",
        SIMPLE_PROJECT_DIR / "01-simple-animation_measuring_600f-10w_naive-coarse.toml"
    ),
    ### dynamic
    (
        RAW_DATA_DIR / "2023-06-13_13-24-29_job-01_simple-animation_measuring_600f-10w_dynamic_raw-trace.json",
        SIMPLE_PROJECT_DIR / "01-simple-animation_measuring_600f-10w_dynamic.toml"
    ),
    ## 20 workers
    ### naive fine
    (
        RAW_DATA_DIR / "2023-06-13_13-31-33_job-01_simple-animation_measuring_600f-20w_naive-fine_raw-trace.json",
        SIMPLE_PROJECT_DIR / "01-simple-animation_measuring_600f-20w_naive-fine.toml"
    ),
    (
        RAW_DATA_DIR / "2023-06-14_11-49-06_job-01_simple-animation_measuring_600f-20w_naive-fine_raw-trace.json",
        SIMPLE_PROJECT_DIR / "01-simple-animation_measuring_600f-20w_naive-fine.toml"
    ),
    ### naive coarse
    (
        RAW_DATA_DIR / "2023-06-13_13-31-43_job-01_simple-animation_measuring_600f-20w_naive-coarse_raw-trace.json",
        SIMPLE_PROJECT_DIR / "01-simple-animation_measuring_600f-20w_naive-coarse.toml"
    ),
    ### dynamic
    (
        RAW_DATA_DIR / "2023-06-13_13-31-48_job-01_simple-animation_measuring_600f-20w_dynamic_raw-trace.json",
        SIMPLE_PROJECT_DIR / "01-simple-animation_measuring_600f-20w_dynamic.toml"
    ),
    (
        RAW_DATA_DIR / "2023-06-14_11-49-12_job-01_simple-animation_measuring_600f-20w_dynamic_raw-trace.json",
        SIMPLE_PROJECT_DIR / "01-simple-animation_measuring_600f-20w_dynamic.toml"
    ),
]

TRACES: List[FullTrace] = [
    FullTrace.load_from_trace_and_job_definition_file(trace_file_path, job_definition_file_path)
    for (trace_file_path, job_definition_file_path)
    in TRACE_PATHS
]



@dataclass
class WorkerUtilization:
    total_job_time: float
    total_job_time_without_setup_and_teardown: float

    total_idle_time: float
    total_active_time: float

    idle_before_first_frame: float
    idle_after_last_frame: float

    def utilization_rate(self) -> float:
        return self.total_active_time / self.total_job_time

    def utilization_rate_excluding_setup_and_teardown(self) -> float:
        return self.total_active_time / self.total_job_time_without_setup_and_teardown

    @classmethod
    def from_worker_trace(cls, worker_trace: WorkerTrace) -> Self:
        job_start_time: datetime = worker_trace.worker_job_start_time
        job_finish_time: datetime = worker_trace.worker_job_finish_time

        total_timedelta: timedelta = job_finish_time - job_start_time
        total_time: float = total_timedelta.total_seconds()

        total_timedelta_excluding_setup_and_teardown: timedelta = \
            worker_trace.frame_render_traces[len(worker_trace.frame_render_traces) - 1].frame_finish_time \
            - worker_trace.frame_render_traces[0].frame_start_time
        total_time_excluding_setup_and_teardown: float = total_timedelta_excluding_setup_and_teardown.total_seconds()

        total_idle_time: float = 0
        total_active_time: float = 0

        idle_time_before_first_frame: Optional[float] = None
        idle_time_after_last_frame: Optional[float] = None

        for (index, frame_trace) in enumerate(worker_trace.frame_render_traces):
            index: int
            frame_trace: WorkerFrameTrace

            frame_start_time: datetime = frame_trace.frame_start_time
            frame_finish_time: datetime = frame_trace.frame_finish_time

            total_active_time += (frame_finish_time - frame_start_time).total_seconds()

            if index == 0:
                idle_time_before_first_frame: timedelta = frame_start_time - job_start_time

                total_idle_time += idle_time_before_first_frame.total_seconds()
            elif index + 1 == len(worker_trace.frame_render_traces):
                previous_frame = worker_trace.frame_render_traces[index - 1]
                previous_frame_finish_time: datetime = previous_frame.frame_finish_time

                idle_time_between_last_two_frames = frame_start_time - previous_frame_finish_time
                idle_time_after_last_frame = (job_finish_time - frame_finish_time).total_seconds()

                total_idle_time += idle_time_between_last_two_frames.total_seconds()
                total_idle_time += idle_time_after_last_frame
            else:
                previous_frame = worker_trace.frame_render_traces[index - 1]
                previous_frame_finish_time: datetime = previous_frame.frame_finish_time

                idle_time_between_frames = frame_start_time - previous_frame_finish_time

                total_idle_time += idle_time_between_frames.total_seconds()

        # Sums are complete, calculate the utilization rate.
        return cls(
            total_job_time=total_time,
            total_job_time_without_setup_and_teardown=total_time_excluding_setup_and_teardown,
            total_idle_time=total_idle_time,
            total_active_time=total_active_time,
            idle_before_first_frame=idle_time_before_first_frame,
            idle_after_last_frame=idle_time_after_last_frame,
        )



def analyze_utilization():
    utilization_per_strategy: Dict[FrameDistributionStrategy, List[float]] = {
        FrameDistributionStrategy.NAIVE_FINE: [],
        FrameDistributionStrategy.EAGER_NAIVE_COARSE: [],
        FrameDistributionStrategy.DYNAMIC: [],
    }

    print("Overall results (per-run):")
    for run_trace in TRACES:
        print(f"Run {run_trace.job_name} ({run_trace.number_of_workers}):")

        utilization_per_worker: List[WorkerUtilization] = [
            WorkerUtilization.from_worker_trace(worker_trace)
            for worker_trace in run_trace.worker_traces.values()
        ]

        max_utilization = max([u.utilization_rate() for u in utilization_per_worker])
        min_utilization = min([u.utilization_rate() for u in utilization_per_worker])
        average_utilization = sum([u.utilization_rate() for u in utilization_per_worker]) / len(utilization_per_worker)

        utilization_per_strategy[run_trace.distribution_strategy].append(average_utilization)

        print(f"  max utilization: {max_utilization}")
        print(f"  average utilization: {average_utilization}")
        print(f"  min utilization: {min_utilization}")

    print(f"\n{'-' * 20}\n")
    print("Overall average (per-strategy):")

    for strategy, average_values in utilization_per_strategy.items():
        real_average_utilization = sum(average_values) / len(average_values)
        print(f"Strategy: {strategy} -> average utilization: {real_average_utilization}")


def analyze_tail_delay():
    for cluster_size in [5, 10, 20]:
        print(f"Cluster size: {cluster_size}")
        naive_fine_results = [
            run
            for run in TRACES
            if run.distribution_strategy == FrameDistributionStrategy.NAIVE_FINE
               and run.number_of_workers == cluster_size
        ]
        naive_coarse_results = [
            run
            for run in TRACES
            if run.distribution_strategy == FrameDistributionStrategy.EAGER_NAIVE_COARSE
               and run.number_of_workers == cluster_size
        ]
        dynamic_results = [
            run
            for run in TRACES
            if run.distribution_strategy == FrameDistributionStrategy.DYNAMIC
               and run.number_of_workers == cluster_size
        ]

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


def analyze_duration():
    for cluster_size in [5, 10, 20]:
        print(f"Cluster size: {cluster_size}")

        naive_fine_results = [
            run
            for run in TRACES
            if run.distribution_strategy == FrameDistributionStrategy.NAIVE_FINE
               and run.number_of_workers == cluster_size
        ]
        naive_coarse_results = [
            run
            for run in TRACES
            if run.distribution_strategy == FrameDistributionStrategy.EAGER_NAIVE_COARSE
               and run.number_of_workers == cluster_size
        ]
        dynamic_results = [
            run
            for run in TRACES
            if run.distribution_strategy == FrameDistributionStrategy.DYNAMIC
               and run.number_of_workers == cluster_size
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



if __name__ == '__main__':
    analyze_utilization()
    analyze_duration()
    analyze_tail_delay()
