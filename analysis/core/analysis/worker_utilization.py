from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Self

from core.models import FullTrace, FrameDistributionStrategy, WorkerFrameTrace, WorkerTrace


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
            worker_trace.frame_render_traces[len(worker_trace.frame_render_traces) - 1].finish_time() \
            - worker_trace.frame_render_traces[0].start_time()
        total_time_excluding_setup_and_teardown: float = total_timedelta_excluding_setup_and_teardown.total_seconds()

        total_idle_time: float = 0
        total_active_time: float = 0

        idle_time_before_first_frame: Optional[float] = None
        idle_time_after_last_frame: Optional[float] = None

        for (index, frame_trace) in enumerate(worker_trace.frame_render_traces):
            index: int
            frame_trace: WorkerFrameTrace

            frame_start_time: datetime = frame_trace.start_time()
            frame_finish_time: datetime = frame_trace.finish_time()

            total_active_time += (frame_finish_time - frame_start_time).total_seconds()

            if index == 0:
                idle_time_before_first_frame: timedelta = frame_start_time - job_start_time

                total_idle_time += idle_time_before_first_frame.total_seconds()
            elif index + 1 == len(worker_trace.frame_render_traces):
                previous_frame = worker_trace.frame_render_traces[index - 1]
                previous_frame_finish_time: datetime = previous_frame.finish_time()

                idle_time_between_last_two_frames = frame_start_time - previous_frame_finish_time
                idle_time_after_last_frame = (job_finish_time - frame_finish_time).total_seconds()

                total_idle_time += idle_time_between_last_two_frames.total_seconds()
                total_idle_time += idle_time_after_last_frame
            else:
                previous_frame = worker_trace.frame_render_traces[index - 1]
                previous_frame_finish_time: datetime = previous_frame.finish_time()

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


def analyze_utilization(traces: List[FullTrace]):
    utilization_per_strategy: Dict[FrameDistributionStrategy, List[float]] = {
        FrameDistributionStrategy.NAIVE_FINE: [],
        FrameDistributionStrategy.EAGER_NAIVE_COARSE: [],
        FrameDistributionStrategy.DYNAMIC: [],
    }

    print("Overall results (per-run):")
    for run_trace in traces:
        print(f"Run {run_trace.job.job_name} ({run_trace.job.wait_for_number_of_workers}):")

        utilization_per_worker: List[WorkerUtilization] = [
            WorkerUtilization.from_worker_trace(worker_trace)
            for worker_trace in run_trace.worker_traces.values()
        ]

        max_utilization = max([u.utilization_rate() for u in utilization_per_worker])
        min_utilization = min([u.utilization_rate() for u in utilization_per_worker])
        average_utilization = sum([u.utilization_rate() for u in utilization_per_worker]) / len(utilization_per_worker)

        utilization_per_strategy[run_trace.job.frame_distribution_strategy].append(average_utilization)

        print(f"  max utilization: {max_utilization}")
        print(f"  average utilization: {average_utilization}")
        print(f"  min utilization: {min_utilization}")

    print(f"\n{'-' * 20}\n")
    print("Overall average (per-strategy):")

    for strategy, average_values in utilization_per_strategy.items():
        real_average_utilization = sum(average_values) / len(average_values)
        print(f"Strategy: {strategy} -> average utilization: {real_average_utilization}")

