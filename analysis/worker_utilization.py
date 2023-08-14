import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Self, Tuple

import matplotlib.pyplot as plt
from matplotlib.pyplot import Axes
from matplotlib.ticker import AutoMinorLocator
from matplotlib.text import Text

from core.models import JobTrace, FrameDistributionStrategy, WorkerFrameTrace, WorkerTrace
from core.parser import load_traces_from_default_path
from core.paths import WORKER_UTILIZATION_OUTPUT_DIRECTORY
from core.timed_context import timed_section


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

    def utilization_rate_without_setup_and_tail_latency(self) -> float:
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


def plot_utilization_rate_against_cluster_sizes(
    traces: List[JobTrace]
):
    cluster_sizes = [1, 5, 10, 20, 40, 80]

    utilization_columns: List[Tuple[List[float], str]] = []
    for cluster_size in cluster_sizes:
        size_specific_utilization_values = [
            WorkerUtilization.from_worker_trace(worker).utilization_rate()
            for trace in traces
            for worker in trace.worker_traces.values()
            if trace.job.wait_for_number_of_workers == cluster_size
        ]

        utilization_columns.append((
            size_specific_utilization_values,
            f"{cluster_size}"
        ))


    figure = plt.figure(figsize=(7, 5), dpi=100, layout="constrained")
    plot: Axes = figure.add_subplot(label="utilization-against-cluster-size")


    # Plot and add x-axis ticks.
    plot.boxplot(
        [data for (data, _) in utilization_columns],
        vert=True,
        patch_artist=True,
        showfliers=False,
        boxprops={"facecolor": "steelblue", "edgecolor": "white", "linewidth": 0.5},
        whiskerprops={"color": "steelblue", "linewidth": 1.5},
        capprops={"color": "steelblue", "linewidth": 1.5},
        medianprops={"color": "white", "linewidth": 1},
    )

    # plot.violinplot(
    #     [data for (data, _) in utilization_columns],
    #     vert=True,
    #     showmedians=True,
    # )

    plot.set_xticks(
        list(range(1, len(utilization_columns) + 1)),
        labels=[label for (_, label) in utilization_columns],
        fontsize="medium"
    )

    # Add x and y labels.
    plot.set_ylabel(
        r"Izkoriščenost $\in [0, 1]$",
        labelpad=12,
        fontsize="medium",
    )
    plot.set_xlabel(
        "Velikost gruče (št. delovnih vozlišč)",
        labelpad=12,
        fontsize="medium",
    )

    plot.set_ybound(
        lower=0.95,
        upper=1,
    )


    # Add 0.01 minor ticks
    plot.yaxis.set_minor_locator(AutoMinorLocator(5))

    # Make 0.95 and 1 (lower and upper y bound) bold to emphasize that
    # the plot doesn't start at 0.
    major_tick_labels = plot.get_ymajorticklabels()
    major_tick_labels_to_emphasize = [major_tick_labels[0], major_tick_labels[-1]]
    for tick_label in major_tick_labels_to_emphasize:
        tick_label: Text
        tick_label.set_fontweight("bold")

    plot.grid(visible=True, axis="y")


    plot.set_title(
        "Izkoriščenost glede na velikost gruče",
        pad=24,
        fontsize="x-large"
    )


    figure.savefig(
        WORKER_UTILIZATION_OUTPUT_DIRECTORY
        / f"worker-utilization_against_cluster-size.png",
        dpi=100,
    )


def plot_utilization_rate_against_strategies(
    traces: List[JobTrace]
):
    strategies = [
        FrameDistributionStrategy.NAIVE_FINE,
        FrameDistributionStrategy.EAGER_NAIVE_COARSE,
        FrameDistributionStrategy.DYNAMIC,
    ]

    strategy_columns: List[Tuple[List[float], str]] = []
    for strategy in strategies:
        size_specific_utilization_values = [
            WorkerUtilization.from_worker_trace(worker).utilization_rate()
            for trace in traces
            for worker in trace.worker_traces.values()
            if trace.job.frame_distribution_strategy == strategy
        ]

        strategy_name: str
        if strategy == FrameDistributionStrategy.NAIVE_FINE:
            strategy_name = "Naivna drobnozrnata"
        elif strategy == FrameDistributionStrategy.EAGER_NAIVE_COARSE:
            strategy_name = "Takojšnja naivna grobozrnata"
        elif strategy == FrameDistributionStrategy.DYNAMIC:
            strategy_name = "Dinamična s krajo"
        else:
            raise RuntimeError("Invalid distribution strategy.")

        strategy_columns.append((
            size_specific_utilization_values,
            f"{strategy_name}"
        ))


    ###
    # Print some statistics
    max_utilization = max([
        WorkerUtilization.from_worker_trace(worker).utilization_rate()
        for trace in traces
        for worker in trace.worker_traces.values()
    ])
    mean_utilization = statistics.mean([
        WorkerUtilization.from_worker_trace(worker).utilization_rate()
        for trace in traces
        for worker in trace.worker_traces.values()
    ])
    median_utilization = statistics.median([
        WorkerUtilization.from_worker_trace(worker).utilization_rate()
        for trace in traces
        for worker in trace.worker_traces.values()
    ])
    min_utilization = min([
        WorkerUtilization.from_worker_trace(worker).utilization_rate()
        for trace in traces
        for worker in trace.worker_traces.values()
    ])

    print("Statistics:\n"
          f"max utilization: {max_utilization}\n"
          f"min utilization: {min_utilization}\n"
          f"mean utilization: {mean_utilization}\n"
          f"median utilization: {median_utilization}\n"
          f"samples per strategy: {', '.join([f'{strategy}: {len(value)}' for (value, strategy) in strategy_columns])}")
    ###

    figure = plt.figure(figsize=(7, 5), dpi=100, layout="constrained")
    plot: Axes = figure.add_subplot(label="utilization-against-strategy")


    # Plot and add x-axis ticks.
    plot.boxplot(
        [data for (data, _) in strategy_columns],
        vert=True,
        patch_artist=True,
        showfliers=False,
        boxprops={"facecolor": "steelblue", "edgecolor": "white", "linewidth": 0.5},
        whiskerprops={"color": "steelblue", "linewidth": 1.5},
        capprops={"color": "steelblue", "linewidth": 1.5},
        medianprops={"color": "white", "linewidth": 1},
    )

    # plot.violinplot(
    #     [data for (data, _) in strategy_columns],
    #     vert=True,
    #     showmedians=True,
    # )

    plot.set_xticks(
        list(range(1, len(strategy_columns) + 1)),
        labels=[label for (_, label) in strategy_columns],
        fontsize="medium"
    )

    # Add x and y labels.
    plot.set_ylabel(
        r"Izkoriščenost $\in [0, 1]$",
        labelpad=12,
        fontsize="medium",
    )
    plot.set_xlabel(
        "Porazdeljevalna strategija",
        labelpad=12,
        fontsize="medium",
    )

    plot.set_ybound(
        lower=0.95,
        upper=1,
    )


    # Add 0.01 minor ticks
    plot.yaxis.set_minor_locator(AutoMinorLocator(5))

    # Make 0.95 and 1 (lower and upper y bound) bold to emphasize that
    # the plot doesn't start at 0.
    major_tick_labels = plot.get_ymajorticklabels()
    major_tick_labels_to_emphasize = [major_tick_labels[0], major_tick_labels[-1]]
    for tick_label in major_tick_labels_to_emphasize:
        tick_label: Text
        tick_label.set_fontweight("bold")

    plot.grid(visible=True, axis="y")


    plot.set_title(
        "Izkoriščenost glede na tip porazdeljevalne strategije",
        pad=24,
        fontsize="x-large"
    )

    figure.savefig(
        WORKER_UTILIZATION_OUTPUT_DIRECTORY
        / f"worker-utilization_against_distribution-strategy.png",
        dpi=100,
    )


def plot_non_tail_utilization_rate_against_cluster_sizes(
    traces: List[JobTrace]
):
    cluster_sizes = [1, 5, 10, 20, 40, 80]

    utilization_columns: List[Tuple[List[float], str]] = []
    for cluster_size in cluster_sizes:
        size_specific_utilization_values = [
            WorkerUtilization.from_worker_trace(worker).utilization_rate_without_setup_and_tail_latency()
            for trace in traces
            for worker in trace.worker_traces.values()
            if trace.job.wait_for_number_of_workers == cluster_size
        ]

        utilization_columns.append((
            size_specific_utilization_values,
            f"{cluster_size}"
        ))

    figure = plt.figure(figsize=(7, 5), dpi=100, layout="constrained")
    plot: Axes = figure.add_subplot(label="non-tail-utilization-against-cluster-size")

    # Plot and add x-axis ticks.
    plot.boxplot(
        [data for (data, _) in utilization_columns],
        vert=True,
        patch_artist=True,
        showfliers=False,
        boxprops={"facecolor": "steelblue", "edgecolor": "white", "linewidth": 0.5},
        whiskerprops={"color": "steelblue", "linewidth": 1.5},
        capprops={"color": "steelblue", "linewidth": 1.5},
        medianprops={"color": "white", "linewidth": 1},
    )

    # plot.violinplot(
    #     [data for (data, _) in utilization_columns],
    #     vert=True,
    #     showmedians=True,
    # )

    plot.set_xticks(
        list(range(1, len(utilization_columns) + 1)),
        labels=[label for (_, label) in utilization_columns],
        fontsize="medium"
    )

    # Add x and y labels.
    plot.set_ylabel(
        r"Izkoriščenost $\in [0, 1]$",
        labelpad=12,
        fontsize="medium",
    )
    plot.set_xlabel(
        "Velikost gruče (št. delovnih vozlišč)",
        labelpad=12,
        fontsize="medium",
    )

    plot.set_ybound(
        lower=0.95,
        upper=1,
    )

    # Add 0.01 minor ticks
    plot.yaxis.set_minor_locator(AutoMinorLocator(5))

    # Make 0.95 and 1 (lower and upper y bound) bold to emphasize that
    # the plot doesn't start at 0.
    major_tick_labels = plot.get_ymajorticklabels()
    major_tick_labels_to_emphasize = [major_tick_labels[0], major_tick_labels[-1]]
    for tick_label in major_tick_labels_to_emphasize:
        tick_label: Text
        tick_label.set_fontweight("bold")

    plot.grid(visible=True, axis="y")

    plot.set_title(
        "Izkoriščenost glede na velikost gruče (brez repa)",
        pad=24,
        fontsize="x-large"
    )

    figure.savefig(
        WORKER_UTILIZATION_OUTPUT_DIRECTORY
        / f"worker-non-tail-utilization_against_cluster-size.png",
        dpi=100,
    )



def main_plot(traces: List[JobTrace]):
    with timed_section("Worker Utilization"):
        with plt.style.context("seaborn-v0_8-paper"):
            plot_utilization_rate_against_cluster_sizes(traces)
            plot_non_tail_utilization_rate_against_cluster_sizes(traces)
            plot_utilization_rate_against_strategies(traces)


def main():
    traces = load_traces_from_default_path()
    main_plot(traces)


if __name__ == '__main__':
    main()
