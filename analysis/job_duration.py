import statistics
from typing import List, Tuple

import matplotlib.pyplot as plt
from matplotlib.container import BarContainer
from matplotlib.patches import Rectangle
from matplotlib.pyplot import Axes, Figure
from matplotlib.ticker import AutoMinorLocator

from core.models import JobTrace, FrameDistributionStrategy
from core.parser import load_traces_from_default_path
from core.paths import JOB_DURATION_OUTPUT_DIRECTORY
from core.timed_context import timed_section


def plot_job_duration_against_cluster_sizes_and_strategies(
    traces: List[JobTrace]
):
    cluster_sizes_to_plot = [5, 10, 20, 40, 80]
    strategies_to_plot = [
        FrameDistributionStrategy.NAIVE_FINE,
        FrameDistributionStrategy.EAGER_NAIVE_COARSE,
        FrameDistributionStrategy.DYNAMIC,
    ]

    strategy_to_colour = {
        FrameDistributionStrategy.NAIVE_FINE: "bisque",
        FrameDistributionStrategy.EAGER_NAIVE_COARSE: "mediumaquamarine",
        FrameDistributionStrategy.DYNAMIC: "slateblue",
    }

    figure: Figure = plt.figure(figsize=(8, 9), dpi=100, layout="constrained")
    plot: Axes = figure.add_subplot(label=f"job-duration-against-cluster-sizes-and-strategies")


    duration_hours_mean_columns_per_strategy: List[
        Tuple[
            List[Tuple[float, str]],
            FrameDistributionStrategy,
            str
        ]
    ] = []

    for strategy in strategies_to_plot:
        columns_per_cluster_size: List[Tuple[float, str]] = []

        for cluster_size in cluster_sizes_to_plot:
            results_in_hours = statistics.mean([
                (run.get_job_finished_at() - run.get_job_started_at()).total_seconds()
                for run in traces
                if run.job.frame_distribution_strategy == strategy
                and run.job.wait_for_number_of_workers == cluster_size
            ]) / (60 * 60)

            columns_per_cluster_size.append((results_in_hours, f"{cluster_size}"))

        strategy_name: str
        if strategy == FrameDistributionStrategy.NAIVE_FINE:
            strategy_name = "Naivna drobnozrnata strategija"
        elif strategy == FrameDistributionStrategy.EAGER_NAIVE_COARSE:
            strategy_name = "Takojšnja naivna grobozrnata strategija"
        elif strategy == FrameDistributionStrategy.DYNAMIC:
            strategy_name = "Dinamična strategija s krajo"
        else:
            raise RuntimeError("Invalid distribution strategy.")

        duration_hours_mean_columns_per_strategy.append((
            columns_per_cluster_size,
            strategy,
            f"{strategy_name}",
        ))

    bar_width = 0.3

    # Plot 1-worker data
    mean_hours_single_worker_eager_data = statistics.mean([
        (run.get_job_finished_at() - run.get_job_started_at()).total_seconds()
        for run in traces
        if run.job.frame_distribution_strategy == FrameDistributionStrategy.EAGER_NAIVE_COARSE
        and run.job.wait_for_number_of_workers == 1
    ]) / (60 * 60)

    single_worker_bar_plot: BarContainer = plot.bar(
        [1],
        [mean_hours_single_worker_eager_data],
        width=bar_width,
        label="_Takojšnja naivna grobozrnata strategija"
    )

    plot.bar_label(
        single_worker_bar_plot,
        labels=[round(mean_hours_single_worker_eager_data, 1)]
    )

    for patch in single_worker_bar_plot.patches:
        patch: Rectangle
        patch.set_facecolor(strategy_to_colour[FrameDistributionStrategy.EAGER_NAIVE_COARSE])


    plot.set_xticks(
        list(range(1, len(cluster_sizes_to_plot) + 1)),
        [f"{size}" for size in cluster_sizes_to_plot],
        rotation=3,
    )


    # Plot multi-worker data
    for index, (per_size_values, strategy, strategy_label) in enumerate(duration_hours_mean_columns_per_strategy):
        multi_worker_bar_plot: BarContainer = plot.bar(
            [value + bar_width * index - bar_width for value in range(2, len(per_size_values) + 2)],
            [per_size_value for (per_size_value, _) in per_size_values],
            width=bar_width,
            label=strategy_label,
        )

        plot.bar_label(
            multi_worker_bar_plot,
            labels=[round(per_size_value, 1) for (per_size_value, _) in per_size_values]
        )

        for patch in multi_worker_bar_plot.patches:
            patch: Rectangle
            patch.set_facecolor(strategy_to_colour[strategy])


    plot.set_xticks(
        list(range(1, len(cluster_sizes_to_plot) + 2)),
        [f"{size}" for size in [1, *cluster_sizes_to_plot]],
        rotation=3,
    )

    plot.legend(
        loc="upper right",
        ncols=1,
        fontsize="medium",
    )

    plot.set_xlabel(
        "Velikost gruče (št. delovnih vozlišč)",
        labelpad=12,
        fontsize="medium",
    )
    plot.set_ylabel(
        "Čas izrisovanja projekta v urah",
        labelpad=12,
        fontsize="medium",
    )

    # Add 0.01 minor ticks
    plot.yaxis.set_minor_locator(AutoMinorLocator(5))

    plot.grid(visible=True, axis="y")

    plot.set_title(
        "Čas izrisovanja primerjalnega Blender projekta",
        pad=24,
        fontsize="x-large",
    )


    figure.savefig(
        JOB_DURATION_OUTPUT_DIRECTORY
        / "job-duration.png"
    )


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



def main_plot(traces: List[JobTrace]):
    with timed_section("Job Duration"):
        with plt.style.context("seaborn-v0_8-paper"):
            plot_job_duration_against_cluster_sizes_and_strategies(traces)


def main():
    traces = load_traces_from_default_path()
    main_plot(traces)


if __name__ == '__main__':
    main()
