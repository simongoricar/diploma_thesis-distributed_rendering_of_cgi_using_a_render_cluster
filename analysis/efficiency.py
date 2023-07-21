import statistics
from typing import List, Tuple

import matplotlib.pyplot as plt
from matplotlib.container import BarContainer
from matplotlib.patches import Rectangle
from matplotlib.pyplot import Axes, Figure
from matplotlib.text import Text
from matplotlib.ticker import AutoMinorLocator

from core.models import JobTrace, FrameDistributionStrategy
from core.parser import load_traces_from_default_path
from core.paths import EFFICIENCY_OUTPUT_DIRECTORY
from core.timed_context import timed_section


def plot_efficiency(traces: List[JobTrace]):
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
    bar_width = 0.32


    figure: Figure = plt.figure(figsize=(10, 6), dpi=100, layout="constrained")
    plot: Axes = figure.add_subplot(label=f"efficiency")


    mean_sequential_job_time_seconds: float = statistics.mean([
        (job.get_job_finished_at() - job.get_job_started_at()).total_seconds()
        for job in traces
        if job.job.wait_for_number_of_workers == 1
        and job.job.frame_distribution_strategy == FrameDistributionStrategy.EAGER_NAIVE_COARSE
    ])

    mean_speedup_columns_per_strategy: List[
        Tuple[
            List[Tuple[float, int, str]],
            FrameDistributionStrategy,
            str,
        ]
    ] = []

    for strategy in strategies_to_plot:
        speedups_on_strategy: List[Tuple[float, int, str]] = []

        for cluster_size in cluster_sizes_to_plot:
            mean_parallel_job_time_seconds: float = statistics.mean([
                (job.get_job_finished_at() - job.get_job_started_at()).total_seconds()
                for job in traces
                if job.job.wait_for_number_of_workers == cluster_size
            ])

            mean_parallel_job_efficiency: float = \
                mean_sequential_job_time_seconds / (cluster_size * mean_parallel_job_time_seconds)

            speedups_on_strategy.append((
                mean_parallel_job_efficiency,
                cluster_size,
                f"{cluster_size}"
            ))

        strategy_name: str
        if strategy == FrameDistributionStrategy.NAIVE_FINE:
            strategy_name = "Naivna drobnozrnata strategija"
        elif strategy == FrameDistributionStrategy.EAGER_NAIVE_COARSE:
            strategy_name = "Takojšnja naivna grobozrnata strategija"
        elif strategy == FrameDistributionStrategy.DYNAMIC:
            strategy_name = "Dinamična strategija s krajo"
        else:
            raise RuntimeError("Invalid distribution strategy.")

        mean_speedup_columns_per_strategy.append((
            speedups_on_strategy,
            strategy,
            strategy_name
        ))


    # Plot single worker reference
    single_worker_bar_plot: BarContainer = plot.bar(
        [1],
        [1],
        width=bar_width,
        label="_Takojšnja naivna grobozrnata strategija"
    )

    plot.bar_label(
        single_worker_bar_plot,
        labels=[1],
        padding=-20,
        fontweight="bold"
    )

    for patch in single_worker_bar_plot.patches:
        patch: Rectangle
        patch.set_facecolor(strategy_to_colour[FrameDistributionStrategy.EAGER_NAIVE_COARSE])


    # Plot multi-worker speedups per-strategy
    for index, (per_strategy_data, strategy, strategy_label) in enumerate(mean_speedup_columns_per_strategy):
        multi_worker_bar_plot: BarContainer = plot.bar(
            [offset + bar_width * index - bar_width for offset in range(2, len(per_strategy_data) + 2)],
            [value for (value, _, _) in per_strategy_data],
            width=bar_width,
            label=strategy_label
        )

        plot.bar_label(
            multi_worker_bar_plot,
            labels=[round(value, 3) for (value, _, _) in per_strategy_data],
            padding=-20,
        )

        for patch in multi_worker_bar_plot.patches:
            patch: Rectangle
            patch.set_facecolor(strategy_to_colour[strategy])


    plot.set_xticks(
        list(range(1, len(cluster_sizes_to_plot) + 2)),
        [f"{size}" for size in [1, *cluster_sizes_to_plot]],
        rotation=3,
    )


    plot.set_ybound(
        lower=0,
        upper=1,
    )

    # Make 0 and 1.0 bold to emphasize them.
    major_tick_labels = plot.get_ymajorticklabels()
    major_tick_labels_to_emphasize = [major_tick_labels[0], major_tick_labels[-1]]
    for tick_label in major_tick_labels_to_emphasize:
        tick_label: Text
        tick_label.set_fontweight("bold")


    plot.legend(
        loc="lower left",
        ncols=1,
        fontsize="large",
    )

    plot.set_xlabel(
        "Velikost gruče (št. delovnih vozlišč)",
        labelpad=12,
        fontsize="medium",
    )
    plot.set_ylabel(
        "Učinkovitost",
        labelpad=12,
        fontsize="medium",
    )


    # Add minor ticks
    plot.yaxis.set_minor_locator(AutoMinorLocator(5))

    plot.grid(visible=True, axis="y")

    plot.set_title(
        "Učinkovitost v primerjavi s sekvenčnim sistemom",
        pad=24,
        fontsize="x-large",
    )


    figure.savefig(
        EFFICIENCY_OUTPUT_DIRECTORY
        / "efficiency.png"
    )



def main_plot(traces: List[JobTrace]):
    with timed_section("Efficiency"):
        with plt.style.context("seaborn-v0_8-paper"):
            plot_efficiency(traces)

def main():
    traces = load_traces_from_default_path()
    main_plot(traces)


if __name__ == '__main__':
    main()
