from typing import List

import matplotlib.pyplot as plt
from matplotlib.pyplot import Axes
from matplotlib.patches import Patch

from core.models import JobTrace, FrameDistributionStrategy
from core.parser import load_traces_from_default_path
from core.paths import JOB_TAIL_DELAY_OUTPUT_DIRECTORY


def plot_tail_delay_for_cluster(
    traces: List[JobTrace],
    cluster_size: int,
    plot: Axes,
    plot_y_maximum: float,
):
    # Extract data
    naive_fine_results = [
        run
        for run in traces
        if run.job.frame_distribution_strategy == FrameDistributionStrategy.NAIVE_FINE
        and run.job.wait_for_number_of_workers == cluster_size
    ]
    naive_fine_tail_delays = [
        max(worker.get_tail_delay() for worker in run.worker_traces.values())
        for run in naive_fine_results
    ]

    eager_naive_coarse_results = [
        run
        for run in traces
        if run.job.frame_distribution_strategy == FrameDistributionStrategy.EAGER_NAIVE_COARSE
        and run.job.wait_for_number_of_workers == cluster_size
    ]
    eager_naive_coarse_tail_delays = [
        max(worker.get_tail_delay() for worker in run.worker_traces.values())
        for run in eager_naive_coarse_results
    ]

    dynamic_results = [
        run
        for run in traces
        if run.job.frame_distribution_strategy == FrameDistributionStrategy.DYNAMIC
        and run.job.wait_for_number_of_workers == cluster_size
    ]
    dynamic_tail_delays = [
        max(worker.get_tail_delay() for worker in run.worker_traces.values())
        for run in dynamic_results
    ]

    # Compose into box plot

    box_plot = plot.boxplot(
        [
            naive_fine_tail_delays, eager_naive_coarse_tail_delays, dynamic_tail_delays
        ],
        vert=True,
        patch_artist=True,
    )

    plot.set_ylabel(
        "Zamik v sekundah",
        labelpad=12,
        fontsize="medium",
    )
    plot.set_xlabel(
        "Porazdeljevalna strategija",
        labelpad=12,
        fontsize="medium",
    )

    plot.set_ybound(
        lower=0,
        upper=plot_y_maximum
    )

    plot.grid(visible=True)

    plot.set_xticks(
        [1, 2, 3],
        labels=[
            "Naivno drobnozrnato",
            "Takojšnje naivno grobozrnato",
            "Dinamično s krajo"
        ],
        rotation=6,
    )

    patch_colours = ["mediumaquamarine", "lightskyblue", "palegoldenrod"]
    for patch, color in zip(box_plot["boxes"], patch_colours):
        patch: Patch
        patch.set_facecolor(color)

    plot.set_title(
        f"Repni zamik ({cluster_size} delovnih vozlišč)",
        pad=24,
        fontsize="x-large",
    )




def plot_tail_delay(
    traces: List[JobTrace],
):
    global_maximum_tail_delay = max([
        max(worker.get_tail_delay() for worker in run.worker_traces.values())
        for run in traces
    ])


    def plot_and_save(
        cluster_size: int,
    ):
        figure = plt.figure(figsize=(7, 5), dpi=100, layout="constrained")
        plot = figure.add_subplot(label=f"td-{cluster_size}")

        plot_tail_delay_for_cluster(
            traces,
            cluster_size,
            plot,
            global_maximum_tail_delay,
        )

        figure.savefig(
            JOB_TAIL_DELAY_OUTPUT_DIRECTORY / f"job-tail-delay_{cluster_size:02}-workers",
            dpi=100,
        )

    plot_and_save(5)
    plot_and_save(10)
    plot_and_save(20)
    plot_and_save(40)
    plot_and_save(80)


def main():
    traces = load_traces_from_default_path()
    plot_tail_delay(traces)


if __name__ == '__main__':
    main()
