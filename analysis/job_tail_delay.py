from typing import List

import matplotlib.pyplot as plt
from matplotlib.pyplot import Axes
from matplotlib.patches import Patch
import numpy as np

from core.models import JobTrace, FrameDistributionStrategy
from core.parser import load_traces_from_default_path

def plot_single_worker_tail_delay(
    traces: List[JobTrace],
    plot: Axes,
    plot_y_maximum: float,
):
    # Extract data
    results = [
        run for run in traces
        if run.job.wait_for_number_of_workers == 1
    ]
    tail_delays = [
        max(worker.get_tail_delay() for worker in run.worker_traces.values())
        for run in results
    ]

    # Compose into box plot
    box_plot = plot.boxplot(
        tail_delays,
        vert=True,
        patch_artist=True,
    )

    plot.set_xticks(
        [1],
        labels=[
            "Takojšnje naivno grobozrnato",
        ],
        rotation=6,
    )

    plot.set_xlabel("Zamik v sekundah")
    plot.set_ylabel("Porazdeljevalna strategija")

    plot.set_ybound(
        lower=0,
        upper=plot_y_maximum
    )

    plot.grid(visible=True)

    patch: Patch = box_plot["boxes"][0]
    patch.set_facecolor("lightskyblue")

    plot.set_xticks(
        [1],
        labels=["Takojšnje naivno grobozrnato"],
        rotation=6,
    )

    plot.set_title(f"Repni zamik (1 delovno vozlišče)")

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

    plot.set_xlabel("Zamik v sekundah")
    plot.set_ylabel("Porazdeljevalna strategija")

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

    plot.set_title(f"Repni zamik ({cluster_size} delovnih vozlišč)")




def plot_tail_delay(traces: List[JobTrace]):
    figure = plt.figure(figsize=(20, 12), dpi=100, layout="constrained")

    subplots: np.ndarray = figure.subplots(nrows=3, ncols=2)

    global_maximum_tail_delay = max([
        max(worker.get_tail_delay() for worker in run.worker_traces.values())
        for run in traces
    ])

    # TODO Hold up, 1-worker versions doesn't even make sense for this plot.
    plot_single_worker_tail_delay(
        traces,
        subplots[0, 0],
        global_maximum_tail_delay
    )
    plot_tail_delay_for_cluster(
        traces,
        5,
        subplots[0, 1],
        global_maximum_tail_delay
    )
    plot_tail_delay_for_cluster(
        traces,
        10,
        subplots[1, 0],
        global_maximum_tail_delay
    )
    plot_tail_delay_for_cluster(
        traces,
        20,
        subplots[1, 1],
        global_maximum_tail_delay
    )
    plot_tail_delay_for_cluster(
        traces,
        40,
        subplots[2, 0],
        global_maximum_tail_delay
    )
    plot_tail_delay_for_cluster(
        traces,
        80,
        subplots[2, 1],
        global_maximum_tail_delay
    )

    figure.show()

    # TODO Save to file, etc.


def analyze_tail_delay(traces: List[JobTrace]):
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


def main():
    traces = load_traces_from_default_path()
    plot_tail_delay(traces)


if __name__ == '__main__':
    main()
