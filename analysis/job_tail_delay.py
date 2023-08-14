import statistics
from datetime import datetime
from itertools import chain
from typing import List

import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.pyplot import Axes
from matplotlib.patches import Patch
from matplotlib.text import Text
from matplotlib.ticker import LinearLocator, MaxNLocator

from core.models import JobTrace, FrameDistributionStrategy
from core.parser import load_traces_from_default_path
from core.paths import JOB_TAIL_DELAY_OUTPUT_DIRECTORY
from core.timed_context import timed_section


def _plot_tail_delay_for_cluster_in_seconds(
    traces: List[JobTrace],
    cluster_size: int,
    plot: Axes,
    plot_y_maximum: float,
    set_x_label: bool,
    set_y_label: bool,
):
    # Extract data
    naive_fine_runs = [
        run
        for run in traces
        if run.job.frame_distribution_strategy == FrameDistributionStrategy.NAIVE_FINE
        and run.job.wait_for_number_of_workers == cluster_size
    ]
    naive_fine_tail_delays: List[float] = []
    for run in naive_fine_runs:
        run_last_global_frame_finished_at = run.get_last_frame_finished_at()
        naive_fine_tail_delays.append(
            max(
                worker.get_tail_delay_without_teardown(run_last_global_frame_finished_at)
                for worker in run.worker_traces.values()
            )
        )


    eager_naive_coarse_runs = [
        run
        for run in traces
        if run.job.frame_distribution_strategy == FrameDistributionStrategy.EAGER_NAIVE_COARSE
        and run.job.wait_for_number_of_workers == cluster_size
    ]
    eager_naive_coarse_tail_delays: List[float] = []
    for run in eager_naive_coarse_runs:
        run_last_global_frame_finished_at = run.get_last_frame_finished_at()
        eager_naive_coarse_tail_delays.append(
            max(
                worker.get_tail_delay_without_teardown(run_last_global_frame_finished_at)
                for worker in run.worker_traces.values()
            )
        )


    dynamic_runs = [
        run
        for run in traces
        if run.job.frame_distribution_strategy == FrameDistributionStrategy.DYNAMIC
        and run.job.wait_for_number_of_workers == cluster_size
    ]
    dynamic_tail_delays: List[float] = []
    for run in dynamic_runs:
        run_last_global_frame_finished_at = run.get_last_frame_finished_at()
        dynamic_tail_delays.append(
            max(
                worker.get_tail_delay_without_teardown(run_last_global_frame_finished_at)
                for worker in run.worker_traces.values()
            )
        )


    ###
    # Print some statistics
    max_tail_delay = max(chain(naive_fine_tail_delays, eager_naive_coarse_tail_delays, dynamic_tail_delays))
    min_tail_delay = min(chain(naive_fine_tail_delays, eager_naive_coarse_tail_delays, dynamic_tail_delays))

    print(f"Statistics for cluster size: {cluster_size}:\n"
          f"max tail delay: {max_tail_delay}\n"
          f"min tail delay: {min_tail_delay}")
    ###

    # Compose into box plot

    box_plot = plot.boxplot(
        [
            naive_fine_tail_delays, eager_naive_coarse_tail_delays, dynamic_tail_delays
        ],
        vert=True,
        patch_artist=True,
        medianprops={"color": "dimgrey", "linewidth": 1},
        showfliers=False,
    )

    if set_y_label:
        plot.set_ylabel(
            "Repni zamik v sekundah",
            labelpad=12,
            fontsize="medium",
        )
    if set_x_label:
        plot.set_xlabel(
            "Porazdeljevalna strategija",
            labelpad=12,
            fontsize="medium",
        )

    plot.set_ybound(
        lower=0,
        upper=plot_y_maximum
    )

    plot.grid(visible=True, axis="y")

    plot.set_xticks(
        [1, 2, 3],
        labels=[
            "Naivna drobnozrnata",
            "Takojšnja naivna grobozrnata",
            "Dinamična s krajo"
        ],
        rotation=3,
    )

    patch_colours = ["mediumaquamarine", "lightskyblue", "palegoldenrod"]
    for patch, color in zip(box_plot["boxes"], patch_colours):
        patch: Patch
        patch.set_facecolor(color)

    plot.set_title(
        f"{cluster_size} delovnih vozlišč",
        pad=10,
        fontsize="large",
    )


def _plot_tail_delay_for_cluster_scaled_to_average_frame_time(
    traces: List[JobTrace],
    cluster_size: int,
    plot: Axes,
    set_x_label: bool,
    set_y_label: bool,
):
    # Extract data
    naive_fine_runs = [
        run
        for run in traces
        if run.job.frame_distribution_strategy == FrameDistributionStrategy.NAIVE_FINE
        and run.job.wait_for_number_of_workers == cluster_size
    ]

    naive_fine_tail_delays_scaled_to_avg_frame_time: List[float] = []
    for run in naive_fine_runs:
        run_last_global_frame_finished_at = run.get_last_frame_finished_at()
        run_average_frame_time = statistics.mean([
            (frame.finish_time() - frame.start_time()).total_seconds()
            for worker in run.worker_traces.values()
            for frame in worker.frame_render_traces
        ])

        naive_fine_tail_delays_scaled_to_avg_frame_time.append(
            max(
                worker.get_tail_delay_without_teardown(run_last_global_frame_finished_at)
                for worker in run.worker_traces.values()
            ) / run_average_frame_time
        )


    eager_naive_coarse_runs = [
        run
        for run in traces
        if run.job.frame_distribution_strategy == FrameDistributionStrategy.EAGER_NAIVE_COARSE
        and run.job.wait_for_number_of_workers == cluster_size
    ]
    eager_naive_coarse_tail_delays: List[float] = []
    for run in eager_naive_coarse_runs:
        run_last_global_frame_finished_at = run.get_last_frame_finished_at()
        run_average_frame_time = statistics.mean([
            (frame.finish_time() - frame.start_time()).total_seconds()
            for worker in run.worker_traces.values()
            for frame in worker.frame_render_traces
        ])

        eager_naive_coarse_tail_delays.append(
            max(
                worker.get_tail_delay_without_teardown(run_last_global_frame_finished_at)
                for worker in run.worker_traces.values()
            ) / run_average_frame_time
        )


    dynamic_runs = [
        run
        for run in traces
        if run.job.frame_distribution_strategy == FrameDistributionStrategy.DYNAMIC
        and run.job.wait_for_number_of_workers == cluster_size
    ]
    dynamic_tail_delays: List[float] = []
    for run in dynamic_runs:
        run_last_global_frame_finished_at = run.get_last_frame_finished_at()
        run_average_frame_time = statistics.mean([
            (frame.finish_time() - frame.start_time()).total_seconds()
            for worker in run.worker_traces.values()
            for frame in worker.frame_render_traces
        ])

        dynamic_tail_delays.append(
            max(
                worker.get_tail_delay_without_teardown(run_last_global_frame_finished_at)
                for worker in run.worker_traces.values()
            ) / run_average_frame_time
        )


    # Compose into box plot
    box_plot = plot.boxplot(
        [
            naive_fine_tail_delays_scaled_to_avg_frame_time,
            eager_naive_coarse_tail_delays,
            dynamic_tail_delays,
        ],
        vert=True,
        patch_artist=True,
        medianprops={"color": "dimgrey", "linewidth": 1},
        showfliers=False,
    )

    if set_y_label:
        plot.set_ylabel(
            r"$\frac{Repni~zamik~v~sekundah}{Povprečen~čas~izrisa~ene~sličice}$",
            labelpad=12,
            fontsize="medium",
        )
    if set_x_label:
        plot.set_xlabel(
            "Porazdeljevalna strategija",
            labelpad=12,
            fontsize="medium",
        )

    plot.set_ybound(
        lower=0,
        upper=2,
    )


    # Make 0.00 and 2.00 bold to emphasize them.
    major_tick_labels = plot.get_ymajorticklabels()
    if len(major_tick_labels) > 0:
        major_tick_labels[0].set_fontweight("bold")
        major_tick_labels[-1].set_fontweight("bold")


    plot.grid(visible=True, axis="y")

    plot.set_xticks(
        [1, 2, 3],
        labels=[
            "Naivna drobnozrnata",
            "Takojšnja naivna grobozrnata",
            "Dinamična s krajo"
        ],
        rotation=5,
    )

    patch_colours = ["mediumaquamarine", "lightskyblue", "palegoldenrod"]
    for patch, color in zip(box_plot["boxes"], patch_colours):
        patch: Patch
        patch.set_facecolor(color)

    plot.set_title(
        f"{cluster_size} delovnih vozlišč",
        pad=10,
        fontsize="large",
    )




def plot_tail_delay(
    traces: List[JobTrace],
):
    global_maximum_tail_delay = max([
        max(worker.get_tail_delay() for worker in run.worker_traces.values())
        for run in traces
    ])

    ####
    ## Scaled to seconds
    ####
    first_figure: Figure = plt.figure(figsize=(10, 14), dpi=100, layout="constrained")

    first_plots = first_figure.subplots(nrows=3, ncols=2, sharey="col", sharex="row")
    first_figure.delaxes(first_plots[2, 1])

    first_figure.suptitle(
        "Latenca ob zaključevanju poslov"
    )

    _plot_tail_delay_for_cluster_in_seconds(
        traces,
        5,
        first_plots[0, 0],
        global_maximum_tail_delay,
        False,
        True,
    )
    _plot_tail_delay_for_cluster_in_seconds(
        traces,
        10,
        first_plots[0, 1],
        global_maximum_tail_delay,
        False,
        False,
    )
    _plot_tail_delay_for_cluster_in_seconds(
        traces,
        20,
        first_plots[1, 0],
        global_maximum_tail_delay,
        False,
        True,
    )
    _plot_tail_delay_for_cluster_in_seconds(
        traces,
        40,
        first_plots[1, 1],
        global_maximum_tail_delay,
        False,
        False,
    )
    _plot_tail_delay_for_cluster_in_seconds(
        traces,
        80,
        first_plots[2, 0],
        global_maximum_tail_delay,
        True,
        True,
    )

    first_figure.savefig(
        JOB_TAIL_DELAY_OUTPUT_DIRECTORY / "job-tail-delay_all-in-one.png",
        dpi=100,
    )

    ####
    ## Scaled to average frame time
    ####
    second_figure: Figure = plt.figure(figsize=(10, 14), dpi=100, layout="constrained")

    second_plots = second_figure.subplots(nrows=3, ncols=2, sharey="col", sharex="row")
    second_figure.delaxes(second_plots[2, 1])

    second_figure.suptitle(
        "Latenca ob zaključevanju poslov, normalizirana na povprečen čas izrisa sličice"
    )

    _plot_tail_delay_for_cluster_scaled_to_average_frame_time(
        traces,
        5,
        second_plots[0, 0],
        False,
        True,
    )
    _plot_tail_delay_for_cluster_scaled_to_average_frame_time(
        traces,
        10,
        second_plots[0, 1],
        False,
        False,
    )
    _plot_tail_delay_for_cluster_scaled_to_average_frame_time(
        traces,
        20,
        second_plots[1, 0],
        False,
        True,
    )
    _plot_tail_delay_for_cluster_scaled_to_average_frame_time(
        traces,
        40,
        second_plots[1, 1],
        False,
        False,
    )
    _plot_tail_delay_for_cluster_scaled_to_average_frame_time(
        traces,
        80,
        second_plots[2, 0],
        True,
        True,
    )


    # second_plots[1, 1].grid(visible=True, axis="y")
    # second_plots[1, 1].set_xticks(
    #     [1, 2, 3],
    #     labels=[
    #         "Naivna drobnozrnata",
    #         "Takojšnja naivna grobozrnata",
    #         "Dinamična s krajo"
    #     ],
    #     rotation=5,
    # )



    second_figure.savefig(
        JOB_TAIL_DELAY_OUTPUT_DIRECTORY / "job-tail-delay_scaled-to-avg-frame-time_all-in-one.png",
        dpi=100,
    )


def main_plot(traces: List[JobTrace]):
    with timed_section("Tail Delay"):
        with plt.style.context("seaborn-v0_8-paper"):
            plot_tail_delay(traces)


def main():
    traces = load_traces_from_default_path()
    main_plot(traces)


if __name__ == '__main__':
    main()
