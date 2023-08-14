import statistics
from typing import List, Tuple

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.pyplot import Axes, Figure
from matplotlib.text import Text
from matplotlib.ticker import AutoMinorLocator

from core.models import JobTrace
from core.parser import load_traces_from_default_path
from core.paths import WORKER_LATENCY_OUTPUT_DIRECTORY
from core.timed_context import timed_section


def plot_latency_against_cluster_size(
    traces: List[JobTrace]
):
    cluster_sizes_to_plot = [1, 5, 10, 20, 40, 80]


    figure: Figure = plt.figure(figsize=(7, 5), dpi=100, layout="constrained")
    plot: Axes = figure.add_subplot(label=f"job-duration-against-cluster-sizes-and-strategies")


    ping_latency_in_ms_per_size: List[Tuple[List[float], str]] = []
    for cluster_size in cluster_sizes_to_plot:
        ping_traces_at_cluster_size = [
            worker.ping_traces for job in traces
            if job.job.wait_for_number_of_workers == cluster_size
            for worker in job.worker_traces.values()
        ]

        ping_latencies_in_ms_at_cluster_size: List[float] = [
            ping.latency() * 1000
            for ping_list in ping_traces_at_cluster_size
            for ping in ping_list
        ]

        ping_latency_in_ms_per_size.append((
            ping_latencies_in_ms_at_cluster_size,
            f"{cluster_size}"
        ))


    ###
    # Print some statistics
    overall_maximum_ping_latency = max([
        ping_value
        for ping_data, _ in ping_latency_in_ms_per_size
        for ping_value in ping_data
    ])
    overall_mean_ping_latency = statistics.mean([
        ping_value
        for ping_data, _ in ping_latency_in_ms_per_size
        for ping_value in ping_data
    ])
    overall_median_ping_latency = statistics.median([
        ping_value
        for ping_data, _ in ping_latency_in_ms_per_size
        for ping_value in ping_data
    ])
    overall_minimum_ping_latency = min([
        ping_value
        for ping_data, _ in ping_latency_in_ms_per_size
        for ping_value in ping_data
    ])

    num_pings = len([
        ping_value
        for ping_data, _ in ping_latency_in_ms_per_size
        for ping_value in ping_data
    ])
    num_pings_above_25 = len([
        ping_value
        for ping_data, _ in ping_latency_in_ms_per_size
        for ping_value in ping_data
        if ping_value > 25
    ])

    print("Statistics:\n"
          f"pings performed: {num_pings}\n"
          f"overall maximum ping ms: {overall_maximum_ping_latency}\n"
          f"overall minimum ping ms: {overall_minimum_ping_latency}\n"
          f"overall mean ping ms: {overall_mean_ping_latency}\n"
          f"overall median ping ms: {overall_median_ping_latency}\n"
          f"overall number of pings above 50 ms: {num_pings_above_25}")
    ###


    # plot.violinplot(
    #     [data for (data, _) in ping_latency_in_ms_per_size],
    #     vert=True,
    #     showmeans=True,
    #     showmedians=False,
    #     showextrema=False,
    #     points=2000,
    # )

    plot.boxplot(
        [data for (data, _) in ping_latency_in_ms_per_size],
        vert=True,
        patch_artist=True,
        showfliers=False,
        boxprops={"facecolor": "steelblue", "edgecolor": "white", "linewidth": 0.5},
        whiskerprops={"color": "steelblue", "linewidth": 1.5},
        capprops={"color": "steelblue", "linewidth": 1.5},
        medianprops={"color": "white", "linewidth": 1},
    )

    plot.set_xticks(
        list(range(1, len(ping_latency_in_ms_per_size) + 1)),
        labels=[label for (_, label) in ping_latency_in_ms_per_size],
        fontsize="medium",
    )


    plot.set_ylabel(
        "Komunikacijska latenca v milisekundah",
        labelpad=12,
        fontsize="medium",
    )
    plot.set_xlabel(
        "Velikost gruče (št. delovnih vozlišč)",
        labelpad=12,
        fontsize="medium",
    )

    plot.set_ybound(
        lower=0,
        upper=5,
    )

    plot.yaxis.set_minor_locator(AutoMinorLocator(5))

    # Make 0 and 5 ms (lower and upper y bound) bold to emphasize that
    # the maximum is tiny.
    major_tick_labels = plot.get_ymajorticklabels()
    major_tick_labels_to_emphasize = [major_tick_labels[0], major_tick_labels[-1]]
    for tick_label in major_tick_labels_to_emphasize:
        tick_label: Text
        tick_label.set_fontweight("bold")

    plot.grid(visible=True, axis="y")


    plot.set_title(
        "Komunikacijska latenca glede na velikost gruče",
        pad=24,
        fontsize="x-large",
    )


    figure.savefig(
        WORKER_LATENCY_OUTPUT_DIRECTORY
        / "worker-latency_against_cluster-size.png"
    )



def main_plot(traces: List[JobTrace]):
    with timed_section("Worker Latency"):
        with plt.style.context("seaborn-v0_8-paper"):
            matplotlib.rcParams["path.simplify_threshold"] = 0.005
            plot_latency_against_cluster_size(traces)


def main():
    traces = load_traces_from_default_path()
    main_plot(traces)


if __name__ == '__main__':
    main()
