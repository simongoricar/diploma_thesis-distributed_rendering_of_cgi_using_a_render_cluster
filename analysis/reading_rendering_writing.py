import statistics
from enum import Enum
from typing import List, Tuple

import matplotlib.pyplot as plt
from matplotlib.container import BarContainer
from matplotlib.figure import Figure
from matplotlib.pyplot import Axes
from matplotlib.text import Text

from core.models import JobTrace
from core.parser import load_traces_from_default_path
from core.paths import READING_RENDERING_WRITING_OUTPUT_DIRECTORY
from core.timed_context import timed_section


class TimeType(Enum):
    LOADING = "loading-time"
    RENDERING = "rendering-time"
    SAVING = "saving-time"


def plot_reading_render_writing_distribution_against_cluster_size(
    traces: List[JobTrace]
):
    figure: Figure = plt.figure(figsize=(8, 5), dpi=100, layout="constrained")
    plot: Axes = figure.add_subplot()

    cluster_sizes_to_plot = [1, 5, 10, 20, 40, 80]
    bar_height = 0.65
    color_per_time_type = {
        TimeType.LOADING: "mistyrose",
        TimeType.RENDERING: "wheat",
        TimeType.SAVING: "lavender",
    }

    data: List[Tuple[
        List[Tuple[float, TimeType]],
        str,
    ]] = []
    for cluster_size in cluster_sizes_to_plot:
        runs_at_cluster_size: List[JobTrace] = [
            run for run in traces
            if run.job.wait_for_number_of_workers == cluster_size
        ]

        mean_loading_time_seconds: float = statistics.mean([
            (frame.finished_loading_at - frame.started_process_at).total_seconds()
            for run in runs_at_cluster_size
            for worker in run.worker_traces.values()
            for frame in worker.frame_render_traces
        ])
        mean_rendering_time_seconds: float = statistics.mean([
            (frame.finished_rendering_at - frame.started_rendering_at).total_seconds()
            for run in runs_at_cluster_size
            for worker in run.worker_traces.values()
            for frame in worker.frame_render_traces
        ])
        mean_saving_time_seconds: float = statistics.mean([
            (frame.file_saving_finished_at - frame.file_saving_started_at).total_seconds()
            for run in runs_at_cluster_size
            for worker in run.worker_traces.values()
            for frame in worker.frame_render_traces
        ])

        mean_time_total = mean_loading_time_seconds + mean_rendering_time_seconds + mean_saving_time_seconds

        mean_loading_time_fraction = mean_loading_time_seconds / mean_time_total
        mean_rendering_time_fraction = mean_rendering_time_seconds / mean_time_total
        mean_saving_time_fraction = mean_saving_time_seconds / mean_time_total

        data.append((
            [
                (mean_loading_time_fraction, TimeType.LOADING),
                (mean_rendering_time_fraction, TimeType.RENDERING),
                (mean_saving_time_fraction, TimeType.SAVING),
            ],
            f"{cluster_size}"
        ))

    # Render data

    def find_time_type_at_cluster_size(time_type_: TimeType, cluster_size_: int) -> float:
        for (cluster_size_values, cluster_size_label) in data:
            if cluster_size_label == f"{cluster_size_}":
                cluster_size_values: List[Tuple[float, TimeType]]

                for (value, current_time_type) in cluster_size_values:
                    if current_time_type == time_type_:
                        return value


    left_offsets_per_cluster_size: List[float] = [0, 0, 0, 0, 0, 0]
    for time_type in [TimeType.LOADING, TimeType.RENDERING, TimeType.SAVING]:
        data_per_cluster_size: List[Tuple[float, str]] = [
            (find_time_type_at_cluster_size(time_type, cluster_size), f"{cluster_size}")
            for cluster_size in cluster_sizes_to_plot
        ]

        time_type_name: str
        if time_type == TimeType.LOADING:
            time_type_name = "Nalaganje projektnih datotek"
        elif time_type == TimeType.RENDERING:
            time_type_name = "Izrisovanje"
        elif time_type == TimeType.SAVING:
            time_type_name = "Shranjevanje izrisane sličice"
        else:
            raise RuntimeError("Invalid time type.")

        barh_plot: BarContainer = plot.barh(
            [size_str for (_, size_str) in data_per_cluster_size],
            [mean_value for (mean_value, _) in data_per_cluster_size],
            height=bar_height,
            left=left_offsets_per_cluster_size,
            label=time_type_name,
            color=color_per_time_type[time_type]
        )

        labels: List[Text] = plot.bar_label(
            barh_plot,
            label_type="center",
            padding=2,
            fontsize="medium",
            fmt=lambda value: f"{round(value * 100, 1)}%",
        )

        if time_type == TimeType.SAVING:
            for label in labels:
                label.set_x(label.get_unitless_position()[0] - 28)

        for index, (previous_value, this_value) in enumerate(zip(
            left_offsets_per_cluster_size,
            [mean_value for (mean_value, _) in data_per_cluster_size]
        )):
            left_offsets_per_cluster_size[index] = previous_value + this_value

    plot.legend(
        ncols=3,
        bbox_to_anchor=(0, 1),
        loc="lower left",
        fontsize="small",
    )

    plot.set_title(
        "Delitev izrisovalnega časa na nalaganje, izrisovanje in shranjevanje",
        pad=40,
        fontsize="large",
    )

    plot.set_xlabel(
        "Povprečni delež izrisovalnega časa",
        labelpad=12,
        fontsize="medium",
    )
    plot.set_ylabel(
        "Število delovnih vozlišč v gruči",
        labelpad=12,
        fontsize="medium",
    )

    plot.set_xticks([])
    plot.set_xticks([], minor=True)

    plot.set_xbound(
        lower=0,
        upper=1
    )

    figure.savefig(
        READING_RENDERING_WRITING_OUTPUT_DIRECTORY
        / "reading-rendering-writing-distribution.png",
        dpi=100,
    )


def main_plot(traces: List[JobTrace]):
    with timed_section("Reading-Rendering-Writing"):
        with plt.style.context("seaborn-v0_8-paper"):
            plot_reading_render_writing_distribution_against_cluster_size(traces)


def main():
    traces = load_traces_from_default_path()
    main_plot(traces)


if __name__ == '__main__':
    main()
