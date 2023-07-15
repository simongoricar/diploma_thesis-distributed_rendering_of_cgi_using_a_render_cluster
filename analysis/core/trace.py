from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from os import PathLike
from pathlib import Path
from typing import Union, Self, List, Dict, Any, Optional
import json


class FrameDistributionStrategy(Enum):
    NAIVE_FINE = "naive_fine"
    EAGER_NAIVE_COARSE = "naive_coarse"
    DYNAMIC = "dynamic"

    @classmethod
    def from_raw_data(cls, raw_distribution_strategy_data: Dict[str, Any]) -> Self:
        strategy_type = str(raw_distribution_strategy_data["strategy_type"])

        if strategy_type == "naive-fine":
            return FrameDistributionStrategy.NAIVE_FINE
        elif strategy_type == "eager-naive-coarse":
            return FrameDistributionStrategy.EAGER_NAIVE_COARSE
        elif strategy_type == "dynamic":
            return FrameDistributionStrategy.DYNAMIC
        else:
            raise RuntimeError(f"Invalid strategy type: {strategy_type}!")

    @classmethod
    def from_job_definition_file_contents(cls, job_definition_data: dict) -> Self:
        strategy_dict: dict = job_definition_data["frame_distribution_strategy"]
        strategy_type = str(strategy_dict["strategy_type"])

        if strategy_type == "naive-fine":
            return FrameDistributionStrategy.NAIVE_FINE
        elif strategy_type == "naive-coarse":
            return FrameDistributionStrategy.EAGER_NAIVE_COARSE
        elif strategy_type == "dynamic":
            return FrameDistributionStrategy.DYNAMIC
        else:
            raise RuntimeError(f"Invalid strategy type: {strategy_type}!")


@dataclass
class WorkerFrameTrace:
    frame_index: int

    started_process_at: datetime
    finished_loading_at: datetime
    started_rendering_at: datetime
    finished_rendering_at: datetime
    file_saving_started_at: datetime
    file_saving_finished_at: datetime
    exited_process_at: datetime

    @classmethod
    def from_raw_data(cls, raw_render_trace_data: Dict[str, Any]) -> Self:
        frame_index = int(raw_render_trace_data["frame_index"])

        details: Dict[str, Any] = raw_render_trace_data["details"]

        started_process_at = datetime.fromtimestamp(details["started_process_at"])
        finished_loading_at = datetime.fromtimestamp(details["finished_loading_at"])
        started_rendering_at = datetime.fromtimestamp(details["started_rendering_at"])
        finished_rendering_at = datetime.fromtimestamp(details["finished_rendering_at"])
        file_saving_started_at = datetime.fromtimestamp(details["file_saving_started_at"])
        file_saving_finished_at = datetime.fromtimestamp(details["file_saving_finished_at"])
        exited_process_at = datetime.fromtimestamp(details["exited_process_at"])

        return cls(
            frame_index=frame_index,
            started_process_at=started_process_at,
            finished_loading_at=finished_loading_at,
            started_rendering_at=started_rendering_at,
            finished_rendering_at=finished_rendering_at,
            file_saving_started_at=file_saving_started_at,
            file_saving_finished_at=file_saving_finished_at,
            exited_process_at=exited_process_at,
        )

    def start_time(self) -> datetime:
        return self.started_process_at

    def finish_time(self) -> datetime:
        return self.exited_process_at


@dataclass
class WorkerPingTrace:
    pinged_at: datetime
    received_at: datetime

    @classmethod
    def from_raw_data(cls, raw_ping_trace_data: Dict[str, Any]) -> Self:
        pinged_at = datetime.fromtimestamp(raw_ping_trace_data["pinged_at"])
        received_at = datetime.fromtimestamp(raw_ping_trace_data["received_at"])

        return cls(
            pinged_at=pinged_at,
            received_at=received_at,
        )

@dataclass
class WorkerReconnectionTrace:
    lost_connection_at: datetime
    reconnected_at: datetime

    @classmethod
    def from_raw_data(cls, raw_reconnection_trace_data: Dict[str, Any]) -> Self:
        lost_connection_at = datetime.fromtimestamp(raw_reconnection_trace_data["lost_connection_at"])
        reconnected_at = datetime.fromtimestamp(raw_reconnection_trace_data["reconnected_at"])

        return cls(
            lost_connection_at=lost_connection_at,
            reconnected_at=reconnected_at,
        )


@dataclass
class WorkerTrace:
    total_queued_frames: int
    total_queued_frames_removed_from_queue: int

    worker_job_start_time: datetime
    worker_job_finish_time: datetime

    frame_render_traces: List[WorkerFrameTrace]
    ping_traces: List[WorkerPingTrace]
    reconnection_traces: List[WorkerReconnectionTrace]

    @classmethod
    def from_raw_data(cls, raw_worker_trace_data: Dict[str, Any]) -> Self:
        total_queued_frames = int(raw_worker_trace_data["total_queued_frames"])
        total_queued_frames_removed_from_queue = \
            int(raw_worker_trace_data["total_queued_frames_removed_from_queue"])

        job_start_time = datetime.fromtimestamp(raw_worker_trace_data["job_start_time"])
        job_finish_time = datetime.fromtimestamp(raw_worker_trace_data["job_finish_time"])

        frame_render_traces: List[WorkerFrameTrace] = [
            WorkerFrameTrace.from_raw_data(raw_frame_trace)
            for raw_frame_trace in raw_worker_trace_data["frame_render_traces"]
        ]

        ping_traces: List[WorkerPingTrace] = [
            WorkerPingTrace.from_raw_data(raw_ping_trace)
            for raw_ping_trace in raw_worker_trace_data["ping_traces"]
        ]

        reconnection_traces: List[WorkerReconnectionTrace] = [
            WorkerReconnectionTrace.from_raw_data(raw_reconnection_trace)
            for raw_reconnection_trace in raw_worker_trace_data["reconnection_traces"]
        ]

        return cls(
            total_queued_frames=total_queued_frames,
            total_queued_frames_removed_from_queue=total_queued_frames_removed_from_queue,
            worker_job_start_time=job_start_time,
            worker_job_finish_time=job_finish_time,
            frame_render_traces=frame_render_traces,
            ping_traces=ping_traces,
            reconnection_traces=reconnection_traces,
        )

    def has_ping_traces(self) -> bool:
        return self.ping_traces is not None

    def get_tail_delay(self) -> float:
        last_frame = self.frame_render_traces[len(self.frame_render_traces) - 1]

        return (self.worker_job_finish_time - last_frame.exited_process_at).total_seconds()



@dataclass
class BlenderJob:
    job_name: str
    job_description: Optional[str]

    project_file_path: str
    render_script_path: str

    frame_range_from: int
    frame_range_to: int

    wait_for_number_of_workers: int

    frame_distribution_strategy: FrameDistributionStrategy

    output_directory_path: str
    output_file_name_format: str
    output_file_format: str

    @classmethod
    def from_raw_data(cls, raw_blender_job_data: Dict[str, Any]) -> Self:
        job_name = str(raw_blender_job_data["job_name"])
        job_description = str(raw_blender_job_data["job_description"])

        project_file_path = str(raw_blender_job_data["project_file_path"])
        render_script_path = str(raw_blender_job_data["render_script_path"])

        frame_range_from = int(raw_blender_job_data["frame_range_from"])
        frame_range_to = int(raw_blender_job_data["frame_range_to"])

        wait_for_number_of_workers = int(raw_blender_job_data["wait_for_number_of_workers"])

        frame_distribution_strategy = \
            FrameDistributionStrategy.from_raw_data(raw_blender_job_data["frame_distribution_strategy"])

        output_directory_path = str(raw_blender_job_data["output_directory_path"])
        output_file_name_format = str(raw_blender_job_data["output_file_name_format"])
        output_file_format = str(raw_blender_job_data["output_file_format"])

        return cls(
            job_name=job_name,
            job_description=job_description,
            project_file_path=project_file_path,
            render_script_path=render_script_path,
            frame_range_from=frame_range_from,
            frame_range_to=frame_range_to,
            wait_for_number_of_workers=wait_for_number_of_workers,
            frame_distribution_strategy=frame_distribution_strategy,
            output_directory_path=output_directory_path,
            output_file_name_format=output_file_name_format,
            output_file_format=output_file_format
        )




@dataclass
class FullTrace:
    job: BlenderJob

    job_started_at: datetime
    job_finished_at: datetime

    worker_traces: Dict[str, WorkerTrace]

    @classmethod
    def load_from_trace_file(
            cls,
            trace_file_path: Union[str, PathLike],
    ) -> Self:
        trace_file_path = Path(trace_file_path)

        # Validate paths
        if not trace_file_path.is_file():
            raise RuntimeError(f"Missing raw trace file: {trace_file_path}!")

        # Load files
        with trace_file_path.open(mode="r", encoding="utf8") as file:
            trace_data: dict = json.load(file)

        # Extract information
        job = BlenderJob.from_raw_data(trace_data["job"])

        _master_trace: dict = trace_data["master_trace"]
        master_job_started_at = datetime.fromtimestamp(_master_trace["job_start_time"])
        master_job_finished_at = datetime.fromtimestamp(_master_trace["job_finish_time"])


        worker_traces: Dict[str, WorkerTrace] = {
            name: WorkerTrace.from_raw_data(raw_worker_data)
            for name, raw_worker_data in trace_data["worker_traces"].items()
        }

        if len(worker_traces) != job.wait_for_number_of_workers:
            raise ValueError(
                f"Invalid data: len(worker_traces) = {len(worker_traces)}, "
                f"but wait_for_number_of_workers = {job.wait_for_number_of_workers}!"
            )

        return cls(
            job=job,
            job_started_at=master_job_started_at,
            job_finished_at=master_job_finished_at,
            worker_traces=worker_traces,
        )

    def estimate_job_started_at(self) -> datetime:
        return min(
            trace.worker_job_start_time
            for trace in self.worker_traces.values()
        )

    def estimate_job_finished_at(self) -> datetime:
        return min(
            trace.worker_job_finish_time
            for trace in self.worker_traces.values()
        )

    def get_job_started_at(self) -> datetime:
        return self.job_started_at or self.estimate_job_started_at()

    def get_job_finished_at(self) -> datetime:
        return self.job_finished_at or self.estimate_job_finished_at()
