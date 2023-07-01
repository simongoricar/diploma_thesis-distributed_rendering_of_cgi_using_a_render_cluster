import argparse
import time
import os
import sys
import operator
import contextlib
from dataclasses import dataclass
import json

# noinspection PyUnresolvedReferences
import bpy

time_init: float = time.time()


@dataclass
class CLIArguments:
    output_path: str
    output_format: str
    frame_number: int


def parse_cli_arguments() -> CLIArguments:
    parser = argparse.ArgumentParser(
        prog="render-and-timing-script",
        exit_on_error=False
    )

    parser.add_argument(
        "--render-output",
        type=str,
        dest="output_path",
    )

    parser.add_argument(
        "--render-format",
        type=str,
        dest="output_format",
    )

    parser.add_argument(
        "--render-frame",
        type=int,
        dest="frame_number",
    )

    try:
        raw_arguments = list(sys.argv)
        dash_dash_position: int = len(raw_arguments) - operator.indexOf(reversed(raw_arguments), "--") - 1
        script_arguments: list = raw_arguments[dash_dash_position + 1:]

        args = parser.parse_args(args=script_arguments)
        return CLIArguments(
            str(args.output_path),
            str(args.output_format),
            int(args.frame_number)
        )
    except argparse.ArgumentError:
        print("Invalid render-and-timing-script arguments!")
        bpy.ops.wm.quit_blender()
    except ValueError:
        print("Missing render-and-timing-script arguments!")
        bpy.ops.wm.quit_blender()


arguments: CLIArguments = parse_cli_arguments()

bpy.context.scene.frame_set(arguments.frame_number)
bpy.context.scene.render.filepath = arguments.output_path
bpy.context.scene.render.image_settings.file_format = arguments.output_format
bpy.context.scene.render.image_settings.quality = 90

time_render_start: float = time.time()

with open(os.devnull, "w") as null:
    with contextlib.redirect_stdout(null):
        bpy.ops.render.render(animation=False, write_still=True, use_viewport=False)

time_render_end: float = time.time()

results: str = json.dumps({
    "project_loaded_at": time_init,
    "project_started_rendering_at": time_render_start,
    "project_finished_rendering_at": time_render_end,
})

print("RESULTS={}".format(results))

bpy.ops.wm.quit_blender()
