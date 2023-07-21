import math
import time
from contextlib import contextmanager

@contextmanager
def timed_section(
    title: str,
    header_target_width: int = 80,
    header_padding_min: int = 2,
    header_padding_character: str = "=",
    precision: int = 2,
):
    start_time_padding_length = header_target_width - len(title) - 2
    padding_left: int
    padding_right: int
    if start_time_padding_length % 2 == 0:
        padding_left = max(header_padding_min, start_time_padding_length // 2)
        padding_right = max(header_padding_min, start_time_padding_length // 2)
    else:
        padding_left = max(header_padding_min, math.floor(start_time_padding_length / 2))
        padding_right = max(header_padding_min, math.ceil(start_time_padding_length / 2))

    print(f"{header_padding_character * padding_left} {title} {header_padding_character * padding_right}")

    time_begin = time.time()

    try:
        yield
    finally:
        time_end = time.time()
        time_delta = round(time_end - time_begin, precision)

        end_time_padding_length = header_target_width - len(title) - 33 - len(str(time_delta))
        padding_left: int
        padding_right: int
        if end_time_padding_length % 2 == 0:
            padding_left = max(header_padding_min, end_time_padding_length // 2)
            padding_right = max(header_padding_min, end_time_padding_length // 2)
        else:
            padding_left = max(header_padding_min, math.floor(end_time_padding_length / 2))
            padding_right = max(header_padding_min, math.ceil(end_time_padding_length / 2))

        print(f"{header_padding_character * padding_left} END OF {title} (completed in {time_delta} seconds) {header_padding_character * padding_right}")
        print()
