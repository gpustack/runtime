from __future__ import annotations

import json
import os
import time
from typing import TYPE_CHECKING

from ..detector import Devices, detect_devices
from .__types__ import SubCommand

if TYPE_CHECKING:
    from argparse import Namespace, _SubParsersAction


class DetectDevicesSubCommand(SubCommand):
    """
    Command to detect GPUs and their properties.
    """

    format: str = "table"
    watch: int = 0

    @staticmethod
    def register(parser: _SubParsersAction):
        detect_parser = parser.add_parser(
            "detect",
            help="detect GPUs and their properties",
        )

        detect_parser.add_argument(
            "--format",
            type=str,
            choices=["table", "json"],
            default="table",
            help="output format",
        )

        detect_parser.add_argument(
            "--watch",
            "-w",
            type=int,
            help="continuously watch for GPU in intervals of N seconds",
        )

        detect_parser.set_defaults(func=DetectDevicesSubCommand)

    def __init__(self, args: Namespace):
        self.format = args.format
        self.watch = args.watch

    def run(self):
        try:
            while True:
                devs: Devices = detect_devices()
                print("\033[2J\033[H", end="")
                match self.format.lower():
                    case "json":
                        print(format_devices_json(devs))
                    case _:
                        print(format_devices_table(devs))
                if not self.watch:
                    break
                time.sleep(self.watch)
        except KeyboardInterrupt:
            print("\033[2J\033[H", end="")


def format_devices_json(devs: Devices) -> str:
    return json.dumps([dev.to_dict() for dev in devs], indent=2)


def format_devices_table(devs: Devices) -> str:
    if not devs:
        return "No GPUs detected."

    width = 100

    # Header section
    dev = devs[0]
    header_content = f"{dev.manufacturer.upper()} "
    header_content += (
        f"Driver Version: {dev.driver_version if dev.driver_version else 'N/A'} "
    )
    header_content += f"Runtime Version: {dev.runtime_version if dev.runtime_version else 'N/A'}".rjust(
        width - len(header_content) - 4,
    )

    header_lines = [
        "+" + "-" * (width - 2) + "+",
        f"| {header_content.ljust(width - 4)} |",
        "|" + "-" * (width - 2) + "|",
    ]

    # Column headers
    col_headers = ["GPU", "Name", "Memory-Usage", "GPU-Util", "Temp", "CC"]
    col_widths = [5, 31, 20, 10, 6, 6]

    # Adjust column widths to fit total width
    total_current_width = sum(col_widths) + len(col_widths) * 3 - 1
    if total_current_width < width - 2:
        # Distribute extra space to the name column
        col_widths[1] += width - 2 - total_current_width

    # Create column header line
    col_header_line = "|"
    for i, header in enumerate(col_headers):
        col_header_line += f" {header.center(col_widths[i])} |"
    header_lines.append(col_header_line)

    # Separator line
    separator = "|" + "|".join(["-" * (w + 2) for w in col_widths]) + "|"
    header_lines.append(separator)

    # Device rows
    device_lines = []
    for dev in devs:
        row_data: list[str] = [
            str(dev.index),
            dev.name if dev.name else "N/A",
            f"{dev.memory_used}MiB / {dev.memory}MiB",
            f"{dev.cores_utilization}%",
            f"{dev.temperature}C" if dev.temperature is not None else "N/A",
            dev.compute_capability if dev.compute_capability else "N/A",
        ]

        row_line = "|"
        for j, data in enumerate(row_data):
            # Truncate name if too long
            if j == 1 and len(data) > col_widths[j]:
                data = data[: col_widths[j] - 3] + "..."  # noqa: PLW2901
            row_line += f" {data.ljust(col_widths[j])} |"

        device_lines.append(row_line)

    # Footer section
    footer_lines = [
        "+" + "-" * (width - 2) + "+",
    ]

    # Combine all parts
    return os.linesep.join(header_lines + device_lines + footer_lines)
