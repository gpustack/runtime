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
            help="Detect GPUs and their properties",
        )

        detect_parser.add_argument(
            "--format",
            type=str,
            choices=["table", "json"],
            default="table",
            help="Output format",
        )

        detect_parser.add_argument(
            "--watch",
            "-w",
            type=int,
            help="Continuously watch for GPU in intervals of N seconds",
        )

        detect_parser.set_defaults(func=DetectDevicesSubCommand)

    def __init__(self, args: Namespace):
        self.format = args.format
        self.watch = args.watch

    def run(self):
        try:
            while True:
                devs: Devices = detect_devices(fast=False)
                print("\033[2J\033[H", end="")
                match self.format.lower():
                    case "json":
                        print(format_devices_json(devs))
                    case _:
                        # Group devs by manufacturer
                        grouped_devs: dict[str, Devices] = {}
                        for dev in devs:
                            if dev.manufacturer not in grouped_devs:
                                grouped_devs[dev.manufacturer] = []
                            grouped_devs[dev.manufacturer].append(dev)
                        for manu in sorted(grouped_devs.keys()):
                            print(format_devices_table(grouped_devs[manu]))
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

    # Column headers
    col_headers = ["GPU", "Name", "Memory-Usage", "GPU-Util", "Temp", "CC"]
    # Gather all rows to determine max width for each column
    rows = []
    for dev in devs:
        row = [
            str(dev.index),
            dev.name if dev.name else "N/A",
            f"{dev.memory_used}MiB / {dev.memory}MiB",
            f"{dev.cores_utilization}%",
            f"{dev.temperature}C" if dev.temperature is not None else "N/A",
            dev.compute_capability if dev.compute_capability else "N/A",
        ]
        rows.append(row)

    # Calculate max width for each column
    col_widths = [len(header) for header in col_headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    # Add padding
    col_widths = [w + 2 for w in col_widths]

    # Calculate table width
    width = sum(col_widths) + len(col_widths) + 1

    # Header section
    dev = devs[0]
    header_content = f"{dev.manufacturer.upper()} "
    header_content += (
        f"Driver Version: {dev.driver_version if dev.driver_version else 'N/A'} "
    )
    runtime_version_str = (
        f"Runtime Version: {dev.runtime_version if dev.runtime_version else 'N/A'}"
    )
    header_lines = [
        "+" + "-" * (width - 2) + "+",
        f"| {header_content.ljust(width - 4 - len(runtime_version_str))}{runtime_version_str} |",
        "|" + "-" * (width - 2) + "|",
    ]

    # Column header line
    col_header_line = "|"
    for i, header in enumerate(col_headers):
        col_header_line += f" {header.center(col_widths[i] - 2)} |"
    header_lines.append(col_header_line)

    # Separator line
    separator = "|" + "|".join(["-" * w for w in col_widths]) + "|"
    header_lines.append(separator)

    # Device rows
    device_lines = []
    for row in rows:
        row_line = "|"
        for j, data in enumerate(row):
            cell = str(data)
            # Truncate if too long
            if len(cell) > col_widths[j] - 2:
                cell = cell[: col_widths[j] - 5] + "..."
            row_line += f" {cell.ljust(col_widths[j] - 2)} |"
        device_lines.append(row_line)

    # Footer section
    footer_lines = [
        "+" + "-" * (width - 2) + "+",
    ]

    # Combine all parts
    return os.linesep.join(header_lines + device_lines + footer_lines)
