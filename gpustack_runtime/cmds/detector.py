from __future__ import annotations

import json
import os
import time
from typing import TYPE_CHECKING

from ..detector import (
    Devices,
    Topology,
    detect_devices,
    get_devices_topologies,
    group_devices_by_manufacturer,
)
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
        while True:
            devs: Devices = detect_devices(fast=False)
            print("\033[2J\033[H", end="")
            match self.format.lower():
                case "json":
                    print(format_devices_json(devs))
                case _:
                    # Group devices by manufacturer.
                    group_devs = group_devices_by_manufacturer(devs)
                    if not group_devs:
                        print("No GPUs detected.")
                    else:
                        # Print each group separately.
                        for devs in group_devs.values():
                            print(format_devices_table(devs))
            if not self.watch:
                break
            time.sleep(self.watch)


class GetDevicesTopologySubCommand(SubCommand):
    """
    Command to detect GPUs topology.
    """

    format: str = "table"

    @staticmethod
    def register(parser: _SubParsersAction):
        topo_parser = parser.add_parser(
            "topology",
            help="Detect GPUs topology",
            aliases=["topo"],
        )

        topo_parser.add_argument(
            "--format",
            type=str,
            choices=["table", "json"],
            default="table",
            help="Output format",
        )

        topo_parser.set_defaults(func=GetDevicesTopologySubCommand)

    def __init__(self, args: Namespace):
        self.format = args.format

    def run(self):
        topologies = get_devices_topologies(fast=False)
        print("\033[2J\033[H", end="")
        if not topologies:
            print("No GPU topology information available.")
            return

        match self.format.lower():
            case "json":
                print(format_topologies_json(topologies))
            case _:
                for topo in topologies:
                    print(format_topology_table(topo))

                # Legend
                legend_lines = [
                    "",
                    "Legend (from nearest to farthest):",
                    "  X    = Self",
                    "  LINK = Connection traversing with High-Speed Link (e.g., AMD XGMI, Ascend HCCS, NVIDIA NVLink)",
                    "  PIX  = Connection traversing at most a single PCIe bridge",
                    "  PXB  = Connection traversing multiple PCIe bridges (without traversing the PCIe Host Bridge)",
                    "  PHB  = Connection traversing PCIe as well as a PCIe Host Bridge (typically the CPU)",
                    "  NODE = Connection traversing PCIe and the interconnect between NUMA nodes",
                    "  SYS  = Connection traversing PCIe as well as the SMP interconnect between NUMA nodes (e.g., QPI/UPI)",
                    "  N/A  = Unknown or unavailable information",
                ]
                print(os.linesep.join(legend_lines))


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


def format_topology_table(topo: Topology) -> str:
    content = topo.stringify()

    # Column headers
    col_headers = [str(topo.manufacturer).upper()]
    col_headers += ["Device " + str(idx) for idx in range(len(content))]
    col_headers += [
        "CPU Affinity",
        "NUMA Affinity",
    ]
    # Gather all rows to determine max width for each column
    rows = []
    for row_idx, row_devs in enumerate(content):
        row = ["Device " + str(row_idx), *row_devs]
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
    header_lines = [
        "+" + "-" * (width - 2) + "+",
    ]

    # Column header line
    col_header_line = "|"
    for i, header in enumerate(col_headers):
        col_header_line += f" {header.center(col_widths[i] - 2)} |"
    header_lines.append(col_header_line)

    # Separator line
    separator = "|" + "|".join(["-" * w for w in col_widths]) + "|"
    header_lines.append(separator)

    # Topology rows
    topology_lines = []
    for row in rows:
        row_line = "|"
        for j, data in enumerate(row):
            cell = str(data)
            # Truncate if too long
            if len(cell) > col_widths[j] - 2:
                cell = cell[: col_widths[j] - 5] + "..."
            row_line += f" {cell.ljust(col_widths[j] - 2)} |"
        topology_lines.append(row_line)

    # Footer section
    footer_lines = [
        "+" + "-" * (width - 2) + "+",
    ]

    # Combine all parts
    return os.linesep.join(header_lines + topology_lines + footer_lines)


def format_topologies_json(topologies: list[Topology]) -> str:
    return json.dumps([topo.to_dict() for topo in topologies], indent=2)
