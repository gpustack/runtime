# PYTHON_ARGCOMPLETE_OK
from __future__ import annotations

import contextlib
import sys
import time
from argparse import ArgumentParser

import argcomplete

from . import deployer, detector
from ._version import commit_id, version
from .cmds import (
    CopyImagesSubCommand,
    CreateRunnerWorkloadSubCommand,
    CreateWorkloadSubCommand,
    DeleteWorkloadsSubCommand,
    DeleteWorkloadSubCommand,
    DetectDevicesSubCommand,
    ExecWorkloadSubCommand,
    GetDevicesTopologySubCommand,
    GetWorkloadSubCommand,
    InspectWorkloadSubCommand,
    ListImagesSubCommand,
    ListWorkloadsSubCommand,
    LogsWorkloadSubCommand,
    SaveImagesSubCommand,
)
from .logging import setup_logging


def main():
    setup_logging()

    parser = ArgumentParser(
        "gpustack-runtime",
        description="GPUStack Runtime CLI",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {version}({commit_id})",
        help="Show the version and exit",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Display available behaviors",
    )
    parser.add_argument(
        "--watch",
        "-w",
        type=int,
        help="Continuously display available behaviors (used with --profile) in intervals of N seconds",
        default=0,
    )

    # Register
    subcommand_parser = parser.add_subparsers(
        help="gpustack-runtime command helpers",
    )
    CreateRunnerWorkloadSubCommand.register(subcommand_parser)
    CreateWorkloadSubCommand.register(subcommand_parser)
    DeleteWorkloadSubCommand.register(subcommand_parser)
    DeleteWorkloadsSubCommand.register(subcommand_parser)
    GetWorkloadSubCommand.register(subcommand_parser)
    ListWorkloadsSubCommand.register(subcommand_parser)
    LogsWorkloadSubCommand.register(subcommand_parser)
    ExecWorkloadSubCommand.register(subcommand_parser)
    InspectWorkloadSubCommand.register(subcommand_parser)
    DetectDevicesSubCommand.register(subcommand_parser)
    GetDevicesTopologySubCommand.register(subcommand_parser)
    ListImagesSubCommand.register(subcommand_parser)
    SaveImagesSubCommand.register(subcommand_parser)
    CopyImagesSubCommand.register(subcommand_parser)

    # Autocomplete
    argcomplete.autocomplete(parser)

    # Parse
    args = parser.parse_args()
    if getattr(args, "profile", False):
        profile(getattr(args, "watch", 0))
        sys.exit(0)
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    # Run
    service = args.func(args)
    try:
        service.run()
    except KeyboardInterrupt:
        print("\033[2J\033[H", end="")


def profile(watch: int):
    try:
        while True:
            available_deployers: list[str] = []
            available_detectors: list[str] = []
            with contextlib.suppress(Exception):
                for dep in deployer.supported_list():
                    available_deployers.append(dep.name)
                for det in detector.supported_list():
                    available_detectors.append(det.name)
            print("\033[2J\033[H", end="")
            print(f"Available Deployers: [{', '.join(available_deployers)}]")
            print(f"Available Detectors: [{', '.join(available_detectors)}]")
            if not watch:
                break
            time.sleep(watch)
    except KeyboardInterrupt:
        print("\033[2J\033[H", end="")


if __name__ == "__main__":
    main()
