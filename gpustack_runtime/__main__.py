from __future__ import annotations

import contextlib
import sys
from argparse import ArgumentParser

from gpustack_runtime import deployer, detector

from ._version import commit_id, version
from .cmds import (
    CreateRunnerWorkloadSubCommand,
    CreateWorkloadSubCommand,
    DeleteWorkloadsSubCommand,
    DeleteWorkloadSubCommand,
    DetectDevicesSubCommand,
    ExecWorkloadSubCommand,
    GetWorkloadSubCommand,
    ListWorkloadsSubCommand,
    LogsWorkloadSubCommand,
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
        help="show the version and exit",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="display available behaviors",
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
    DetectDevicesSubCommand.register(subcommand_parser)

    # Parse
    args = parser.parse_args()
    if getattr(args, "profile", False):
        profile()
        sys.exit(0)
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    # Run
    service = args.func(args)
    service.run()


def profile():
    print("\033[2J\033[H", end="")

    available_deployers: list[str] = []
    available_detectors: list[str] = []
    with contextlib.suppress(Exception):
        for dep in deployer.deployers:
            if dep.is_supported():
                available_deployers.append(dep.name)
        for det in detector.detectors:
            if det.is_supported():
                available_detectors.append(str(det.manufacturer))

    print(f"Available Deployers: [{', '.join(available_deployers)}]")
    print(f"Available Detectors: [{', '.join(available_detectors)}]")


if __name__ == "__main__":
    main()
