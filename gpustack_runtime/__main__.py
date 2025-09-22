from __future__ import annotations

import sys
from argparse import ArgumentParser

from ._version import commit_id, version
from .cmds import (
    CreateRunnerWorkloadSubCommand,
    CreateWorkloadSubCommand,
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

    # Register
    subcommand_parser = parser.add_subparsers(
        help="gpustack-runtime command helpers",
    )
    CreateRunnerWorkloadSubCommand.register(subcommand_parser)
    CreateWorkloadSubCommand.register(subcommand_parser)
    DeleteWorkloadSubCommand.register(subcommand_parser)
    GetWorkloadSubCommand.register(subcommand_parser)
    ListWorkloadsSubCommand.register(subcommand_parser)
    LogsWorkloadSubCommand.register(subcommand_parser)
    ExecWorkloadSubCommand.register(subcommand_parser)
    DetectDevicesSubCommand.register(subcommand_parser)

    # Parse
    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)

    # Run
    service = args.func(args)
    service.run()


if __name__ == "__main__":
    main()
