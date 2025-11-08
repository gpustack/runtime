from __future__ import annotations

from .deployer import (
    CreateRunnerWorkloadSubCommand,
    CreateWorkloadSubCommand,
    DeleteWorkloadsSubCommand,
    DeleteWorkloadSubCommand,
    ExecWorkloadSubCommand,
    GetWorkloadSubCommand,
    ListWorkloadsSubCommand,
    LogsWorkloadSubCommand,
)
from .detector import DetectDevicesSubCommand
from .images import (
    CopyImagesSubCommand,
    ListImagesSubCommand,
    SaveImagesSubCommand,
)

__all__ = [
    "CopyImagesSubCommand",
    "CreateRunnerWorkloadSubCommand",
    "CreateWorkloadSubCommand",
    "DeleteWorkloadSubCommand",
    "DeleteWorkloadsSubCommand",
    "DetectDevicesSubCommand",
    "ExecWorkloadSubCommand",
    "GetWorkloadSubCommand",
    "ListImagesSubCommand",
    "ListWorkloadsSubCommand",
    "LogsWorkloadSubCommand",
    "SaveImagesSubCommand",
]
