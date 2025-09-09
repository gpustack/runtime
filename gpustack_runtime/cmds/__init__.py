from __future__ import annotations

from .deployer import (
    CreateRunnerWorkloadSubCommand,
    CreateWorkloadSubCommand,
    DeleteWorkloadSubCommand,
    GetWorkloadSubCommand,
    ListWorkloadsSubCommand,
    LogsWorkloadSubCommand,
)
from .detector import DetectDevicesSubCommand

__all__ = [
    "CreateRunnerWorkloadSubCommand",
    "CreateWorkloadSubCommand",
    "DeleteWorkloadSubCommand",
    "DetectDevicesSubCommand",
    "GetWorkloadSubCommand",
    "ListWorkloadsSubCommand",
    "LogsWorkloadSubCommand",
]
