from __future__ import annotations

from .deployer import (
    CDIGenerateSubCommand,
    CreateWorkloadSubCommand,
    DeleteWorkloadsSubCommand,
    DeleteWorkloadSubCommand,
    ExecSelfSubCommand,
    ExecWorkloadSubCommand,
    GetWorkloadSubCommand,
    InspectSelfSubCommand,
    InspectWorkloadSubCommand,
    ListWorkloadsSubCommand,
    LogsSelfSubCommand,
    LogsWorkloadSubCommand,
)
from .detector import DetectDevicesSubCommand, GetDevicesTopologySubCommand
from .images import (
    CopyImagesSubCommand,
    ListImagesSubCommand,
    LoadImagesSubCommand,
    PlatformedImage,
    SaveImagesSubCommand,
    append_images,
    list_images,
)

__all__ = [
    "CDIGenerateSubCommand",
    "CopyImagesSubCommand",
    "CreateWorkloadSubCommand",
    "DeleteWorkloadSubCommand",
    "DeleteWorkloadsSubCommand",
    "DetectDevicesSubCommand",
    "ExecSelfSubCommand",
    "ExecWorkloadSubCommand",
    "GetDevicesTopologySubCommand",
    "GetWorkloadSubCommand",
    "InspectSelfSubCommand",
    "InspectWorkloadSubCommand",
    "ListImagesSubCommand",
    "ListWorkloadsSubCommand",
    "LoadImagesSubCommand",
    "LogsSelfSubCommand",
    "LogsWorkloadSubCommand",
    "PlatformedImage",
    "SaveImagesSubCommand",
    "append_images",
    "list_images",
]
