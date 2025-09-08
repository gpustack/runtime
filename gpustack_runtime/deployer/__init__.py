from __future__ import annotations

from typing import TYPE_CHECKING

from .__types__ import (
    Container,
    ContainerCapabilities,
    ContainerCheck,
    ContainerCheckExecution,
    ContainerCheckHTTP,
    ContainerCheckTCP,
    ContainerEnv,
    ContainerExecution,
    ContainerFile,
    ContainerMount,
    ContainerMountModeEnum,
    ContainerPort,
    ContainerPortProtocolEnum,
    ContainerProfileEnum,
    ContainerResources,
    ContainerRestartPolicyEnum,
    ContainerSecurity,
    OperationError,
    UnsupportedError,
    WorkloadOperationToken,
    WorkloadPlan,
    WorkloadSecurity,
    WorkloadSecuritySysctl,
    WorkloadStatus,
    WorkloadStatusStateEnum,
)
from .docker import DockerDeployer, DockerWorkloadPlan, DockerWorkloadStatus

if TYPE_CHECKING:
    from .__types__ import Deployer, WorkloadName

deployers: list[Deployer] = [
    DockerDeployer(),
]


def create_workload(workload: WorkloadPlan):
    """
    Deploy the given workload.

    Args:
        workload:
            The workload to deploy.

    Raises:
        UnsupportedError:
            If no deployer supports the given workload.
        OperationError:
            If the deployer fails to deploy the workload.

    """
    for dep in deployers:
        if not dep.is_supported():
            continue

        dep.create(workload)
        return

    msg = "No deployer supports"
    raise UnsupportedError(msg)


def get_workload(name: WorkloadName) -> WorkloadStatus | None:
    """
    Get the status of a workload.

    Args:
        name:
            The name of the workload.

    Returns:
        The status of the workload, or None if not found.

    Raises:
        UnsupportedError:
            If no deployer supports the given workload.
        OperationError:
            If the deployer fails to get the status of the workload.

    """
    for dep in deployers:
        if not dep.is_supported():
            continue

        return dep.get(name)

    msg = "No deployer supports"
    raise UnsupportedError(msg)


def delete_workload(name: WorkloadName) -> WorkloadStatus | None:
    """
    Delete the given workload.

    Args:
        name:
            The name of the workload to delete.

    Return:
        The status if found, None otherwise.

    Raises:
        UnsupportedError:
            If no deployer supports the given workload.
        OperationError:
            If the deployer fails to delete the workload.

    """
    for dep in deployers:
        if not dep.is_supported():
            continue

        return dep.delete(name)

    msg = "No deployer supports"
    raise UnsupportedError(msg)


def list_workloads(labels: dict[str, str] | None = None) -> list[WorkloadStatus]:
    """
    List all workloads.

    Args:
        labels:
            Labels to filter workloads.

    Returns:
        A list of workload statuses.

    Raises:
        UnsupportedError:
            If no deployer supports listing workloads.
        OperationError:
            If the deployer fails to list workloads.

    """
    for dep in deployers:
        if not dep.is_supported():
            continue

        return dep.list(labels)

    msg = "No deployer supports"
    raise UnsupportedError(msg)


def logs_workload(
    name: WorkloadName,
    token: WorkloadOperationToken | None = None,
    timestamps: bool = False,
    tail: int | None = None,
    since: int | None = None,
    follow: bool = False,
):
    """
    Get the logs of a workload.

    Args:
        name:
            The name of the workload to get logs.
        token:
            The operation token for authentication.
        timestamps:
            Whether to include timestamps in the logs.
        tail:
            The number of lines from the end of the logs to show.
        since:
            Show logs since a given time (in seconds).
        follow:
            Whether to follow the logs.

    Returns:
        The logs as a byte string or a generator yielding byte strings if follow is True.

    Raises:
        UnsupportedError:
            If no deployer supports the given workload.
        OperationError:
            If the deployer fails to get the logs of the workload.

    """
    for dep in deployers:
        if not dep.is_supported():
            continue

        return dep.logs(name, token, timestamps, tail, since, follow)

    msg = "No deployer supports"
    raise UnsupportedError(msg)


__all__ = [
    "Container",
    "ContainerCapabilities",
    "ContainerCheck",
    "ContainerCheckExecution",
    "ContainerCheckHTTP",
    "ContainerCheckTCP",
    "ContainerEnv",
    "ContainerExecution",
    "ContainerFile",
    "ContainerMount",
    "ContainerMountModeEnum",
    "ContainerPort",
    "ContainerPortProtocolEnum",
    "ContainerProfileEnum",
    "ContainerResources",
    "ContainerRestartPolicyEnum",
    "ContainerSecurity",
    "DockerWorkloadPlan",
    "DockerWorkloadStatus",
    "OperationError",
    "UnsupportedError",
    "WorkloadOperationToken",
    "WorkloadPlan",
    "WorkloadPlan",
    "WorkloadSecurity",
    "WorkloadSecuritySysctl",
    "WorkloadStatus",
    "WorkloadStatusStateEnum",
    "create_workload",
    "delete_workload",
    "get_workload",
    "list_workloads",
    "logs_workload",
]
