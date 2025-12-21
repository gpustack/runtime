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
    WorkloadExecStream,
    WorkloadNamespace,
    WorkloadOperationToken,
    WorkloadPlan,
    WorkloadSecurity,
    WorkloadSecuritySysctl,
    WorkloadStatus,
    WorkloadStatusStateEnum,
)
from .docker import (
    DockerDeployer,
    DockerWorkloadPlan,
    DockerWorkloadStatus,
)
from .kuberentes import (
    KubernetesDeployer,
    KubernetesWorkloadPlan,
    KubernetesWorkloadStatus,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from .__types__ import Deployer, WorkloadName

_DEPLOYERS: list[Deployer] = [
    DockerDeployer(),
    KubernetesDeployer(),
]
"""
List of all deployers.
"""

_DEPLOYERS_MAP: dict[str, Deployer] = {dep.name: dep for dep in _DEPLOYERS}
"""
Mapping from deployer name to deployer.
"""


def supported_list() -> list[Deployer]:
    """
    Return supported deployers.

    Returns:
        A list of supported deployers.

    """
    return [dep for dep in _DEPLOYERS if dep.is_supported()]


def create_workload(workload: WorkloadPlan):
    """
    Deploy the given workload.

    Args:
        workload:
            The workload to deploy.

    Raises:
        TypeError:
            If the workload type is invalid.
        ValueError:
            If the workload fails to validate.
        UnsupportedError:
            If no deployer supports the given workload.
        OperationError:
            If the deployer fails to deploy the workload.

    """
    for dep in _DEPLOYERS:
        if not dep.is_supported():
            continue

        dep.create(workload=workload)
        return

    msg = (
        "No available deployer. "
        "Please provide a container runtime, e.g. "
        "bind mount the host `/var/run/docker.sock` on Docker, "
        "or allow (in-)cluster access on Kubernetes"
    )
    raise UnsupportedError(msg)


def get_workload(
    name: WorkloadName,
    namespace: WorkloadNamespace | None = None,
) -> WorkloadStatus | None:
    """
    Get the status of a workload.

    Args:
        name:
            The name of the workload.
        namespace:
            The namespace of the workload.

    Returns:
        The status of the workload, or None if not found.

    Raises:
        UnsupportedError:
            If no deployer supports the given workload.
        OperationError:
            If the deployer fails to get the status of the workload.

    """
    for dep in _DEPLOYERS:
        if not dep.is_supported():
            continue

        return dep.get(name=name, namespace=namespace)

    msg = (
        "No available deployer. "
        "Please provide a container runtime, e.g. "
        "bind mount the host `/var/run/docker.sock` on Docker, "
        "or allow (in-)cluster access on Kubernetes"
    )
    raise UnsupportedError(msg)


def delete_workload(
    name: WorkloadName,
    namespace: WorkloadNamespace | None = None,
) -> WorkloadStatus | None:
    """
    Delete the given workload.

    Args:
        name:
            The name of the workload to delete.
        namespace:
            The namespace of the workload.

    Return:
        The status if found, None otherwise.

    Raises:
        UnsupportedError:
            If no deployer supports the given workload.
        OperationError:
            If the deployer fails to delete the workload.

    """
    for dep in _DEPLOYERS:
        if not dep.is_supported():
            continue

        return dep.delete(name=name, namespace=namespace)

    msg = (
        "No available deployer. "
        "Please provide a container runtime, e.g. "
        "bind mount the host `/var/run/docker.sock` on Docker, "
        "or allow (in-)cluster access on Kubernetes"
    )
    raise UnsupportedError(msg)


def list_workloads(
    namespace: WorkloadNamespace | None = None,
    labels: dict[str, str] | None = None,
) -> list[WorkloadStatus]:
    """
    List all workloads.

    Args:
        namespace:
            The namespace to filter workloads.
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
    for dep in _DEPLOYERS:
        if not dep.is_supported():
            continue

        return dep.list(namespace=namespace, labels=labels)

    msg = (
        "No available deployer. "
        "Please provide a container runtime, e.g. "
        "bind mount the host `/var/run/docker.sock` on Docker, "
        "or allow (in-)cluster access on Kubernetes"
    )
    raise UnsupportedError(msg)


def logs_workload(
    name: WorkloadName,
    namespace: WorkloadNamespace | None = None,
    token: WorkloadOperationToken | None = None,
    timestamps: bool = False,
    tail: int | None = None,
    since: int | None = None,
    follow: bool = False,
) -> Generator[bytes | str, None, None] | bytes | str:
    """
    Get the logs of a workload.

    Args:
        name:
            The name of the workload to get logs.
        namespace:
            The namespace of the workload.
        token:
            The token for operation.
        timestamps:
            Whether to include timestamps in the logs.
        tail:
            The number of lines from the end of the logs to show.
        since:
            Show logs since a given time (in seconds).
        follow:
            Whether to follow the logs.

    Returns:
        The logs as a byte string, a string or a generator yielding byte strings or strings if follow is True.

    Raises:
        UnsupportedError:
            If no deployer supports the given workload.
        OperationError:
            If the deployer fails to get the logs of the workload.

    """
    for dep in _DEPLOYERS:
        if not dep.is_supported():
            continue

        return dep.logs(
            name=name,
            namespace=namespace,
            token=token,
            timestamps=timestamps,
            tail=tail,
            since=since,
            follow=follow,
        )

    msg = (
        "No available deployer. "
        "Please provide a container runtime, e.g. "
        "bind mount the host `/var/run/docker.sock` on Docker, "
        "or allow (in-)cluster access on Kubernetes"
    )
    raise UnsupportedError(msg)


async def async_logs_workload(
    name: WorkloadName,
    namespace: WorkloadNamespace | None = None,
    token: WorkloadOperationToken | None = None,
    timestamps: bool = False,
    tail: int | None = None,
    since: int | None = None,
    follow: bool = False,
) -> AsyncGenerator[bytes | str, None, None] | bytes | str:
    """
    Asynchronously get the logs of a workload.

    Args:
        name:
            The name of the workload to get logs.
        namespace:
            The namespace of the workload.
        token:
            The token for operation.
        timestamps:
            Whether to include timestamps in the logs.
        tail:
            The number of lines from the end of the logs to show.
        since:
            Show logs since a given time (in seconds).
        follow:
            Whether to follow the logs.

    Returns:
        The logs as a byte string, a string or a generator yielding byte strings or strings if follow is True.

    Raises:
        UnsupportedError:
            If no deployer supports the given workload.
        OperationError:
            If the deployer fails to get the logs of the workload.

    """
    for dep in _DEPLOYERS:
        if not dep.is_supported():
            continue

        return await dep.async_logs(
            name=name,
            namespace=namespace,
            token=token,
            timestamps=timestamps,
            tail=tail,
            since=since,
            follow=follow,
        )

    msg = (
        "No available deployer. "
        "Please provide a container runtime, e.g. "
        "bind mount the host `/var/run/docker.sock` on Docker, "
        "or allow (in-)cluster access on Kubernetes"
    )
    raise UnsupportedError(msg)


def exec_workload(
    name: WorkloadName,
    namespace: WorkloadNamespace | None = None,
    token: WorkloadOperationToken | None = None,
    detach: bool = True,
    command: list[str] | None = None,
    args: list[str] | None = None,
) -> WorkloadExecStream | bytes | str:
    """
    Execute a command in a running workload.

    Args:
        name:
            The name of the workload to execute the command in.
        namespace:
            The namespace of the workload.
        token:
            The token for operation.
        detach:
            Whether to detach from the command execution.
        command:
            The command to execute.
        args:
            The arguments to pass to the command.

    Returns:
        If detach is False, return a WorkloadExecStream.
        otherwise, return the output of the command as a byte string or string.

    Raises:
        UnsupportedError:
            If no deployer supports the given workload.
        OperationError:
            If the deployer fails to execute the command in the workload.

    """
    for dep in _DEPLOYERS:
        if not dep.is_supported():
            continue

        return dep.exec(
            name=name,
            namespace=namespace,
            token=token,
            detach=detach,
            command=command,
            args=args,
        )

    msg = (
        "No available deployer. "
        "Please provide a container runtime, e.g. "
        "bind mount the host `/var/run/docker.sock` on Docker, "
        "or allow (in-)cluster access on Kubernetes"
    )
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
    "KubernetesWorkloadPlan",
    "KubernetesWorkloadStatus",
    "OperationError",
    "UnsupportedError",
    "WorkloadExecStream",
    "WorkloadOperationToken",
    "WorkloadPlan",
    "WorkloadPlan",
    "WorkloadSecurity",
    "WorkloadSecuritySysctl",
    "WorkloadStatus",
    "WorkloadStatusStateEnum",
    "async_logs_workload",
    "create_workload",
    "delete_workload",
    "exec_workload",
    "get_workload",
    "list_workloads",
    "logs_workload",
    "supported_list",
]
