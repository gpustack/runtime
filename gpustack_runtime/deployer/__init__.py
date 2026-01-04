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
from .podman import (
    PodmanDeployer,
    PodmanWorkloadPlan,
    PodmanWorkloadStatus,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from .__types__ import Deployer, WorkloadName

_DEPLOYERS: list[Deployer] = [
    DockerDeployer(),
    KubernetesDeployer(),
    PodmanDeployer(),
]
"""
List of all deployers.
"""

_DEPLOYERS_MAP: dict[str, Deployer] = {dep.name: dep for dep in _DEPLOYERS}
"""
Mapping from deployer name to deployer.
"""

_NO_AVAILABLE_DEPLOYER_MSG = (
    "No available deployer. "
    "Please provide a container runtime, e.g. "
    "bind mount the host `/var/run/docker.sock` on Docker, "
    "or allow (in-)cluster access on Kubernetes, "
    "or bind mount the host `/run/podman/podman.sock` on Podman."
)


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

    raise UnsupportedError(_NO_AVAILABLE_DEPLOYER_MSG)


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

    raise UnsupportedError(_NO_AVAILABLE_DEPLOYER_MSG)


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

    raise UnsupportedError(_NO_AVAILABLE_DEPLOYER_MSG)


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

    raise UnsupportedError(_NO_AVAILABLE_DEPLOYER_MSG)


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

    raise UnsupportedError(_NO_AVAILABLE_DEPLOYER_MSG)


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

    raise UnsupportedError(_NO_AVAILABLE_DEPLOYER_MSG)


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

    raise UnsupportedError(_NO_AVAILABLE_DEPLOYER_MSG)


def inspect_workload(
    name: WorkloadName,
    namespace: WorkloadNamespace | None = None,
) -> str:
    """
    Inspect the given workload.

    Args:
        name:
            The name of the workload to inspect.
        namespace:
            The namespace of the workload.

    Returns:
        The inspection data as a dictionary.

    Raises:
        UnsupportedError:
            If no deployer supports the given workload.
        OperationError:
            If the deployer fails to inspect the workload.

    """
    for dep in _DEPLOYERS:
        if not dep.is_supported():
            continue

        return dep.inspect(
            name=name,
            namespace=namespace,
        )

    raise UnsupportedError(_NO_AVAILABLE_DEPLOYER_MSG)


def logs_self(
    timestamps: bool = False,
    tail: int | None = None,
    since: int | None = None,
    follow: bool = False,
) -> Generator[bytes | str, None, None] | bytes | str:
    """
    Get the logs of the deployer itself.
    Only works in mirrored deployment mode.

    Args:
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

        if hasattr(dep, "endoscopic_logs"):
            return dep.endoscopic_logs(
                timestamps=timestamps,
                tail=tail,
                since=since,
                follow=follow,
            )

    raise UnsupportedError(_NO_AVAILABLE_DEPLOYER_MSG)


async def async_logs_self(
    timestamps: bool = False,
    tail: int | None = None,
    since: int | None = None,
    follow: bool = False,
) -> AsyncGenerator[bytes | str, None, None] | bytes | str:
    """
    Asynchronously get the logs of the deployer itself.
    Only works in mirrored deployment mode.

    Args:
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

        if hasattr(dep, "async_endoscopic_logs"):
            return await dep.async_endoscopic_logs(
                timestamps=timestamps,
                tail=tail,
                since=since,
                follow=follow,
            )

    raise UnsupportedError(_NO_AVAILABLE_DEPLOYER_MSG)


def exec_self(
    detach: bool = True,
    command: list[str] | None = None,
    args: list[str] | None = None,
) -> WorkloadExecStream | bytes | str:
    """
    Execute a command in the deployer itself.
    Only works in mirrored deployment mode.

    Args:
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

        if hasattr(dep, "endoscopic_exec"):
            return dep.endoscopic_exec(
                detach=detach,
                command=command,
                args=args,
            )

    raise UnsupportedError(_NO_AVAILABLE_DEPLOYER_MSG)


def inspect_self() -> str:
    """
    Inspect the deployer itself.
    Only works in mirrored deployment mode.

    Returns:
        The inspection data as a dictionary.

    Raises:
        UnsupportedError:
            If no deployer supports the given workload.
        OperationError:
            If the deployer fails to inspect the workload.

    """
    for dep in _DEPLOYERS:
        if not dep.is_supported():
            continue

        if hasattr(dep, "endoscopic_inspect"):
            return dep.endoscopic_inspect()

    raise UnsupportedError(_NO_AVAILABLE_DEPLOYER_MSG)


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
    "PodmanDeployer",
    "PodmanWorkloadPlan",
    "PodmanWorkloadStatus",
    "UnsupportedError",
    "WorkloadExecStream",
    "WorkloadOperationToken",
    "WorkloadPlan",
    "WorkloadPlan",
    "WorkloadSecurity",
    "WorkloadSecuritySysctl",
    "WorkloadStatus",
    "WorkloadStatusStateEnum",
    "async_logs_self",
    "async_logs_workload",
    "create_workload",
    "delete_workload",
    "exec_self",
    "exec_workload",
    "get_workload",
    "inspect_self",
    "inspect_workload",
    "list_workloads",
    "logs_self",
    "logs_workload",
    "supported_list",
]
