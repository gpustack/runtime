from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, NamedTuple

from dataclasses_json import dataclass_json

if TYPE_CHECKING:
    from collections.abc import Generator


class UnsupportedError(Exception):
    """
    Base class for unsupported errors.
    """


class OperationError(Exception):
    """
    Base class for operation errors.
    """


@dataclass
class ContainerCapabilities:
    """
    Capabilities for a container.

    Attributes:
        add (list[str] | None):
            Capabilities to add.
        drop (list[str] | None):
            Capabilities to drop.

    """

    add: list[str] | None = None
    """
    Capabilities to add.
    """
    drop: list[str] | None = None
    """
    Capabilities to drop.
    """


@dataclass
class ContainerSecurity:
    """
    Security context for a container.

    Attributes:
        run_as_user (int | None):
            User ID to run the container as.
        run_as_group (int | None):
            Group ID to run the container as.
        readonly_rootfs (bool):
            Whether the root filesystem is read-only.
        privileged (bool):
            Privileged mode for the container.
        capabilities (ContainerCapabilities | None):
            Capabilities for the container.

    """

    run_as_user: int | None = None
    """
    User ID to run the container as.
    """
    run_as_group: int | None = None
    """
    Group ID to run the container as.
    """
    readonly_rootfs: bool = False
    """
    Whether the root filesystem is read-only.
    """
    privileged: bool = False
    """
    Privileged mode for the container.
    """
    capabilities: ContainerCapabilities | None = None
    """
    Capabilities for the container.
    """


@dataclass
class ContainerExecution(ContainerSecurity):
    """
    Execution for a container.

    Attributes:
        working_dir (str | None):
            Working directory for the container.
        command (list[str] | None):
            Command to run in the container.
        args (list[str] | None):
            Arguments to pass to the command.

    """

    working_dir: str | None = None
    """
    Working directory for the container.
    """
    command: list[str] | None = None
    """
    Command to run in the container.
    """
    args: list[str] | None = None
    """
    Arguments to pass to the command.
    """


@dataclass
class ContainerResources(dict[str, float | int | str]):
    """
    Resources for a container.

    Attributes:
        cpu (float | None):
            CPU limit for the container in cores.
        memory (str | int | float | None):
            Memory limit for the container.

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def cpu(self) -> float | None:
        return self.get("cpu", None)

    @cpu.setter
    def cpu(self, value: float):
        self["cpu"] = value

    @cpu.deleter
    def cpu(self):
        if "cpu" in self:
            del self["cpu"]

    @property
    def memory(self) -> str | int | float | None:
        return self.get("memory", None)

    @memory.setter
    def memory(self, value: str | float):
        self["memory"] = value

    @memory.deleter
    def memory(self):
        if "memory" in self:
            del self["memory"]


@dataclass
class ContainerEnv:
    """
    Environment variable for a container.

    Attributes:
        name (str):
            Name of the environment variable.
        value (str):
            Value of the environment variable.

    """

    name: str
    """
    Name of the environment variable.
    """
    value: str
    """
    Value of the environment variable.
    """


@dataclass
class ContainerFile:
    """
    File for a container.

    Attributes:
        path (str):
            Path of the file.
            If `content` is not specified, mount from host.
        mode (int):
            File mounted mode.
        content (str | None):
            Content of the file.

    """

    path: str
    """
    Path of the file.
    If `content` is not specified, mount from host.
    """
    mode: int = 0o644
    """
    File mounted mode.
    """
    content: str | None = None
    """
    Content of the file.
    """


class ContainerMountModeEnum(str, Enum):
    """
    Enum for container mount modes.
    """

    RWO = "ReadWriteOnce"
    """
    Read-write once mode.
    """
    ROX = "ReadOnlyMany"
    """
    Read-only many mode.
    """
    RWX = "ReadWriteMany"
    """
    Read-write many mode.
    """


@dataclass
class ContainerMount:
    """
    Mount for a container.

    Attributes:
        path (str):
            Path to mount.
            If `volume` is not specified, mount from host.
        mode (ContainerMountModeEnum):
            Path mounted mode.
        volume (str | None):
            Volume to mount.
        subpath (str | None):
            Sub-path of volume to mount.

    """

    path: str
    """
    Path to mount.
    If `volume` is not specified, mount from host.
    """
    mode: ContainerMountModeEnum = ContainerMountModeEnum.RWX
    """
    Path mounted mode.
    """
    volume: str | None = None
    """
    Volume to mount.
    """
    subpath: str | None = None
    """
    Sub-path of volume to mount.
    """


class ContainerPortProtocolEnum(str, Enum):
    """
    Enum for container port protocols.
    """

    TCP = "TCP"
    """
    TCP protocol.
    """
    UDP = "UDP"
    """
    UDP protocol.
    """
    SCTP = "SCTP"
    """
    SCTP protocol.
    """


@dataclass
class ContainerPort:
    """
    Port for a container.

    Attributes:
        internal (int):
            Internal port of the container.
        external (int | None):
            External port of the container.
        protocol (ContainerPortProtocolEnum):
            Protocol of the port.

    """

    internal: int
    """
    Internal port of the container.
    If `external` is not specified, expose the same number.
    """
    external: int | None = None
    """
    External port of the container.
    """
    protocol: ContainerPortProtocolEnum = ContainerPortProtocolEnum.TCP


@dataclass
class ContainerCheckExecution:
    """
    An execution container check.

    Attributes:
        command (list[str]):
            Command to run in the check.

    """

    command: list[str]
    """
    Command to run in the check.
    """


@dataclass
class ContainerCheckTCP:
    """
    An TCP container check.

    Attributes:
        port (int):
            Port to check.

    """

    port: int
    """
    Port to check.
    """


@dataclass
class ContainerCheckHTTP:
    """
    An HTTP(s) container check.

    Attributes:
        port (int):
            Port to check.
        headers (dict[str, str] | None):
            Headers to include in the request.
        path (str | None):
            Path to check.

    """

    port: int
    """
    Port to check.
    """
    headers: dict[str, str] | None = None
    """
    Headers to include in the request.
    """
    path: str | None = None
    """
    Path to check.
    """


@dataclass
class ContainerCheck:
    """
    Health check for a container.

    Attributes:
        delay (int | None):
            Delay before starting the check.
        interval (int | None):
            Interval between checks.
        timeout (int | None):
            Timeout for each check.
        retries (int | None):
            Number of retries before considering the container unhealthy.
        teardown (bool):
            Teardown the container if the check fails.
        execution (ContainerCheckExecution | None):
            Command execution for the check.
        tcp (ContainerCheckTCP | None):
            TCP execution for the check.
        http (ContainerCheckHTTP | None):
            HTTP execution for the check.
        https (ContainerCheckHTTP | None):
            HTTPS execution for the check.

    """

    delay: int | None
    """
    Delay before starting the check.
    """
    interval: int | None
    """
    Interval between checks.
    """
    timeout: int | None
    """
    Timeout for each check.
    """
    retries: int | None
    """
    Number of retries before considering the container unhealthy.
    """
    teardown: bool = True
    """
    Teardown the container if the check fails.
    """
    execution: ContainerCheckExecution | None = None
    """
    Command execution for the check.
    """
    tcp: ContainerCheckTCP | None = None
    """
    TCP execution for the check.
    """
    http: ContainerCheckHTTP | None = None
    """
    HTTP execution for the check.
    """
    https: ContainerCheckHTTP | None = None
    """
    HTTPS execution for the check.
    """


class ContainerProfileEnum(str, Enum):
    """
    Enum for container profiles.
    """

    RUN = "Run"
    """
    Run profile.
    """
    INIT = "Init"
    """
    Init profile.
    """


class ContainerRestartPolicyEnum(str, Enum):
    """
    Enum for container restart policies.
    """

    ALWAYS = "Always"
    """
    Always restart the container.
    """
    ON_FAILURE = "OnFailure"
    """
    Restart the container on failure.
    """
    NEVER = "Never"
    """
    Never restart the container.
    """


@dataclass
class Container:
    """
    Container specification.

    Attributes:
        image (str):
            Image of the container.
        name (str):
            Name of the container.
        profile (ContainerProfileEnum):
            Profile of the container.
        restart_policy (ContainerRestartPolicyEnum | None):
            Restart policy for the container, select from: "Always", "OnFailure", "Never"
            1. Default to "Never" for init containers.
            2. Default to "Always" for run containers.
        execution (ContainerExecution | None):
            Execution specification of the container.
        envs (list[ContainerEnv] | None):
            Environment variables of the container.
        resources (ContainerResources | None):
            Resources specification of the container.
        files (list[ContainerFile] | None):
            Files of the container.
        mounts (list[ContainerMount] | None):
            Mounts of the container.
        ports (list[ContainerPort] | None):
            Ports of the container.
        checks (list[ContainerCheck] | None):
            Health checks of the container.

    """

    image: str
    """
    Image of the container.
    """
    name: str
    """
    Name of the container.
    """
    profile: ContainerProfileEnum = ContainerProfileEnum.RUN
    """
    Profile of the container.
    """
    restart_policy: ContainerRestartPolicyEnum | None = None
    """
    Restart policy for the container, select from: "Always", "OnFailure", "Never".
    1. Default to "Never" for init containers.
    2. Default to "Always" for run containers.
    """
    execution: ContainerExecution | None = None
    """
    Execution specification of the container.
    """
    envs: list[ContainerEnv] | None = None
    """
    Environment variables of the container.
    """
    resources: ContainerResources | None = None
    """
    Resources specification of the container.
    """
    files: list[ContainerFile] | None = None
    """
    Files of the container.
    """
    mounts: list[ContainerMount] | None = None
    """
    Mounts of the container.
    """
    ports: list[ContainerPort] | None = None
    """
    Ports of the container.
    """
    checks: list[ContainerCheck] | None = None
    """
    Health checks of the container.
    """


@dataclass
class WorkloadSecuritySysctl:
    """
    Sysctl settings for a workload.

    Attributes:
        name (str):
            Name of the sysctl setting.
        value (str):
            Value of the sysctl setting.

    """

    name: str
    """
    Name of the sysctl setting.
    """
    value: str
    """
    Value of the sysctl setting.
    """


@dataclass
class WorkloadSecurity:
    """
    Security context for a workload.

    Attributes:
        run_as_user (int | None):
            User ID to run the workload as.
        run_as_group (int | None):
            Group ID to run the workload as.
        fs_group (int | None):
            The group ID to own the filesystem of the workload.
        sysctls (list[WorkloadSecuritySysctl] | None):
            Sysctls to set for the workload.

    """

    run_as_user: int | None = None
    """
    User ID to run the workload as.
    """
    run_as_group: int | None = None
    """
    Group ID to run the workload as.
    """
    fs_group: int | None = None
    """
    The group ID to own the filesystem of the workload.
    """
    sysctls: list[WorkloadSecuritySysctl] | None = None
    """
    Sysctls to set for the workload.
    """


WorkloadName = str
"""
Name for a workload.
"""


@dataclass
class WorkloadPlan(WorkloadSecurity):
    """
    Base plan class for all workloads.

    Attributes:
        name (WorkloadName):
            Name for the workload, it should be unique in the deployer.
        labels (dict[str, str] | None):
            Labels for the workload.
        host_network (bool):
            Indicates if the workload uses the host network.

        shm_size (int | str | None):
            Configure shared memory size for the workload.
        run_as_user (int | None):
            The user ID to run the workload as.
        run_as_group (int | None):
            The group ID to run the workload as.
        fs_group (int | None):
            The group ID to own the filesystem of the workload.
        sysctls (dict[str, str] | None):
            Sysctls to set for the workload.
        containers (list[Container] | None):
            Containers in the workload.
            It must contain at least one "RUN" profile container.

    """

    name: WorkloadName = "default"
    """
    Name for the workload,
    it should be unique in the deployer.
    """
    labels: dict[str, str] | None = None
    """
    Labels for the workload.
    """
    host_network: bool = False
    """
    Indicates if the workload uses the host network.
    """
    pid_shared: bool = False
    """
    Indicates if the workload shares the PID namespace.
    """
    shm_size: int | str | None = None
    """
    Configure shared memory size for the workload.
    """
    containers: list[Container] | None = None
    """
    Containers in the workload.
    It must contain at least one "RUN" profile container.
    """


class WorkloadStatusStateEnum(str, Enum):
    """
    Enum for workload status states.

    Transitions:
    ```
                                    > - - - - - - - -
                                   |                |
    UNKNOWN - -> PENDING - -> INITIALIZING          - - - - - > FAILED | UNHEALTHY
                   |               |                |                        |
                   |               - - - - - - > RUNNING <- - - - - - - - - -
                   |                               |
                   - - - - - - - - - - - - - - - >
    ```
    """

    UNKNOWN = "Unknown"
    """
    The workload state is unknown.
    """
    PENDING = "Pending"
    """
    The workload is pending.
    """
    INITIALIZING = "Initializing"
    """
    The workload is initializing.
    """
    RUNNING = "Running"
    """
    The workload is running.
    """
    UNHEALTHY = "Unhealthy"
    """
    The workload is unhealthy.
    """
    FAILED = "Failed"
    """
    The workload has failed.
    """


WorkloadOperationToken = str
"""
Token for a workload operation.
"""


@dataclass
class WorkloadStatusOperation:
    """
    An operation for a workload.
    """

    name: str
    """
    Name representing the operating target, e.g., human-readable container name.
    """
    token: WorkloadOperationToken
    """
    Token of the operation, e.g, container ID.
    """


@dataclass_json
@dataclass
class WorkloadStatus:
    """
    Base status class for all workloads.

    Attributes:
        name (WorkloadName):
            Name for the workload, it should be unique in the deployer.
        created_at (str | None):
            Creation time of the workload.
        labels (dict[str, str] | None):
            Labels for the workload.
        executable (list[WorkloadStatusOperation]):
            The operation for the executable containers of the workload.
        loggable (list[WorkloadStatusOperation]):
            The operation for the loggable containers of the workload.
        state (WorkloadStatusStateEnum):
            Current state of the workload.

    """

    name: WorkloadName
    """
    Name for the workload,
    it should be unique in the deployer.
    """
    created_at: str
    """
    Creation time of the workload.
    """
    labels: dict[str, str] | None = field(default_factory=dict)
    """
    Labels for the workload.
    """
    executable: list[WorkloadStatusOperation] | None = field(default_factory=list)
    """
    The operation for the executable containers of the workload.
    """
    loggable: list[WorkloadStatusOperation] | None = field(default_factory=list)
    """
    The operation for the loggable containers of the workload.
    """
    state: WorkloadStatusStateEnum = WorkloadStatusStateEnum.UNKNOWN
    """
    The current state of the workload.
    """


class WorkloadExecResult(NamedTuple):
    """
    Result of an exec command.

    """

    exit_code: int | None
    output: str | bytes | object | None


class Deployer(ABC):
    """
    Base class for all deployers.
    """

    @staticmethod
    @abstractmethod
    def is_supported() -> bool:
        """
        Check if the deployer is supported in the current environment.

        Returns:
            True if supported, False otherwise.

        """
        raise NotImplementedError

    @abstractmethod
    def create(self, workload: WorkloadPlan):
        """
        Deploy the given workload.

        Args:
            workload:
                The workload to deploy.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workload fails to deploy.

        """
        raise NotImplementedError

    @abstractmethod
    def get(self, name: WorkloadName) -> WorkloadStatus | None:
        """
        Get the status of a workload.

        Args:
            name:
                The name of the workload.

        Returns:
            The status if found, None otherwise.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workload fails to get.

        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, name: WorkloadName) -> WorkloadStatus | None:
        """
        Delete a workload.

        Args:
            name:
                The name of the workload.

        Return:
            The status if found, None otherwise.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workload fails to delete.

        """
        raise NotImplementedError

    @abstractmethod
    def list(self, labels: dict[str, str] | None = None) -> list[WorkloadStatus]:
        """
        List all workloads.

        Args:
            labels:
                Labels to filter the workloads.

        Returns:
            A list of workload statuses.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workloads fail to list.

        """
        raise NotImplementedError

    @abstractmethod
    def logs(
        self,
        name: WorkloadName,
        token: WorkloadOperationToken | None = None,
        timestamps: bool = False,
        tail: int | None = None,
        since: int | None = None,
        follow: bool = False,
    ) -> Generator[bytes, None, None] | bytes:
        """
        Get the logs of a workload.

        Args:
            name:
                The name of the workload.
            token:
                The operation token of the workload.
                If not specified, get logs from the first executable container.
            timestamps:
                Show timestamps in the logs.
            tail:
                Number of lines to show from the end of the logs.
            since:
                Show logs since the given epoch in seconds.
            follow:
                Whether to follow the logs.

        Returns:
            The logs as a byte string or a generator yielding byte strings if follow is True.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workload fails to get logs.

        """
        raise NotImplementedError

    @abstractmethod
    def exec(
        self,
        name: WorkloadName,
        token: WorkloadOperationToken | None = None,
        detach: bool = True,
        command: list[str] | None = None,
        args: list[str] | None = None,
    ) -> WorkloadExecResult:
        """
        Execute a command in a workload.

        Args:
            name:
                The name of the workload.
            token:
                The operation token of the workload.
                If not specified, execute in the first executable container.
            detach:
                Whether to detach from the command.
            command:
                The command to execute.
                If not specified, use /bin/sh and implicitly attach.
            args:
                The arguments to pass to the command.

        Returns:
            If detach is False, return a socket object in the output of WorkloadExecResult.
            otherwise, return the exit code and output of the command in WorkloadExecResult.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workload fails to execute the command.

        """
        raise NotImplementedError
