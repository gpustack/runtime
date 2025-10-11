from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from dataclasses_json import dataclass_json

from .. import envs
from ..detector import detect_backend
from .__utils__ import correct_runner_image, safe_json, safe_yaml

if TYPE_CHECKING:
    from collections.abc import Generator

_RE_RFC1123_DNS_SUBDOMAIN_NAME = re.compile(
    r"(?!-)[a-z0-9-]{1,63}(?<!-)(\.(?!-)[a-z0-9-]{1,63}(?<!-))*",
)
"""
Regex for RFC 1123 DNS subdomain names, which must:
    - contain no more than 253 characters
    - contain only lowercase alphanumeric characters, '-' or '.'
    - start with an alphanumeric character
    - end with an alphanumeric character
"""

_RE_RFC1123_DNS_LABEL_NAME = re.compile(r"(?!-)[a-z0-9-]{1,63}(?<!-)")
"""
Regex for RFC 1123 DNS label names, which must:
    - contain no more than 63 characters
    - contain only lowercase alphanumeric characters or '-'
    - start with an alphanumeric character
    - end with an alphanumeric character
"""

_RE_RFC1035_DNS_LABEL_NAME = re.compile(r"(?![0-9-])[a-zA-Z0-9_.-]{1,63}(?<![.-])")
"""
Regex for RFC 1035 DNS label names, which must:
    - contain no more than 63 characters
    - contain only lowercase alphanumeric characters or '-'
    - start with an alphabetic character
    - end with an alphanumeric character
"""


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


@dataclass_json
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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def cpu(self) -> float | None:
        return self.get("cpu", None)

    @cpu.setter
    def cpu(self, value: float | None):
        self["cpu"] = value

    @cpu.deleter
    def cpu(self):
        if "cpu" in self:
            self.pop("cpu")

    @property
    def memory(self) -> str | int | float | None:
        return self.get("memory", None)

    @memory.setter
    def memory(self, value: str | float | None):
        self["memory"] = value

    @memory.deleter
    def memory(self):
        if "memory" in self:
            self.pop("memory")


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
    """
    Protocol of the port.
    """


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
        host (str | None):
            Host to check, defaults to the container loopback address.

    """

    port: int
    """
    Port to check.
    """
    host: str | None = None
    """
    Host to check, defaults to the container loopback address.
    """


@dataclass
class ContainerCheckHTTP:
    """
    An HTTP(s) container check.

    Attributes:
        port (int):
            Port to check.
        host (str | None):
            Host to check, defaults to the container loopback address.
        headers (dict[str, str] | None):
            Headers to include in the request.
        path (str | None):
            Path to check.

    """

    port: int
    """
    Port to check.
    """
    host: str | None = None
    """
    Host to check, defaults to the container loopback address.
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
            Delay (in seconds) before starting the check.
        interval (int | None):
            Interval (in seconds) between checks.
        timeout (int | None):
            Timeout (in seconds) for each check.
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
    Image of the container,
    if GPUSTACK_RUNTIME_DEPLOY_CORRECT_RUNNER_IMAGE is enabled,
    a gpustack-runner formatted image will be corrected if possible,
    see `correct_runner_image` for details.
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
    resources: ContainerResources | None = field(
        default=None,
        metadata={"dataclasses_json": {"encoder": lambda v: dict(v) if v else None}},
    )
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


WorkloadNamespace = str
"""
Namespace for a workload.
"""

WorkloadName = str
"""
Name for a workload.
"""


@dataclass_json
@dataclass
class WorkloadPlan(WorkloadSecurity):
    """
    Base plan class for all workloads.

    Attributes:
        resource_key_runtime_env_mapping: (dict[str, str]):
            Mapping from resource names to environment variable names for device allocation,
            which is used to tell the Container Runtime which GPUs to mount into the container.
            For example, {"nvidia.com/gpu": "NVIDIA_VISIBLE_DEVICES"},
            which sets the "NVIDIA_VISIBLE_DEVICES" environment variable to the allocated GPU device IDs.
            With privileged mode, the container can access all GPUs even if specified.
        resource_key_backend_env_mapping: (dict[str, list[str]]):
            Mapping from resource names to environment variable names for device runtime,
            which is used to tell the Device Runtime (e.g., ROCm, CUDA, OneAPI) which GPUs to use inside the container.
            For example, {"nvidia.com/gpu": ["CUDA_VISIBLE_DEVICES"]},
            which sets the "CUDA_VISIBLE_DEVICES" environment variable to the allocated GPU device IDs.
        name (WorkloadName):
            Name for the workload, it should be unique in the deployer.
        labels (dict[str, str] | None):
            Labels for the workload.
        host_network (bool):
            Indicates if the containers of the workload use the host network.
        host_ipc (bool):
            Indicates if the containers of the workload use the host IPC.
        pid_shared (bool):
            Indicates if the containers of the workload share the PID namespace.
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

    resource_key_runtime_env_mapping: dict[str, str] = field(
        default_factory=lambda: envs.GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_RUNTIME_VISIBLE_DEVICES,
    )
    """
    Mapping from resource names to environment variable names for device allocation,
    which is used to tell the Container Runtime which GPUs to mount into the container.
    For example, {"nvidia.com/gpu": "NVIDIA_VISIBLE_DEVICES"},
    which sets the "NVIDIA_VISIBLE_DEVICES" environment variable to the allocated GPU device IDs.
    With privileged mode, the container can access all GPUs even if specified.
    """
    resource_key_backend_env_mapping: dict[str, list[str]] = field(
        default_factory=lambda: envs.GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_BACKEND_VISIBLE_DEVICES,
    )
    """
    Mapping from resource names to environment variable names for device runtime,
    which is used to tell the Device Runtime (e.g., ROCm, CUDA, OneAPI) which GPUs to use inside the container.
    For example, {"nvidia.com/gpu": ["CUDA_VISIBLE_DEVICES"]},
    which sets the "CUDA_VISIBLE_DEVICES" environment variable to the allocated GPU device IDs.
    """
    namespace: WorkloadNamespace | None = None
    """
    Namespace for the workload.
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
    Indicates if the containers of the workload use the host network.
    """
    host_ipc: bool | None = None
    """
    Indicates if the containers of the workload use the host IPC.
    """
    pid_shared: bool = False
    """
    Indicates if the containers of the workload share the PID namespace.
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

    def validate_and_default(self):
        """
        Validate the workload plan and set defaults.

        Raises:
            ValueError:
                If the workload plan is invalid.

        """
        # Validate
        if not _RE_RFC1123_DNS_LABEL_NAME.match(self.name):
            msg = (
                f'Workload name "{self.name}" is invalid, '
                "it must match RFC 1123 DNS label format"
            )
            raise ValueError(msg)
        for ln in self.labels or {}:
            for p in ln.split("/"):
                if not _RE_RFC1123_DNS_SUBDOMAIN_NAME.match(p):
                    msg = (
                        f'Workload label name "{ln}" is invalid, '
                        "it must match RFC 1123 DNS subdomain format"
                    )
                    raise ValueError(msg)
        names = [c.name for c in self.containers or []]
        if len(names) != len(set(names)):
            msg = "Container names must be unique in a workload."
            raise ValueError(msg)
        for n in names:
            if not _RE_RFC1035_DNS_LABEL_NAME.match(n):
                msg = (
                    f'Container name "{n}" is invalid, '
                    "it must match RFC 1035 DNS label format"
                )
                raise ValueError(msg)
        if not any(
            c.profile == ContainerProfileEnum.RUN for c in self.containers or []
        ):
            msg = 'Workload must contain at least one "RUN" profile container.'
            raise ValueError(msg)

        # Default
        self.labels = self.labels or {}
        for c in self.containers:
            if c.profile == ContainerProfileEnum.INIT:
                if not c.restart_policy:
                    c.restart_policy = ContainerRestartPolicyEnum.NEVER
            elif not c.restart_policy:
                c.restart_policy = ContainerRestartPolicyEnum.ALWAYS
            if envs.GPUSTACK_RUNTIME_DEPLOY_CORRECT_RUNNER_IMAGE:
                c.image = correct_runner_image(c.image)

    def to_json(self) -> str:
        """
        Convert the workload plan to a JSON string.

        Returns:
            The JSON string.

        """
        return safe_json(self, indent=2)

    def to_yaml(self) -> str:
        """
        Convert the workload plan to a YAML string.

        Returns:
            The YAML string.

        """
        return safe_yaml(self, indent=2, sort_keys=False)


class WorkloadStatusStateEnum(str, Enum):
    """
    Enum for workload status states.

    Transitions:
    ```
                                    > - - - - - - - -
                                   |                |
    UNKNOWN - -> PENDING - -> INITIALIZING          - - - - - > FAILED | UNHEALTHY | INACTIVE
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
    INACTIVE = "Inactive"
    """
    The workload is inactive.
    """


WorkloadOperationToken = str
"""
Token for a workload operation.
"""


@dataclass_json
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
        created_at str:
            Creation time of the workload.
        namespace (WorkloadNamespace | None):
            Namespace for the workload.
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
    namespace: WorkloadNamespace | None = None
    """
    Namespace for the workload.
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


class WorkloadExecStream(ABC):
    """
    Base class for exec stream.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def closed(self) -> bool:
        return False

    @abstractmethod
    def fileno(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def read(self, size: int = -1) -> bytes | None:
        raise NotImplementedError

    @abstractmethod
    def write(self, data: bytes) -> int:
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError


class Deployer(ABC):
    """
    Base class for all deployers.
    """

    _runtime_visible_devices_env_name: str | None = None
    """
    Recorded backend visible devices env name,
    such as "NVIDIA_VISIBLE_DEVICES", "AMD_VISIBLE_DEVICES", etc.
    If failed to detect backend, it will be "UNKNOWN_VISIBLE_DEVICES".
    """
    _backend_visible_devices_env_names: list[str] | None = None
    """
    Recorded runtime visible devices env name list,
    such as ["CUDA_VISIBLE_DEVICES"], ["ROCR_VISIBLE_DEVICES"], etc.
    If failed to detect backend, it will be ["UNKNOWN_VISIBLE_DEVICES"].
    """

    def __init__(self):
        self._runtime_visible_devices_env_name = (
            "UNKNOWN_RUNTIME_BACKEND_VISIBLE_DEVICES"
        )
        self._backend_visible_devices_env_names = []

        if backend := detect_backend():
            rk = envs.GPUSTACK_RUNTIME_DETECT_BACKEND_MAP_RESOURCE_KEY.get(backend)
            ren = envs.GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_RUNTIME_VISIBLE_DEVICES.get(
                rk,
            )
            ben = envs.GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_BACKEND_VISIBLE_DEVICES.get(
                rk,
            )
            if ren:
                self._runtime_visible_devices_env_name = ren
            if ben:
                self._backend_visible_devices_env_names = ben

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
            TypeError:
                If the workload type is invalid.
            ValueError:
                If the workload fails to validate.
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workload fails to deploy.

        """
        raise NotImplementedError

    @abstractmethod
    def get(
        self,
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
            The status if found, None otherwise.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workload fails to get.

        """
        raise NotImplementedError

    @abstractmethod
    def delete(
        self,
        name: WorkloadName,
        namespace: WorkloadNamespace | None = None,
    ) -> WorkloadStatus | None:
        """
        Delete a workload.

        Args:
            name:
                The name of the workload.
            namespace:
                The namespace of the workload.

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
    def list(
        self,
        namespace: WorkloadNamespace | None = None,
        labels: dict[str, str] | None = None,
    ) -> list[WorkloadStatus]:
        """
        List all workloads.

        Args:
            namespace:
                The namespace of the workloads.
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
        namespace: WorkloadNamespace | None = None,
        token: WorkloadOperationToken | None = None,
        detach: bool = True,
        command: list[str] | None = None,
        args: list[str] | None = None,
    ) -> WorkloadExecStream | bytes | str:
        """
        Execute a command in a workload.

        Args:
            name:
                The name of the workload to execute the command in.
            namespace:
                The namespace of the workload.
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
            If detach is False, return a WorkloadExecStream.
            otherwise, return the output of the command as a byte string or string.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workload fails to execute the command.

        """
        raise NotImplementedError
