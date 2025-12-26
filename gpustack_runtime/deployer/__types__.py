from __future__ import annotations

import asyncio
import atexit
import contextlib
import re
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from dataclasses_json import dataclass_json

from .. import envs
from ..detector import (
    Topology,
    detect_devices,
    get_devices_topologies,
    group_devices_by_manufacturer,
    manufacturer_to_backend,
)
from .__utils__ import (
    correct_runner_image,
    fnv1a_32_hex,
    fnv1a_64_hex,
    is_rfc1123_domain_name,
    safe_json,
    safe_yaml,
    validate_rfc1123_subdomain_name,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

_RE_LABEL_VALUE = re.compile(r"^(?!-)[A-Za-z0-9_.-]{0,63}(?<!-)$")
"""
Regex for label values, which must:
    - contain no more than 63 characters
    - contain only alphanumeric characters, '_', '-', or '.'
    - start with an alphanumeric character
    - end with an alphanumeric character
"""


def validate_label_name_segment(segment: str):
    """
    Validate a label name segment.

    Args:
        segment:
            The label name segment to validate.

    Raises:
        ValueError:
            If the label name segment is invalid.

    """
    try:
        validate_rfc1123_subdomain_name(segment)
    except ValueError as e:
        msg = f"Invalid label name segment'{segment}'. "
        raise ValueError(msg) from e


def validate_label_value(value: str):
    """
    Validate a label value.

    Args:
        value:
            The label value to validate.

    Raises:
        ValueError:
            If the label value is invalid.

    """
    if not _RE_LABEL_VALUE.match(value):
        msg = (
            f"Invalid label value '{value}'. "
            "It must contain no more than 63 characters, "
            "contain only alphanumeric characters, '_', '-', or '.', "
            "start with an alphanumeric character, "
            "and end with an alphanumeric character."
        )
        raise ValueError(msg)


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
        command_script (str | None):
            Command(in form of script) for the container,
            only work if `readonly_rootfs` is False.
            When it is set, it will override the `command` field.
            For example,

            ```
                #!/bin/sh

                echo "Hello, World!"
                exec /path/to/bin "$@"
            ```
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
    command_script: str | None = None
    """
    Command(in form of script) for the container,
    only work if `readonly_rootfs` is False.
    When it is set, it will override the `command` field.
    For example,

    ```
        #!/bin/sh

        echo "Hello, World!"
        exec /path/to/bin "$@"
    ```
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

    def __str__(self):
        return self.value


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

    def __str__(self):
        return self.value


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


class ContainerImagePullPolicyEnum(str, Enum):
    """
    Enum for container image pull policies.
    """

    ALWAYS = "Always"
    """
    Always pull the image.
    """
    IF_NOT_PRESENT = "IfNotPresent"
    """
    Pull the image if not present.
    """
    NEVER = "Never"
    """
    Never pull the image.
    """

    def __str__(self):
        return self.value


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

    def __str__(self):
        return self.value


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

    def __str__(self):
        return self.value


@dataclass
class Container:
    """
    Container specification.

    Attributes:
        image (str):
            Image of the container.
        name (str):
            Name of the container.
        image_pull_policy (ContainerImagePullPolicyEnum):
            Image pull policy of the container.
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
    image_pull_policy: ContainerImagePullPolicyEnum = field(
        default_factory=lambda: envs.GPUSTACK_RUNTIME_DEPLOY_IMAGE_PULL_POLICY,
    )
    """
    Image pull policy of the container.
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

    ##
    ## The below are internal use only fields.
    ##

    _name_rfc1123_guard: WorkloadName | None = field(
        default=None,
        init=False,
        repr=False,
        metadata={"dataclasses_json": {"exclude": lambda _: True}},
    )
    """
    Name for the workload,
    but has been guard by rfc1123 validation for certain deployment environments,
    internal use only.
    """

    @property
    def name_rfc1123_guard(self) -> WorkloadName:
        if not self._name_rfc1123_guard:
            self._name_rfc1123_guard = (
                f"gpustack-{fnv1a_64_hex(self.name)}"
                if not is_rfc1123_domain_name(self.name)
                else self.name
            )
        return self._name_rfc1123_guard


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
    name: WorkloadName | None = None
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

    ##
    ## The below are internal use only fields.
    ##

    _name_rfc1123_guard: WorkloadName | None = field(
        default=None,
        init=False,
        repr=False,
        metadata={"dataclasses_json": {"exclude": lambda _: True}},
    )
    """
    Name for the workload,
    but has been guard by rfc1123 validation for certain deployment environments,
    internal use only.
    """

    @property
    def name_rfc1123_guard(self) -> WorkloadName:
        if not self._name_rfc1123_guard:
            self._name_rfc1123_guard = (
                f"gpustack-{fnv1a_64_hex(self.name)}"
                if not is_rfc1123_domain_name(self.name)
                else self.name
            )
        return self._name_rfc1123_guard

    def validate_and_default(self):
        """
        Validate and set defaults for the workload plan.

        Raises:
            ValueError:
                If the workload plan is invalid.

        """
        # Validate workload name.
        if not self.name:
            msg = "Workload name is required."
            raise ValueError(msg)

        # Validate at least one RUN profile container.
        if not self.containers or not any(
            c.profile == ContainerProfileEnum.RUN for c in self.containers
        ):
            msg = 'Workload must contain at least one "RUN" profile container.'
            raise ValueError(msg)

        # Validate workload labels, including label names and values.
        for ln, lv in self.labels.items():
            for s in ln.split("/"):
                validate_label_name_segment(s)
            validate_label_value(lv)

        # Validate container names and images.
        container_names = []
        for ci, c in enumerate(self.containers):
            if not c.name or not c.name.strip():
                msg = f"Container at index {ci} is missing a name."
                raise ValueError(msg)
            if not c.image:
                msg = f"Container '{c.name}' is missing an image."
                raise ValueError(msg)
            container_names.append(c.name)
        if len(container_names) != len(set(container_names)):
            msg = "Container names must be unique in the same workload."
            raise ValueError(msg)

        for c in self.containers:
            # Default restart policy.
            if c.profile == ContainerProfileEnum.INIT:
                if not c.restart_policy:
                    c.restart_policy = ContainerRestartPolicyEnum.NEVER
            elif not c.restart_policy:
                c.restart_policy = ContainerRestartPolicyEnum.ALWAYS
            # Mutate container command.
            if c.execution and c.execution.command_script:
                if c.execution.readonly_rootfs:
                    msg = f"Readonly rootfs Container '{c.name}' cannot configure `command_script`."
                    raise ValueError(msg)
                command_script_name = f"/gpustack-command-{fnv1a_32_hex(c.name)}"
                command_script = ContainerFile(
                    path=command_script_name,
                    mode=0o755,
                    content=c.execution.command_script,
                )
                if not c.files:
                    c.files = []
                c.files.append(command_script)
                c.execution.command = [command_script_name]  # Override command.
                c.execution.command_script = None
            # Add default registry if needed.
            if (
                envs.GPUSTACK_RUNTIME_DEPLOY_DEFAULT_REGISTRY
                and envs.GPUSTACK_RUNTIME_DEPLOY_DEFAULT_REGISTRY
                not in ["docker.io", "index.docker.io"]
            ):
                image_registry = envs.GPUSTACK_RUNTIME_DEPLOY_DEFAULT_NAMESPACE
                image_split = c.image.split("/")
                if len(image_split) == 1:
                    c.image = f"{image_registry}/library/{c.image}"
                elif len(image_split) == 2:
                    c.image = f"{image_registry}/{c.image}"
            # Correct runner image if needed.
            if envs.GPUSTACK_RUNTIME_DEPLOY_CORRECT_RUNNER_IMAGE:
                c.image, ok = correct_runner_image(c.image)
                if not ok and ":Host" in c.image:
                    msg = (
                        f"Runner image correction failed for Container image {c.image}"
                    )
                    raise ValueError(msg)

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

    def __str__(self):
        return self.value


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

    ##
    ## The below are internal use only fields.
    ##

    _name_rfc1123_guard: WorkloadName | None = field(
        default=None,
        init=False,
        repr=False,
        metadata={"dataclasses_json": {"exclude": lambda _: True}},
    )
    """
    Name for the workload,
    but has been guard by rfc1123 validation for certain deployment environments,
    internal use only.
    """

    @property
    def name_rfc1123_guard(self) -> WorkloadName:
        if not self._name_rfc1123_guard:
            self._name_rfc1123_guard = (
                f"gpustack-{fnv1a_64_hex(self.name)}"
                if not is_rfc1123_domain_name(self.name)
                else self.name
            )
        return self._name_rfc1123_guard


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

    _name: str = "unknown"
    """
    Name of the deployer.
    """
    _pool: ThreadPoolExecutor | None = None
    """
    Thread pool for the deployer.
    """
    _visible_devices_env: dict[str, list[str]] | None = None
    """
    Recorded visible devices envs,
    the key is the runtime visible devices env name,
    the value is the list of backend visible devices env names.
    For example:
    {
        "NVIDIA_VISIBLE_DEVICES": ["CUDA_VISIBLE_DEVICES"],
        "AMD_VISIBLE_DEVICES": ["HIP_VISIBLE_DEVICES", "ROCR_VISIBLE_DEVICES"]
    }.
    """
    _visible_devices_values: dict[str, list[str]] | None = None
    """
    Recorded visible devices values,
    the key is the runtime visible devices env name,
    the value is the list of device indexes or uuids.
    For example:
    {
        "NVIDIA_VISIBLE_DEVICES": ["0"],
        "AMD_VISIBLE_DEVICES": ["0", "1"]
    }.
    """
    _visible_devices_topologies: dict[str, Topology] | None = None
    """
    Recorded visible devices topologies,
    the key is the runtime visible devices env name,
    the value is the corresponding topology.
    For example:
    {
        "NVIDIA_VISIBLE_DEVICES": Topology(...),
        "AMD_VISIBLE_DEVICES": Topology(...)
    }.
    """
    _backend_visible_devices_values_alignment: dict[str, dict[str, str]] | None = None
    """
    Recorded backend visible devices values alignment,
    the key is the runtime visible devices env name,
    the value is the mapping from backend device index to aligned index.
    For example:
    {
        "CUDA_VISIBLE_DEVICES": {"0": "0"},
        "HIP_VISIBLE_DEVICES": {"0": "0", "1": "1"}
    }.
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

    @staticmethod
    def _default_args(func):
        def wrapper(self, *args, async_mode=None, **kwargs):
            if async_mode is None:
                async_mode = envs.GPUSTACK_RUNTIME_DEPLOY_ASYNC
            return func(
                self,
                *args,
                async_mode=async_mode,
                **kwargs,
            )

        return wrapper

    def __init__(self, name: str):
        self._name = name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _prepare(self):
        """
        Detect devices once, and construct critical elements for post processing, including:
        - Prepare visible devices environment variables mapping.
        - Prepare visible devices values mapping.
        - Prepare topology.
        """
        if self._visible_devices_env:
            return

        self._visible_devices_env = {}
        self._visible_devices_values = {}
        self._visible_devices_topologies = {}
        self._backend_visible_devices_values_alignment = {}

        group_devices = group_devices_by_manufacturer(
            detect_devices(fast=False),
        )

        if group_devices:
            for manu, devs in group_devices.items():
                backend = manufacturer_to_backend(manu)
                rk = envs.GPUSTACK_RUNTIME_DETECT_BACKEND_MAP_RESOURCE_KEY.get(backend)
                ren = envs.GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_RUNTIME_VISIBLE_DEVICES.get(
                    rk,
                )
                ben = envs.GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_BACKEND_VISIBLE_DEVICES.get(
                    rk,
                )
                if ren and ben:
                    dev_uuids: list[str] = []
                    dev_indexes: list[str] = []
                    for dev in devs:
                        dev_uuids.append(dev.uuid)
                        dev_indexes.append(str(dev.index))
                    dev_indexes_alignment: dict[str, str] = {
                        dev_indexes[i]: str(i) for i in range(len(devs))
                    }
                    self._visible_devices_env[ren] = ben
                    self._visible_devices_values[ren] = (
                        dev_uuids
                        if ren
                        in envs.GPUSTACK_RUNTIME_DEPLOY_RUNTIME_VISIBLE_DEVICES_VALUE_UUID
                        else dev_indexes
                    )
                    for ben_item in ben:
                        if (
                            ben_item
                            in envs.GPUSTACK_RUNTIME_DEPLOY_BACKEND_VISIBLE_DEVICES_VALUE_ALIGNMENT
                        ):
                            self._backend_visible_devices_values_alignment[ben_item] = (
                                dev_indexes_alignment
                            )
                    if (
                        envs.GPUSTACK_RUNTIME_DEPLOY_CPU_AFFINITY
                        or envs.GPUSTACK_RUNTIME_DEPLOY_NUMA_AFFINITY
                    ):
                        topos = get_devices_topologies(devices=devs)
                        if topos:
                            self._visible_devices_topologies[ren] = topos[0]

            if self._visible_devices_env:
                return

        # Fallback to unknown backend
        self._visible_devices_env["UNKNOWN_RUNTIME_VISIBLE_DEVICES"] = []
        self._visible_devices_values["UNKNOWN_RUNTIME_VISIBLE_DEVICES"] = ["all"]

    def get_visible_devices_env_values(
        self,
    ) -> (dict[str, list[str]], dict[str, list[str]]):
        """
        Return the visible devices environment variables and values mappings.
        For example:
        (
            {
                "NVIDIA_VISIBLE_DEVICES": ["CUDA_VISIBLE_DEVICES"],
                "AMD_VISIBLE_DEVICES": ["HIP_VISIBLE_DEVICES", "ROCR_VISIBLE_DEVICES"]
            }.
            {
                "NVIDIA_VISIBLE_DEVICES": ["0"],
                "AMD_VISIBLE_DEVICES": ["0", "1"]
            }
        ).

        Returns:
            A tuple of two dictionaries:
            - The first dictionary maps runtime visible devices environment variable names
              to lists of backend visible devices environment variable names.
            - The second dictionary maps runtime visible devices environment variable names
              to lists of device indexes or UUIDs.

        """
        self._prepare()
        return self._visible_devices_env, self._visible_devices_values

    def get_visible_devices_affinities(
        self,
        runtime_env: list[str],
        resource_value: str,
    ) -> tuple[str, str]:
        """
        Get the CPU and NUMA affinities for the given runtime environment and resource value.

        Args:
            runtime_env:
                The list of runtime visible devices environment variable names.
            resource_value:
                The resource value, which can be "all" or a comma-separated list of device indexes

        Returns:
            A tuple containing:
            - A comma-separated string of CPU affinities.
            - A comma-separated string of NUMA affinities.

        """
        dev_indexes = []
        if resource_value != "all":
            dev_indexes = [int(v.strip()) for v in resource_value.split(",")]

        cpus_set: list[str] = []
        numas_set: list[str] = []
        for re_ in runtime_env:
            topo = self._visible_devices_topologies.get(re_)
            if topo:
                cs, ns = topo.get_affinities(dev_indexes, deduplicate=False)
                cpus_set.extend(cs)
                numas_set.extend(ns)

        return ",".join(set(cpus_set)), ",".join(set(numas_set))

    def align_backend_visible_devices_env_values(
        self,
        backend_visible_devices_env: str,
        resource_key_values: str,
    ) -> str:
        """
        Return the aligned backend visible devices environment variable values.
        For example, if the backend visible devices env is "ASCEND_RT_VISIBLE_DEVICES",
        and the `resource_key_values` is "4,6", and the detected devices are with indexes
        [4,5,6,7], then the aligned result will be "0,2".

        Args:
            backend_visible_devices_env:
                The backend visible devices environment variable name.
            resource_key_values:
                The resource key values to align.

        Returns:
            The aligned backend visible devices environment variable values.
            If no alignment is needed, return the original `resource_key_values`.

        """
        if (
            backend_visible_devices_env
            not in envs.GPUSTACK_RUNTIME_DEPLOY_BACKEND_VISIBLE_DEVICES_VALUE_ALIGNMENT
        ):
            return resource_key_values
        self._prepare()
        alignments = self._backend_visible_devices_values_alignment.get(
            backend_visible_devices_env,
        )
        if not alignments:
            return resource_key_values
        return ",".join(
            [alignments.get(v, v) for v in resource_key_values.split(",")],
        )

    @property
    def name(self) -> str:
        """
        Return the name of the deployer.

        Returns:
            The name of the deployer.

        """
        return self._name

    def close(self):
        if self._pool:
            self._pool.shutdown(cancel_futures=True)
            self._pool = None
            if hasattr(atexit, "unregister"):
                atexit.unregister(self.close)

    @property
    def pool(self):
        """
        Create thread pool on first request
        avoids instantiating unused threadpool for blocking clients.
        """
        if self._pool is None:
            atexit.register(self.close)
            pool_threads = envs.GPUSTACK_RUNTIME_DEPLOY_ASYNC_THREADS
            if pool_threads and pool_threads < 1:
                pool_threads = None
            self._pool = ThreadPoolExecutor(
                max_workers=pool_threads,
                thread_name_prefix=f"runtime-deployer-{self.name}",
            )
        return self._pool

    @_default_args
    def create(
        self,
        workload: WorkloadPlan,
        async_mode: bool | None = None,
    ):
        """
        Deploy the given workload.

        Args:
            workload:
                The workload to deploy.
            async_mode:
                Whether to execute in a separate thread.

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
        if async_mode:
            try:
                future = self.pool.submit(
                    self._create,
                    workload,
                )
                future.result()
            except OperationError:
                raise
            except Exception as e:
                msg = "Asynchronous workload creation failed."
                raise OperationError(msg) from e
        else:
            self._create(workload)

    @abstractmethod
    def _create(self, workload: WorkloadPlan):
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

    @_default_args
    def get(
        self,
        name: WorkloadName,
        namespace: WorkloadNamespace | None = None,
        async_mode: bool | None = None,
    ) -> WorkloadStatus | None:
        """
        Get the status of a workload.

        Args:
            name:
                The name of the workload.
            namespace:
                The namespace of the workload.
            async_mode:
                Whether to execute in a separate thread.

        Returns:
            The status if found, None otherwise.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workload fails to get.

        """
        if async_mode:
            try:
                future = self.pool.submit(
                    self._get,
                    name,
                    namespace,
                )
                return future.result()
            except OperationError:
                raise
            except Exception as e:
                msg = "Asynchronous workload get failed."
                raise OperationError(msg) from e
        else:
            return self._get(name, namespace)

    @abstractmethod
    def _get(
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

    @_default_args
    def delete(
        self,
        name: WorkloadName,
        namespace: WorkloadNamespace | None = None,
        async_mode: bool | None = None,
    ) -> WorkloadStatus | None:
        """
        Delete a workload.

        Args:
            name:
                The name of the workload.
            namespace:
                The namespace of the workload.
            async_mode:
                Whether to execute in a separate thread.

        Return:
            The status if found, None otherwise.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workload fails to delete.

        """
        if async_mode:
            try:
                future = self.pool.submit(
                    self._delete,
                    name,
                    namespace,
                )
                return future.result()
            except OperationError:
                raise
            except Exception as e:
                msg = "Asynchronous workload delete failed."
                raise OperationError(msg) from e
        else:
            return self._delete(name, namespace)

    @abstractmethod
    def _delete(
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

    @_default_args
    def list(
        self,
        namespace: WorkloadNamespace | None = None,
        labels: dict[str, str] | None = None,
        async_mode: bool | None = None,
    ) -> list[WorkloadStatus]:
        """
        List all workloads.

        Args:
            namespace:
                The namespace of the workloads.
            labels:
                Labels to filter the workloads.
            async_mode:
                Whether to execute in a separate thread.

        Returns:
            A list of workload statuses.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workloads fail to list.

        """
        if async_mode:
            try:
                future = self.pool.submit(
                    self._list,
                    namespace,
                    labels,
                )
                return future.result()
            except OperationError:
                raise
            except Exception as e:
                msg = "Asynchronous workload list failed."
                raise OperationError(msg) from e
        else:
            return self._list(namespace, labels)

    @abstractmethod
    def _list(
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

    async def async_logs(
        self,
        name: WorkloadName,
        namespace: WorkloadNamespace | None = None,
        token: WorkloadOperationToken | None = None,
        timestamps: bool = False,
        tail: int | None = None,
        since: int | None = None,
        follow: bool = False,
        async_mode: bool | None = None,
    ) -> AsyncGenerator[bytes | str, None, None] | bytes | str:
        """
        Asynchronously get the logs of a workload.

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
            async_mode:
                Whether to execute in a separate thread.

        Returns:
            The logs as a byte string or a generator yielding byte strings if follow is True.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workload fails to get logs.

        """

        def run_sync_gen(f: bool) -> Generator[bytes | str, None, None] | bytes | str:
            return self.logs(
                name=name,
                namespace=namespace,
                token=token,
                timestamps=timestamps,
                tail=tail,
                since=since,
                follow=f,
                async_mode=async_mode,
            )

        if not follow:
            return run_sync_gen(False)

        async def async_gen() -> AsyncGenerator[bytes | str, None, None]:
            loop = asyncio.get_event_loop()
            sync_gen = await loop.run_in_executor(self.pool, run_sync_gen, True)
            with contextlib.closing(sync_gen) as gen:
                for chunk in gen:
                    yield chunk

        return async_gen()

    @_default_args
    def logs(
        self,
        name: WorkloadName,
        namespace: WorkloadNamespace | None = None,
        token: WorkloadOperationToken | None = None,
        timestamps: bool = False,
        tail: int | None = None,
        since: int | None = None,
        follow: bool = False,
        async_mode: bool | None = None,
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
            async_mode:
                Whether to execute in a separate thread.

        Returns:
            The logs as a byte string or a generator yielding byte strings if follow is True.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workload fails to get logs.

        """
        if async_mode:
            try:
                future = self.pool.submit(
                    self._logs,
                    name,
                    namespace,
                    token,
                    timestamps,
                    tail,
                    since,
                    follow,
                )
                return future.result()
            except OperationError:
                raise
            except Exception as e:
                msg = "Asynchronous workload logs failed."
                raise OperationError(msg) from e
        else:
            return self._logs(name, namespace, token, timestamps, tail, since, follow)

    @abstractmethod
    def _logs(
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

    @_default_args
    def exec(
        self,
        name: WorkloadName,
        namespace: WorkloadNamespace | None = None,
        token: WorkloadOperationToken | None = None,
        detach: bool = True,
        command: list[str] | None = None,
        args: list[str] | None = None,
        async_mode: bool | None = None,
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
            async_mode:
                Whether to execute in a separate thread.

        Returns:
            If detach is False, return a WorkloadExecStream.
            otherwise, return the output of the command as a byte string or string.

        Raises:
            UnsupportedError:
                If the deployer is not supported in the current environment.
            OperationError:
                If the workload fails to execute the command.

        """
        if async_mode:
            try:
                future = self.pool.submit(
                    self._exec,
                    name,
                    namespace,
                    token,
                    detach,
                    command,
                    args,
                )
                return future.result()
            except OperationError:
                raise
            except Exception as e:
                msg = "Asynchronous workload exec failed."
                raise OperationError(msg) from e
        else:
            return self._exec(name, namespace, token, detach, command, args)

    @abstractmethod
    def _exec(
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
