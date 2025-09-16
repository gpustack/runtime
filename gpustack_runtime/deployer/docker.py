from __future__ import annotations

import logging
from dataclasses import dataclass, field
from functools import lru_cache
from math import ceil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import docker
import docker.errors
import docker.models.containers
import docker.models.images
import docker.models.volumes
import docker.types
from dataclasses_json import dataclass_json

from .. import envs  # noqa: TID252
from .__types__ import (
    Container,
    ContainerExecution,
    ContainerMountModeEnum,
    ContainerProfileEnum,
    ContainerResources,
    ContainerRestartPolicyEnum,
    Deployer,
    OperationError,
    UnsupportedError,
    WorkloadExecResult,
    WorkloadName,
    WorkloadOperationToken,
    WorkloadPlan,
    WorkloadStatus,
    WorkloadStatusOperation,
    WorkloadStatusStateEnum,
)

if TYPE_CHECKING:
    from collections.abc import Generator

logger = logging.getLogger(__name__)

_DEFAULT_RESOURCE_DEVICE_ENV_MAPPING = {
    "nvidia.com/gpu": "NVIDIA_VISIBLE_DEVICES",
    "amd.com/gpu": "AMD_VISIBLE_DEVICES",
    "huawei.com/Ascend910A": "ASCEND_VISIBLE_DEVICES",
    "huawei.com/Ascend910B": "ASCEND_VISIBLE_DEVICES",
    "huawei.com/Ascend310P": "ASCEND_VISIBLE_DEVICES",
    "cambricon.com/vmlu": "CAMBRICON_VISIBLE_DEVICES",
    "hygon.com/dcunum": "HYGON_VISIBLE_DEVICES",
    "mthreads.com/vgpu": "METHERDS_VISIBLE_DEVICES",
    "iluvatar.ai/vgpu": "ILUVATAR_VISIBLE_DEVICES",
    "enflame.com/vgcu": "ENFLAME_VISIBLE_DEVICES",
    "metax-tech.com/sgpu": "METAX_VISIBLE_DEVICES",
}
_LABEL_WORKLOAD = "runtime.gpustack.ai/workload"
_LABEL_COMPONENT = "runtime.gpustack.ai/component"
_LABEL_COMPONENT_NAME = "runtime.gpustack.ai/component-name"
_LABEL_COMPONENT_INDEX = "runtime.gpustack.ai/component-index"
_LABEL_COMPONENT_HEAL_PREFIX = "runtime.gpustack.ai/component-heal"


@dataclass
class DockerWorkloadPlan(WorkloadPlan):
    """
    Workload plan implementation for Docker containers.

    Attributes:
        pause_image (str):
            Image used for the pause container.
        unhealthy_restart_image (str):
            Image used for unhealthy restart container.
        resource_device_env_mapping (dict[str, str]):
            Mapping from resource names to environment variable names for device allocation.
            For example, {"nvidia.com/gpu": "NVIDIA_VISIBLE_DEVICES"},
            which sets the "NVIDIA_VISIBLE_DEVICES" environment variable to the allocated GPU device IDs.
        name (str):
            Name of the workload,
            it should be unique in the deployer.
        labels (dict[str, str] | None):
            Labels to attach to the workload.
        host_network (bool):
            Whether to use the host network for the workload.
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
        containers (list[tuple[int, Container]] | None):
            List of containers in the workload.
            It must contain at least one "RUN" profile container.

    """

    pause_image: str = envs.GPUSTACK_RUNTIME_DOCKER_PAUSE_IMAGE
    """
    Image used for the pause container.
    """
    unhealthy_restart_image: str = envs.GPUSTACK_RUNTIME_DOCKER_UNHEALTHY_RESTART_IMAGE
    """
    Image used for unhealthy restart container.
    """
    resource_device_env_mapping: dict[str, str] = field(
        default_factory=lambda: _DEFAULT_RESOURCE_DEVICE_ENV_MAPPING,
    )
    """
    Mapping from resource names to environment variable names for device allocation.

    For example, {"nvidia.com/gpu": "NVIDIA_VISIBLE_DEVICES"},
    which sets the "NVIDIA_VISIBLE_DEVICES" environment variable to the allocated GPU device IDs.
    """


@dataclass_json
@dataclass
class DockerWorkloadStatus(WorkloadStatus):
    """
    Workload status implementation for Docker containers.
    """

    _d_containers: list[docker.models.containers.Container] | None = None
    """
    List of Docker containers in the workload,
    internal use only.
    """

    def __init__(
        self,
        name: WorkloadName,
        d_containers: list[docker.models.containers],
        **kwargs,
    ):
        created_at = d_containers[0].attrs["Created"]
        labels = {
            k: v
            for k, v in d_containers[0].labels.items()
            if not k.startswith("runtime.gpustack.ai/")
        }

        super().__init__(
            name=name,
            created_at=created_at,
            labels=labels,
            **kwargs,
        )

        self._d_containers = d_containers

        for c in d_containers:
            op = WorkloadStatusOperation(
                name=c.labels.get(_LABEL_COMPONENT_NAME, "") or c.name,
                token=c.attrs.get("Id", "") or c.name,
            )
            match c.labels.get(_LABEL_COMPONENT):
                case "init":
                    if c.status == "running" and _has_restart_policy(c):
                        self.executable.append(op)
                    self.loggable.append(op)
                case "run":
                    self.executable.append(op)
                    self.loggable.append(op)

        self.state = self.parse_state(d_containers)

    @staticmethod
    def parse_state(
        d_containers: list[docker.models.containers],
    ) -> WorkloadStatusStateEnum:
        """
        Parse the state of the workload based on the status of its containers.

        Args:
            d_containers:
                List of Docker containers in the workload.

        Returns:
            The state of the workload.

        """
        d_init_containers: list[docker.models.containers.Container] = []
        d_run_containers: list[docker.models.containers.Container] = []
        for c in d_containers:
            if c.labels.get(_LABEL_COMPONENT) == "init":
                d_init_containers.append(c)
            elif c.labels.get(_LABEL_COMPONENT) == "run":
                d_run_containers.append(c)

        if not d_run_containers:
            if not d_init_containers:
                return WorkloadStatusStateEnum.UNKNOWN
            return WorkloadStatusStateEnum.PENDING

        for cr in d_run_containers:
            if cr.status == "created":
                if not d_init_containers:
                    return WorkloadStatusStateEnum.PENDING
                for ci in d_init_containers or []:
                    if ci.status == "created":
                        return WorkloadStatusStateEnum.PENDING
                    if ci.status == "dead" or (
                        ci.status == "exited" and ci.attrs["State"]["ExitCode"] != 0
                    ):
                        return WorkloadStatusStateEnum.FAILED
                    if ci.status != "exited" and not _has_restart_policy(ci):
                        return WorkloadStatusStateEnum.INITIALIZING
                return WorkloadStatusStateEnum.INITIALIZING
            if cr.status == "dead" or (
                cr.status == "exited" and cr.attrs["State"]["ExitCode"] != 0
            ):
                if not _has_restart_policy(cr):
                    return WorkloadStatusStateEnum.FAILED
                return WorkloadStatusStateEnum.UNHEALTHY
            if cr.status != "running" and not _has_restart_policy(cr):
                return WorkloadStatusStateEnum.PENDING

        return WorkloadStatusStateEnum.RUNNING


class DockerDeployer(Deployer):
    """
    Deployer implementation for Docker containers.
    """

    _client: docker.DockerClient | None = None
    """
    Client for interacting with the Docker daemon.
    """
    default_file_mode: int = 0o644
    """
    Default file mode for container files.
    """

    @staticmethod
    @lru_cache
    def is_supported() -> bool:
        """
        Check if Docker is supported in the current environment.

        Returns:
            True if supported, False otherwise.

        """
        supported = False

        client = DockerDeployer._get_client()
        if client:
            try:
                supported = client.ping()
            except docker.errors.APIError:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.exception("Docker ping failed")

        return supported

    @staticmethod
    def _get_client() -> docker.DockerClient | None:
        client = None

        try:
            if Path("/var/run/docker.sock").exists():
                client = docker.DockerClient(base_url="unix://var/run/docker.sock")
            else:
                client = docker.from_env()
        except docker.errors.DockerException:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to get Docker client")

        return client

    @staticmethod
    def _supported(func):
        def wrapper(self, *args, **kwargs):
            if not self.is_supported():
                msg = "Docker is not supported in the current environment."
                raise UnsupportedError(msg)
            return func(self, *args, **kwargs)

        return wrapper

    def _pull_image(self, image: str) -> docker.models.images.Image:
        try:
            return self._client.images.get(image)
        except docker.errors.ImageNotFound:
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"Pulling image {image}")
            try:
                return self._client.images.pull(image)
            except docker.errors.APIError as e:
                msg = f"Failed to pull image {image}"
                raise OperationError(msg) from e
        except docker.errors.APIError as e:
            msg = f"Failed to get image {image}"
            raise OperationError(msg) from e

    def _create_ephemeral_volumes(self, workload: DockerWorkloadPlan) -> dict:
        ephemeral_volume_name_mapping: dict[str, str] = {
            m.volume: f"{workload.name}-{m.volume}"
            for c in workload.containers
            for m in c.mounts or []
            if m.volume
        }

        try:
            for _, n in ephemeral_volume_name_mapping.values():
                self._client.volumes.create(
                    name=n,
                    driver="local",
                    labels=workload.labels,
                )
        except docker.errors.APIError as e:
            msg = "Failed to create ephemeral volumes"
            raise OperationError(msg) from e

        return ephemeral_volume_name_mapping

    def _create_ephemeral_files(self, workload: DockerWorkloadPlan) -> dict:
        ephemeral_filename_mapping: dict[tuple[int, str], str] = {}
        ephemeral_files: list[tuple[str, str, int]] = []
        for ci, c in enumerate(workload.containers):
            for fi, f in enumerate(c.files or []):
                if f.content is not None:
                    fn = f"{workload.name}-{ci}-{fi}"
                    ephemeral_filename_mapping[(ci, f.path)] = fn
                    ephemeral_files.append((fn, f.content, f.mode))

        try:
            for fn, fc, fm in ephemeral_files:
                fp = envs.GPUSTACK_RUNTIME_DOCKER_EPHEMERAL_FILES_DIR.joinpath(fn)
                with fp.open("w", encoding="utf-8") as f:
                    f.write(fc)
                fp.chmod(fm if fm else self.default_file_mode)
        except OSError as e:
            msg = "Failed to create ephemeral files"
            raise OperationError(msg) from e

        return ephemeral_filename_mapping

    def _create_pause_container(
        self,
        workload: DockerWorkloadPlan,
    ) -> docker.models.containers.Container:
        container_name = f"{workload.name}-pause"
        try:
            container = self._client.containers.get(container_name)
        except docker.errors.NotFound:
            pass
        except docker.errors.APIError as e:
            msg = f"Failed to confirm whether container {container_name} exists"
            raise OperationError(msg) from e
        else:
            # TODO(thxCode): check if the container matches the spec
            return container

        create_params: dict[str, Any] = {
            "name": container_name,
            "restart_policy": {"Name": "always"},
            "network_mode": "bridge",
            "ipc_mode": "shareable",
            "labels": {
                **workload.labels,
                _LABEL_COMPONENT: "pause",
            },
        }

        if workload.host_network:
            create_params["network_mode"] = "host"
        else:
            port_mapping: dict[str, int] = {
                # <internal port>/<protocol>: <external port>
                f"{p.internal}/{p.protocol.lower()}": p.external or p.internal
                for c in workload.containers
                if c.profile == ContainerProfileEnum.RUN
                for p in c.ports or []
            }
            if port_mapping:
                create_params["ports"] = port_mapping

        if workload.host_ipc:
            create_params["ipc_mode"] = "host"

        try:
            d_container = self._client.containers.create(
                image=self._pull_image(workload.pause_image),
                detach=True,
                **create_params,
            )
        except docker.errors.APIError as e:
            msg = f"Failed to create container {container_name}"
            raise OperationError(msg) from e
        else:
            return d_container

    def _create_unhealthy_restart_container(
        self,
        workload: DockerWorkloadPlan,
    ) -> docker.models.containers.Container | None:
        # Check if the first check of any RUN container has teardown enabled.
        enabled = any(
            c.checks[0].teardown
            for c in workload.containers
            if c.profile == ContainerProfileEnum.RUN and c.checks
        )
        if not enabled:
            return None

        container_name = f"{workload.name}-unhealthy-restart"
        try:
            d_container = self._client.containers.get(container_name)
        except docker.errors.NotFound:
            pass
        except docker.errors.APIError as e:
            msg = f"Failed to confirm whether container {container_name} exists"
            raise OperationError(msg) from e
        else:
            # TODO(thxCode): check if the container matches the spec
            return d_container

        create_params: dict[str, Any] = {
            "name": container_name,
            "restart_policy": {"Name": "always"},
            "network_mode": "none",
            "labels": {
                **workload.labels,
                _LABEL_COMPONENT: "unhealthy-restart",
            },
            "environment": [
                f"AUTOHEAL_CONTAINER_LABEL={_LABEL_COMPONENT_HEAL_PREFIX}-{workload.name}",
            ],
            "volumes": [
                "/var/run/docker.sock:/var/run/docker.sock",
            ],
        }

        try:
            d_container = self._client.containers.create(
                image=self._pull_image(workload.unhealthy_restart_image),
                detach=True,
                **create_params,
            )
        except docker.errors.APIError as e:
            msg = f"Failed to create container {container_name}"
            raise OperationError(msg) from e
        else:
            return d_container

    @staticmethod
    def _parameterize_container_execution(
        workload: DockerWorkloadPlan,
        container: Container,
        _: int,
        create_params: dict[str, Any],
    ):
        execution: ContainerExecution = container.execution or {}
        if not execution:
            return

        if execution.working_dir:
            create_params["working_dir"] = execution.working_dir
        if execution.command:
            create_params["entrypoint"] = execution.command
        if execution.args:
            create_params["command"] = execution.args
        run_as_user = execution.run_as_user or workload.run_as_user
        run_as_group = execution.run_as_group or workload.run_as_group
        if run_as_user is not None:
            create_params["user"] = run_as_user
            if run_as_group is not None:
                create_params["user"] = f"{run_as_user}:{run_as_group}"
        if run_as_group is not None:
            create_params["group_add"] = [run_as_group]
            if workload.fs_group is not None:
                create_params["group_add"] = [run_as_group, workload.fs_group]
        elif workload.fs_group is not None:
            create_params["group_add"] = [workload.fs_group]
        if workload.sysctls:
            create_params["sysctls"] = {
                sysctl.name: sysctl.value for sysctl in workload.sysctls
            }
        if execution.readonly_rootfs:
            create_params["read_only"] = True
        if execution.privileged:
            create_params["privileged"] = True
        elif cap := execution.capabilities:
            if cap.add:
                create_params["cap_add"] = cap.add
            elif cap.drop:
                create_params["cap_drop"] = cap.drop

    @staticmethod
    def _parameterize_container_resources(
        workload: DockerWorkloadPlan,
        container: Container,
        _: int,
        create_params: dict[str, Any],
    ):
        resources: ContainerResources = container.resources or {}
        if not resources:
            return

        for r_k, r_v in resources.items():
            match r_k:
                case "cpu":
                    if isinstance(r_v, int | float):
                        create_params["cpu_shares"] = ceil(r_v * 1024)
                case "memory":
                    if isinstance(r_v, int):
                        create_params["mem_limit"] = r_v
                        create_params["mem_reservation"] = create_params["mem_limit"]
                        create_params["memswap_limit"] = create_params["mem_limit"]
                    elif isinstance(r_v, str):
                        create_params["mem_limit"] = r_v.lower().removesuffix("i")
                        create_params["mem_reservation"] = create_params["mem_limit"]
                        create_params["memswap_limit"] = create_params["mem_limit"]
                case _:
                    if (
                        workload.resource_device_env_mapping
                        and r_k in workload.resource_device_env_mapping
                    ):
                        env_name = workload.resource_device_env_mapping[r_k]
                        if "environment" not in create_params:
                            create_params["environment"] = {}
                        create_params["environment"][env_name] = str(r_v)

    @staticmethod
    def _parameterize_container_files_and_mounts(
        _: DockerWorkloadPlan,
        container: Container,
        container_index: int,
        create_params: dict[str, Any],
        ephemeral_filename_mapping: dict[tuple[int, str] : str],
        ephemeral_volume_name_mapping: dict[str, str],
    ):
        mount_binding: list[docker.types.Mount] = []

        if files := container.files:
            for f in files:
                binding = docker.types.Mount(
                    type="bind",
                    source="",
                    target="",
                )

                if f.content is not None:
                    if (container_index, f.path) not in ephemeral_filename_mapping:
                        continue
                    fn = ephemeral_filename_mapping[(container_index, f.path)]
                    path = str(
                        envs.GPUSTACK_RUNTIME_DOCKER_EPHEMERAL_FILES_DIR.joinpath(fn),
                    )
                    binding["source"] = path
                elif f.path:
                    binding["source"] = f.path
                else:
                    continue

                binding["target"] = f"/{f.path.lstrip('/')}"

                if f.mode < 0o600:
                    binding["read_only"] = True

                mount_binding.append(binding)

        if mounts := container.mounts:
            for m in mounts:
                binding = docker.types.Mount(
                    type="volume",
                    source="",
                    target="",
                )

                if m.volume:
                    binding["source"] = ephemeral_volume_name_mapping.get(
                        m.volume,
                        m.volume,
                    )
                    # TODO(thxCode): support subpath.
                elif m.path:
                    binding["type"] = "bind"
                    binding["source"] = m.path
                else:
                    continue

                binding["target"] = f"/{m.path.lstrip('/')}"

                if m.mode == ContainerMountModeEnum.ROX:
                    binding["read_only"] = True

                mount_binding.append(binding)

        if mount_binding:
            create_params["mounts"] = mount_binding

    def _create_containers(
        self,
        workload: DockerWorkloadPlan,
        ephemeral_filename_mapping: dict[tuple[int, str] : str],
        ephemeral_volume_name_mapping: dict[str, str],
    ) -> (
        list[docker.models.containers.Container],
        list[docker.models.containers.Container],
    ):
        d_init_containers: list[docker.models.containers.Container] = []
        d_run_containers: list[docker.models.containers.Container] = []

        pause_container_namespace = f"container:{workload.name}-pause"
        for ci, c in enumerate(workload.containers):
            container_name = f"{workload.name}-{c.profile.lower()}-{ci}"
            try:
                d_container = self._client.containers.get(container_name)
            except docker.errors.NotFound:
                pass
            except docker.errors.APIError as e:
                msg = f"Failed to confirm whether container {container_name} exists"
                raise OperationError(msg) from e
            else:
                # TODO(thxCode): check if the container matches the spec
                if c.profile == ContainerProfileEnum.INIT:
                    d_init_containers.append(d_container)
                else:
                    d_run_containers.append(d_container)
                continue

            detach = c.profile == ContainerProfileEnum.RUN

            create_params: dict[str, Any] = {
                "name": container_name,
                "network_mode": pause_container_namespace,
                "ipc_mode": pause_container_namespace,
                "labels": {
                    **workload.labels,
                    _LABEL_COMPONENT: f"{c.profile.lower()}",
                    _LABEL_COMPONENT_NAME: c.name,
                    _LABEL_COMPONENT_INDEX: str(ci),
                },
            }

            if not workload.host_network:
                create_params["hostname"] = c.name

            if workload.pid_shared:
                create_params["pid_mode"] = pause_container_namespace

            if workload.shm_size:
                create_params["shm_size"] = workload.shm_size

            # Parameterize restart policy
            match c.profile:
                case ContainerProfileEnum.INIT:
                    if c.restart_policy:
                        if c.restart_policy == ContainerRestartPolicyEnum.ON_FAILURE:
                            create_params["restart_policy"] = {
                                "Name": "on-failure",
                            }
                        elif c.restart_policy == ContainerRestartPolicyEnum.ALWAYS:
                            create_params["restart_policy"] = {
                                "Name": "always",
                            }
                            detach = True
                case ContainerProfileEnum.RUN:
                    if not c.restart_policy:
                        create_params["restart_policy"] = {
                            "Name": "always",
                        }
                    elif c.restart_policy == ContainerRestartPolicyEnum.ON_FAILURE:
                        create_params["restart_policy"] = {
                            "Name": "on-failure",
                        }

            # Parameterize execution
            self._parameterize_container_execution(
                workload,
                c,
                ci,
                create_params,
            )

            # Parameterize environment variables
            if c.envs:
                create_params["environment"] = {}
                for e in c.envs:
                    if e.name.endswith("_VISIBLE_DEVICES"):
                        if e.value in ["none", "void"]:
                            create_params.pop("runtime", None)
                        else:
                            create_params["runtime"] = e.name.removesuffix(
                                "_VISIBLE_DEVICES",
                            ).lower()
                    create_params["environment"][e.name] = e.value

            # Parameterize resources
            self._parameterize_container_resources(
                workload,
                c,
                ci,
                create_params,
            )

            # Parameterize files and mounts
            self._parameterize_container_files_and_mounts(
                workload,
                c,
                ci,
                create_params,
                ephemeral_filename_mapping,
                ephemeral_volume_name_mapping,
            )

            # Parameterize health check from the first check.
            if c.profile == ContainerProfileEnum.RUN and c.checks:
                # If the first check has teardown enabled, enable auto-heal for the container.
                if c.checks[0].teardown:
                    create_params["labels"][
                        f"{_LABEL_COMPONENT_HEAL_PREFIX}-{workload.name}"
                    ] = "true"

                healthcheck: dict[str, Any] = {}
                for attr_k in ["interval", "timeout", "retries", "delay"]:
                    attr_v = getattr(c.checks[0], attr_k, None)
                    if not attr_v:
                        continue
                    healthcheck[attr_k if attr_k != "delay" else "start_period"] = (
                        attr_v
                    )
                for attr_k in ["execution", "tcp", "http", "https"]:
                    attr_v = getattr(c.checks[0], attr_k, None)
                    if not attr_v:
                        continue
                    match attr_k:
                        case "execution":
                            if attr_v.command:
                                healthcheck["test"] = [
                                    "CMD",
                                    *attr_v.command,
                                ]
                        case "tcp":
                            port = attr_v.port or 80
                            healthcheck["test"] = [
                                "CMD",
                                "sh",
                                "-c",
                                (
                                    f"if [ `command -v netstat` ]; then netstat -an | grep -w {port} >/dev/null || exit 1; "
                                    f"else if [ `command -v nc` ]; then nc -z localhost:{port} >/dev/null || exit 1 ; "
                                    f"else cat /etc/services | grep -w {port}/tcp >/dev/null || exit 1 ; "
                                    f"fi",
                                ),
                            ]
                        case "http" | "https":
                            curl_options = "-fsSL -o /dev/null"
                            wget_options = "-q -O /dev/null"
                            if attr_k == "https":
                                curl_options += " -k"
                                wget_options += " --no-check-certificate"
                            if attr_v.headers:
                                for hk, hv in attr_v.headers.items():
                                    curl_options += f" -H '{hk}: {hv}'"
                                    wget_options += f" --header='{hk}: {hv}'"
                            url = f"{attr_k}://localhost:{attr_v.port or 80}{attr_v.path or '/'}"
                            healthcheck["test"] = [
                                "CMD",
                                "sh",
                                "-c",
                                (
                                    f"if [ `command -v curl` ]; then curl {curl_options} {url}; "
                                    f"else wget {wget_options} {url}; "
                                    f"fi"
                                ),
                            ]

            try:
                d_container = self._client.containers.create(
                    image=self._pull_image(c.image),
                    detach=detach,
                    **create_params,
                )
            except docker.errors.APIError as e:
                msg = f"Failed to create container {container_name}"
                raise OperationError(msg) from e
            else:
                if c.profile == ContainerProfileEnum.INIT:
                    d_init_containers.append(d_container)
                else:
                    d_run_containers.append(d_container)

        return d_init_containers, d_run_containers

    @staticmethod
    def _start_containers(
        container: docker.models.containers.Container
        | list[docker.models.containers.Container],
    ):
        if isinstance(container, list):
            for c in container:
                DockerDeployer._start_containers(c)
            return

        match container.status:
            case "created":
                container.start()
            case "exited" | "dead":
                container.restart()
            case "paused":
                container.unpause()

        if not _has_restart_policy(container):
            exit_status = container.wait()["StatusCode"]
            if exit_status != 0:
                config = container.attrs.get("Config", {})
                command = config.get("Cmd", [])
                image = config.get("Image", "")
                raise docker.errors.ContainerError(
                    container,
                    exit_status,
                    command,
                    image,
                    "",
                )

    def __init__(self):
        self._client = self._get_client()

    @_supported
    def create(self, workload: WorkloadPlan):
        """
        Deploy a Docker workload.

        Args:
            workload:
                The workload to deploy.

        Raises:
            UnsupportedError:
                If Docker is not supported in the current environment.
            OperationError:
                If the Docker workload fails to deploy.

        """
        if not isinstance(workload, DockerWorkloadPlan | WorkloadPlan):
            msg = f"Invalid workload type: {type(workload)}"
            raise OperationError(msg)
        if isinstance(workload, WorkloadPlan):
            workload = DockerWorkloadPlan(**workload.__dict__)

        # Validate workload.
        if not workload.containers:
            msg = "Workload must have at least one container"
            raise OperationError(msg)
        if not any(c.profile == ContainerProfileEnum.RUN for c in workload.containers):
            msg = "Workload must have at least one RUN container"
            raise OperationError(msg)

        # Default workload.
        workload.labels = {**(workload.labels or {}), _LABEL_WORKLOAD: workload.name}

        # Create ephemeral file if needed,
        # (container index, configured path): <actual filename>
        ephemeral_filename_mapping: dict[tuple[int, str] : str] = (
            self._create_ephemeral_files(workload)
        )

        # Create ephemeral volumes if needed,
        # <configured volume name>: <actual volume name>
        ephemeral_volume_name_mapping: dict[str, str] = self._create_ephemeral_volumes(
            workload,
        )

        # Create pause container.
        pause_container = self._create_pause_container(workload)

        # Create init/run containers.
        init_containers, run_containers = self._create_containers(
            workload,
            ephemeral_filename_mapping,
            ephemeral_volume_name_mapping,
        )

        # Create unhealthy restart container if needed.
        unhealthy_restart_container = self._create_unhealthy_restart_container(workload)

        # Start containers in order: pause -> init(s) -> run(s) -> unhealthy restart
        try:
            self._start_containers(pause_container)
            self._start_containers(init_containers)
            self._start_containers(run_containers)
            if unhealthy_restart_container:
                self._start_containers(unhealthy_restart_container)
        except docker.errors.APIError as e:
            msg = "Failed to apply workload"
            raise OperationError(msg) from e

    @_supported
    def get(self, name: WorkloadName) -> WorkloadStatus | None:
        """
        Get the status of a Docker workload.

        Args:
            name:
                The name of the workload.

        Returns:
            The status if found, None otherwise.

        Raises:
            UnsupportedError:
                If Docker is not supported in the current environment.
            OperationError:
                If the Docker workload fails to get.

        """
        list_options = {
            "filters": {
                "label": [
                    f"{_LABEL_WORKLOAD}={name}",
                    _LABEL_COMPONENT,
                ],
            },
        }

        try:
            d_containers = self._client.containers.list(
                all=True,
                **list_options,
            )
        except docker.errors.APIError as e:
            msg = f"Failed to list containers for workload {name}"
            raise OperationError(msg) from e

        if not d_containers:
            return None

        return DockerWorkloadStatus(
            name=name,
            d_containers=d_containers,
        )

    @_supported
    def delete(self, name: WorkloadName) -> WorkloadStatus | None:
        """
        Delete a Docker workload.

        Args:
            name:
                The name of the workload.

        Return:
            The status if found, None otherwise.

        Raises:
            UnsupportedError:
                If Docker is not supported in the current environment.
            OperationError:
                If the Docker workload fails to delete.

        """
        # Check if the workload exists.
        workload = self.get(name)
        if not workload:
            return None

        # Remove all containers with the workload label.
        try:
            d_containers = getattr(workload, "_d_containers", [])
            for c in d_containers:
                c.remove(
                    force=True,
                )
        except docker.errors.APIError as e:
            msg = f"Failed to delete containers for workload {name}"
            raise OperationError(msg) from e

        # Remove all ephemeral volumes with the workload label.
        try:
            list_options = {
                "filters": {
                    "label": [
                        f"{_LABEL_WORKLOAD}={name}",
                    ],
                },
            }
            d_volumes = self._client.volumes.list(
                **list_options,
            )

            for v in d_volumes:
                v.remove(
                    force=True,
                )
        except docker.errors.APIError as e:
            msg = f"Failed to delete volumes for workload {name}"
            raise OperationError(msg) from e

        # Remove all ephemeral files for the workload.
        try:
            for fp in envs.GPUSTACK_RUNTIME_DOCKER_EPHEMERAL_FILES_DIR.glob(
                f"{name}-*",
            ):
                if fp.is_file():
                    fp.unlink(missing_ok=True)
        except OSError as e:
            msg = f"Failed to delete ephemeral files for workload {name}"
            raise OperationError(msg) from e

        return workload

    @_supported
    def list(self, labels: dict[str, str] | None = None) -> list[WorkloadStatus]:
        """
        List all Docker workloads.

        Args:
            labels:
                Labels to filter workloads.

        Returns:
            A list of workload statuses.

        Raises:
            UnsupportedError:
                If Docker is not supported in the current environment.
            OperationError:
                If the Docker workloads fail to list.

        """
        list_options = {
            "filters": {
                "label": [
                    *[
                        f"{k}={v}"
                        for k, v in (labels or {}).items()
                        if k
                        not in (
                            _LABEL_WORKLOAD,
                            _LABEL_COMPONENT,
                            _LABEL_COMPONENT_INDEX,
                        )
                    ],
                    _LABEL_WORKLOAD,
                    _LABEL_COMPONENT,
                ],
            },
        }

        try:
            d_containers = self._client.containers.list(
                all=True,
                **list_options,
            )
        except docker.errors.APIError as e:
            msg = "Failed to list workloads' containers"
            raise OperationError(msg) from e

        # Group containers by workload name.
        # <workload name>: [docker.models.containers.Container, ...]
        workload_mapping: dict[str, list[docker.models.containers.Container]] = {}
        for c in d_containers:
            n = c.labels.get(_LABEL_WORKLOAD, None)
            if not n:
                continue
            if n not in workload_mapping:
                workload_mapping[n] = []
            workload_mapping[n].append(c)

        return [
            DockerWorkloadStatus(
                name=name,
                d_containers=d_containers,
            )
            for name, d_containers in workload_mapping.items()
        ]

    @_supported
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
        Get logs of a Docker workload or a specific container.

        Args:
            name:
                The name of the workload.
            token:
                The operation token representing a specific container ID.
                If None, fetch logs from the main RUN container of the workload.
            timestamps:
                Whether to include timestamps in the logs.
            tail:
                Number of lines from the end of the logs to show. If None, show all logs.
            since:
                Show logs since this time (in seconds since epoch). If None, show all logs.
            follow:
                Whether to stream the logs in real-time.

        Returns:
            The logs as a byte string or a generator yielding byte strings if follow is True.

        Raises:
            UnsupportedError:
                If Docker is not supported in the current environment.
            OperationError:
                If the Docker workload fails to fetch logs.

        """
        workload = self.get(name)
        if not workload:
            msg = f"Workload {name} not found"
            raise OperationError(msg)

        d_containers = getattr(workload, "_d_containers", [])
        container = next(
            (
                c
                for c in d_containers
                if (c.id == token if token else c.labels.get(_LABEL_COMPONENT) == "run")
            ),
            None,
        )
        if not container:
            msg = f"Loggable container of workload {name} not found"
            raise OperationError(msg)

        kwargs = {
            "timestamps": timestamps,
            "follow": follow,
        }
        if tail is not None:
            kwargs["tail"] = tail
        if since is not None:
            kwargs["since"] = since

        try:
            output = container.logs(
                stream=follow,
                **kwargs,
            )
        except docker.errors.APIError as e:
            msg = f"Failed to fetch logs for container {container.name} of workload {name}"
            raise OperationError(msg) from e
        else:
            return output

    @_supported
    def exec(
        self,
        name: WorkloadName,
        token: WorkloadOperationToken | None = None,
        detach: bool = True,
        command: list[str] | None = None,
        args: list[str] | None = None,
    ) -> WorkloadExecResult:
        """
        Execute a command in a Docker workload or a specific container.

        Args:
            name:
                The name of the workload.
            token:
                The operation token representing a specific container ID.
                If None, execute in the main RUN container of the workload.
            detach:
                Whether to run the command in detached mode.
            command:
                The command to execute. If None, defaults to "/bin/sh".
            args:
                Additional arguments for the command.

        Returns:
            A WorkloadExecResult containing the exit code and output.

        Raises:
            UnsupportedError:
                If Docker is not supported in the current environment.
            OperationError:
                If the Docker workload fails to execute the command.

        """
        workload = self.get(name)
        if not workload:
            msg = f"Workload {name} not found"
            raise OperationError(msg)

        d_containers = getattr(workload, "_d_containers", [])
        container = next(
            (
                c
                for c in d_containers
                if (c.id == token if token else c.labels.get(_LABEL_COMPONENT) == "run")
            ),
            None,
        )
        if not container:
            msg = f"Executable container of workload {name} not found"
            raise OperationError(msg)

        attach = not detach
        if not command:
            attach = True
            command = ["/bin/sh"]

        try:
            result = container.exec_run(
                socket=attach,
                stdin=attach,
                tty=attach,
                cmd=[*command, *(args or [])],
            )
        except docker.errors.APIError as e:
            msg = f"Failed to exec command in container {container.name} of workload {name}"
            raise OperationError(msg) from e
        else:
            return WorkloadExecResult(
                exit_code=result.exit_code,
                output=result.output,
            )


def _has_restart_policy(
    container: docker.models.containers.Container,
) -> bool:
    return (
        container.attrs["HostConfig"].get("RestartPolicy", {}).get("Name", "no") != "no"
    )
