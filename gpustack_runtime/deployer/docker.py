from __future__ import annotations

import logging
import socket
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

from .. import envs
from .__types__ import (
    Container,
    ContainerCheck,
    ContainerMountModeEnum,
    ContainerProfileEnum,
    ContainerRestartPolicyEnum,
    Deployer,
    OperationError,
    UnsupportedError,
    WorkloadExecStream,
    WorkloadName,
    WorkloadNamespace,
    WorkloadOperationToken,
    WorkloadPlan,
    WorkloadStatus,
    WorkloadStatusOperation,
    WorkloadStatusStateEnum,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

logger = logging.getLogger(__name__)

_LABEL_WORKLOAD = f"{envs.GPUSTACK_RUNTIME_DEPLOY_LABEL_PREFIX}/workload"
_LABEL_COMPONENT = f"{envs.GPUSTACK_RUNTIME_DEPLOY_LABEL_PREFIX}/component"
_LABEL_COMPONENT_NAME = f"{_LABEL_COMPONENT}-name"
_LABEL_COMPONENT_INDEX = f"{_LABEL_COMPONENT}-index"
_LABEL_COMPONENT_HEAL_PREFIX = f"{_LABEL_COMPONENT}-heal"


@dataclass_json
@dataclass
class DockerWorkloadPlan(WorkloadPlan):
    """
    Workload plan implementation for Docker containers.

    Attributes:
        pause_image (str):
            Image used for the pause container.
        unhealthy_restart_image (str):
            Image used for unhealthy restart container.
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
        namespace (str | None):
            Namespace of the workload.
        name (str):
            Name of the workload,
            it should be unique in the deployer.
        labels (dict[str, str] | None):
            Labels to attach to the workload.
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


@dataclass_json
@dataclass
class DockerWorkloadStatus(WorkloadStatus):
    """
    Workload status implementation for Docker containers.
    """

    _d_containers: list[docker.models.containers.Container] | None = field(
        default=None,
        repr=False,
        metadata={
            "dataclasses_json": {
                "exclude": lambda _: True,
                "encoder": lambda _: None,
                "decoder": lambda _: [],
            },
        },
    )
    """
    List of Docker containers in the workload,
    internal use only.
    """

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
            health = cr.attrs["State"].get("Health", {})
            if health and health.get("Status") != "healthy":
                return WorkloadStatusStateEnum.UNHEALTHY

        return WorkloadStatusStateEnum.RUNNING

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
            if not k.startswith(f"{envs.GPUSTACK_RUNTIME_DEPLOY_LABEL_PREFIX}/")
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


class DockerDeployer(Deployer):
    """
    Deployer implementation for Docker containers.
    """

    _client: docker.DockerClient | None = None
    """
    Client for interacting with the Docker daemon.
    """
    _mutate_create_options: Callable[[dict[str, Any]], dict[str, Any]] | None = None
    """
    Function to handle mirrored deployment, internal use only.
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
        if envs.GPUSTACK_RUNTIME_DEPLOY.lower() not in ("auto", "docker"):
            return supported

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
        """
        Return a Docker client.

        Returns:
            A Docker client if available, None otherwise.

        """
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
        """
        Decorator to check if Docker is supported in the current environment.

        """

        def wrapper(self, *args, **kwargs):
            if not self.is_supported():
                msg = "Docker is not supported in the current environment."
                raise UnsupportedError(msg)
            return func(self, *args, **kwargs)

        return wrapper

    @staticmethod
    def _create_ephemeral_files(
        workload: DockerWorkloadPlan,
    ) -> dict[tuple[int, str], str]:
        """
        Create ephemeral files as local file for the workload.

        Returns:
            A mapping from (container index, configured path) to actual filename.

        Raises:
            OperationError:
                If the ephemeral files fail to create.

        """
        # Map (container index, configured path) to actual filename.
        ephemeral_filename_mapping: dict[tuple[int, str], str] = {}
        ephemeral_files: list[tuple[str, str, int]] = []
        for ci, c in enumerate(workload.containers):
            for fi, f in enumerate(c.files or []):
                if f.content is not None:
                    fn = f"{workload.name}-{ci}-{fi}"
                    ephemeral_filename_mapping[(ci, f.path)] = fn
                    ephemeral_files.append((fn, f.content, f.mode))
        if not ephemeral_filename_mapping:
            return ephemeral_filename_mapping

        # Create ephemeral files directory if not exists.
        try:
            for fn, fc, fm in ephemeral_files:
                fp = envs.GPUSTACK_RUNTIME_DOCKER_EPHEMERAL_FILES_DIR.joinpath(fn)
                with fp.open("w", encoding="utf-8") as f:
                    f.write(fc)
                fp.chmod(fm)
                logger.debug("Created local file %s with mode %s", fp, oct(fm))
        except OSError as e:
            msg = "Failed to create ephemeral files"
            raise OperationError(msg) from e

        return ephemeral_filename_mapping

    def _create_ephemeral_volumes(self, workload: DockerWorkloadPlan) -> dict[str, str]:
        """
        Create ephemeral volumes for the workload.

        Returns:
            A mapping from configured volume name to actual volume name.

        Raises:
            OperationError:
                If the ephemeral volumes fail to create.

        """
        # Map configured volume name to actual volume name.
        ephemeral_volume_name_mapping: dict[str, str] = {
            m.volume: f"{workload.name}-{m.volume}"
            for c in workload.containers
            for m in c.mounts or []
            if m.volume
        }
        if not ephemeral_volume_name_mapping:
            return ephemeral_volume_name_mapping

        # Create volumes.
        try:
            for n in ephemeral_volume_name_mapping.values():
                self._client.volumes.create(
                    name=n,
                    driver="local",
                    labels=workload.labels,
                )
                logger.debug("Created volume %s", n)
        except docker.errors.APIError as e:
            msg = "Failed to create ephemeral volumes"
            raise OperationError(msg) from e

        return ephemeral_volume_name_mapping

    def _pull_image(self, image: str) -> docker.models.images.Image:
        """
        Pull image if not exists locally.

        Returns:
            The image object.

        Raises:
            OperationError:
                If the image fails to pull.

        """
        try:
            return self._client.images.get(image)
        except docker.errors.ImageNotFound:
            logger.info(f"Pulling image {image}")
            try:
                # TODO(thxCode): display pull progress
                return self._client.images.pull(image)
            except docker.errors.APIError as e:
                msg = f"Failed to pull image {image}"
                raise OperationError(msg) from e
        except docker.errors.APIError as e:
            msg = f"Failed to get image {image}"
            raise OperationError(msg) from e

    def _create_pause_container(
        self,
        workload: DockerWorkloadPlan,
    ) -> docker.models.containers.Container:
        """
        Create the pause container for the workload.

        Returns:
            The pause container object.

        Raises:
            OperationError:
                If the pause container fails to create.

        """
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

        create_options: dict[str, Any] = {
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
            create_options["network_mode"] = "host"
        else:
            create_options["hostname"] = workload.name
            port_mapping: dict[str, int] = {
                # <internal port>/<protocol>: <external port>
                f"{p.internal}/{p.protocol.lower()}": p.external or p.internal
                for c in workload.containers
                if c.profile == ContainerProfileEnum.RUN
                for p in c.ports or []
            }
            if port_mapping:
                create_options["ports"] = port_mapping

        if workload.host_ipc:
            create_options["ipc_mode"] = "host"

        try:
            d_container = self._client.containers.create(
                image=self._pull_image(workload.pause_image),
                detach=True,
                **create_options,
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
        """
        Create the unhealthy restart container for the workload if needed.

        Returns:
            The unhealthy restart container object if created, None otherwise.

        Raises:
            OperationError:
                If the unhealthy restart container fails to create.

        """
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

        create_options: dict[str, Any] = {
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
                **create_options,
            )
        except docker.errors.APIError as e:
            msg = f"Failed to create container {container_name}"
            raise OperationError(msg) from e
        else:
            return d_container

    @staticmethod
    def _append_container_mounts(
        create_options: dict[str, Any],
        c: Container,
        ci: int,
        ephemeral_filename_mapping: dict[tuple[int, str] : str],
        ephemeral_volume_name_mapping: dict[str, str],
    ):
        """
        Append (bind) mounts into the create_options.
        """
        mount_binding: list[docker.types.Mount] = []

        if files := c.files:
            for f in files:
                binding = docker.types.Mount(
                    type="bind",
                    source="",
                    target="",
                )

                if f.content is not None:
                    # Ephemeral file, use from local ephemeral files directory.
                    if (ci, f.path) not in ephemeral_filename_mapping:
                        continue
                    fn = ephemeral_filename_mapping[(ci, f.path)]
                    path = str(
                        envs.GPUSTACK_RUNTIME_DOCKER_EPHEMERAL_FILES_DIR.joinpath(fn),
                    )
                    binding["source"] = path
                    binding["target"] = f"/{f.path.lstrip('/')}"
                elif f.path:
                    # Host file, bind directly.
                    binding["source"] = f.path
                    binding["target"] = f.path
                else:
                    continue

                if f.mode < 0o600:
                    binding["read_only"] = True

                mount_binding.append(binding)

        if mounts := c.mounts:
            for m in mounts:
                binding = docker.types.Mount(
                    type="volume",
                    source="",
                    target="",
                )

                if m.volume:
                    # Ephemeral volume, use the created volume.
                    binding["source"] = ephemeral_volume_name_mapping.get(
                        m.volume,
                        m.volume,
                    )
                    binding["target"] = f"/{m.path.lstrip('/')}"
                    # TODO(thxCode): support subpath.
                elif m.path:
                    # Host path, bind directly.
                    binding["type"] = "bind"
                    binding["source"] = m.path
                    binding["target"] = m.path
                else:
                    continue

                if m.mode == ContainerMountModeEnum.ROX:
                    binding["read_only"] = True

                mount_binding.append(binding)

        if mount_binding:
            create_options["mounts"] = mount_binding

    @staticmethod
    def _parameterize_healthcheck(
        chk: ContainerCheck,
    ) -> dict[str, Any]:
        """
        Parameterize health check for a container.

        Returns:
            A dictionary representing the health check configuration.

        Raises:
            ValueError:
                If the health check configuration is invalid.

        """
        healthcheck: dict[str, Any] = {
            "start_period": chk.delay * 1000000000,
            "interval": chk.interval * 1000000000,
            "timeout": chk.timeout * 1000000000,
            "retries": chk.retries,
        }

        configured = False
        for attr_k in ["execution", "tcp", "http", "https"]:
            attr_v = getattr(chk, attr_k, None)
            if not attr_v:
                continue
            configured = True
            match attr_k:
                case "execution":
                    if attr_v.command:
                        healthcheck["test"] = [
                            "CMD",
                            *attr_v.command,
                        ]
                case "tcp":
                    host = attr_v.host or "127.0.0.1"
                    port = attr_v.port or 80
                    healthcheck["test"] = [
                        "CMD",
                        "sh",
                        "-c",
                        f"if [ `command -v netstat` ]; then netstat -an | grep -w {port} >/dev/null || exit 1; elif [ `command -v nc` ]; then nc -z {host}:{port} >/dev/null || exit 1; else cat /etc/services | grep -w {port}/tcp >/dev/null || exit 1; fi",
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
                    url = f"{attr_k}://{attr_v.host or '127.0.0.1'}:{attr_v.port or 80}{attr_v.path or '/'}"
                    healthcheck["test"] = [
                        "CMD",
                        "sh",
                        "-c",
                        f"if [ `command -v curl` ]; then curl {curl_options} {url}; else wget {wget_options} {url}; fi",
                    ]
            break
        if not configured:
            msg = "Invalid health check configuration"
            raise ValueError(msg)

        return healthcheck

    def _create_containers(
        self,
        workload: DockerWorkloadPlan,
        ephemeral_filename_mapping: dict[tuple[int, str] : str],
        ephemeral_volume_name_mapping: dict[str, str],
    ) -> (
        list[docker.models.containers.Container],
        list[docker.models.containers.Container],
    ):
        """
        Create init and run containers for the workload.


        Returns:
            A tuple of two lists: (init containers, run containers).

        Raises:
            OperationError:
                If the containers fail to create.

        """
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

            create_options: dict[str, Any] = {
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

            if workload.pid_shared:
                create_options["pid_mode"] = pause_container_namespace

            if workload.shm_size:
                create_options["shm_size"] = workload.shm_size

            # Parameterize restart policy.
            match c.restart_policy:
                case ContainerRestartPolicyEnum.ON_FAILURE:
                    create_options["restart_policy"] = {
                        "Name": "on-failure",
                    }
                case ContainerRestartPolicyEnum.ALWAYS:
                    create_options["restart_policy"] = {
                        "Name": "always",
                    }

            # Parameterize execution.
            if c.execution:
                create_options["working_dir"] = c.execution.working_dir
                create_options["entrypoint"] = c.execution.command
                create_options["command"] = c.execution.args
                run_as_user = c.execution.run_as_user or workload.run_as_user
                run_as_group = c.execution.run_as_group or workload.run_as_group
                if run_as_user is not None:
                    create_options["user"] = run_as_user
                    if run_as_group is not None:
                        create_options["user"] = f"{run_as_user}:{run_as_group}"
                if run_as_group is not None:
                    create_options["group_add"] = [run_as_group]
                    if workload.fs_group is not None:
                        create_options["group_add"] = [run_as_group, workload.fs_group]
                elif workload.fs_group is not None:
                    create_options["group_add"] = [workload.fs_group]
                create_options["sysctls"] = (
                    {sysctl.name: sysctl.value for sysctl in workload.sysctls or []}
                    if workload.sysctls
                    else None
                )
                create_options["read_only"] = c.execution.readonly_rootfs
                create_options["privileged"] = c.execution.privileged
                if cap := c.execution.capabilities:
                    create_options["cap_add"] = cap.add
                    create_options["cap_drop"] = cap.drop

            # Parameterize environment variables.
            create_options["environment"] = {e.name: e.value for e in c.envs or []}

            # Parameterize resources.
            if c.resources:
                r_k_rem = workload.resource_key_runtime_env_mapping or {}
                r_k_bem = workload.resource_key_backend_env_mapping or {}
                for r_k, r_v in c.resources.items():
                    match r_k:
                        case "cpu":
                            if isinstance(r_v, int | float):
                                create_options["cpu_shares"] = ceil(r_v * 1024)
                            elif isinstance(r_v, str) and r_v.isdigit():
                                create_options["cpu_shares"] = ceil(float(r_v) * 1024)
                        case "memory":
                            if isinstance(r_v, int):
                                create_options["mem_limit"] = r_v
                                create_options["mem_reservation"] = r_v
                                create_options["memswap_limit"] = r_v
                            elif isinstance(r_v, str):
                                v = r_v.lower().removesuffix("i")
                                create_options["mem_limit"] = v
                                create_options["mem_reservation"] = v
                                create_options["memswap_limit"] = v
                        case _:
                            if r_k in r_k_rem:
                                # Set env if resource key is mapped.
                                re = r_k_rem[r_k]
                            elif (
                                r_k == envs.GPUSTACK_RUNTIME_DEPLOY_AUTOMAP_RESOURCE_KEY
                            ):
                                # Set env if auto-mapping key is matched.
                                re = self._runtime_visible_devices_env_name
                            else:
                                continue

                            if r_k in r_k_bem:
                                # Set env if resource key is mapped.
                                bes = r_k_bem[r_k]
                            else:
                                # Otherwise, use the default backend env names.
                                bes = self._backend_visible_devices_env_names

                            # Configure device access environment variable.
                            if r_v == "all" and bes:
                                # Configure privileged if requested all devices.
                                create_options["privileged"] = True
                                # Then, set container backend visible devices env to "0",
                                # so that the container backend (e.g., NVIDIA Container Toolkit) can handle it,
                                # and mount corresponding libs if needed.
                                create_options["environment"][re] = "0"
                            else:
                                # Set env to the allocated device IDs.
                                create_options["environment"][re] = r_v

                            # Configure runtime device access environment variables.
                            if r_v != "all" and create_options.get("privileged", True):
                                for be in bes:
                                    create_options["environment"][be] = r_v

            # Parameterize mounts.
            self._append_container_mounts(
                create_options,
                c,
                ci,
                ephemeral_filename_mapping,
                ephemeral_volume_name_mapping,
            )

            # Parameterize health checks.
            # Since Docker only support one complete check,
            # we always pick the first check as target.
            if c.profile == ContainerProfileEnum.RUN and c.checks:
                # If the first check is teardown-enabled,
                # enable auto-heal for the container.
                if c.checks[0].teardown:
                    create_options["labels"][
                        f"{_LABEL_COMPONENT_HEAL_PREFIX}-{workload.name}"
                    ] = "true"

                create_options["healthcheck"] = self._parameterize_healthcheck(
                    c.checks[0],
                )

            # Create the container.
            try:
                if c.profile == ContainerProfileEnum.RUN:
                    create_options = self._mutate_create_options(create_options)
                d_container = self._client.containers.create(
                    image=self._pull_image(c.image),
                    detach=detach,
                    **create_options,
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
        """
        Start or restart the container(s) based on their current status.

        Args:
            container:
                A Docker container or a list of Docker containers to start or restart.

        Raises:
            docker.errors.APIError:
                If the container fails to start or restart.

        """
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
        super().__init__()
        self._client = self._get_client()

    def _prepare_create(self):
        """
        Prepare for creation.

        """
        # Prepare mirrored deployment if enabled.
        if self._mutate_create_options:
            return
        self._mutate_create_options = lambda o: o
        if not envs.GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT:
            logger.debug("Skipping mirrored deployment")
            return

        # Retrieve self-container info.
        ## - Get Container name, default to hostname if not set.
        self_container_id = envs.GPUSTACK_RUNTIME_DEPLOY_MIRRORED_NAME
        if not self_container_id:
            self_container_id = socket.gethostname()
            logger.warning(
                "Mirrored deployment enabled but no Container name set, using hostname(%s) instead",
                self_container_id,
            )
        try:
            self_container = self._client.containers.get(self_container_id)
            self_image = self_container.image
        except docker.errors.APIError as e:
            msg = f"Mirrored deployment enabled but failed to get self Container {self_container_id}"
            raise OperationError(msg) from e

        # Process mirrored deployment options.
        ## - Container runtime
        mirrored_runtime: str = self_container.attrs["HostConfig"].get("Runtime", "")
        ## - Container customized envs
        self_container_envs: dict[str, str] = dict(
            item.split("=", 1) for item in self_container.attrs["Config"].get("Env", [])
        )
        self_image_envs: dict[str, str] = dict(
            item.split("=", 1) for item in self_image.attrs["Config"].get("Env", [])
        )
        mirrored_envs: dict[str, str] = {
            # Filter out gpustack-internal envs and same-as-image envs.
            k: v
            for k, v in self_container_envs.items()
            if (
                not k.startswith("GPUSTACK_")
                and (k not in self_image_envs or v != self_image_envs[k])
            )
        }
        if igs := envs.GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_ENVIRONMENTS:
            mirrored_envs = {
                # Filter out ignored envs.
                k: v
                for k, v in mirrored_envs.items()
                if k not in igs
            }
        ## - Container customized mounts
        mirrored_mounts: list[dict[str, Any]] = [
            # Always filter out Docker Socket mount.
            m
            for m in (self_container.attrs["Mounts"] or [])
            if m.get("Destination") != "/var/run/docker.sock"
        ]
        if igs := envs.GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_VOLUMES:
            mirrored_mounts = [
                # Filter out ignored volume mounts.
                m
                for m in mirrored_mounts
                if m.get("Destination") not in igs
            ]
        ## - Container customized devices
        mirrored_devices: list[dict[str, Any]] = (
            self_container.attrs["HostConfig"].get("Devices", []) or []
        )
        if igs := envs.GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_VOLUMES:
            mirrored_devices = [
                # Filter out ignored device mounts.
                d
                for d in mirrored_devices
                if d.get("PathInContainer") not in igs
            ]
        ## - Container customized device requests
        mirrored_device_requests: list[dict[str, Any]] = (
            self_container.attrs["HostConfig"].get("DeviceRequests", []) or []
        )

        # Construct mutation function.
        def mutate_create_options(create_options: dict[str, Any]) -> dict[str, Any]:
            if mirrored_runtime and "runtime" not in create_options:
                create_options["runtime"] = mirrored_runtime

            if mirrored_envs:
                c_envs: dict[str, str] = create_options.get("environment", {})
                for k, v in mirrored_envs.items():
                    if k not in c_envs:
                        c_envs[k] = v
                create_options["environment"] = c_envs

            if mirrored_mounts:
                c_mounts: list[dict[str, Any]] = create_options.get("mounts", [])
                c_mounts_paths = {m.get("Target") for m in c_mounts}
                for m in mirrored_mounts:
                    if m.get("Destination") in c_mounts_paths:
                        continue
                    type_ = m.get("Type", "volume")
                    source = m.get("Source")
                    if type_ == "volume":
                        source = m.get("Name")
                    target = m.get("Destination")
                    read_only = (
                        m.get("Mode", "") in ("ro", "readonly")
                        or m.get("RW", True) is False
                    )
                    propagation = (
                        m.get("Propagation") if m.get("Propagation", "") else None
                    )
                    c_mounts.append(
                        docker.types.Mount(
                            type=type_,
                            source=source,
                            target=target,
                            read_only=read_only,
                            propagation=propagation,
                        ),
                    )
                    c_mounts_paths.add(target)
                create_options["mounts"] = c_mounts

            if mirrored_devices:
                c_devices: list[dict[str, Any]] = []
                for c_device in create_options.get("devices", []):
                    sp = c_device.split(":")
                    c_device.append(
                        {
                            "PathOnHost": sp[0],
                            "PathInContainer": sp[1] if len(sp) > 1 else sp[0],
                            "CgroupPermissions": sp[2] if len(sp) > 2 else "rwm",
                        },
                    )
                c_devices_paths = {d.get("PathInContainer") for d in c_devices}
                for d in mirrored_devices:
                    if d.get("PathInContainer") in c_devices_paths:
                        continue
                    c_devices.append(d)
                    c_devices_paths.add(d.get("PathInContainer"))
                create_options["devices"] = [
                    f"{d['PathOnHost']}:{d['PathInContainer']}:{d['CgroupPermissions']}"
                    for d in c_devices
                ]

            if mirrored_device_requests:
                c_device_requests: list[dict[str, Any]] = create_options.get(
                    "device_requests",
                    [],
                )
                c_device_requests_ids = {
                    f"{r.get('Driver')}:{did}"
                    for r in c_device_requests
                    for did in r.get("DeviceIDs", [])
                }
                for r in mirrored_device_requests:
                    dri: str = r.get("Driver")
                    dids: list[str] = []
                    for did in r.get("DeviceIDs", []):
                        if f"{dri}:{did}" in c_device_requests_ids:
                            continue
                        dids.append(did)
                    if not dids:
                        continue
                    c_device_requests.append(
                        docker.types.DeviceRequest(
                            driver=dri,
                            count=r.get("Count"),
                            device_ids=dids,
                            capabilities=r.get("Capabilities", None),
                            options=r.get("Options", None),
                        ),
                    )
                create_options["device_requests"] = c_device_requests

            return create_options

        self._mutate_create_options = mutate_create_options

    @_supported
    def create(self, workload: WorkloadPlan):
        """
        Deploy a Docker workload.

        Args:
            workload:
                The workload to deploy.

        Raises:
            TypeError:
                If the Docker workload type is invalid.
            ValueError:
                If the Docker workload fails to validate.
            UnsupportedError:
                If Docker is not supported in the current environment.
            OperationError:
                If the Docker workload fails to deploy.

        """
        if not isinstance(workload, DockerWorkloadPlan | WorkloadPlan):
            msg = f"Invalid workload type: {type(workload)}"
            raise TypeError(msg)
        if isinstance(workload, WorkloadPlan):
            workload = DockerWorkloadPlan(**workload.__dict__)

        self._prepare_create()

        workload.validate_and_default()
        workload.labels[_LABEL_WORKLOAD] = workload.name
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Creating workload:\n%s", workload.to_yaml())

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
            msg = f"Failed to create workload {workload.name}"
            raise OperationError(msg) from e

    @_supported
    def get(
        self,
        name: WorkloadName,
        namespace: WorkloadNamespace | None = None,  # noqa: ARG002
    ) -> WorkloadStatus | None:
        """
        Get the status of a Docker workload.

        Args:
            name:
                The name of the workload.
            namespace:
                The namespace of the workload.

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
    def delete(
        self,
        name: WorkloadName,
        namespace: WorkloadNamespace | None = None,
    ) -> WorkloadStatus | None:
        """
        Delete a Docker workload.

        Args:
            name:
                The name of the workload.
            namespace:
                The namespace of the workload.

        Return:
            The status if found, None otherwise.

        Raises:
            UnsupportedError:
                If Docker is not supported in the current environment.
            OperationError:
                If the Docker workload fails to delete.

        """
        # Check if the workload exists.
        workload = self.get(name=name, namespace=namespace)
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
    def list(
        self,
        namespace: WorkloadNamespace | None = None,  # noqa: ARG002
        labels: dict[str, str] | None = None,
    ) -> list[WorkloadStatus]:
        """
        List all Docker workloads.

        Args:
            namespace:
                The namespace of the workloads.
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

        # Group containers by workload name,
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
        namespace: WorkloadNamespace | None = None,
        token: WorkloadOperationToken | None = None,
        timestamps: bool = False,
        tail: int | None = None,
        since: int | None = None,
        follow: bool = False,
    ) -> Generator[bytes | str, None, None] | bytes | str:
        """
        Get logs of a Docker workload or a specific container.

        Args:
            name:
                The name of the workload.
            namespace:
                The namespace of the workload.
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
            The logs as a byte string, a string or a generator yielding byte strings or strings if follow is True.

        Raises:
            UnsupportedError:
                If Docker is not supported in the current environment.
            OperationError:
                If the Docker workload fails to fetch logs.

        """
        workload = self.get(name=name, namespace=namespace)
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
            if token:
                msg += f" with token {token}"
            raise OperationError(msg)

        logs_options = {
            "timestamps": timestamps,
            "tail": tail,
            "since": since,
            "follow": follow,
        }

        try:
            output = container.logs(
                stream=follow,
                **logs_options,
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
        namespace: WorkloadNamespace | None = None,
        token: WorkloadOperationToken | None = None,
        detach: bool = True,
        command: list[str] | None = None,
        args: list[str] | None = None,
    ) -> WorkloadExecStream | bytes | str:
        """
        Execute a command in a Docker workload or a specific container.

        Args:
            name:
                The name of the workload.
            namespace:
                The namespace of the workload.
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
            If detach is False, return a WorkloadExecStream.
            otherwise, return the output of the command as a byte string or string.

        Raises:
            UnsupportedError:
                If Docker is not supported in the current environment.
            OperationError:
                If the Docker workload fails to execute the command.

        """
        workload = self.get(name=name, namespace=namespace)
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
            if token:
                msg += f" with token {token}"
            raise OperationError(msg)

        attach = not detach or not command
        exec_options = {
            "stdout": True,
            "stderr": True,
            "stdin": attach,
            "socket": attach,
            "tty": attach,
            "cmd": [*command, *(args or [])] if command else ["/bin/sh"],
        }

        try:
            result = container.exec_run(
                detach=False,
                **exec_options,
            )
        except docker.errors.APIError as e:
            msg = f"Failed to exec command in container {container.name} of workload {name}"
            raise OperationError(msg) from e
        else:
            if not attach:
                return result.output
            return DockerWorkloadExecStream(result.output)


def _has_restart_policy(
    container: docker.models.containers.Container,
) -> bool:
    return (
        container.attrs["HostConfig"].get("RestartPolicy", {}).get("Name", "no") != "no"
    )


class DockerWorkloadExecStream(WorkloadExecStream):
    """
    A WorkloadExecStream implementation for Docker exec socket streams.
    """

    _sock: socket.SocketIO | None = None

    def __init__(self, sock: socket.SocketIO):
        super().__init__()
        self._sock = sock

    @property
    def closed(self) -> bool:
        return not (self._sock and not self._sock.closed)

    def fileno(self) -> int:
        return self._sock.fileno()

    def read(self, size=-1) -> bytes | None:
        if self.closed:
            return None
        return self._sock.read(size)

    def write(self, data: bytes) -> int:
        if self.closed:
            return 0
        return self._sock.write(data)

    def close(self):
        if self.closed:
            return
        self._sock.close()
