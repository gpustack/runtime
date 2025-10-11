from __future__ import annotations

import contextlib
import hashlib
import logging
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache
from typing import TYPE_CHECKING

import kubernetes
import kubernetes.stream.ws_client
import urllib3.connection
from dataclasses_json import dataclass_json

from .. import envs
from . import ContainerCheck, ContainerMountModeEnum, OperationError
from .__types__ import (
    Container,
    ContainerPort,
    ContainerProfileEnum,
    Deployer,
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
    from collections.abc import Generator


logger = logging.getLogger(__name__)

_LABEL_WORKLOAD = f"{envs.GPUSTACK_RUNTIME_DEPLOY_LABEL_PREFIX}/workload"


class KubernetesWorkloadServiceType(str, Enum):
    """
    Types for Kubernetes Service.
    """

    CLUSTER_IP = "ClusterIP"
    """
    ClusterIP: Exposes the service on a cluster-internal IP.
    """
    NODE_PORT = "NodePort"
    """
    NodePort: Exposes the service on each Node's IP at a static port.
    """
    LOAD_BALANCER = "LoadBalancer"
    """
    LoadBalancer: Exposes the service externally using a cloud provider's load balancer.
    """


@dataclass_json
@dataclass
class KubernetesWorkloadPlan(WorkloadPlan):
    """
    Workload plan implementation for Kubernetes Deployment.

    Attributes:
        domain_suffix (str):
            Domain suffix for the cluster. Default is "cluster.local".
        service_type (KubernetesWorkloadServiceType):
            Service type for the workload. Default is CLUSTER_IP.
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

    domain_suffix: str = envs.GPUSTACK_RUNTIME_KUBERNETES_DOMAIN_SUFFIX
    """
    Domain suffix for the cluster.
    """
    service_type: KubernetesWorkloadServiceType = field(
        default_factory=lambda: envs.GPUSTACK_RUNTIME_KUBERNETES_SERVICE_TYPE,
    )
    """
    Service type for the workload.
    """


@dataclass_json
@dataclass
class KubernetesWorkloadStatus(WorkloadStatus):
    """
    Workload status implementation for Kubernetes Deployment.
    """

    _k_pod: kubernetes.client.V1Pod | None = field(
        default=None,
        repr=False,
        metadata={
            "dataclasses_json": {
                "exclude": lambda _: True,
                "encoder": lambda _: None,
                "decoder": lambda _: None,
            },
        },
    )
    """
    Pod object from Kubernetes API,
    internal use only.
    """

    @staticmethod
    def parse_state(
        k_pod: kubernetes.client.V1Pod,
    ) -> WorkloadStatusStateEnum:
        """
        Parse the state of the workload from the Kubernetes Pod object.

        Args:
            k_pod:
                Pod object from Kubernetes API.

        Returns:
            The state of the workload.

        """
        match k_pod.status.phase:
            case "Pending":
                return WorkloadStatusStateEnum.PENDING
            case "Succeeded":
                return WorkloadStatusStateEnum.INACTIVE
            case "Failed":
                return WorkloadStatusStateEnum.FAILED
            case "Unknown":
                return WorkloadStatusStateEnum.UNKNOWN
            case "Running":
                if not k_pod.status.container_statuses:
                    return WorkloadStatusStateEnum.INITIALIZING
                for cs in k_pod.status.init_container_statuses or []:
                    if not cs.ready:
                        return WorkloadStatusStateEnum.INITIALIZING
                for cs in k_pod.status.container_statuses:
                    if not cs.ready:
                        return WorkloadStatusStateEnum.INITIALIZING

        return WorkloadStatusStateEnum.RUNNING

    def __init__(
        self,
        name: WorkloadName,
        k_pod: kubernetes.client.V1Pod,
        **kwargs,
    ):
        created_at = k_pod.metadata.creation_timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        namespace = (
            k_pod.metadata.namespace
            if k_pod.metadata.namespace != envs.GPUSTACK_RUNTIME_KUBERNETES_NAMESPACE
            else None
        )
        labels = {
            k: v
            for k, v in (k_pod.metadata.labels or {}).items()
            if not k.startswith("runtime.gpustack.ai/")
        }

        super().__init__(
            name=name,
            created_at=created_at,
            namespace=namespace,
            labels=labels,
            **kwargs,
        )

        self._k_pod = k_pod

        for c in k_pod.spec.init_containers or []:
            op = WorkloadStatusOperation(
                name=c.name,
                token=c.name,
            )
            if c.restart_policy != "Never":
                self.executable.append(op)
            self.loggable.append(op)

        for c in k_pod.spec.containers:
            op = WorkloadStatusOperation(
                name=c.name,
                token=c.name,
            )
            self.executable.append(op)
            self.loggable.append(op)

        self.state = self.parse_state(k_pod)


class KubernetesDeployer(Deployer):
    """
    Deployer implementation for Kubernetes.
    """

    _client: kubernetes.client.ApiClient | None = None
    """
    Client for interacting with the Kubernetes API.
    """
    _node_name: str | None = None
    """
    Name of the node where the deployer is running.
    """

    @staticmethod
    @lru_cache
    def is_supported() -> bool:
        """
        Check if the deployer is supported in the current environment.

        Returns:
            True if the deployer is supported, False otherwise.

        """
        supported = False
        if envs.GPUSTACK_RUNTIME_DEPLOY.lower() not in ("auto", "kubernetes"):
            return supported

        client = KubernetesDeployer._get_client()
        if client:
            try:
                version_api = kubernetes.client.VersionApi(client)
                version_info = version_api.get_code()
                supported = version_info is not None
            except kubernetes.client.exceptions.ApiException:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.exception("Failed to get Kubernetes version")
            except urllib3.exceptions.MaxRetryError:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.exception("Failed to connect to Kubernetes API server")

        return supported

    @staticmethod
    def _get_client() -> kubernetes.client.ApiClient | None:
        """
        Return a Kubernetes API client.

        Returns:
            A Kubernetes API client if the configuration is valid, None otherwise.

        """
        client = None

        try:
            kubernetes.config.load_kube_config()
            client = kubernetes.client.ApiClient()
            client.user_agent = "gpustack/runtime"
        except kubernetes.config.config_exception.ConfigException:
            if logger.isEnabledFor(logging.DEBUG):
                logger.exception("Failed to get Kubernetes client")

        return client

    @staticmethod
    def _supported(func):
        """
        Decorator to check if Kubernetes is supported in the current environment.
        """

        def wrapper(self, *args, **kwargs):
            if not self.is_supported():
                msg = "Kubernetes is not supported in the current environment."
                raise UnsupportedError(msg)
            return func(self, *args, **kwargs)

        return wrapper

    def _create_ephemeral_configmaps(
        self,
        workload: KubernetesWorkloadPlan,
    ) -> dict[tuple[int, str], str]:
        """
        Create ephemeral files as ConfigMaps in Kubernetes.

        Returns:
            A mapping from (container index, configured path) to actual ConfigMap name.

        Raises:
            OperationError:
                If creating the ConfigMaps fails.

        """
        ephemeral_filename_mapping: dict[tuple[int, str], str] = {}
        ephemeral_files: list[tuple[str, str, str]] = []
        for ci, c in enumerate(workload.containers):
            for fi, f in enumerate(c.files or []):
                if f.content is not None:
                    fn = f"{workload.name}-{ci}-{fi}"
                    ephemeral_filename_mapping[(ci, f.path)] = fn
                    ephemeral_files.append((fn, f.path, f.content))
        if not ephemeral_filename_mapping:
            return ephemeral_filename_mapping

        core_api = kubernetes.client.CoreV1Api(self._client)
        try:
            for fn, fp, content in ephemeral_files:
                config_map = kubernetes.client.V1ConfigMap(
                    metadata=kubernetes.client.V1ObjectMeta(
                        name=fn,
                        namespace=workload.namespace,
                        labels=workload.labels,
                    ),
                    data={fp: content},
                )

                actual_config_map = None
                with contextlib.suppress(kubernetes.client.exceptions.ApiException):
                    actual_config_map = core_api.read_namespaced_config_map(
                        name=fn,
                        namespace=workload.namespace,
                    )
                if not actual_config_map:
                    core_api.create_namespaced_config_map(
                        namespace=workload.namespace,
                        body=config_map,
                    )
                    logger.debug(
                        "Created configmap %s in namespace %s",
                        fn,
                        workload.namespace,
                    )
                elif not equal_config_maps(actual_config_map, config_map):
                    core_api.patch_namespaced_config_map(
                        name=fn,
                        namespace=workload.namespace,
                        body={
                            "data": config_map.data,
                        },
                    )
                    logger.debug(
                        "Updated configmap %s in namespace %s",
                        fn,
                        workload.namespace,
                    )
        except kubernetes.client.exceptions.ApiException as e:
            msg = f"Failed to create ephemeral files for workload {workload.name}"
            raise OperationError(msg) from e

        return ephemeral_filename_mapping

    @staticmethod
    def _append_pod_volumes(
        pod: kubernetes.client.V1Pod,
        workload: KubernetesWorkloadPlan,
        ephemeral_filename_mapping: dict[tuple[int, str], str],
    ):
        """
        Append volumes into the Pod.
        """
        file_volumes: dict[str, kubernetes.client.V1Volume] = {}
        mount_volumes: dict[str, kubernetes.client.V1Volume] = {}

        for ci, c in enumerate(workload.containers):
            for _fi, f in enumerate(c.files or []):
                if f.content is None and f.path:
                    # Host file, bind directly.
                    fp_hash = hash_string(f.path)
                    if fp_hash not in file_volumes:
                        file_volumes[fp_hash] = kubernetes.client.V1Volume(
                            name=f"f-{fp_hash}",
                            host_path=kubernetes.client.V1HostPathVolumeSource(
                                path=f.path,
                                type="File",
                            ),
                        )
                elif f.path:
                    # Ephemeral file, mount from ConfigMap.
                    fn = ephemeral_filename_mapping[(ci, f.path)]
                    if fn not in file_volumes:
                        file_volumes[fn] = kubernetes.client.V1Volume(
                            name=f"f-{fn}",
                            config_map=kubernetes.client.V1ConfigMapVolumeSource(
                                name=fn,
                                items=[
                                    kubernetes.client.V1KeyToPath(
                                        key=f.path,
                                        path=f.path.lstrip("/"),
                                        mode=f.mode,
                                    ),
                                ],
                            ),
                        )
            for m in c.mounts or []:
                if m.volume is None and m.path:
                    # Host directory, bind directly.
                    mp_hash = hash_string(m.path)
                    if mp_hash not in mount_volumes:
                        mount_volumes[mp_hash] = kubernetes.client.V1Volume(
                            name=f"m-{mp_hash}",
                            host_path=kubernetes.client.V1HostPathVolumeSource(
                                path=m.path,
                                type=(
                                    "DirectoryOrCreate"
                                    if m.mode != ContainerMountModeEnum.ROX
                                    else "Directory"
                                ),
                            ),
                        )
                elif m.path:
                    # Ephemeral volume, mount from emptyDir.
                    if m.volume not in mount_volumes:
                        mount_volumes[m.volume] = kubernetes.client.V1Volume(
                            name=f"m-{m.volume}",
                            empty_dir=kubernetes.client.V1EmptyDirVolumeSource(),
                        )

        if file_volumes or mount_volumes:
            pod.spec.volumes = pod.spec.volumes or []
            if file_volumes:
                pod.spec.volumes.extend(file_volumes.values())
            if mount_volumes:
                pod.spec.volumes.extend(mount_volumes.values())

    @staticmethod
    def _append_container_volume_mounts(
        container: kubernetes.client.V1Container,
        c: Container,
        ci: int,
        ephemeral_filename_mapping: dict[tuple[int, str], str],
    ):
        """
        Append volume mounts into the Container.
        """
        if files := c.files:
            container.volume_mounts = container.volume_mounts or []
            for _fi, f in enumerate(files):
                if f.content is None and f.path:
                    # Host file, mount directly.
                    fp_hash = hash_string(f.path)
                    container.volume_mounts.append(
                        kubernetes.client.V1VolumeMount(
                            name=f"f-{fp_hash}",
                            mount_path=f.path,
                            read_only=(True if f.mode < 0o600 else None),
                        ),
                    )
                elif f.path:
                    # Ephemeral file, mount from ConfigMap.
                    fn = ephemeral_filename_mapping[(ci, f.path)]
                    container.volume_mounts.append(
                        kubernetes.client.V1VolumeMount(
                            name=f"f-{fn}",
                            mount_path=f.path,
                            read_only=(True if f.mode < 0o600 else None),
                        ),
                    )

        if mounts := c.mounts:
            container.volume_mounts = container.volume_mounts or []
            for m in mounts:
                if m.volume is None and m.path:
                    # Host directory, mount directly.
                    mp_hash = hash_string(m.path)
                    container.volume_mounts.append(
                        kubernetes.client.V1VolumeMount(
                            name=f"m-{mp_hash}",
                            mount_path=m.path,
                            read_only=(
                                True if m.mode == ContainerMountModeEnum.ROX else None
                            ),
                        ),
                    )
                elif m.volume and m.path:
                    # Ephemeral volume, mount from emptyDir.
                    container.volume_mounts.append(
                        kubernetes.client.V1VolumeMount(
                            name=f"m-{m.volume}",
                            mount_path=m.path,
                            read_only=(
                                True if m.mode == ContainerMountModeEnum.ROX else None
                            ),
                        ),
                    )

    @staticmethod
    def _parameterize_probe(
        check: ContainerCheck,
    ) -> kubernetes.client.V1Probe:
        """
        Parameterize a ContainerCheck into a Kubernetes V1Probe.

        Returns:
            A V1Probe object representing the health check.

        Raises:
            ValueError:
                If the ContainerCheck is invalid.

        """
        probe = kubernetes.client.V1Probe(
            initial_delay_seconds=check.delay,
            period_seconds=check.interval,
            timeout_seconds=check.timeout,
            failure_threshold=check.retries,
            success_threshold=1,
        )

        configured = False
        for attr_k in ["execution", "tcp", "http", "https"]:
            attr_v = getattr(check, attr_k, None)
            if not attr_v:
                continue
            configured = True
            match attr_k:
                case "execution":
                    probe.exec = kubernetes.client.V1ExecAction(
                        command=attr_v.command,
                    )
                case "tcp":
                    probe.tcp_socket = kubernetes.client.V1TCPSocketAction(
                        port=attr_v.port,
                        host=attr_v.host,
                    )
                case "http" | "https":
                    probe.http_get = kubernetes.client.V1HTTPGetAction(
                        path=attr_v.path or "/",
                        port=attr_v.port or 80,
                        host=attr_v.host,
                        scheme="HTTPS" if attr_k == "https" else "HTTP",
                        http_headers=[
                            kubernetes.client.V1HTTPHeader(name=h.name, value=h.value)
                            for h in (attr_v.headers or [])
                        ],
                    )
            break
        if not configured:
            msg = "Invalid health check configuration"
            raise ValueError(msg)

        return probe

    def _create_service(
        self,
        workload: KubernetesWorkloadPlan,
    ):
        """
        Create Kubernetes Service for the workload.

        Returns:
            The created Service object, or None if no ports are defined.

        Raises:
            OperationError:
                If creating the Service fails.

        """
        ports: dict[str, ContainerPort] = {}
        for c in workload.containers:
            if c.profile == ContainerProfileEnum.RUN and c.ports:
                for p in c.ports:
                    pn = f"{p.protocol.lower()}-{p.external or p.internal}".lower()
                    if pn not in ports:
                        ports[pn] = p
        if not ports:
            return None

        service = kubernetes.client.V1Service(
            metadata=kubernetes.client.V1ObjectMeta(
                name=workload.name,
                namespace=workload.namespace,
                labels=workload.labels,
            ),
            spec=kubernetes.client.V1ServiceSpec(
                selector=workload.labels,
                type=workload.service_type,
                ports=[
                    kubernetes.client.V1ServicePort(
                        name=pn,
                        protocol=p.protocol.value,
                        port=p.external or p.internal,
                        target_port=p.internal,
                    )
                    for pn, p in ports.items()
                ],
            ),
        )

        core_api = kubernetes.client.CoreV1Api(self._client)
        try:
            actual_service = None
            with contextlib.suppress(kubernetes.client.exceptions.ApiException):
                actual_service = core_api.read_namespaced_service(
                    name=workload.name,
                    namespace=workload.namespace,
                )
            if not actual_service:
                service = core_api.create_namespaced_service(
                    namespace=workload.namespace,
                    body=service,
                )
                logger.debug(
                    "Created service %s in namespace %s",
                    workload.name,
                    workload.namespace,
                )
            elif not equal_services(actual_service, service):
                service = core_api.patch_namespaced_service(
                    name=workload.name,
                    namespace=workload.namespace,
                    body={
                        "spec": service.spec,
                    },
                )
                logger.debug(
                    "Updated service %s in namespace %s",
                    workload.name,
                    workload.namespace,
                )
        except kubernetes.client.exceptions.ApiException as e:
            msg = f"Failed to create service for workload {workload.name}"
            raise OperationError(msg) from e

        return service

    def _create_pod(
        self,
        workload: KubernetesWorkloadPlan,
        ephemeral_filename_mapping: dict[tuple[int, str], str],
    ) -> kubernetes.client.V1Pod:
        """
        Create Kubernetes Pod for the workload.

        Returns:
            The created Pod object.

        Raises:
            ValueError:
                If the workload is invalid.
            OperationError:
                If creating the Pod fails.

        """
        if not self._node_name:
            # Get the first node name of the cluster if not configured.
            # Required list permission on Kubernetes Node resources.
            core_api = kubernetes.client.CoreV1Api(self._client)
            with contextlib.suppress(kubernetes.client.exceptions.ApiException):
                nodes = core_api.list_node(limit=1)
                if nodes.items:
                    self._node_name = nodes.items[0].metadata.name

        pod = kubernetes.client.V1Pod(
            metadata=kubernetes.client.V1ObjectMeta(
                name=workload.name,
                namespace=workload.namespace,
                labels=workload.labels,
            ),
            spec=kubernetes.client.V1PodSpec(
                containers=[],
                host_network=workload.host_network,
                host_ipc=workload.host_ipc,
                share_process_namespace=workload.pid_shared,
                node_name=self._node_name,
                automount_service_account_token=False,
                volumes=(
                    [
                        kubernetes.client.V1Volume(
                            name="dshm",
                            empty_dir=kubernetes.client.V1EmptyDirVolumeSource(
                                medium="Memory",
                                size_limit=workload.shm_size,
                            ),
                        ),
                    ]
                    if workload.shm_size
                    else None
                ),
                security_context=kubernetes.client.V1PodSecurityContext(
                    run_as_user=workload.run_as_user,
                    run_as_group=workload.run_as_group,
                    fs_group=workload.fs_group,
                    sysctls=(
                        [
                            kubernetes.client.V1Sysctl(name=k, value=v)
                            for k, v in (workload.sysctls or {}).items()
                        ]
                        if workload.sysctls
                        else None
                    ),
                ),
            ),
        )

        for ci, c in enumerate(workload.containers):
            container = kubernetes.client.V1Container(
                image=c.image,
                name=c.name,
            )

            if c.profile == ContainerProfileEnum.INIT:
                # Set the restart policy of the INIT container.
                container.restart_policy = c.restart_policy.value
            elif not pod.spec.restart_policy:
                # Set the pod-level restart policy from the first RUN container.
                pod.spec.restart_policy = c.restart_policy.value

            # Parameterize execution
            if c.execution:
                container.working_dir = c.execution.working_dir
                container.command = c.execution.command
                container.args = c.execution.args
                container.security_context = kubernetes.client.V1SecurityContext(
                    run_as_user=c.execution.run_as_user,
                    run_as_group=c.execution.run_as_group,
                    read_only_root_filesystem=c.execution.readonly_rootfs,
                    privileged=c.execution.privileged,
                    capabilities=(
                        kubernetes.client.V1Capabilities(
                            add=c.execution.capabilities.add,
                            drop=c.execution.capabilities.drop,
                        )
                        if c.execution.capabilities
                        else None
                    ),
                )

            # Parameterize environment variables
            container.env = [
                kubernetes.client.V1EnvVar(name=e.name, value=e.value)
                for e in c.envs or []
            ]

            # Parameterize resources
            if c.resources:
                resources: dict[str, str] = {}
                r_k_rem = workload.resource_key_runtime_env_mapping or {}
                r_k_bem = workload.resource_key_backend_env_mapping or {}
                for r_k, r_v in c.resources.items():
                    if r_k in ("cpu", "memory"):
                        resources[r_k] = str(r_v)
                    else:
                        re = None
                        if r_k in r_k_rem:
                            # Set env if resource key is mapped.
                            re = r_k_rem[r_k]
                        elif r_k == envs.GPUSTACK_RUNTIME_DEPLOY_AUTOMAP_RESOURCE_KEY:
                            # Set env if auto-mapping key is matched.
                            re = self._runtime_visible_devices_env_name
                        if not re:
                            resources[r_k] = str(r_v)
                            continue

                        bes = []
                        if r_k in r_k_bem:
                            # Set env if resource key is mapped.
                            bes = r_k_bem[r_k]
                        else:
                            # Otherwise, use the default backend env names.
                            bes = self._backend_visible_devices_env_names

                        # Configure device access environment variable.
                        if r_v == "all" and bes:
                            # Configure privileged if requested all devices.
                            container.security_context = (
                                container.security_context
                                or kubernetes.client.V1SecurityContext()
                            )
                            container.security_context.privileged = True
                            # Then, set container backend visible devices env to "0",
                            # so that the container backend (e.g., NVIDIA Container Toolkit) can handle it,
                            # and mount corresponding libs if needed.
                            container.env.append(
                                kubernetes.client.V1EnvVar(
                                    name=re,
                                    value="0",
                                ),
                            )
                        else:
                            # Set env to the allocated device IDs.
                            container.env.append(
                                kubernetes.client.V1EnvVar(
                                    name=re,
                                    value=str(r_v),
                                ),
                            )

                        # Configure runtime device access environment variables.
                        if (
                            r_v != "all"
                            and container.security_context
                            and container.security_context.privileged
                        ):
                            for be in bes:
                                container.env.append(
                                    kubernetes.client.V1EnvVar(
                                        name=be,
                                        value=str(r_v),
                                    ),
                                )

                container.resources = kubernetes.client.V1ResourceRequirements(
                    limits=(resources if resources else None),
                    requests=(resources if resources else None),
                )

            # Parameterize volumes
            self._append_pod_volumes(
                pod,
                workload,
                ephemeral_filename_mapping,
            )

            # Parameterize mounts
            self._append_container_volume_mounts(
                container,
                c,
                ci,
                ephemeral_filename_mapping,
            )

            # Parameterize ports
            if c.ports:
                container.ports = [
                    kubernetes.client.V1ContainerPort(
                        container_port=p.internal,
                        host_port=p.external or p.internal
                        if workload.host_network
                        else None,
                        protocol=p.protocol.value,
                    )
                    for p in c.ports
                ]

            # Parameterize health checks
            if c.profile == ContainerProfileEnum.RUN and c.checks:
                # Find the first teardown-enabled check,
                # make it as the liveness probe.
                chk = next(
                    (chk for chk in c.checks if chk.teardown),
                    None,
                )
                if chk:
                    container.liveness_probe = self._parameterize_probe(chk)

                # Find the first non-teardown-enabled check,
                # make it as the readiness probe.
                chk = next(
                    (chk for chk in c.checks if not chk.teardown),
                    None,
                )
                if chk:
                    container.readiness_probe = self._parameterize_probe(chk)

            # Concat shared memory volume if needed.
            if workload.shm_size:
                if not container.volume_mounts:
                    container.volume_mounts = []
                container.volume_mounts.append(
                    kubernetes.client.V1VolumeMount(
                        name="dshm",
                        mount_path="/dev/shm",  # noqa: S108
                    ),
                )

            # Append the container.
            if c.profile == ContainerProfileEnum.INIT:
                pod.spec.init_containers = pod.spec.init_containers or []
                pod.spec.init_containers.append(container)
            else:
                pod.spec.containers.append(container)

        core_api = kubernetes.client.CoreV1Api(self._client)
        try:
            actual_pod = None
            with contextlib.suppress(kubernetes.client.exceptions.ApiException):
                actual_pod = core_api.read_namespaced_pod(
                    name=workload.name,
                    namespace=workload.namespace,
                )
            if not actual_pod:
                pod = core_api.create_namespaced_pod(
                    namespace=workload.namespace,
                    body=pod,
                )
                logger.debug(
                    "Created pod %s in namespace %s",
                    workload.name,
                    workload.namespace,
                )
            elif not equal_pods(actual_pod, pod):
                # Delete the existing Pod first, then create a new one.
                with watch(
                    core_api.list_namespaced_pod,
                    resource_version=(
                        None if envs.GPUSTACK_RUNTIME_KUBERNETES_QUORUM_READ else "0"
                    ),
                    namespace=workload.namespace,
                ) as es:
                    core_api.delete_namespaced_pod(
                        name=workload.name,
                        namespace=workload.namespace,
                    )
                    for e in es:
                        if (
                            e["type"] == "DELETED"
                            and e["object"].metadata.name == workload.name
                        ):
                            break
                pod = core_api.create_namespaced_pod(
                    namespace=workload.namespace,
                    body=pod,
                )
                logger.debug(
                    "Updated pod %s in namespace %s",
                    workload.name,
                    workload.namespace,
                )
        except kubernetes.client.exceptions.ApiException as e:
            msg = f"Failed to create pod for workload {workload.name}"
            raise OperationError(msg) from e

        return pod

    def __init__(self):
        super().__init__()
        self._client = self._get_client()
        self._node_name = envs.GPUSTACK_RUNTIME_KUBERNETES_NODE_NAME

    @_supported
    def create(self, workload: WorkloadPlan):
        """
        Deploy a Kubernetes workload.

        Args:
            workload:
                The workload to deploy.

        Raises:
            TypeError:
                If the Docker workload type is invalid.
            ValueError:
                If the Kubernetes workload fails to validate.
            UnsupportedError:
                If Kubernetes is not supported in the current environment.
            OperationError:
                If the Kubernetes workload fails to deploy.

        """
        if not isinstance(workload, KubernetesWorkloadPlan | WorkloadPlan):
            msg = f"Invalid workload plan type: {type(workload)}"
            raise TypeError(msg)
        if isinstance(workload, WorkloadPlan):
            workload = KubernetesWorkloadPlan(**workload.__dict__)

        workload.validate_and_default()
        workload.labels[_LABEL_WORKLOAD] = workload.name
        workload.namespace = (
            workload.namespace or envs.GPUSTACK_RUNTIME_KUBERNETES_NAMESPACE
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Creating workload:\n%s", workload.to_yaml())

        # Create ephemeral file if needed,
        # (container index, configured path): <actual ConfigMap name>
        ephemeral_filename_mapping: dict[tuple[int, str], str] = (
            self._create_ephemeral_configmaps(workload)
        )

        # Create Service if needed.
        self._create_service(workload)

        # Create Pod.
        self._create_pod(
            workload,
            ephemeral_filename_mapping,
        )

    @_supported
    def get(
        self,
        name: WorkloadName,
        namespace: WorkloadNamespace | None = None,
    ) -> WorkloadStatus | None:
        """
        Get the status of a Kubernetes workload.

        Args:
            name:
                The name of the workload.
            namespace:
                The namespace of the workload.

        Returns:
            The status if found, None otherwise.

        Raises:
            UnsupportedError:
                If Kubernetes is not supported in the current environment.
            OperationError:
                If the Kubernetes workload fails to get.

        """
        namespace = namespace or envs.GPUSTACK_RUNTIME_KUBERNETES_NAMESPACE

        list_options = {
            "label_selector": f"{_LABEL_WORKLOAD}={name}",
            "resource_version": (
                None if envs.GPUSTACK_RUNTIME_KUBERNETES_QUORUM_READ else "0"
            ),
        }

        core_api = kubernetes.client.CoreV1Api(self._client)

        try:
            k_pods = core_api.list_namespaced_pod(
                namespace=namespace,
                **list_options,
            )
        except kubernetes.client.exceptions.ApiException as e:
            msg = f"Failed to get deployment of workload {name}"
            raise OperationError(msg) from e

        if len(k_pods.items) > 1:
            namespaced_names = [
                f"{d.metadata.namespace}/{d.metadata.name}" for d in k_pods.items
            ]
            logger.warning(
                "Multiple pods found for workload %s: %s",
                name,
                namespaced_names,
            )

        if not k_pods.items:
            return None

        k_pod = k_pods.items[0]
        return KubernetesWorkloadStatus(
            name=name,
            k_pod=k_pod,
        )

    @_supported
    def delete(
        self,
        name: WorkloadName,
        namespace: WorkloadNamespace | None = None,
    ) -> WorkloadStatus | None:
        """
        Delete a Kubernetes workload.

        Args:
            name:
                The name of the workload.
            namespace:
                The namespace of the workload.

        Returns:
            The status if found, None otherwise.

        Raises:
            UnsupportedError:
                If Kubernetes is not supported in the current environment.
            OperationError:
                If the Kubernetes workload fails to delete.

        """
        namespace = namespace or envs.GPUSTACK_RUNTIME_KUBERNETES_NAMESPACE

        # Check if the workload exists.
        workload = self.get(name=name, namespace=namespace)
        if not workload:
            return None

        core_api = kubernetes.client.CoreV1Api(self._client)

        # Remove all Pods with the workload label.
        try:
            core_api.delete_collection_namespaced_pod(
                namespace=namespace,
                label_selector=f"{_LABEL_WORKLOAD}={name}",
                propagation_policy="Foreground",
            )
        except kubernetes.client.exceptions.ApiException as e:
            msg = f"Failed to delete pod of workload {name}"
            raise OperationError(msg) from e

        # Remove all Services with the workload label.
        try:
            core_api.delete_collection_namespaced_service(
                namespace=namespace,
                label_selector=f"{_LABEL_WORKLOAD}={name}",
                propagation_policy="Foreground",
            )
        except kubernetes.client.exceptions.ApiException as e:
            msg = f"Failed to delete service of workload {name}"
            raise OperationError(msg) from e

        # Remove all ConfigMaps with the workload label.
        try:
            core_api.delete_collection_namespaced_config_map(
                namespace=namespace,
                label_selector=f"{_LABEL_WORKLOAD}={name}",
                propagation_policy="Foreground",
            )
        except kubernetes.client.exceptions.ApiException as e:
            msg = f"Failed to delete configmap of workload {name}"
            raise OperationError(msg) from e

        return workload

    @_supported
    def list(
        self,
        namespace: WorkloadNamespace | None = None,
        labels: dict[str, str] | None = None,
    ) -> list[WorkloadStatus]:
        """
        List all Kubernetes workloads.

        Args:
            namespace:
                The namespace of the workloads.
            labels:
                Labels to filter the workloads.

        Returns:
            A list of workload statuses.

        Raises:
            UnsupportedError:
                If Kubernetes is not supported in the current environment.
            OperationError:
                If the Kubernetes workloads fail to list.

        """
        namespace = namespace or envs.GPUSTACK_RUNTIME_KUBERNETES_NAMESPACE

        list_options = {
            "label_selector": ",".join(
                [
                    *[
                        f"{k}={v}"
                        for k, v in (labels or {}).items()
                        if k != _LABEL_WORKLOAD
                    ],
                    _LABEL_WORKLOAD,
                ],
            ),
            "resource_version": (
                None if envs.GPUSTACK_RUNTIME_KUBERNETES_QUORUM_READ else "0"
            ),
        }

        core_api = kubernetes.client.CoreV1Api(self._client)

        try:
            k_pods = core_api.list_namespaced_pod(
                namespace=namespace,
                **list_options,
            )
        except kubernetes.client.exceptions.ApiException as e:
            msg = "Failed to list workloads' deployments"
            raise OperationError(msg) from e

        return [
            KubernetesWorkloadStatus(
                name=k_pod.metadata.labels.get(_LABEL_WORKLOAD, k_pod.metadata.name),
                k_pod=k_pod,
            )
            for k_pod in k_pods.items or []
            if (
                k_pod.metadata.labels
                and (_LABEL_WORKLOAD in k_pod.metadata.labels)
                and k_pod.metadata.labels[_LABEL_WORKLOAD] == k_pod.metadata.name
            )
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
        Get logs of a Kubernetes workload or a specific container.

        Args:
            name:
                The name of the workload.
            namespace:
                The namespace of the workload.
            token:
                The operation token to identify the container.
            timestamps:
                Whether to include timestamps in the logs.
            tail:
                Number of lines from the end of the logs to retrieve.
            since:
                Only return logs newer than a relative duration in seconds.
            follow:
                Whether to stream the logs.

        Returns:
            The logs as a byte string, a string or a generator yielding byte strings or strings if follow is True.

        Raises:
            UnsupportedError:
                If Kubernetes is not supported in the current environment.
            OperationError:
                If the Kubernetes workload logs fail to retrieve.

        """
        namespace = namespace or envs.GPUSTACK_RUNTIME_KUBERNETES_NAMESPACE

        workload = self.get(name=name, namespace=namespace)
        if not workload:
            msg = f"Workload {name} not found"
            raise OperationError(msg)

        k_pod = getattr(workload, "_k_pod", kubernetes.client.V1Pod())
        container = next(
            (
                c
                for ci, c in enumerate(k_pod.spec.containers)
                if (c.name == token if token else ci == 0)
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
            "tail_lines": tail if tail >= 0 else None,
            "since_seconds": since,
            "follow": follow,
            "_preload_content": not follow,
        }

        core_api = kubernetes.client.CoreV1Api(self._client)

        try:
            output = core_api.read_namespaced_pod_log(
                namespace=k_pod.metadata.namespace,
                name=k_pod.metadata.name,
                container=container.name,
                **logs_options,
            )
        except kubernetes.client.exceptions.ApiException as e:
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
        Execute a command in a Kubernetes workload or a specific container.

        Args:
            name:
                The name of the workload.
            namespace:
                The namespace of the workload.
            token:
                The operation token to identify the container.
            detach:
                Whether to run the command in detached mode.
            command:
                The command to execute.
            args:
                The arguments to pass to the command.

        Returns:
            If detach is False, return a WorkloadExecStream.
            otherwise, return the output of the command as a byte string or string.

        Raises:
            UnsupportedError:
                If Kubernetes is not supported in the current environment.
            OperationError:
                If the Kubernetes workload exec fails.

        """
        namespace = namespace or envs.GPUSTACK_RUNTIME_KUBERNETES_NAMESPACE

        workload = self.get(name=name, namespace=namespace)
        if not workload:
            msg = f"Workload {name} not found"
            raise OperationError(msg)

        k_pod = getattr(workload, "_k_pod", kubernetes.client.V1Pod())
        container = next(
            (
                c
                for ci, c in enumerate(k_pod.spec.containers)
                if (c.name == token if token else ci == 0)
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
            "tty": attach,
            "command": [*command, *(args or [])] if command else ["/bin/sh"],
            "_preload_content": not attach,
        }

        core_api = kubernetes.client.CoreV1Api(self._client)

        try:
            result = kubernetes.stream.stream(
                core_api.connect_get_namespaced_pod_exec,
                namespace=k_pod.metadata.namespace,
                name=k_pod.metadata.name,
                container=container.name,
                **exec_options,
            )
        except kubernetes.client.exceptions.ApiException as e:
            msg = f"Failed to exec command in container {container.name} of workload {name}"
            raise OperationError(msg) from e
        else:
            if not attach:
                return result
            return KubernetesWorkloadExecStream(result)


def hash_string(s: str) -> str:
    """
    Hash a string using MD5 and return the hexadecimal digest.

    Args:
        s:
            The string to hash.

    Returns:
        The hexadecimal digest of the MD5 hash.

    """
    return hashlib.md5(s.encode("UTF-8")).hexdigest()  # noqa: S324


def equal_config_maps(
    a: kubernetes.client.V1ConfigMap,
    b: kubernetes.client.V1ConfigMap,
) -> bool:
    """
    Compare two Kubernetes ConfigMap specs for equality, ignoring certain fields.

    Args:
        a:
            The first ConfigMap spec.
        b:
            The second ConfigMap spec.

    Returns:
        True if the ConfigMap specs are equal, False otherwise.

    """
    return (a.data or {}) == (b.data or {})


def equal_services(
    a: kubernetes.client.V1Service,
    b: kubernetes.client.V1Service,
) -> bool:
    """
    Compare two Kubernetes Service specs for equality, ignoring certain fields.

    Args:
        a:
            The first Service spec.
        b:
            The second Service spec.

    Returns:
        True if the Service specs are equal, False otherwise.

    """
    aspec = a.spec
    bspec = b.spec
    if (aspec.selector or {}) != (bspec.selector or {}):
        return False
    if aspec.type != bspec.type:
        return False
    return (aspec.ports or []) == (bspec.ports or [])


def equal_containers(
    a: kubernetes.client.V1Container,
    b: kubernetes.client.V1Container,
) -> bool:
    """
    Compare two Kubernetes Container specs for equality, ignoring certain fields.

    Args:
        a:
            The first Container spec.
        b:
            The second Container spec.

    Returns:
        True if the Container specs are equal, False otherwise.

    """
    if a.name != b.name:
        return False
    if a.restart_policy != b.restart_policy:
        return False
    if a.image != b.image:
        return False
    if (a.command or []) != (b.command or []):
        return False
    if (a.args or []) != (b.args or []):
        return False
    if (a.working_dir or "") != (b.working_dir or ""):
        return False
    if (a.ports or []) != (b.ports or []):
        return False
    if (a.resources or {}) != (b.resources or {}):
        return False
    if (a.volume_mounts or []) != (b.volume_mounts or []):
        return False
    if (a.liveness_probe or {}) != (b.liveness_probe or {}):
        return False
    if (a.readiness_probe or {}) != (b.readiness_probe or {}):
        return False
    if (a.security_context or {}) != (b.security_context or {}):
        return False
    if len(a.env or []) != len(b.env or []):
        return False
    aenv = {e.name: e.value for e in a.env or []}
    benv = {e.name: e.value for e in b.env or []}
    return all(not (k not in benv or benv[k] != v) for k, v in aenv.items())


def equal_pods(
    a: kubernetes.client.V1Pod,
    b: kubernetes.client.V1Pod,
) -> bool:
    """
    Compare two Kubernetes Pod specs for equality, ignoring certain fields.

    Args:
        a:
            The first Pod spec.
        b:
            The second Pod spec.

    Returns:
        True if the Pod specs are equal, False otherwise.

    """
    aspec = a.spec
    bspec = b.spec
    if len(aspec.init_containers or []) != len(bspec.init_containers or []):
        return False
    for ac, bc in zip(
        aspec.init_containers or [],
        bspec.init_containers or [],
        strict=False,
    ):
        if not equal_containers(ac, bc):
            return False
    if aspec.host_network != bspec.host_network:
        return False
    if aspec.host_ipc != bspec.host_ipc:
        return False
    if aspec.share_process_namespace != bspec.share_process_namespace:
        return False
    if (aspec.restart_policy or "Always") != (bspec.restart_policy or "Always"):
        return False
    if aspec.node_name and bspec.node_name and aspec.node_name != bspec.node_name:
        return False
    if (aspec.volumes or []) != (bspec.volumes or []):
        return False
    if (aspec.security_context or {}) != (bspec.security_context or {}):
        return False
    if len(aspec.containers or []) != len(bspec.containers or []):
        return False
    for ac, bc in zip(
        aspec.containers or [],
        bspec.containers or [],
        strict=False,
    ):
        if not equal_containers(ac, bc):
            return False

    return True


@contextlib.contextmanager
def watch(func, *args, **kwargs):
    w: kubernetes.watch.Watch | None = None
    try:
        w = kubernetes.watch.Watch()
        yield w.stream(func, *args, **kwargs)
    finally:
        if w:
            w.stop()


class KubernetesWorkloadExecStream(WorkloadExecStream):
    """
    A WorkloadExecStream implementation for Kubernetes exec streams.
    """

    _ws: kubernetes.stream.ws_client.WSClient | None = None

    def __init__(self, ws: kubernetes.stream.ws_client.WSClient):
        super().__init__()
        self._ws = ws
        self._ws.run_forever(timeout=1)
        self._ws.write_stdin(b" \b")

    @property
    def closed(self) -> bool:
        return not (self._ws and self._ws.is_open())

    def fileno(self) -> int:
        return self._ws.sock.fileno()

    def recv(self, size=-1) -> bytes | None:
        return self.read(size)

    def send(self, data: bytes) -> int:
        return self.write(data)

    def read(self, *_) -> bytes | None:
        if self.closed:
            return None
        self._ws.update(timeout=1)
        return self._ws.read_all().encode("utf-8", errors="replace")

    def write(self, data: bytes) -> int:
        if self.closed:
            return 0
        data_len = len(data)
        self._ws.write_stdin(data.decode("utf-8", errors="replace"))
        return data_len

    def close(self):
        if not self.closed:
            return
        self._ws.close()
