# The cases below drive the deployers' own plan-to-container conversion,
# which is where the device ordering pin becomes observable without a live
# Docker daemon, Podman socket or Kubernetes cluster.
# ruff: noqa: SLF001

from types import SimpleNamespace

import docker.errors
import kubernetes.client
import podman.errors
import pytest

from gpustack_runtime import envs
from gpustack_runtime.deployer.__types__ import (
    Container,
    ContainerEnv,
    ContainerExecution,
    ContainerResources,
    Deployer,
    DevicesMaterial,
)
from gpustack_runtime.deployer.docker import DockerDeployer, DockerWorkloadPlan
from gpustack_runtime.deployer.kuberentes import (
    KubernetesDeployer,
    KubernetesWorkloadPlan,
)
from gpustack_runtime.deployer.podman import PodmanDeployer, PodmanWorkloadPlan
from gpustack_runtime.detector import ManufacturerEnum

_ORDERING_ENV = "CUDA_DEVICE_ORDER"

_NVIDIA_MATERIALS = {
    "NVIDIA_VISIBLE_DEVICES": DevicesMaterial(
        manufacturer=ManufacturerEnum.NVIDIA,
        runtime_env="NVIDIA_VISIBLE_DEVICES",
        backend_env=["CUDA_VISIBLE_DEVICES"],
        cdi="nvidia.com/gpu",
        runtime_values={"0": "0", "1": "1"},
        backend_values={"CUDA_VISIBLE_DEVICES": {"0": "0", "1": "1"}},
    ),
}
_SINGLE_NVIDIA_MATERIALS = {
    "NVIDIA_VISIBLE_DEVICES": DevicesMaterial(
        manufacturer=ManufacturerEnum.NVIDIA,
        runtime_env="NVIDIA_VISIBLE_DEVICES",
        backend_env=["CUDA_VISIBLE_DEVICES"],
        cdi="nvidia.com/gpu",
        runtime_values={"0": "0"},
        backend_values={"CUDA_VISIBLE_DEVICES": {"0": "0"}},
    ),
}
_AMD_MATERIALS = {
    "AMD_VISIBLE_DEVICES": DevicesMaterial(
        manufacturer=ManufacturerEnum.AMD,
        runtime_env="AMD_VISIBLE_DEVICES",
        backend_env=["HIP_VISIBLE_DEVICES"],
        cdi="amd.com/gpu",
        runtime_values={"0": "0"},
        backend_values={"HIP_VISIBLE_DEVICES": {"0": "0"}},
    ),
}
_MIXED_MATERIALS = {**_NVIDIA_MATERIALS, **_AMD_MATERIALS}


def _deployer(cls, materials: dict[str, DevicesMaterial]):
    # Bypass the deployer's __init__, which reaches out to a live daemon or
    # API server, and pin the materials so _prepare() short-circuits instead
    # of detecting devices.
    deployer = object.__new__(cls)
    Deployer.__init__(deployer, "test")
    deployer._materials = materials
    return deployer


def _container(
    resources: dict,
    privileged: bool = False,
    declared_envs: dict[str, str] | None = None,
) -> Container:
    container_resources = ContainerResources()
    container_resources.update(resources)
    return Container(
        name="default",
        image="gpustack/runner:latest",
        execution=ContainerExecution(privileged=privileged),
        envs=[ContainerEnv(name=n, value=v) for n, v in (declared_envs or {}).items()],
        resources=container_resources,
    )


class _FakeContainers:
    """
    Stand-in for the Docker/Podman clients' container collection.
    """

    def __init__(self, not_found, created: list[dict]):
        self._not_found = not_found
        self._created = created

    def get(self, name):
        raise self._not_found(name)

    def create(self, **kwargs):
        self._created.append(kwargs)
        return SimpleNamespace(name=kwargs.get("name"))


def _docker_container_envs(
    materials: dict[str, DevicesMaterial],
    container: Container,
    monkeypatch,
) -> list[tuple[str, str]]:
    # Keep the requested devices on the env injection path,
    # so no CDI specification is generated.
    monkeypatch.setattr(
        envs,
        "GPUSTACK_RUNTIME_DOCKER_RESOURCE_INJECTION_POLICY",
        "Env",
    )

    created: list[dict] = []
    deployer = _deployer(DockerDeployer, materials)
    deployer._client = SimpleNamespace(
        containers=_FakeContainers(docker.errors.NotFound, created),
    )
    deployer._get_image = lambda *_args, **_kwargs: container.image
    deployer._mutate_create_options = lambda create_options: create_options

    workload = DockerWorkloadPlan(name="test", containers=[container])
    workload.validate_and_default()
    deployer._create_containers(workload, {}, SimpleNamespace(id="pause"))

    return list(created[0]["environment"].items())


def _podman_container_envs(
    materials: dict[str, DevicesMaterial],
    container: Container,
    monkeypatch,
) -> list[tuple[str, str]]:
    # Podman always requests devices via CDI,
    # skip generating the CDI specification.
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_PODMAN_CDI_SPECS_GENERATE", False)

    created: list[dict] = []
    deployer = _deployer(PodmanDeployer, materials)
    deployer._client = SimpleNamespace(
        containers=_FakeContainers(podman.errors.NotFound, created),
    )
    deployer._get_image = lambda *_args, **_kwargs: container.image
    deployer._mutate_create_options = lambda create_options: create_options

    workload = PodmanWorkloadPlan(name="test", containers=[container])
    workload.validate_and_default()
    deployer._create_containers(workload, {}, SimpleNamespace(id="pause"))

    return list(created[0]["environment"].items())


def _kubernetes_container_envs(
    materials: dict[str, DevicesMaterial],
    container: Container,
    monkeypatch,
    policy: str = "env",
) -> list[tuple[str, str]]:
    monkeypatch.setattr(
        "gpustack_runtime.deployer.kuberentes.get_resource_injection_policy",
        lambda *_args: policy,
    )
    # Resolving the RuntimeClass reads the cluster.
    monkeypatch.setattr(
        "gpustack_runtime.deployer.kuberentes._resolve_runtime_class_name",
        lambda *_args: None,
    )

    class _FakeCoreV1Api:
        def __init__(self, client=None):
            pass

        def read_namespaced_pod(self, name, namespace):
            raise kubernetes.client.exceptions.ApiException(status=404)

        def create_namespaced_pod(self, namespace, body):
            return body

    monkeypatch.setattr(kubernetes.client, "CoreV1Api", _FakeCoreV1Api)

    deployer = _deployer(KubernetesDeployer, materials)
    deployer._client = None
    deployer._node_name = None
    deployer._image_pull_secret = None
    deployer._mutate_create_pod = lambda pod: pod

    workload = KubernetesWorkloadPlan(
        name="test",
        namespace="default",
        containers=[container],
    )
    workload.validate_and_default()
    pod = deployer._create_pod(workload, {})

    return [(e.name, e.value) for e in pod.spec.containers[0].env]


@pytest.mark.parametrize(
    "name, materials, runtime_envs, expected",
    [
        (
            "NVIDIA",
            _NVIDIA_MATERIALS,
            ["NVIDIA_VISIBLE_DEVICES"],
            {"CUDA_DEVICE_ORDER": "PCI_BUS_ID"},
        ),
        (
            "non-NVIDIA",
            _AMD_MATERIALS,
            ["AMD_VISIBLE_DEVICES"],
            {},
        ),
        (
            "mixed manufacturers",
            _MIXED_MATERIALS,
            ["NVIDIA_VISIBLE_DEVICES", "AMD_VISIBLE_DEVICES"],
            {"CUDA_DEVICE_ORDER": "PCI_BUS_ID"},
        ),
        (
            "unknown runtime visible devices env",
            _NVIDIA_MATERIALS,
            ["UNKNOWN_RUNTIME_VISIBLE_DEVICES"],
            {},
        ),
        (
            "no runtime visible devices env",
            _NVIDIA_MATERIALS,
            [],
            {},
        ),
    ],
)
def test_map_visible_devices_ordering(name, materials, runtime_envs, expected):
    deployer = _deployer(DockerDeployer, materials)
    actual = deployer.map_visible_devices_ordering(runtime_envs)
    assert actual == expected, f"case {name} expected {expected}, but got {actual}"


@pytest.mark.parametrize(
    "runner",
    [
        _docker_container_envs,
        _podman_container_envs,
        _kubernetes_container_envs,
    ],
    ids=["docker", "podman", "kubernetes"],
)
@pytest.mark.parametrize(
    "name, materials, resources, privileged, declared_envs, expected",
    [
        (
            "all devices",
            _NVIDIA_MATERIALS,
            {"nvidia.com/devices": "all"},
            False,
            None,
            ["PCI_BUS_ID"],
        ),
        (
            "specific devices, privileged",
            _NVIDIA_MATERIALS,
            {"nvidia.com/devices": "0"},
            True,
            None,
            ["PCI_BUS_ID"],
        ),
        (
            "specific devices, unprivileged",
            _NVIDIA_MATERIALS,
            {"nvidia.com/devices": "0"},
            False,
            None,
            [],
        ),
        (
            # Several devices number themselves inside the container just as
            # "all" does, so the ordering is pinned without privilege and
            # without asking for every device of the host.
            "several specific devices, unprivileged",
            _NVIDIA_MATERIALS,
            {"nvidia.com/devices": "0,1"},
            False,
            None,
            ["PCI_BUS_ID"],
        ),
        (
            # "all" on a single-device host resolves to one device, which has
            # no ordering to pin -- the request is measured, not special-cased.
            "all devices on a single-device host",
            _SINGLE_NVIDIA_MATERIALS,
            {"nvidia.com/devices": "all"},
            False,
            None,
            [],
        ),
        (
            "all devices, non-NVIDIA",
            _AMD_MATERIALS,
            {"amd.com/devices": "all"},
            False,
            None,
            [],
        ),
        (
            "all devices, ordering declared by the container",
            _NVIDIA_MATERIALS,
            {"nvidia.com/devices": "all"},
            False,
            {"CUDA_DEVICE_ORDER": "FASTEST_FIRST"},
            ["FASTEST_FIRST"],
        ),
        (
            "all devices, auto-mapped on a mixed-manufacturer node",
            _MIXED_MATERIALS,
            {"gpustack.ai/devices": "all"},
            False,
            None,
            ["PCI_BUS_ID"],
        ),
    ],
)
def test_visible_devices_ordering_injection(
    name,
    materials,
    resources,
    privileged,
    declared_envs,
    expected,
    runner,
    monkeypatch,
):
    container_envs = runner(
        materials,
        _container(resources, privileged, declared_envs),
        monkeypatch,
    )
    actual = [v for n, v in container_envs if n == _ORDERING_ENV]
    assert actual == expected, f"case {name} expected {expected}, but got {actual}"


@pytest.mark.parametrize(
    "name, resources, expected",
    [
        (
            "all devices",
            {"nvidia.com/devices": "all"},
            ["PCI_BUS_ID"],
        ),
        (
            "specific devices",
            {"nvidia.com/devices": "0"},
            [],
        ),
    ],
)
def test_visible_devices_ordering_injection_with_device_plugin(
    name,
    resources,
    expected,
    monkeypatch,
):
    # Under the KDP injection policy the devices are allocated by a device
    # plugin, so a privileged request loses its privilege: an "all" request
    # still receives every card and gets the ordering pinned, a specific
    # request does not.
    container_envs = _kubernetes_container_envs(
        _NVIDIA_MATERIALS,
        _container(resources, privileged=True),
        monkeypatch,
        policy="kdp",
    )
    actual = [v for n, v in container_envs if n == _ORDERING_ENV]
    assert actual == expected, f"case {name} expected {expected}, but got {actual}"
