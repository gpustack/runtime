# ruff: noqa: SLF001

from pathlib import Path
from types import SimpleNamespace

import docker.errors
import kubernetes.client
import pytest

from gpustack_runtime import envs
from gpustack_runtime.deployer.__types__ import (
    Container,
    ContainerExecution,
    ContainerFile,
    ContainerMount,
    ContainerMountModeEnum,
    ContainerResources,
    Deployer,
    DevicesMaterial,
)
from gpustack_runtime.deployer.cdi import ascend as cdi_ascend
from gpustack_runtime.deployer.docker import DockerDeployer, DockerWorkloadPlan
from gpustack_runtime.deployer.kubernetes import KubernetesDeployer
from gpustack_runtime.deployer.podman import PodmanDeployer
from gpustack_runtime.detector import ManufacturerEnum


def _make_host_root(tmp_path: Path) -> Path:
    root = tmp_path / "host"
    for marker in ("etc", "proc", "usr"):
        (root / marker).mkdir(parents=True)
    return root


def _ascend_materials(arch_family: str) -> dict[str, DevicesMaterial]:
    return {
        "ASCEND_VISIBLE_DEVICES": DevicesMaterial(
            manufacturer=ManufacturerEnum.ASCEND,
            runtime_env="ASCEND_VISIBLE_DEVICES",
            backend_env=["ASCEND_RT_VISIBLE_DEVICES", "NPU_VISIBLE_DEVICES"],
            cdi="huawei.com/npu",
            runtime_values={"0": "0", "1": "1"},
            backend_values={
                "ASCEND_RT_VISIBLE_DEVICES": {"0": "0", "1": "1"},
                "NPU_VISIBLE_DEVICES": {"0": "0", "1": "1"},
            },
            arch_families={"0": arch_family, "1": arch_family},
        ),
    }


def _nvidia_materials() -> dict[str, DevicesMaterial]:
    return {
        "NVIDIA_VISIBLE_DEVICES": DevicesMaterial(
            manufacturer=ManufacturerEnum.NVIDIA,
            runtime_env="NVIDIA_VISIBLE_DEVICES",
            backend_env=["CUDA_VISIBLE_DEVICES"],
            cdi="nvidia.com/gpu",
            runtime_values={"0": "GPU-0"},
            backend_values={"CUDA_VISIBLE_DEVICES": {"0": "0"}},
            arch_families={"0": "Hopper"},
        ),
    }


def _deployer(cls, materials: dict[str, DevicesMaterial] | None = None):
    deployer = object.__new__(cls)
    Deployer.__init__(deployer, "test")
    deployer._materials = materials
    return deployer


class _FakeContainers:
    def __init__(self, not_found, created: list[dict]):
        self._not_found = not_found
        self._created = created

    def get(self, name):
        raise self._not_found(name)

    def create(self, **kwargs):
        self._created.append(kwargs)
        return SimpleNamespace(
            name=kwargs.get("name"),
            put_archive=lambda **_kw: True,
        )


def _create_docker_container(
    materials: dict[str, DevicesMaterial],
    container: Container,
    monkeypatch,
    mutate_create_options=None,
) -> dict:
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
    deployer._mutate_create_options = (
        mutate_create_options
        if mutate_create_options is not None
        else (lambda opts: opts)
    )

    workload = DockerWorkloadPlan(name="test", containers=[container])
    workload.validate_and_default()
    deployer._create_containers(workload, {}, SimpleNamespace(id="pause"))
    return created[0]


def _container(
    resources: dict,
    *,
    privileged: bool = False,
    mounts: list[ContainerMount] | None = None,
    files: list[ContainerFile] | None = None,
) -> Container:
    res = ContainerResources()
    res.update(resources)
    return Container(
        name="default",
        image="gpustack/runner:latest",
        execution=ContainerExecution(privileged=privileged),
        resources=res,
        mounts=mounts,
        files=files,
    )


@pytest.mark.parametrize(
    "requested, privileged",
    [
        ("0", False),
        ("0,1", False),
        ("all", False),
        ("0", True),
    ],
)
def test_docker_env_a5_mounts_eligible_host_files_read_only(
    tmp_path,
    monkeypatch,
    requested,
    privileged,
):
    host_root = _make_host_root(tmp_path)
    (host_root / "etc" / "hccl_rootinfo.json").write_text(
        '{"version": "2.0"}',
        encoding="utf-8",
    )
    (host_root / "usr" / "lib").mkdir(parents=True)
    (host_root / "usr" / "lib" / "route.conf").write_text("route\n", encoding="utf-8")
    (host_root / "lib").symlink_to("/usr/lib")
    # /etc/hixlep must never be auto-mounted by gpustack-runtime.
    (host_root / "etc" / "hixlep").mkdir()
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT", str(host_root))

    opts = _create_docker_container(
        _ascend_materials("Ascend950PR"),
        _container({"huawei.com/devices": requested}, privileged=privileged),
        monkeypatch,
    )

    mounts = {m["Target"]: m for m in opts.get("mounts", [])}
    assert set(mounts) == {
        cdi_ascend._HCCL_RANKTABLE_PATH,
        cdi_ascend._ROUTE_CONF_PATH,
    }
    for path in (cdi_ascend._HCCL_RANKTABLE_PATH, cdi_ascend._ROUTE_CONF_PATH):
        assert mounts[path]["Type"] == "bind"
        assert mounts[path]["Source"] == path
        assert mounts[path]["ReadOnly"] is True
    assert "/etc/hixlep" not in mounts


def test_docker_env_a5_skips_when_host_root_unavailable(tmp_path, monkeypatch):
    monkeypatch.setattr(
        envs,
        "GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT",
        str(tmp_path / "missing"),
    )

    opts = _create_docker_container(
        _ascend_materials("Ascend950DT"),
        _container({"huawei.com/devices": "0"}),
        monkeypatch,
    )

    assert opts.get("mounts", []) == []


def test_docker_env_non_a5_and_other_vendors_skip_conditional_mounts(
    tmp_path,
    monkeypatch,
):
    host_root = _make_host_root(tmp_path)
    (host_root / "etc" / "hccl_rootinfo.json").write_text(
        '{"version": "2.0"}',
        encoding="utf-8",
    )
    (host_root / "lib").mkdir()
    (host_root / "lib" / "route.conf").write_text("route\n", encoding="utf-8")
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT", str(host_root))

    for materials, res in (
        (_ascend_materials("Ascend910B4"), {"huawei.com/devices": "0"}),
        (_ascend_materials("Ascend910C"), {"huawei.com/devices": "0"}),
        (_nvidia_materials(), {"nvidia.com/devices": "0"}),
    ):
        opts = _create_docker_container(
            materials,
            _container(res),
            monkeypatch,
        )
        assert opts.get("mounts", []) == []


def test_docker_env_a5_deduplicates_explicit_and_mirrored_targets(
    tmp_path,
    monkeypatch,
):
    host_root = _make_host_root(tmp_path)
    (host_root / "etc" / "hccl_rootinfo.json").write_text(
        '{"version": "2.0"}',
        encoding="utf-8",
    )
    (host_root / "lib").mkdir()
    (host_root / "lib" / "route.conf").write_text("route\n", encoding="utf-8")
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT", str(host_root))

    container = _container(
        {"huawei.com/devices": "0"},
        mounts=[
            ContainerMount(
                path="/lib//route.conf",
                mode=ContainerMountModeEnum.RWX,
            ),
        ],
        files=[
            ContainerFile(
                path="/etc/hccl_rootinfo.json/",
                content=None,
                mode=0o644,
            ),
        ],
    )

    opts = _create_docker_container(
        _ascend_materials("Ascend950PR"),
        container,
        monkeypatch,
    )

    targets = [m["Target"] for m in opts.get("mounts", [])]
    assert targets == ["/etc/hccl_rootinfo.json/", "/lib//route.conf"]

    # Also verify deduplication against mirrored mounts.
    def _mirror(create_options):
        mounts = create_options.get("mounts") or []
        mounts.append(
            {
                "Type": "bind",
                "Source": "/custom/ranktable.json",
                "Target": "/etc/hccl_rootinfo.json",
                "ReadOnly": True,
            },
        )
        create_options["mounts"] = mounts
        return create_options

    mirrored_opts = _create_docker_container(
        _ascend_materials("Ascend950PR"),
        _container({"huawei.com/devices": "0"}),
        monkeypatch,
        mutate_create_options=_mirror,
    )
    mirrored_mounts = {m["Target"]: m for m in mirrored_opts.get("mounts", [])}
    assert set(mirrored_mounts) == {
        cdi_ascend._HCCL_RANKTABLE_PATH,
        cdi_ascend._ROUTE_CONF_PATH,
    }
    assert mirrored_mounts[cdi_ascend._HCCL_RANKTABLE_PATH]["Source"] == (
        "/custom/ranktable.json"
    )


def test_docker_mirrored_deployment_excludes_host_root(monkeypatch):
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT", "/host")
    monkeypatch.setattr(
        envs,
        "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_VOLUMES",
        set(),
    )
    monkeypatch.setattr(
        envs,
        "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_ENVIRONMENTS",
        set(),
    )

    self_container = SimpleNamespace(
        short_id="worker1",
        image=SimpleNamespace(attrs={"Config": {"Env": []}}),
        attrs={
            "Config": {"Env": []},
            "HostConfig": {"Runtime": "ascend"},
            "Mounts": [
                {
                    "Type": "bind",
                    "Source": "/",
                    "Destination": "/host",
                    "Mode": "ro",
                    "RW": False,
                },
                {
                    "Type": "bind",
                    "Source": "/var/run/docker.sock",
                    "Destination": "/var/run/docker.sock",
                    "Mode": "",
                    "RW": True,
                },
                {
                    "Type": "bind",
                    "Source": "/var/lib/gpustack",
                    "Destination": "/var/lib/gpustack",
                    "Mode": "",
                    "RW": True,
                },
            ],
        },
    )

    deployer = _deployer(DockerDeployer, {})
    deployer._mutate_create_options = None
    deployer._find_self_container = lambda: self_container
    deployer._prepare_mirrored_deployment()

    mutated = deployer._mutate_create_options({"name": "workload-run-0"})
    targets = [m["Target"] for m in mutated.get("mounts", [])]
    assert targets == ["/var/lib/gpustack"]


def test_podman_mirrored_deployment_excludes_host_root(monkeypatch):
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT", "/host")
    monkeypatch.setattr(
        envs,
        "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_VOLUMES",
        set(),
    )
    monkeypatch.setattr(
        envs,
        "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_ENVIRONMENTS",
        set(),
    )

    self_container = SimpleNamespace(
        short_id="worker1",
        image=SimpleNamespace(attrs={"Config": {"Env": []}}),
        attrs={
            "Config": {"Env": []},
            "HostConfig": {},
            "Mounts": [
                {
                    "Type": "bind",
                    "Source": "/",
                    "Destination": "/host/",
                    "Mode": "ro",
                    "RW": False,
                },
                {
                    "Type": "bind",
                    "Source": "/run/podman/podman.sock",
                    "Destination": "/run/podman/podman.sock",
                    "Mode": "",
                    "RW": True,
                },
                {
                    "Type": "bind",
                    "Source": "/var/lib/gpustack",
                    "Destination": "/var/lib/gpustack",
                    "Mode": "",
                    "RW": True,
                },
            ],
        },
    )

    deployer = _deployer(PodmanDeployer, {})
    deployer._mutate_create_options = None
    deployer._find_self_container = lambda: self_container
    deployer._prepare_mirrored_deployment()

    mutated = deployer._mutate_create_options({"name": "workload-run-0"})
    targets = [m["target"] for m in mutated.get("mounts", [])]
    assert targets == ["/var/lib/gpustack"]


def test_kubernetes_mirrored_deployment_excludes_host_root(monkeypatch):
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT", "/host")
    monkeypatch.setattr(
        envs,
        "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_VOLUMES",
        set(),
    )
    monkeypatch.setattr(
        envs,
        "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_ENVIRONMENTS",
        set(),
    )
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_KUBERNETES_NAMESPACE", "default")

    self_pod = kubernetes.client.V1Pod(
        metadata=kubernetes.client.V1ObjectMeta(name="worker", namespace="default"),
        spec=kubernetes.client.V1PodSpec(
            containers=[
                kubernetes.client.V1Container(
                    name="default",
                    image="gpustack/gpustack:latest",
                    volume_mounts=[
                        kubernetes.client.V1VolumeMount(
                            name="host-root",
                            mount_path="/host",
                            read_only=True,
                        ),
                        kubernetes.client.V1VolumeMount(
                            name="data",
                            mount_path="/var/lib/gpustack",
                        ),
                    ],
                ),
            ],
            volumes=[
                kubernetes.client.V1Volume(
                    name="host-root",
                    host_path=kubernetes.client.V1HostPathVolumeSource(path="/"),
                ),
                kubernetes.client.V1Volume(
                    name="data",
                    host_path=kubernetes.client.V1HostPathVolumeSource(
                        path="/var/lib/gpustack",
                    ),
                ),
            ],
        ),
    )

    deployer = _deployer(KubernetesDeployer, {})
    deployer._client = None
    deployer._node_name = "node-1"
    deployer._image_pull_secret = None
    deployer._mutate_create_pod = None
    deployer._find_self_pod = lambda: self_pod
    deployer._prepare_mirrored_deployment()

    target_pod = kubernetes.client.V1Pod(
        metadata=kubernetes.client.V1ObjectMeta(name="workload", namespace="default"),
        spec=kubernetes.client.V1PodSpec(
            containers=[
                kubernetes.client.V1Container(
                    name="default",
                    image="gpustack/runner:latest",
                ),
            ],
            volumes=[],
        ),
    )
    mutated = deployer._mutate_create_pod(target_pod)
    mount_paths = [
        m.mount_path for m in (mutated.spec.containers[0].volume_mounts or [])
    ]
    volume_names = [v.name for v in (mutated.spec.volumes or [])]
    assert mount_paths == ["/var/lib/gpustack"]
    assert volume_names == ["data"]
