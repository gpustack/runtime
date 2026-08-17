import pytest

from gpustack_runtime.deployer.__types__ import (
    Container,
    ContainerExecution,
    ContainerResources,
)
from gpustack_runtime.deployer.kuberentes import _resolve_privileged


def _container(privileged: bool | None, resources: dict | None = None) -> Container:
    container_resources = None
    if resources is not None:
        container_resources = ContainerResources()
        container_resources.update(resources)
    return Container(
        name="default",
        image="gpustack/runner:latest",
        execution=(
            ContainerExecution(privileged=privileged)
            if privileged is not None
            else None
        ),
        resources=container_resources,
    )


@pytest.mark.parametrize(
    "name, privileged, resources, policy, expected",
    [
        (
            "no execution",
            None,
            None,
            "env",
            False,
        ),
        (
            "not requested",
            False,
            {"nvidia.com/devices": "0"},
            "env",
            False,
        ),
        (
            "no resources",
            True,
            None,
            "env",
            True,
        ),
        (
            "non-device resources only",
            True,
            {"cpu": "2", "memory": "4Gi"},
            "env",
            True,
        ),
        (
            "specific whole cards, env injection",
            True,
            {"cpu": "2", "nvidia.com/devices": "0,1"},
            "env",
            True,
        ),
        (
            "all devices, env injection",
            True,
            {"nvidia.com/devices": "all"},
            "env",
            True,
        ),
        (
            "specific whole cards, kdp injection",
            True,
            {"nvidia.com/devices": "0,1"},
            "kdp",
            False,
        ),
        (
            "auto-mapped devices, kdp injection",
            True,
            {"gpustack.ai/devices": "0"},
            "kdp",
            False,
        ),
        (
            "exclusive whole card",
            True,
            {"nvidia.com/gpu": "1"},
            "env",
            False,
        ),
        (
            "soft slice",
            True,
            {
                "nvidia.com/gpu.sliced": "1",
                "nvidia.com/gpu.sliced.memory-percentage": "50",
                "nvidia.com/gpu.sliced.cores-percentage": "50",
            },
            "env",
            False,
        ),
        (
            "hard partition",
            True,
            {
                "nvidia.com/gpu.partitioned": "1",
                "nvidia.com/gpu.partitioned.mig-1g.20gb": "1",
            },
            "env",
            False,
        ),
        (
            "non-NVIDIA soft slice",
            True,
            {"amd.com/gpu.sliced": "1"},
            "env",
            False,
        ),
        (
            "resource key merely prefixed by a CDI kind",
            True,
            {"nvidia.com/gpu-alike": "1"},
            "env",
            True,
        ),
    ],
)
def test_resolve_privileged(name, privileged, resources, policy, expected):
    actual = _resolve_privileged(_container(privileged, resources), policy == "kdp")
    assert actual == expected, f"case {name} expected {expected}, but got {actual}"
