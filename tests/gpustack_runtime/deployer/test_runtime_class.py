import logging

import kubernetes
import pytest

from gpustack_runtime import envs
from gpustack_runtime.deployer.kuberentes import (
    _match_runtime_class,
    _resolve_runtime_class_name,
)


@pytest.mark.parametrize(
    "name, resource_key, expected",
    [
        ("base key", "huawei.com/npu", "ascend"),
        ("sliced variant", "huawei.com/npu.sliced", "ascend"),
        ("sliced units variant", "huawei.com/npu.sliced.units", "ascend"),
        ("shared variant", "nvidia.com/gpu.shared", "nvidia"),
        (
            "partitioned variant",
            "nvidia.com/gpu.partitioned.mig-1g.20gb",
            "nvidia",
        ),
        ("unmapped base key", "hygon.com/dcu", ""),
        ("unrelated resource", "cpu", ""),
        ("gpustack automap key", "gpustack.ai/devices", ""),
        ("key merely prefixed by a base key", "nvidia.com/gpu-alike", ""),
    ],
)
def test_match_runtime_class(name, resource_key, expected):
    actual = _match_runtime_class(resource_key)
    assert actual == expected, f"case {name} expected {expected}, but got {actual}"


def _pod(
    containers_requests: list[dict | None],
    runtime_class_name: str | None = None,
) -> kubernetes.client.V1Pod:
    containers = []
    for i, requests in enumerate(containers_requests):
        container = kubernetes.client.V1Container(name=f"container-{i}")
        if requests is not None:
            container.resources = kubernetes.client.V1ResourceRequirements(
                requests=requests,
                limits=requests,
            )
        containers.append(container)
    return kubernetes.client.V1Pod(
        spec=kubernetes.client.V1PodSpec(
            containers=containers,
            runtime_class_name=runtime_class_name,
        ),
    )


def _fake_node_api(monkeypatch, existing: set[str], forbidden: bool = False):
    """
    Patch NodeV1Api so that reading a RuntimeClass succeeds for names in
    `existing`, raises 403 when `forbidden`, and 404 otherwise.
    """
    calls = []

    class FakeNodeV1Api:
        def __init__(self, client):
            pass

        def read_runtime_class(self, name):
            calls.append(name)
            if forbidden:
                raise kubernetes.client.exceptions.ApiException(
                    status=403,
                    reason="Forbidden",
                )
            if name not in existing:
                raise kubernetes.client.exceptions.ApiException(
                    status=404,
                    reason="Not Found",
                )
            return kubernetes.client.V1RuntimeClass(
                metadata=kubernetes.client.V1ObjectMeta(name=name),
                handler=name,
            )

    monkeypatch.setattr(kubernetes.client, "NodeV1Api", FakeNodeV1Api)
    return calls


def test_resolve_runtime_class_name_base_key(monkeypatch):
    _fake_node_api(monkeypatch, existing={"nvidia"})
    pod = _pod([{"nvidia.com/gpu": "1"}])

    _resolve_runtime_class_name(pod, None)

    assert pod.spec.runtime_class_name == "nvidia"


def test_resolve_runtime_class_name_suffixed_family(monkeypatch):
    _fake_node_api(monkeypatch, existing={"ascend"})
    pod = _pod(
        [
            {
                "huawei.com/npu.sliced": "1",
                "huawei.com/npu.sliced.cores-percentage": "30",
                "huawei.com/npu.sliced.memory-percentage": "20",
            },
        ],
    )

    _resolve_runtime_class_name(pod, None)

    assert pod.spec.runtime_class_name == "ascend"


def test_resolve_runtime_class_name_missing_class(monkeypatch, caplog):
    _fake_node_api(monkeypatch, existing=set())
    pod = _pod([{"huawei.com/npu.sliced": "1"}])

    with caplog.at_level(logging.WARNING):
        _resolve_runtime_class_name(pod, None)

    assert pod.spec.runtime_class_name is None
    assert "ascend" in caplog.text


def test_resolve_runtime_class_name_forbidden(monkeypatch, caplog):
    _fake_node_api(monkeypatch, existing={"ascend"}, forbidden=True)
    pod = _pod([{"huawei.com/npu.sliced": "1"}])

    with caplog.at_level(logging.WARNING):
        _resolve_runtime_class_name(pod, None)

    assert pod.spec.runtime_class_name is None
    assert "ascend" in caplog.text
    assert "permission" in caplog.text


def test_resolve_runtime_class_name_never_overwritten(monkeypatch):
    calls = _fake_node_api(monkeypatch, existing={"ascend"})
    pod = _pod([{"huawei.com/npu.sliced": "1"}], runtime_class_name="custom")

    _resolve_runtime_class_name(pod, None)

    assert pod.spec.runtime_class_name == "custom"
    assert not calls


def test_resolve_runtime_class_name_conflict_keeps_first(monkeypatch, caplog):
    _fake_node_api(monkeypatch, existing={"nvidia", "ascend"})
    pod = _pod(
        [
            {"nvidia.com/gpu": "1"},
            {"huawei.com/npu.sliced": "1"},
        ],
    )

    with caplog.at_level(logging.WARNING):
        _resolve_runtime_class_name(pod, None)

    assert pod.spec.runtime_class_name == "nvidia"
    assert "ascend" in caplog.text


def test_resolve_runtime_class_name_no_mapped_resources(monkeypatch):
    calls = _fake_node_api(monkeypatch, existing={"nvidia"})
    pod = _pod([{"cpu": "2", "memory": "4Gi"}, None])

    _resolve_runtime_class_name(pod, None)

    assert pod.spec.runtime_class_name is None
    assert not calls


def test_default_runtime_class_map():
    mapping = envs.GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_RUNTIME_CLASS
    assert mapping == {
        "amd.com/gpu": "amd",
        "huawei.com/npu": "ascend",
        "cambricon.com/mlu": "cambricon",
        "iluvatar.com/gpu": "iluvatar",
        "mthreads.com/gpu": "mthreads",
        "nvidia.com/gpu": "nvidia",
    }
