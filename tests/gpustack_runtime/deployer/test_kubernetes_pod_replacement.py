from __future__ import annotations as __future_annotations__

# ruff: noqa: SLF001
import kubernetes
import pytest

from gpustack_runtime.deployer import Container, ContainerProfileEnum, OperationError
from gpustack_runtime.deployer.__types__ import Deployer
from gpustack_runtime.deployer.kubernetes import (
    KubernetesDeployer,
    KubernetesWorkloadPlan,
)


class FakeCore:
    def __init__(self, outcomes, delete_error=None):
        self.outcomes = iter(outcomes)
        self.delete_error = delete_error
        self.calls = []

    def read_namespaced_pod(self, **kwargs):
        self.calls.append(("read", kwargs))
        value = next(self.outcomes)
        if isinstance(value, Exception):
            raise value
        return value

    def delete_namespaced_pod(self, **kwargs):
        self.calls.append(("delete", kwargs))
        if self.delete_error:
            raise self.delete_error

    def create_namespaced_pod(self, **kwargs):
        self.calls.append(("create", kwargs))
        return kwargs["body"]


def pod(uid="old"):
    return kubernetes.client.V1Pod(
        metadata=kubernetes.client.V1ObjectMeta(name="test", uid=uid),
        spec=kubernetes.client.V1PodSpec(
            termination_grace_period_seconds=0,
            containers=[kubernetes.client.V1Container(name="run", image="old")],
        ),
    )


def not_found():
    return kubernetes.client.exceptions.ApiException(status=404)


@pytest.fixture
def deploy(monkeypatch):
    monkeypatch.setattr(
        "gpustack_runtime.deployer.kubernetes.get_resource_injection_policy",
        lambda *_: "env",
    )
    monkeypatch.setattr(
        "gpustack_runtime.deployer.kubernetes._resolve_runtime_class_name",
        lambda *_: None,
    )
    monkeypatch.setattr(
        "gpustack_runtime.deployer.kubernetes.time.sleep",
        lambda _: None,
    )
    dep = object.__new__(KubernetesDeployer)
    Deployer.__init__(dep, "test")
    dep._materials = {}
    dep._client = None
    dep._node_name = None
    dep._image_pull_secret = None
    dep._mutate_create_pod = lambda p: p
    workload = KubernetesWorkloadPlan(
        name="test",
        namespace="default",
        containers=[
            Container(name="run", image="new", profile=ContainerProfileEnum.RUN),
        ],
    )
    workload.validate_and_default()

    def run(core):
        monkeypatch.setattr(kubernetes.client, "CoreV1Api", lambda _: core)
        return dep._create_pod(workload, {})

    return run


def test_pod_already_deleted_before_wait_is_recreated(deploy):
    core = FakeCore([pod(), not_found()])
    deploy(core)
    assert [call for call, _ in core.calls] == ["read", "delete", "read", "create"]
    assert core.calls[1][1]["body"].preconditions.uid == "old"


def test_pod_must_be_gone_before_replacement(deploy):
    core = FakeCore([pod(), pod(), pod(), not_found()])
    deploy(core)
    assert [call for call, _ in core.calls] == [
        "read",
        "delete",
        "read",
        "read",
        "read",
        "create",
    ]


def test_concurrent_replacement_is_never_deleted_or_overwritten(deploy):
    core = FakeCore([pod(), pod("new-uid")])
    with pytest.raises(OperationError, match="replaced"):
        deploy(core)
    assert not any(call == "create" for call, _ in core.calls)


def test_delete_precondition_conflict_does_not_create(deploy):
    core = FakeCore([pod()], kubernetes.client.exceptions.ApiException(status=409))
    with pytest.raises(OperationError):
        deploy(core)
    assert not any(call == "create" for call, _ in core.calls)


def test_pod_disappearing_before_delete_is_recreated(deploy):
    core = FakeCore([pod(), not_found()], not_found())
    deploy(core)
    assert [call for call, _ in core.calls] == ["read", "delete", "read", "create"]


def test_permission_error_during_wait_does_not_create(deploy):
    core = FakeCore([pod(), kubernetes.client.exceptions.ApiException(status=403)])
    with pytest.raises(OperationError):
        deploy(core)
    assert not any(call == "create" for call, _ in core.calls)


def test_initial_read_permission_error_does_not_create(deploy):
    core = FakeCore([kubernetes.client.exceptions.ApiException(status=403)])
    with pytest.raises(OperationError):
        deploy(core)
    assert [call for call, _ in core.calls] == ["read"]


def test_deletion_timeout_does_not_create(deploy, monkeypatch):
    core = FakeCore([pod(), pod()])
    ticks = iter([0, 0, 0, 121])
    monkeypatch.setattr(
        "gpustack_runtime.deployer.kubernetes.time.monotonic",
        lambda: next(ticks),
    )
    with pytest.raises(OperationError, match="Timed out"):
        deploy(core)
    assert not any(call == "create" for call, _ in core.calls)


def test_initially_absent_pod_is_created(deploy):
    core = FakeCore([not_found()])
    deploy(core)
    assert [call for call, _ in core.calls] == ["read", "create"]
