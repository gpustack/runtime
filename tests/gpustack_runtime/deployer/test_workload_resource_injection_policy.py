# The generated Pod and device mapping exercise the deployer's private state.
# ruff: noqa: SLF001

from __future__ import annotations as __future_annotations__

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import MagicMock

import kubernetes.client
import pytest

from gpustack_runtime import envs
from gpustack_runtime.deployer import (
    Container,
    ContainerExecution,
    ContainerResources,
    KubernetesDeployer,
    KubernetesResourceInjectionPolicyEnum,
    KubernetesWorkloadPlan,
    OperationError,
    WorkloadPlan,
    create_workload,
    list_workloads,
)
from gpustack_runtime.deployer.k8s.devicemanager import get_resource_injection_policy
from gpustack_runtime.detector import Device, ManufacturerEnum


@pytest.mark.parametrize(
    "configured, override, allocatable, expected",
    [
        ("KDP", "Env", {}, "env"),
        ("Env", "KDP", {}, "kdp"),
        ("KDP", "Auto", {}, "env"),
        ("Env", "Auto", {"nvidia.com/gpu.shared": "2"}, "kdp"),
        ("Env", "Auto", None, "kdp"),
        ("Env", None, {}, "env"),
        ("KDP", None, {}, "kdp"),
        ("Auto", None, {}, "env"),
        ("KDP", KubernetesResourceInjectionPolicyEnum.ENV, {}, "env"),
        ("Env", KubernetesResourceInjectionPolicyEnum.KDP, {}, "kdp"),
        ("KDP", KubernetesResourceInjectionPolicyEnum.AUTO, {}, "env"),
    ],
)
def test_workload_policy_precedence(
    monkeypatch,
    configured,
    override,
    allocatable,
    expected,
):
    monkeypatch.setattr(
        envs,
        "GPUSTACK_RUNTIME_KUBERNETES_RESOURCE_INJECTION_POLICY",
        configured,
    )
    probe = MagicMock(return_value=allocatable)

    assert get_resource_injection_policy(probe, policy=override) == expected
    assert probe.call_count == int((override or configured) == "Auto")
    assert configured == envs.GPUSTACK_RUNTIME_KUBERNETES_RESOURCE_INJECTION_POLICY


def _plan(policy=None, name="test", devices="all"):
    return KubernetesWorkloadPlan(
        name=name,
        namespace="default",
        resource_injection_policy=policy,
        host_ipc=True,
        containers=[
            Container(
                name="default",
                image="example.com/cache:test",
                execution=ContainerExecution(privileged=False),
                resources=ContainerResources(
                    **{"nvidia.com/devices": devices, "cpu": 2, "memory": "20Gi"},
                ),
            ),
        ],
    )


@pytest.fixture
def cluster(monkeypatch):
    monkeypatch.setattr(KubernetesDeployer, "is_supported", staticmethod(lambda: True))
    monkeypatch.setattr(KubernetesDeployer, "_get_client", staticmethod(lambda: None))
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_KUBERNETES_NODE_NAME", "node-a")
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DEPLOY_ASYNC", False)
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DEPLOY_CORRECT_RUNNER_IMAGE", False)
    monkeypatch.setattr(
        envs,
        "GPUSTACK_RUNTIME_KUBERNETES_RESOURCE_INJECTION_POLICY",
        "KDP",
    )
    monkeypatch.setattr(
        envs,
        "GPUSTACK_RUNTIME_DEPLOY_RUNTIME_VISIBLE_DEVICES_VALUE_UUID",
        {"NVIDIA_VISIBLE_DEVICES"},
    )
    devices = [
        Device(
            manufacturer=ManufacturerEnum.NVIDIA,
            index=i,
            uuid=f"GPU-{i}",
            appendix={},
        )
        for i in range(2)
    ]
    detect = MagicMock(return_value=devices)
    monkeypatch.setattr("gpustack_runtime.deployer.__types__.detect_devices", detect)
    worker = kubernetes.client.V1Pod(
        metadata=kubernetes.client.V1ObjectMeta(name="worker", namespace="default"),
        spec=kubernetes.client.V1PodSpec(
            runtime_class_name="nvidia",
            containers=[kubernetes.client.V1Container(name="default")],
        ),
    )
    monkeypatch.setattr(KubernetesDeployer, "_find_self_pod", lambda _self: worker)
    core = MagicMock()
    core.list_node.return_value = kubernetes.client.V1NodeList(
        items=[
            kubernetes.client.V1Node(
                metadata=kubernetes.client.V1ObjectMeta(name="node-a"),
                status=kubernetes.client.V1NodeStatus(
                    allocatable={"nvidia.com/gpu.shared": "2"},
                ),
            ),
        ],
    )
    core.read_namespaced_pod.side_effect = kubernetes.client.ApiException(status=404)
    pods = {}

    def create_pod(namespace, body):
        assert namespace == "default"
        pods[body.metadata.name] = body
        return body

    core.create_namespaced_pod.side_effect = create_pod
    monkeypatch.setattr(kubernetes.client, "CoreV1Api", lambda _client: core)
    monkeypatch.setattr(kubernetes.client, "NodeV1Api", lambda _client: MagicMock())
    deployer = KubernetesDeployer()
    monkeypatch.setattr("gpustack_runtime.deployer._DEPLOYERS", [deployer])
    return SimpleNamespace(
        deployer=deployer,
        core=core,
        pods=pods,
        detect=detect,
        devices=devices,
    )


def _assert_pod(pod, policy, devices="all"):
    assert pod.spec.runtime_class_name == "nvidia"
    assert pod.spec.host_ipc is True
    assert pod.spec.node_name == "node-a"
    container = pod.spec.containers[0]
    expected = {"cpu": "2", "memory": "20Gi"}
    variables = {e.name: e.value for e in container.env}
    if policy == "KDP":
        expected["nvidia.com/gpu.shared"] = "2" if devices == "all" else "1"
        assert "NVIDIA_VISIBLE_DEVICES" not in variables
        assert container.security_context.privileged is False
    else:
        assert variables["NVIDIA_VISIBLE_DEVICES"] == (
            "GPU-0,GPU-1" if devices == "all" else "GPU-0"
        )
        assert container.security_context.privileged is (devices == "all")
    assert container.resources.requests == expected
    assert container.resources.limits == expected


@pytest.mark.parametrize(
    "policy",
    [None, "Auto", "Env", "KDP", *KubernetesResourceInjectionPolicyEnum],
)
def test_policy_survives_serialization(policy):
    plan = _plan(policy)
    plan.containers[0].resources = None
    serialized = plan.to_json()
    assert json.loads(serialized)["resource_injection_policy"] == (
        str(policy) if policy is not None else None
    )
    restored = KubernetesWorkloadPlan.from_json(serialized)
    if policy is not None:
        assert (
            restored.resource_injection_policy
            is KubernetesResourceInjectionPolicyEnum(policy)
        )
    restored.validate_and_default()
    assert restored.resource_injection_policy == policy


@pytest.mark.parametrize("policy", list(KubernetesResourceInjectionPolicyEnum))
def test_enum_and_string_policies_create_equivalent_pods(cluster, policy):
    expected = "Env" if policy == KubernetesResourceInjectionPolicyEnum.ENV else "KDP"
    for name, value in (("enum", policy), ("string", policy.value)):
        plan = _plan(value, name)
        create_workload(plan)
        assert plan.resource_injection_policy is policy
        _assert_pod(cluster.pods[name], expected)


@pytest.mark.parametrize("policy", ["", "invalid", "CDI", 1])
def test_invalid_policy_is_rejected_before_cluster_access(cluster, policy):
    with pytest.raises(ValueError, match="Invalid resource injection policy"):
        create_workload(_plan(policy))
    cluster.core.list_node.assert_not_called()
    cluster.core.create_namespaced_pod.assert_not_called()


@pytest.mark.parametrize("devices", ["all", "0"])
def test_sequential_workloads_keep_independent_policies_and_device_maps(
    cluster,
    devices,
):
    for i, policy in enumerate(["KDP", "Env", None, "Env", "Auto"]):
        name = f"workload-{i}"
        create_workload(_plan(policy, name, devices))
        _assert_pod(cluster.pods[name], "Env" if policy == "Env" else "KDP", devices)
    assert cluster.core.list_node.call_count == 1
    assert envs.GPUSTACK_RUNTIME_KUBERNETES_RESOURCE_INJECTION_POLICY == "KDP"
    assert cluster.detect.call_count == 2


def test_auto_probes_each_workload_once(cluster):
    create_workload(_plan("Auto", "operator"))
    _assert_pod(cluster.pods["operator"], "KDP")
    cluster.core.list_node.return_value.items[0].status.allocatable = {
        "nvidia.com/gpu": "2",
    }
    create_workload(_plan("Auto", "stock"))
    _assert_pod(cluster.pods["stock"], "Env")
    assert cluster.core.list_node.call_count == 2


def test_generic_plan_inherits_worker_policy(cluster):
    data = vars(_plan()).copy()
    for key in ("resource_injection_policy", "domain_suffix", "service_type"):
        data.pop(key)
    create_workload(WorkloadPlan(**data))
    _assert_pod(cluster.pods["test"], "KDP")


def test_concurrent_workloads_keep_independent_device_maps(cluster):
    barrier = Barrier(2)

    def detect(*, fast):
        assert fast is False
        barrier.wait(timeout=10)
        return cluster.devices

    cluster.detect.side_effect = detect
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(create_workload, _plan(policy, policy.lower()))
            for policy in ("Env", "KDP")
        ]
        for future in futures:
            future.result(timeout=15)
    _assert_pod(cluster.pods["env"], "Env")
    _assert_pod(cluster.pods["kdp"], "KDP")


def test_failed_workload_does_not_change_next_workload_policy(cluster):
    create_pod = cluster.core.create_namespaced_pod.side_effect
    cluster.core.create_namespaced_pod.side_effect = kubernetes.client.ApiException(
        status=500,
    )
    with pytest.raises(OperationError):
        create_workload(_plan("Env", "failed"))
    cluster.core.create_namespaced_pod.side_effect = create_pod
    create_workload(_plan(None, "model"))
    _assert_pod(cluster.pods["model"], "KDP")


@pytest.mark.parametrize("policy", list(KubernetesResourceInjectionPolicyEnum))
@pytest.mark.parametrize("case", [str.lower, str.upper, str.swapcase])
def test_policy_accepts_case_insensitive_strings(cluster, policy, case):
    plan = _plan(case(policy.value))
    create_workload(plan)
    assert plan.resource_injection_policy is policy
    _assert_pod(cluster.pods["test"], "Env" if policy.value == "Env" else "KDP")


@pytest.mark.parametrize("policy", ["Env", "KDP"])
def test_concurrent_workloads_reuse_completed_device_maps(cluster, policy):
    barrier = Barrier(2)
    create_pod = cluster.core.create_namespaced_pod.side_effect

    def create(namespace, body):
        barrier.wait(timeout=10)
        return create_pod(namespace, body)

    cluster.core.create_namespaced_pod.side_effect = create
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(create_workload, _plan(policy, name))
            for name in ("first", "second")
        ]
        for future in futures:
            future.result(timeout=15)
    for pod in cluster.pods.values():
        _assert_pod(pod, policy)
    cluster.detect.assert_called_once_with(fast=False)


def test_failed_detection_is_not_cached(cluster):
    cluster.detect.side_effect = [RuntimeError("detection failed"), cluster.devices]
    with pytest.raises(RuntimeError, match="detection failed"):
        create_workload(_plan("Env", "failed"))
    create_workload(_plan("Env", "retry"))
    _assert_pod(cluster.pods["retry"], "Env")
    assert cluster.detect.call_count == 2


def test_workloads_share_default_node_and_remain_listable(cluster, monkeypatch):
    cluster.deployer._node_name = None
    select_node = MagicMock(side_effect=["node-a", "node-b"])
    monkeypatch.setattr(KubernetesDeployer, "_get_default_node_name", select_node)
    barrier = Barrier(2)
    create_pod = cluster.core.create_namespaced_pod.side_effect

    def create(namespace, body):
        barrier.wait(timeout=10)
        return create_pod(namespace, body)

    cluster.core.create_namespaced_pod.side_effect = create
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(create_workload, _plan(policy, policy.lower()))
            for policy in ("Env", "KDP")
        ]
        for future in futures:
            future.result(timeout=15)
    select_node.assert_called_once()
    for pod in cluster.pods.values():
        assert pod.spec.node_name == "node-a"
        pod.metadata.creation_timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
        pod.status = kubernetes.client.V1PodStatus(phase="Running")
    cluster.core.list_namespaced_pod.return_value = kubernetes.client.V1PodList(
        items=list(cluster.pods.values()),
    )
    assert {status.name for status in list_workloads()} == {"env", "kdp"}
