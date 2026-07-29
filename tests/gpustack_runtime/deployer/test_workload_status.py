from datetime import datetime, timezone

import kubernetes.client

from gpustack_runtime.deployer.__types__ import WorkloadStatus
from gpustack_runtime.deployer.kuberentes import (
    KubernetesWorkloadStatus,
    _pin_pod_for_kueue,
)


def _pod(annotations=None) -> kubernetes.client.V1Pod:
    return kubernetes.client.V1Pod(
        metadata=kubernetes.client.V1ObjectMeta(
            name="gpustack-test",
            namespace="default",
            creation_timestamp=datetime(2026, 7, 29, 8, 0, 0, tzinfo=timezone.utc),
            labels={"app": "test"},
            annotations=annotations,
        ),
        spec=kubernetes.client.V1PodSpec(
            containers=[kubernetes.client.V1Container(name="run-0")],
        ),
        status=kubernetes.client.V1PodStatus(phase="Pending"),
    )


def test_kubernetes_workload_status_annotations():
    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod(
            annotations={
                "device.gpustack.ai/accelerator.allocated": '{"run-0": {}}',
                "runtime.gpustack.ai/component-run-0-name": "internal",
            },
        ),
    )
    # Third-party annotations (e.g. device-plugin allocation) are surfaced,
    # runtime-internal annotations are filtered out.
    assert status.annotations == {
        "device.gpustack.ai/accelerator.allocated": '{"run-0": {}}',
    }


def test_kubernetes_workload_status_annotations_absent():
    status = KubernetesWorkloadStatus(name="test", k_pod=_pod(annotations=None))
    assert status.annotations == {}


def test_workload_status_annotations_default_empty():
    # Docker/Podman deployers don't populate annotations; the base default is empty.
    status = WorkloadStatus(name="test", created_at="2026-07-29T08:00:00.000000Z")
    assert status.annotations == {}


def test_workload_status_annotations_json_roundtrip():
    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod(annotations={"device.gpustack.ai/accelerator.allocated": "{}"}),
    )
    restored = WorkloadStatus.from_json(status.to_json())
    assert restored.annotations == status.annotations


def _pinning_pod(labels=None) -> kubernetes.client.V1Pod:
    return kubernetes.client.V1Pod(
        metadata=kubernetes.client.V1ObjectMeta(name="p", labels=labels or {}),
        spec=kubernetes.client.V1PodSpec(
            node_name="node-a",
            containers=[kubernetes.client.V1Container(name="run-0")],
        ),
    )


def test_pin_pod_for_kueue_switches_to_node_selector():
    pod = _pinning_pod(labels={"kueue.x-k8s.io/queue-name": "q"})
    _pin_pod_for_kueue(pod, "node-a")
    assert pod.spec.node_name is None
    assert pod.spec.node_selector == {"kubernetes.io/hostname": "node-a"}


def test_pin_pod_for_kueue_merges_existing_node_selector():
    pod = _pinning_pod(labels={"kueue.x-k8s.io/queue-name": "q"})
    pod.spec.node_selector = {"disk": "ssd"}
    _pin_pod_for_kueue(pod, "node-a")
    assert pod.spec.node_selector == {
        "disk": "ssd",
        "kubernetes.io/hostname": "node-a",
    }


def test_pin_pod_for_kueue_noop_without_queue_label():
    pod = _pinning_pod()
    _pin_pod_for_kueue(pod, "node-a")
    assert pod.spec.node_name == "node-a"
    assert pod.spec.node_selector is None


def test_pin_pod_for_kueue_noop_without_node_name():
    pod = _pinning_pod(labels={"kueue.x-k8s.io/queue-name": "q"})
    _pin_pod_for_kueue(pod, None)
    assert pod.spec.node_name == "node-a"
