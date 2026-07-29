from datetime import datetime, timezone

import kubernetes.client

from gpustack_runtime.deployer.__types__ import WorkloadStatus
from gpustack_runtime.deployer.kuberentes import KubernetesWorkloadStatus


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
