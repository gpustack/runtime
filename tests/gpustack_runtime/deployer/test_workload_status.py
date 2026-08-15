import json
from datetime import datetime, timezone

import kubernetes.client

from gpustack_runtime import envs
from gpustack_runtime.deployer.__types__ import (
    WorkloadStatus,
    WorkloadStatusExit,
    WorkloadStatusStateEnum,
)
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


def test_workload_status_exits_default_empty():
    # Deployers that report no terminated container leave the list empty,
    # so consumers can iterate it without a None check.
    status = WorkloadStatus(name="test", created_at="2026-07-29T08:00:00.000000Z")
    assert status.exits == []


def test_workload_status_no_exits_json_roundtrip():
    status = WorkloadStatus(name="test", created_at="2026-07-29T08:00:00.000000Z")
    restored = WorkloadStatus.from_json(status.to_json())
    assert restored.exits == []


def test_workload_status_exits_json_roundtrip():
    exits = [
        # A terminated container, as Docker/Podman report it.
        WorkloadStatusExit(
            name="run-0",
            token="a1b2c3",  # noqa: S106
            exit_code=137,
            reason="OOMKilled",
            message="",
            started_at="2026-07-29T08:00:00.000000Z",
            finished_at="2026-07-29T08:05:00.000000Z",
            restart_count=2,
        ),
        # A container blocked from starting, as Kubernetes reports it:
        # a reason, and no exit code at all.
        WorkloadStatusExit(
            name="run-1",
            token="run-1",  # noqa: S106
            reason="ImagePullBackOff",
            message='Back-off pulling image "does-not-exist.invalid/x:y"',
        ),
    ]
    status = WorkloadStatus(
        name="test",
        created_at="2026-07-29T08:00:00.000000Z",
        exits=exits,
    )

    restored = WorkloadStatus.from_json(status.to_json())

    assert restored.exits == exits
    assert all(isinstance(e, WorkloadStatusExit) for e in restored.exits)
    assert restored.exits[1].exit_code is None


def test_workload_status_exits_absent_in_legacy_payload():
    # A payload serialized before the exit list existed must keep deserializing.
    legacy = json.dumps(
        {
            "name": "test",
            "created_at": "2026-07-29T08:00:00.000000Z",
            "namespace": None,
            "labels": {},
            "annotations": {},
            "state_message": "",
            "executable": [],
            "loggable": [],
            "state": "Unknown",
        },
    )

    restored = WorkloadStatus.from_json(legacy)

    assert restored.exits == []
    assert restored.state == WorkloadStatusStateEnum.UNKNOWN


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


def test_kubernetes_workload_status_state_message():
    pod = _pod()
    pod.status = kubernetes.client.V1PodStatus(
        phase="Failed",
        message="Allocate failed due to no enough GPU devices",
    )
    status = KubernetesWorkloadStatus(name="test", k_pod=pod)
    assert status.state_message == "Allocate failed due to no enough GPU devices"


def test_kubernetes_workload_status_state_message_default():
    status = KubernetesWorkloadStatus(name="test", k_pod=_pod())
    assert status.state_message == ""


from gpustack_runtime.deployer.kuberentes import (  # noqa: E402
    _apply_instance_type_admission,
)


def test_apply_instance_type_admission_stamps_entrance_queue():
    pod = _pinning_pod()
    _apply_instance_type_admission(
        pod,
        "gpustack--nvidia-geforce-rtx-4090-linux-amd64",
    )
    # Vector observed on a live operator cluster.
    assert (
        pod.metadata.labels["kueue.x-k8s.io/queue-name"]
        == "gpustack-fnv64-1a7145d0a5248d55"
    )


def test_apply_instance_type_admission_skipped_without_instance_type():
    pod = _pinning_pod()
    _apply_instance_type_admission(pod, None)
    assert "kueue.x-k8s.io/queue-name" not in pod.metadata.labels


def test_apply_instance_type_admission_disabled_by_env(monkeypatch):
    monkeypatch.setenv("GPUSTACK_RUNTIME_KUBERNETES_KDP_NO_KUEUE_ADMISSION", "true")

    monkeypatch.setattr(
        envs,
        "GPUSTACK_RUNTIME_KUBERNETES_KDP_NO_KUEUE_ADMISSION",
        True,
    )
    pod = _pinning_pod()
    _apply_instance_type_admission(pod, "some-type")
    assert "kueue.x-k8s.io/queue-name" not in pod.metadata.labels
