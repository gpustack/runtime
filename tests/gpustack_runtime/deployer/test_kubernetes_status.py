# The cases below drive the Kubernetes deployer's own status parsing, which is
# where the exit list, the image-pull verdict and the Pod Events read become
# observable without a live cluster: hand-built kubernetes.client objects plus a
# fake CoreV1Api carrying a call log.

from datetime import datetime, timezone

import kubernetes.client
import pytest

from gpustack_runtime.deployer.__types__ import WorkloadStatusStateEnum
from gpustack_runtime.deployer.kuberentes import KubernetesWorkloadStatus

_REGISTRY_MESSAGE = 'Back-off pulling image "does-not-exist.invalid/x:y"'
_EVENT_MESSAGE = (
    'Failed to pull image "does-not-exist.invalid/x:y": '
    "failed to resolve reference: unauthorized"
)


class _FakeCoreV1Api:
    """
    Stand-in for the Kubernetes CoreV1Api.

    Every call is recorded, so a test can assert that an Events read did *not*
    happen - which cannot be observed from the parsed status alone.
    """

    def __init__(
        self,
        events: list[kubernetes.client.CoreV1Event] | None = None,
        raises: Exception | None = None,
    ):
        self.calls: list[tuple[str, dict]] = []
        self._events = events or []
        self._raises = raises

    def list_namespaced_event(self, **kwargs):
        self.calls.append(("list_namespaced_event", kwargs))
        if self._raises:
            raise self._raises
        return kubernetes.client.CoreV1EventList(items=self._events)


def _event(
    message: str = _EVENT_MESSAGE,
    type_: str = "Warning",
    reason: str = "Failed",
    last_timestamp: datetime | None = None,
) -> kubernetes.client.CoreV1Event:
    return kubernetes.client.CoreV1Event(
        metadata=kubernetes.client.V1ObjectMeta(name="gpustack-test.1"),
        involved_object=kubernetes.client.V1ObjectReference(
            kind="Pod",
            name="gpustack-test",
            namespace="default",
        ),
        message=message,
        reason=reason,
        type=type_,
        last_timestamp=last_timestamp,
    )


def _pod(
    phase: str,
    container_statuses: list[kubernetes.client.V1ContainerStatus] | None = None,
    init_container_statuses: list[kubernetes.client.V1ContainerStatus] | None = None,
    init_containers: list[kubernetes.client.V1Container] | None = None,
    message: str | None = None,
    annotations: dict[str, str] | None = None,
    container_names: list[str] | None = None,
    uid: str | None = "gpustack-test-uid",
) -> kubernetes.client.V1Pod:
    return kubernetes.client.V1Pod(
        metadata=kubernetes.client.V1ObjectMeta(
            name="gpustack-test",
            namespace="default",
            uid=uid,
            creation_timestamp=datetime(2026, 8, 14, 8, 0, 0, tzinfo=timezone.utc),
            labels={"app": "test"},
            annotations=annotations,
        ),
        spec=kubernetes.client.V1PodSpec(
            containers=[
                kubernetes.client.V1Container(name=n)
                for n in (container_names or ["run-0"])
            ],
            init_containers=init_containers,
        ),
        status=kubernetes.client.V1PodStatus(
            phase=phase,
            message=message,
            container_statuses=container_statuses,
            init_container_statuses=init_container_statuses,
        ),
    )


def _waiting_status(
    name: str = "run-0",
    reason: str = "ImagePullBackOff",
    message: str = _REGISTRY_MESSAGE,
    restart_count: int = 0,
) -> kubernetes.client.V1ContainerStatus:
    return kubernetes.client.V1ContainerStatus(
        name=name,
        image="does-not-exist.invalid/x:y",
        image_id="",
        ready=False,
        restart_count=restart_count,
        state=kubernetes.client.V1ContainerState(
            waiting=kubernetes.client.V1ContainerStateWaiting(
                reason=reason,
                message=message,
            ),
        ),
    )


def _terminated_status(
    name: str = "run-0",
    exit_code: int = 1,
    reason: str = "Error",
    restart_count: int = 2,
) -> kubernetes.client.V1ContainerStatus:
    return kubernetes.client.V1ContainerStatus(
        name=name,
        image="gpustack/runner:latest",
        image_id="",
        ready=False,
        restart_count=restart_count,
        state=kubernetes.client.V1ContainerState(
            terminated=kubernetes.client.V1ContainerStateTerminated(
                exit_code=exit_code,
                reason=reason,
                message="the process exited",
                started_at=datetime(2026, 8, 14, 8, 1, 0, tzinfo=timezone.utc),
                finished_at=datetime(2026, 8, 14, 8, 2, 0, tzinfo=timezone.utc),
            ),
        ),
    )


def _restarted_status(
    name: str = "run-0",
) -> kubernetes.client.V1ContainerStatus:
    # A container that has been restarted after an OOM kill: the current state
    # is running, the termination only survives in last_state.
    return kubernetes.client.V1ContainerStatus(
        name=name,
        image="gpustack/runner:latest",
        image_id="",
        ready=True,
        restart_count=1,
        state=kubernetes.client.V1ContainerState(
            running=kubernetes.client.V1ContainerStateRunning(
                started_at=datetime(2026, 8, 14, 8, 3, 0, tzinfo=timezone.utc),
            ),
        ),
        last_state=kubernetes.client.V1ContainerState(
            terminated=kubernetes.client.V1ContainerStateTerminated(
                exit_code=137,
                reason="OOMKilled",
                started_at=datetime(2026, 8, 14, 8, 1, 0, tzinfo=timezone.utc),
                finished_at=datetime(2026, 8, 14, 8, 2, 0, tzinfo=timezone.utc),
            ),
        ),
    )


def _ready_status(name: str = "run-0") -> kubernetes.client.V1ContainerStatus:
    return kubernetes.client.V1ContainerStatus(
        name=name,
        image="gpustack/runner:latest",
        image_id="",
        ready=True,
        restart_count=0,
        state=kubernetes.client.V1ContainerState(
            running=kubernetes.client.V1ContainerStateRunning(
                started_at=datetime(2026, 8, 14, 8, 1, 0, tzinfo=timezone.utc),
            ),
        ),
    )


def test_image_pull_backoff_reports_failed_with_event_message():
    api = _FakeCoreV1Api(events=[_event()])

    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod("Pending", container_statuses=[_waiting_status()]),
        core_api=api,
    )

    # A Pod stuck on an unpullable image has failed, it is not pending.
    assert status.state == WorkloadStatusStateEnum.FAILED
    # The waiting reason alone omits the registry error, so the Event message
    # is appended to it.
    assert "ImagePullBackOff" in status.state_message
    assert _REGISTRY_MESSAGE in status.state_message
    assert _EVENT_MESSAGE in status.state_message

    assert len(status.exits) == 1
    assert status.exits[0].name == "run-0"
    assert status.exits[0].token == "run-0"  # noqa: S105
    assert status.exits[0].exit_code is None
    assert status.exits[0].reason == "ImagePullBackOff"
    assert status.exits[0].message == _REGISTRY_MESSAGE

    # Exactly one Events read, field-selected to this Pod. The UID is part of the
    # selector because a recreated Pod takes the same name.
    assert [c[0] for c in api.calls] == ["list_namespaced_event"]
    assert api.calls[0][1]["namespace"] == "default"
    assert api.calls[0][1]["field_selector"] == (
        "involvedObject.name=gpustack-test,involvedObject.uid=gpustack-test-uid"
    )


def test_pod_event_message_takes_the_latest_by_timestamp():
    # The API returns Events in no particular order, so the newest one is not
    # necessarily last in the list.
    stale = _event(
        message="0/3 nodes are available: insufficient nvidia.com/gpu.",
        reason="FailedScheduling",
        last_timestamp=datetime(2026, 8, 14, 7, 0, 0, tzinfo=timezone.utc),
    )
    latest = _event(last_timestamp=datetime(2026, 8, 14, 9, 0, 0, tzinfo=timezone.utc))
    api = _FakeCoreV1Api(events=[latest, stale])

    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod("Pending", container_statuses=[_waiting_status()]),
        core_api=api,
    )

    assert _EVENT_MESSAGE in status.state_message
    assert "FailedScheduling" not in status.state_message


def test_pod_event_message_prefers_the_image_pull_failure():
    # A warning about something else, stamped later than the pull failure, must
    # not stand in as the diagnosis of an image pull that cannot succeed.
    later_unrelated = _event(
        message="Liveness probe failed: connection refused",
        reason="Unhealthy",
        last_timestamp=datetime(2026, 8, 14, 10, 0, 0, tzinfo=timezone.utc),
    )
    pull_failure = _event(
        last_timestamp=datetime(2026, 8, 14, 9, 0, 0, tzinfo=timezone.utc),
    )
    api = _FakeCoreV1Api(events=[pull_failure, later_unrelated])

    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod("Pending", container_statuses=[_waiting_status()]),
        core_api=api,
    )

    assert _EVENT_MESSAGE in status.state_message
    assert "Liveness probe" not in status.state_message


@pytest.mark.parametrize(
    "reason",
    [
        "ErrImageNeverPull",
        "ErrImagePull",
        "ImagePullBackOff",
        "InvalidImageName",
        "RegistryUnavailable",
    ],
)
def test_image_pull_blocked_reasons_report_failed(reason):
    api = _FakeCoreV1Api()

    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod("Pending", container_statuses=[_waiting_status(reason=reason)]),
        core_api=api,
    )

    assert status.state == WorkloadStatusStateEnum.FAILED
    assert reason in status.state_message


def test_pending_on_another_reason_stays_pending():
    api = _FakeCoreV1Api()

    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod(
            "Pending",
            container_statuses=[
                _waiting_status(reason="ContainerCreating", message=""),
            ],
        ),
        core_api=api,
    )

    assert status.state == WorkloadStatusStateEnum.PENDING
    # Not a blocked Pod, so no Events traffic.
    assert api.calls == []


def test_terminated_container_reports_exit_code():
    api = _FakeCoreV1Api()

    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod("Failed", container_statuses=[_terminated_status()]),
        core_api=api,
    )

    assert len(status.exits) == 1
    exit_ = status.exits[0]
    assert exit_.exit_code == 1
    assert exit_.reason == "Error"
    assert exit_.message == "the process exited"
    assert exit_.started_at == "2026-08-14T08:01:00.000000Z"
    assert exit_.finished_at == "2026-08-14T08:02:00.000000Z"
    assert exit_.restart_count == 2
    # Events are read for blocked Pods only.
    assert api.calls == []


def test_restarted_container_reports_last_terminated_state():
    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod("Running", container_statuses=[_restarted_status()]),
    )

    assert len(status.exits) == 1
    assert status.exits[0].exit_code == 137
    assert status.exits[0].reason == "OOMKilled"
    assert status.exits[0].restart_count == 1


def test_events_forbidden_degrades_to_state_and_reason():
    api = _FakeCoreV1Api(
        raises=kubernetes.client.exceptions.ApiException(status=403),
    )

    # A cluster that has not applied the events RBAC rule must keep working.
    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod("Pending", container_statuses=[_waiting_status()]),
        core_api=api,
    )

    assert status.state == WorkloadStatusStateEnum.FAILED
    assert "ImagePullBackOff" in status.state_message
    assert _REGISTRY_MESSAGE in status.state_message
    assert _EVENT_MESSAGE not in status.state_message
    assert status.exits[0].reason == "ImagePullBackOff"


def test_running_pod_makes_no_events_call():
    api = _FakeCoreV1Api(events=[_event()])

    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod("Running", container_statuses=[_ready_status()]),
        core_api=api,
    )

    assert status.state == WorkloadStatusStateEnum.RUNNING
    assert status.exits == []
    assert api.calls == []


def test_running_pod_with_blocked_sidecar_reports_exit_without_events_call():
    api = _FakeCoreV1Api(events=[_event()])

    # The verdict stays readiness-based - a serving Pod does not fail because a
    # sidecar cannot pull - but the blocked container is still reported.
    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod(
            "Running",
            container_statuses=[_ready_status(), _waiting_status(name="run-1")],
            container_names=["run-0", "run-1"],
        ),
    )
    status_with_api = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod(
            "Running",
            container_statuses=[_ready_status(), _waiting_status(name="run-1")],
            container_names=["run-0", "run-1"],
        ),
        core_api=api,
    )

    assert status.state == WorkloadStatusStateEnum.INITIALIZING
    assert [e.reason for e in status.exits] == ["ImagePullBackOff"]
    assert status_with_api.state == status.state
    assert api.calls == []


def test_blocked_init_container_reports_failed_with_component_name():
    api = _FakeCoreV1Api(events=[_event()])

    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod(
            "Pending",
            init_containers=[kubernetes.client.V1Container(name="init-0")],
            init_container_statuses=[_waiting_status(name="init-0")],
            annotations={"runtime.gpustack.ai/component-init-0-name": "downloader"},
        ),
        core_api=api,
    )

    assert status.state == WorkloadStatusStateEnum.FAILED
    assert len(status.exits) == 1
    assert status.exits[0].name == "downloader"
    assert status.exits[0].token == "init-0"  # noqa: S105
    assert status.exits[0].reason == "ImagePullBackOff"
    assert [c[0] for c in api.calls] == ["list_namespaced_event"]


def test_blocked_pod_without_api_client_still_reports_failed():
    # The pre-manifest path, and any call site that has no client at hand.
    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod("Pending", container_statuses=[_waiting_status()]),
    )

    assert status.state == WorkloadStatusStateEnum.FAILED
    assert "ImagePullBackOff" in status.state_message
    assert _REGISTRY_MESSAGE in status.state_message


def test_pull_diagnosis_overrides_pod_status_message():
    status = KubernetesWorkloadStatus(
        name="test",
        k_pod=_pod(
            "Pending",
            container_statuses=[_waiting_status()],
            message="some generic pod message",
        ),
    )

    assert "some generic pod message" not in status.state_message
    assert "ImagePullBackOff" in status.state_message
