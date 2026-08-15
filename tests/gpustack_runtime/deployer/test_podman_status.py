from types import SimpleNamespace

from gpustack_runtime.deployer.__types__ import WorkloadStatusStateEnum
from gpustack_runtime.deployer.podman import PodmanWorkloadStatus


def _container(
    status: str,
    component: str = "run",
    name: str = "gpustack-test-run-0",
    component_name: str = "run-0",
    container_id: str = "abc123",
    state: dict | None = None,
    restart_policy: str = "no",
    restart_count: int | None = None,
    created: str = "2026-07-29T08:00:00.000000Z",
) -> SimpleNamespace:
    attrs = {
        "Id": container_id,
        "Created": created,
        "HostConfig": {"RestartPolicy": {"Name": restart_policy}},
        "State": state if state is not None else {},
    }
    if restart_count is not None:
        attrs["RestartCount"] = restart_count

    return SimpleNamespace(
        name=name,
        status=status,
        attrs=attrs,
        labels={
            "runtime.gpustack.ai/component": component,
            "runtime.gpustack.ai/component-name": component_name,
        },
    )


def test_podman_workload_status_exit_oomkilled_preserves_state():
    # A container exited 137 with OOMKilled true: the existing state verdict
    # (FAILED, no restart policy) is preserved, and an exit entry carries the
    # exit code plus the OOMKilled reason.
    c = _container(
        status="exited",
        state={
            "ExitCode": 137,
            "OOMKilled": True,
            "Error": "",
            "StartedAt": "2026-07-29T08:00:00.000000Z",
            "FinishedAt": "2026-07-29T08:05:00.000000Z",
        },
        restart_count=2,
    )

    status = PodmanWorkloadStatus(name="test", d_containers=[c])

    assert status.state == WorkloadStatusStateEnum.FAILED
    assert len(status.exits) == 1
    exit_ = status.exits[0]
    assert exit_.name == "run-0"
    assert exit_.token == "abc123"  # noqa: S105
    assert exit_.exit_code == 137
    assert exit_.reason == "OOMKilled"
    assert exit_.started_at == "2026-07-29T08:00:00.000000Z"
    assert exit_.finished_at == "2026-07-29T08:05:00.000000Z"
    assert exit_.restart_count == 2
    assert status.state_message == "OOMKilled"


def test_podman_workload_status_running_container_no_exit_entry():
    # A running container contributes no exit entry.
    c = _container(status="running", state={"ExitCode": 0})

    status = PodmanWorkloadStatus(name="test", d_containers=[c])

    assert status.exits == []


def test_podman_workload_status_exit_missing_state_keys_degrades():
    # A container whose State lacks the keys degrades to an entry with the
    # exit code alone rather than raising.
    c = _container(status="exited", state={"ExitCode": 1})

    status = PodmanWorkloadStatus(name="test", d_containers=[c])

    assert status.state == WorkloadStatusStateEnum.FAILED
    assert len(status.exits) == 1
    exit_ = status.exits[0]
    assert exit_.exit_code == 1
    # The State is Docker-compatible, so both backends derive the reason from the
    # code and agree with what Kubernetes reports for the same event.
    assert exit_.reason == "Error"
    assert exit_.message == ""
    assert exit_.started_at == ""
    assert exit_.finished_at == ""
    assert exit_.restart_count == 0
    assert status.state_message == "Error"


def test_podman_workload_status_exit_ignores_gos_zero_time():
    c = _container(
        status="exited",
        state={
            "ExitCode": 1,
            "StartedAt": "0001-01-01T00:00:00Z",
            "FinishedAt": "0001-01-01T00:00:00Z",
        },
    )

    status = PodmanWorkloadStatus(name="test", d_containers=[c])

    assert status.exits[0].started_at == ""
    assert status.exits[0].finished_at == ""


def test_podman_workload_status_exit_covers_a_restarting_container():
    c = _container(status="restarting", state={"ExitCode": 1})

    status = PodmanWorkloadStatus(name="test", d_containers=[c])

    assert len(status.exits) == 1
    assert status.exits[0].exit_code == 1


def test_podman_workload_status_exit_includes_init_containers():
    # Init containers get an exit entry too, not just run containers.
    init_c = _container(
        status="exited",
        component="init",
        name="gpustack-test-init-0",
        component_name="init-0",
        container_id="init-abc",
        state={"ExitCode": 1, "Error": "context deadline exceeded"},
    )
    run_c = _container(status="running", state={})

    status = PodmanWorkloadStatus(name="test", d_containers=[init_c, run_c])

    assert len(status.exits) == 1
    exit_ = status.exits[0]
    assert exit_.name == "init-0"
    assert exit_.token == "init-abc"  # noqa: S105
    assert exit_.exit_code == 1
    assert exit_.reason == "Error"
    assert exit_.message == "context deadline exceeded"
