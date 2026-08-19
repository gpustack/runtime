# The cases below drive the deployers' own deletion path, which is where the
# graceful termination becomes observable without a live Docker daemon, Podman
# socket or Kubernetes cluster.
# ruff: noqa: SLF001

from types import SimpleNamespace

import pytest

from gpustack_runtime.deployer.__types__ import (
    DEFAULT_TERMINATION_GRACE_PERIOD_SECONDS,
    Container,
    ContainerProfileEnum,
    Deployer,
    WorkloadPlan,
)
from gpustack_runtime.deployer.docker import (
    _LABEL_COMPONENT as _DOCKER_LABEL_COMPONENT,
)
from gpustack_runtime.deployer.docker import (
    _LABEL_TERMINATION_GRACE_PERIOD_SECONDS as _DOCKER_LABEL_GRACE_PERIOD,
)
from gpustack_runtime.deployer.docker import (
    DockerDeployer,
    DockerWorkloadPlan,
)


def _plan(**kwargs) -> WorkloadPlan:
    return WorkloadPlan(
        name="test",
        labels={},
        containers=[
            Container(
                name="run",
                image="busybox:1.37",
                profile=ContainerProfileEnum.RUN,
            ),
        ],
        **kwargs,
    )


def _recorder() -> SimpleNamespace:
    calls = []
    return SimpleNamespace(
        calls=calls,
        _delete=lambda *args, **kwargs: calls.append((args, kwargs)),
    )


def test_workload_plan_defaults_the_termination_grace_period():
    # An unset termination grace period falls back to the package default.
    plan = _plan()
    plan.validate_and_default()

    assert DEFAULT_TERMINATION_GRACE_PERIOD_SECONDS == 15
    assert plan.termination_grace_period_seconds == 15


def test_workload_plan_keeps_a_zero_termination_grace_period():
    # Zero means "kill immediately", it must survive defaulting.
    plan = _plan(termination_grace_period_seconds=0)
    plan.validate_and_default()

    assert plan.termination_grace_period_seconds == 0


def test_workload_plan_defaults_a_none_termination_grace_period():
    # A JSON payload carrying an explicit null degrades to the default.
    plan = _plan(termination_grace_period_seconds=None)
    plan.validate_and_default()

    assert plan.termination_grace_period_seconds == 15


def test_workload_plan_rejects_a_negative_termination_grace_period():
    plan = _plan(termination_grace_period_seconds=-1)

    with pytest.raises(ValueError, match="grace period"):
        plan.validate_and_default()


def test_deployer_delete_forwards_the_grace_period():
    # The delete facade hands the grace period down to the deployer implementation.
    rec = _recorder()

    Deployer.delete(rec, name="test", grace_period_seconds=5, async_mode=False)

    assert rec.calls == [(("test", None, 5), {})]


def test_deployer_delete_defaults_the_grace_period_to_none():
    # Without an explicit override, the implementation resolves the grace period itself.
    rec = _recorder()

    Deployer.delete(rec, name="test", async_mode=False)

    assert rec.calls == [(("test", None, None), {})]


class _FakeContainer:
    """
    A container recording the deletion calls it receives into a shared journal.
    """

    def __init__(
        self,
        journal: list,
        name: str,
        component: str,
        labels: dict[str, str] | None = None,
    ):
        self.name = name
        self.labels = {**(labels or {}), _DOCKER_LABEL_COMPONENT: component}
        self.journal = journal

    def stop(self, **kwargs):
        self.journal.append(("stop", self.name, kwargs))

    def remove(self, **kwargs):
        self.journal.append(("remove", self.name, kwargs))


def _docker_containers(journal: list, labels: dict[str, str] | None = None) -> list:
    return [
        _FakeContainer(journal, "test-pause", "pause", labels),
        _FakeContainer(journal, "test-init-0", "init", labels),
        _FakeContainer(journal, "test-run-1", "run", labels),
        _FakeContainer(journal, "test-unhealthy-restart", "unhealthy-restart", labels),
    ]


def _docker_deployer(containers: list) -> SimpleNamespace:
    workload = SimpleNamespace(_d_containers=containers)
    return SimpleNamespace(
        is_supported=lambda: True,
        get=lambda **_kwargs: workload,
        _client=SimpleNamespace(
            volumes=SimpleNamespace(list=lambda **_kwargs: []),
        ),
    )


def test_docker_delete_drains_the_workload_before_removing_it():
    # The unhealthy restart container goes first, otherwise it restarts the
    # containers being drained; the pause container goes last, as it holds the
    # namespaces the others share.
    journal = []
    dep = _docker_deployer(_docker_containers(journal))

    DockerDeployer._delete(dep, name="test", grace_period_seconds=20)

    assert journal == [
        ("remove", "test-unhealthy-restart", {"force": True}),
        ("stop", "test-init-0", {"timeout": 20}),
        ("remove", "test-init-0", {"force": True}),
        ("stop", "test-run-1", {"timeout": 20}),
        ("remove", "test-run-1", {"force": True}),
        ("remove", "test-pause", {"force": True}),
    ]


def test_docker_delete_reads_the_grace_period_from_the_container_label():
    # Without an explicit override, the grace period declared by the workload
    # plan is read back from the container label.
    journal = []
    dep = _docker_deployer(
        _docker_containers(journal, labels={_DOCKER_LABEL_GRACE_PERIOD: "30"}),
    )

    DockerDeployer._delete(dep, name="test")

    assert [c for c in journal if c[0] == "stop"] == [
        ("stop", "test-init-0", {"timeout": 30}),
        ("stop", "test-run-1", {"timeout": 30}),
    ]


def test_docker_delete_falls_back_to_the_default_grace_period():
    # A workload created before the grace period existed carries no label.
    journal = []
    dep = _docker_deployer(_docker_containers(journal))

    DockerDeployer._delete(dep, name="test")

    assert [c for c in journal if c[0] == "stop"] == [
        ("stop", "test-init-0", {"timeout": 15}),
        ("stop", "test-run-1", {"timeout": 15}),
    ]


def test_docker_delete_kills_immediately_on_a_zero_grace_period():
    journal = []
    dep = _docker_deployer(
        _docker_containers(journal, labels={_DOCKER_LABEL_GRACE_PERIOD: "30"}),
    )

    DockerDeployer._delete(dep, name="test", grace_period_seconds=0)

    assert [c for c in journal if c[0] == "stop"] == [
        ("stop", "test-init-0", {"timeout": 0}),
        ("stop", "test-run-1", {"timeout": 0}),
    ]


def test_docker_workload_plan_stamps_the_grace_period_label():
    # The grace period must survive the create/delete round trip, as the Docker
    # models layer cannot carry a create time stop timeout.
    plan = DockerWorkloadPlan(
        name="test",
        termination_grace_period_seconds=30,
        containers=[
            Container(
                name="run",
                image="busybox:1.37",
                profile=ContainerProfileEnum.RUN,
            ),
        ],
    )
    plan.validate_and_default()

    assert plan.labels[_DOCKER_LABEL_GRACE_PERIOD] == "30"


def test_docker_workload_plan_stamps_the_defaulted_grace_period_label():
    plan = DockerWorkloadPlan(
        name="test",
        termination_grace_period_seconds=None,
        containers=[
            Container(
                name="run",
                image="busybox:1.37",
                profile=ContainerProfileEnum.RUN,
            ),
        ],
    )
    plan.validate_and_default()

    assert plan.labels[_DOCKER_LABEL_GRACE_PERIOD] == "15"
