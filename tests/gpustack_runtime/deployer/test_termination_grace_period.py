# The cases below drive the deployers' own deletion path, which is where the
# graceful termination becomes observable without a live Docker daemon, Podman
# socket or Kubernetes cluster.
# ruff: noqa: SLF001

from types import SimpleNamespace

import kubernetes.client
import kubernetes.client.exceptions
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
from gpustack_runtime.deployer.kuberentes import (
    KubernetesDeployer,
    KubernetesWorkloadPlan,
)
from gpustack_runtime.deployer.podman import (
    _LABEL_COMPONENT as _PODMAN_LABEL_COMPONENT,
)
from gpustack_runtime.deployer.podman import (
    _LABEL_TERMINATION_GRACE_PERIOD_SECONDS as _PODMAN_LABEL_GRACE_PERIOD,
)
from gpustack_runtime.deployer.podman import (
    PodmanDeployer,
    PodmanWorkloadPlan,
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
        component_label: str,
        labels: dict[str, str] | None = None,
    ):
        self.name = name
        self.labels = {**(labels or {}), component_label: component}
        self.journal = journal

    def stop(self, **kwargs):
        self.journal.append(("stop", self.name, kwargs))

    def remove(self, **kwargs):
        self.journal.append(("remove", self.name, kwargs))


def _containers(
    journal: list,
    component_label: str,
    labels: dict[str, str] | None = None,
) -> list:
    return [
        _FakeContainer(journal, "test-pause", "pause", component_label, labels),
        _FakeContainer(journal, "test-init-0", "init", component_label, labels),
        _FakeContainer(journal, "test-run-1", "run", component_label, labels),
        _FakeContainer(
            journal,
            "test-unhealthy-restart",
            "unhealthy-restart",
            component_label,
            labels,
        ),
    ]


def _docker_containers(journal: list, labels: dict[str, str] | None = None) -> list:
    return _containers(journal, _DOCKER_LABEL_COMPONENT, labels)


def _podman_containers(journal: list, labels: dict[str, str] | None = None) -> list:
    return _containers(journal, _PODMAN_LABEL_COMPONENT, labels)


def _deployer(containers: list) -> SimpleNamespace:
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
    dep = _deployer(_docker_containers(journal))

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
    dep = _deployer(
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
    dep = _deployer(_docker_containers(journal))

    DockerDeployer._delete(dep, name="test")

    assert [c for c in journal if c[0] == "stop"] == [
        ("stop", "test-init-0", {"timeout": 15}),
        ("stop", "test-run-1", {"timeout": 15}),
    ]


def test_docker_delete_kills_immediately_on_a_zero_grace_period():
    journal = []
    dep = _deployer(
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


def test_podman_delete_drains_the_workload_before_removing_it():
    # Mirrors the Docker deletion path, but tolerates the "already stopped"
    # answer, which podman-py cannot decode on its own.
    journal = []
    dep = _deployer(_podman_containers(journal))

    PodmanDeployer._delete(dep, name="test", grace_period_seconds=20)

    assert journal == [
        ("remove", "test-unhealthy-restart", {"force": True}),
        ("stop", "test-init-0", {"timeout": 20, "ignore": True}),
        ("remove", "test-init-0", {"force": True}),
        ("stop", "test-run-1", {"timeout": 20, "ignore": True}),
        ("remove", "test-run-1", {"force": True}),
        ("remove", "test-pause", {"force": True}),
    ]


def test_podman_delete_reads_the_grace_period_from_the_container_label():
    journal = []
    dep = _deployer(
        _podman_containers(journal, labels={_PODMAN_LABEL_GRACE_PERIOD: "30"}),
    )

    PodmanDeployer._delete(dep, name="test")

    assert [c for c in journal if c[0] == "stop"] == [
        ("stop", "test-init-0", {"timeout": 30, "ignore": True}),
        ("stop", "test-run-1", {"timeout": 30, "ignore": True}),
    ]


def test_podman_delete_falls_back_to_the_default_grace_period():
    journal = []
    dep = _deployer(_podman_containers(journal))

    PodmanDeployer._delete(dep, name="test")

    assert [c for c in journal if c[0] == "stop"] == [
        ("stop", "test-init-0", {"timeout": 15, "ignore": True}),
        ("stop", "test-run-1", {"timeout": 15, "ignore": True}),
    ]


def test_podman_delete_kills_immediately_on_a_zero_grace_period():
    journal = []
    dep = _deployer(
        _podman_containers(journal, labels={_PODMAN_LABEL_GRACE_PERIOD: "30"}),
    )

    PodmanDeployer._delete(dep, name="test", grace_period_seconds=0)

    assert [c for c in journal if c[0] == "stop"] == [
        ("stop", "test-init-0", {"timeout": 0, "ignore": True}),
        ("stop", "test-run-1", {"timeout": 0, "ignore": True}),
    ]


def test_podman_workload_plan_stamps_the_grace_period_label():
    plan = PodmanWorkloadPlan(
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

    assert plan.labels[_PODMAN_LABEL_GRACE_PERIOD] == "30"


def test_kubernetes_pod_declares_the_termination_grace_period(monkeypatch):
    # The Pod spec is the declarative source of truth on Kubernetes, replacing
    # the API server default of 30 seconds.
    monkeypatch.setattr(
        "gpustack_runtime.deployer.kuberentes.get_resource_injection_policy",
        lambda *_args: "env",
    )
    monkeypatch.setattr(
        "gpustack_runtime.deployer.kuberentes._resolve_runtime_class_name",
        lambda *_args: None,
    )

    class _FakeCoreV1Api:
        def __init__(self, client=None):
            pass

        def read_namespaced_pod(self, name, namespace):
            raise kubernetes.client.exceptions.ApiException(status=404)

        def create_namespaced_pod(self, namespace, body):
            return body

    monkeypatch.setattr(kubernetes.client, "CoreV1Api", _FakeCoreV1Api)

    deployer = object.__new__(KubernetesDeployer)
    Deployer.__init__(deployer, "test")
    deployer._materials = {}
    deployer._client = None
    deployer._node_name = None
    deployer._image_pull_secret = None
    deployer._mutate_create_pod = lambda pod: pod

    workload = KubernetesWorkloadPlan(
        name="test",
        namespace="default",
        termination_grace_period_seconds=30,
        containers=[
            Container(
                name="run",
                image="busybox:1.37",
                profile=ContainerProfileEnum.RUN,
            ),
        ],
    )
    workload.validate_and_default()
    pod = deployer._create_pod(workload, {})

    assert pod.spec.termination_grace_period_seconds == 30


class _FakeCoreV1DeleteApi:
    """
    Stand-in for the Kubernetes core API, recording the deletion calls it receives.
    """

    def __init__(self, journal: list, collection_status: int | None = None):
        self.journal = journal
        self.collection_status = collection_status

    def __call__(self, client=None):
        return self

    def delete_collection_namespaced_pod(self, **kwargs):
        self.journal.append(("delete_collection_namespaced_pod", kwargs))
        if self.collection_status:
            raise kubernetes.client.exceptions.ApiException(
                status=self.collection_status,
            )

    def list_namespaced_pod(self, **_kwargs):
        return SimpleNamespace(
            items=[SimpleNamespace(metadata=SimpleNamespace(name="test"))],
        )

    def delete_namespaced_pod(self, **kwargs):
        self.journal.append(("delete_namespaced_pod", kwargs))

    def delete_collection_namespaced_service(self, **kwargs):
        self.journal.append(("delete_collection_namespaced_service", kwargs))

    def delete_collection_namespaced_config_map(self, **kwargs):
        self.journal.append(("delete_collection_namespaced_config_map", kwargs))


def _kubernetes_deployer(monkeypatch, journal: list, collection_status=None):
    monkeypatch.setattr(
        kubernetes.client,
        "CoreV1Api",
        _FakeCoreV1DeleteApi(journal, collection_status),
    )
    return SimpleNamespace(
        is_supported=lambda: True,
        get=lambda **_kwargs: SimpleNamespace(name="test"),
        _client=None,
    )


def test_kubernetes_delete_forwards_the_grace_period(monkeypatch):
    journal = []
    dep = _kubernetes_deployer(monkeypatch, journal)

    KubernetesDeployer._delete(
        dep,
        name="test",
        namespace="default",
        grace_period_seconds=5,
    )

    pod_calls = [c for c in journal if "pod" in c[0]]
    assert len(pod_calls) == 1
    assert pod_calls[0][1]["grace_period_seconds"] == 5
    # The grace period is meaningless for the other resources.
    assert all("grace_period_seconds" not in c[1] for c in journal if "pod" not in c[0])


def test_kubernetes_delete_omits_an_unset_grace_period(monkeypatch):
    # Without an override, the Pod spec's own declaration applies.
    journal = []
    dep = _kubernetes_deployer(monkeypatch, journal)

    KubernetesDeployer._delete(dep, name="test", namespace="default")

    pod_calls = [c for c in journal if "pod" in c[0]]
    assert pod_calls[0][1]["grace_period_seconds"] is None


def test_kubernetes_delete_forwards_the_grace_period_on_the_fallback_path(monkeypatch):
    # A cluster refusing collection deletion falls back to deleting Pod by Pod.
    journal = []
    dep = _kubernetes_deployer(monkeypatch, journal, collection_status=405)

    KubernetesDeployer._delete(
        dep,
        name="test",
        namespace="default",
        grace_period_seconds=5,
    )

    fallback_calls = [c for c in journal if c[0] == "delete_namespaced_pod"]
    assert len(fallback_calls) == 1
    assert fallback_calls[0][1]["name"] == "test"
    assert fallback_calls[0][1]["grace_period_seconds"] == 5
