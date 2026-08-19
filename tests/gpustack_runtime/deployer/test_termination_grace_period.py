# The cases below drive the deployers' own deletion path, which is where the
# graceful termination becomes observable without a live Docker daemon, Podman
# socket or Kubernetes cluster.
# ruff: noqa: SLF001

import threading
from types import SimpleNamespace

import docker.errors
import kubernetes.client
import kubernetes.client.exceptions
import podman.errors
import pytest

from gpustack_runtime.deployer.__types__ import (
    DEFAULT_TERMINATION_GRACE_PERIOD_SECONDS,
    Container,
    ContainerProfileEnum,
    Deployer,
    OperationError,
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
    equal_containers,
    equal_pods,
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

    assert DEFAULT_TERMINATION_GRACE_PERIOD_SECONDS == 30
    assert plan.termination_grace_period_seconds == 30


def test_workload_plan_keeps_a_zero_termination_grace_period():
    # Zero means "kill immediately", it must survive defaulting.
    plan = _plan(termination_grace_period_seconds=0)
    plan.validate_and_default()

    assert plan.termination_grace_period_seconds == 0


def test_workload_plan_defaults_a_none_termination_grace_period():
    # A JSON payload carrying an explicit null degrades to the default.
    plan = _plan(termination_grace_period_seconds=None)
    plan.validate_and_default()

    assert plan.termination_grace_period_seconds == 30


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
        self.stop_error = None
        self.stop_barrier = None

    def stop(self, **kwargs):
        if self.stop_barrier:
            # Only a container signalled while the others are still draining
            # reaches the barrier, a serial drain times out on it instead.
            self.stop_barrier.wait(timeout=5)
        self.journal.append(("stop", self.name, kwargs))
        if self.stop_error:
            raise self.stop_error

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


_DRAINED = ("test-init-0", "test-run-1")


def _stops(journal: list) -> list:
    # The drainable containers are stopped concurrently,
    # so only the arguments are pinned, never the order between them.
    return sorted(c for c in journal if c[0] == "stop")


def _assert_drained_before_removed(journal: list):
    # Every drainable container must be signaled before any of them is removed,
    # otherwise a slow container eats the grace period of the ones behind it.
    last_stop = max(i for i, c in enumerate(journal) if c[0] == "stop")
    first_remove = min(
        i for i, c in enumerate(journal) if c[0] == "remove" and c[1] in _DRAINED
    )
    assert last_stop < first_remove


def test_docker_delete_drains_the_workload_before_removing_it():
    # The unhealthy restart container goes first, otherwise it restarts the
    # containers being drained; the pause container goes last, as it holds the
    # namespaces the others share.
    journal = []
    dep = _deployer(_docker_containers(journal))

    DockerDeployer._delete(dep, name="test", grace_period_seconds=20)

    assert journal[0] == ("remove", "test-unhealthy-restart", {"force": True})
    assert journal[-1] == ("remove", "test-pause", {"force": True})
    _assert_drained_before_removed(journal)
    assert _stops(journal) == [
        ("stop", "test-init-0", {"timeout": 20}),
        ("stop", "test-run-1", {"timeout": 20}),
    ]


def test_docker_delete_gives_every_container_the_whole_grace_period():
    # Mirrors Kubernetes: the grace period is not a budget shared between the
    # containers, each of them gets all of it.
    journal = []
    dep = _deployer(_docker_containers(journal))

    DockerDeployer._delete(dep, name="test", grace_period_seconds=20)

    assert [c[2]["timeout"] for c in _stops(journal)] == [20, 20]


def test_docker_delete_reads_the_grace_period_from_the_container_label():
    # Without an explicit override, the grace period declared by the workload
    # plan is read back from the container label.
    journal = []
    dep = _deployer(
        _docker_containers(journal, labels={_DOCKER_LABEL_GRACE_PERIOD: "45"}),
    )

    DockerDeployer._delete(dep, name="test")

    assert _stops(journal) == [
        ("stop", "test-init-0", {"timeout": 45}),
        ("stop", "test-run-1", {"timeout": 45}),
    ]


def test_docker_delete_falls_back_to_the_default_grace_period():
    # A workload created before the grace period existed carries no label.
    journal = []
    dep = _deployer(_docker_containers(journal))

    DockerDeployer._delete(dep, name="test")

    assert _stops(journal) == [
        ("stop", "test-init-0", {"timeout": 30}),
        ("stop", "test-run-1", {"timeout": 30}),
    ]


def test_docker_delete_kills_immediately_on_a_zero_grace_period():
    journal = []
    dep = _deployer(
        _docker_containers(journal, labels={_DOCKER_LABEL_GRACE_PERIOD: "45"}),
    )

    DockerDeployer._delete(dep, name="test", grace_period_seconds=0)

    assert _stops(journal) == [
        ("stop", "test-init-0", {"timeout": 0}),
        ("stop", "test-run-1", {"timeout": 0}),
    ]


@pytest.mark.parametrize(
    "deployer, make_containers",
    [
        (DockerDeployer, _docker_containers),
        (PodmanDeployer, _podman_containers),
    ],
)
def test_delete_signals_the_containers_at_the_same_time(deployer, make_containers):
    # Draining serially would leave the containers behind the first one with
    # less than the grace period, which is the bug this branch exists to fix.
    # Both deployers carry their own copy of the drain, so both are guarded.
    journal = []
    containers = make_containers(journal)
    barrier = threading.Barrier(len(_DRAINED))
    for c in containers:
        if c.name in _DRAINED:
            c.stop_barrier = barrier
    dep = _deployer(containers)

    deployer._delete(dep, name="test", grace_period_seconds=20)

    assert len(_stops(journal)) == len(_DRAINED)


def test_podman_delete_survives_a_container_failing_to_stop():
    journal = []
    containers = _podman_containers(journal)
    containers[2].stop_error = podman.errors.APIError("boom")
    dep = _deployer(containers)

    PodmanDeployer._delete(dep, name="test", grace_period_seconds=20)

    assert journal[-1] == ("remove", "test-pause", {"force": True})
    assert ("remove", "test-run-1", {"force": True}) in journal


def test_docker_delete_survives_a_container_failing_to_stop():
    # The forceful removal is the authoritative teardown, a container refusing
    # to drain must not strand the pause container and the volumes.
    journal = []
    containers = _docker_containers(journal)
    containers[2].stop_error = docker.errors.APIError("boom")
    dep = _deployer(containers)

    DockerDeployer._delete(dep, name="test", grace_period_seconds=20)

    assert journal[-1] == ("remove", "test-pause", {"force": True})
    assert ("remove", "test-run-1", {"force": True}) in journal


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

    assert plan.labels[_DOCKER_LABEL_GRACE_PERIOD] == "30"


def test_podman_delete_drains_the_workload_before_removing_it():
    # Mirrors the Docker deletion path, but tolerates the "already stopped"
    # answer, which podman-py cannot decode on its own.
    journal = []
    dep = _deployer(_podman_containers(journal))

    PodmanDeployer._delete(dep, name="test", grace_period_seconds=20)

    assert journal[0] == ("remove", "test-unhealthy-restart", {"force": True})
    assert journal[-1] == ("remove", "test-pause", {"force": True})
    _assert_drained_before_removed(journal)
    assert _stops(journal) == [
        ("stop", "test-init-0", {"timeout": 20, "ignore": True}),
        ("stop", "test-run-1", {"timeout": 20, "ignore": True}),
    ]


def test_podman_delete_reads_the_grace_period_from_the_container_label():
    journal = []
    dep = _deployer(
        _podman_containers(journal, labels={_PODMAN_LABEL_GRACE_PERIOD: "45"}),
    )

    PodmanDeployer._delete(dep, name="test")

    assert _stops(journal) == [
        ("stop", "test-init-0", {"timeout": 45, "ignore": True}),
        ("stop", "test-run-1", {"timeout": 45, "ignore": True}),
    ]


def test_podman_delete_falls_back_to_the_default_grace_period():
    journal = []
    dep = _deployer(_podman_containers(journal))

    PodmanDeployer._delete(dep, name="test")

    assert _stops(journal) == [
        ("stop", "test-init-0", {"timeout": 30, "ignore": True}),
        ("stop", "test-run-1", {"timeout": 30, "ignore": True}),
    ]


def test_podman_delete_kills_immediately_on_a_zero_grace_period():
    journal = []
    dep = _deployer(
        _podman_containers(journal, labels={_PODMAN_LABEL_GRACE_PERIOD: "45"}),
    )

    PodmanDeployer._delete(dep, name="test", grace_period_seconds=0)

    assert _stops(journal) == [
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


def test_podman_workload_plan_stamps_the_defaulted_grace_period_label():
    plan = PodmanWorkloadPlan(
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

    assert plan.labels[_PODMAN_LABEL_GRACE_PERIOD] == "30"


def test_kubernetes_delete_reports_a_transport_failure_as_an_operation_error(
    monkeypatch,
):
    # A transport failure carries no HTTP status, so it must not escape the
    # deletion as something other than an OperationError.
    class _FailingCoreV1Api:
        def __init__(self, client=None):
            pass

        def delete_collection_namespaced_pod(self, **_kwargs):
            msg = "connection reset"
            raise OSError(msg)

    monkeypatch.setattr(kubernetes.client, "CoreV1Api", _FailingCoreV1Api)
    dep = SimpleNamespace(
        is_supported=lambda: True,
        get=lambda **_kwargs: SimpleNamespace(name="test"),
        _client=None,
    )

    with pytest.raises(OperationError):
        KubernetesDeployer._delete(dep, name="test", namespace="default")


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


def _pod(
    termination_grace_period_seconds=None,
    host_network=None,
    resources=None,
) -> kubernetes.client.V1Pod:
    return kubernetes.client.V1Pod(
        metadata=kubernetes.client.V1ObjectMeta(name="test"),
        spec=kubernetes.client.V1PodSpec(
            host_network=host_network,
            termination_grace_period_seconds=termination_grace_period_seconds,
            containers=[
                kubernetes.client.V1Container(
                    name="run",
                    image="busybox:1.37",
                    resources=resources,
                ),
            ],
        ),
    )


def test_kubernetes_equal_pods_ignores_the_api_server_defaults():
    # The API server drops a disabled toggle and fills an empty resources
    # declaration back in, neither of which is a change worth recreating for.
    actual = _pod(
        termination_grace_period_seconds=30,
        host_network=None,
        resources=kubernetes.client.V1ResourceRequirements(),
    )
    desired = _pod(
        termination_grace_period_seconds=30,
        host_network=False,
        resources=None,
    )

    assert equal_pods(actual, desired)


def test_kubernetes_equal_pods_treats_an_unset_grace_period_as_the_default():
    # A Pod created before the grace period existed carries the API server
    # default, which is what the plan default settles on too.
    actual = _pod(termination_grace_period_seconds=None)
    desired = _pod(termination_grace_period_seconds=30)

    assert equal_pods(actual, desired)


def test_kubernetes_equal_pods_detects_a_changed_grace_period():
    actual = _pod(termination_grace_period_seconds=30)
    desired = _pod(termination_grace_period_seconds=10)

    assert not equal_pods(actual, desired)


def _container(cpu, memory) -> kubernetes.client.V1Container:
    return kubernetes.client.V1Container(
        name="run",
        image="busybox:1.37",
        resources=kubernetes.client.V1ResourceRequirements(
            limits={"cpu": cpu, "memory": memory},
            requests={"cpu": cpu, "memory": memory},
        ),
    )


@pytest.mark.parametrize(
    "declared, stored",
    [
        # The API server rewrites a fractional quantity into its milli form.
        ("0.5", "500m"),
        # And a suffixed quantity into the largest suffix dividing it exactly.
        ("1.5Gi", "1536Mi"),
        # While an already canonical quantity is kept verbatim.
        ("1Gi", "1Gi"),
        ("1073741824", "1073741824"),
    ],
)
def test_kubernetes_equal_containers_reads_a_rewritten_quantity(declared, stored):
    # The API server stores a quantity in its own spelling, which must not read
    # as a change, or the Pod is recreated on every deployment.
    assert equal_containers(_container(stored, stored), _container(declared, declared))


def test_kubernetes_equal_containers_detects_a_changed_quantity():
    assert not equal_containers(_container("1", "1Gi"), _container("2", "1Gi"))
    assert not equal_containers(_container("1", "1Gi"), _container("1", "2Gi"))


def test_kubernetes_equal_containers_keeps_a_non_quantity_verbatim():
    # Device resources carry values that do not spell a quantity.
    assert equal_containers(_container("1", "all"), _container("1", "all"))
    assert not equal_containers(_container("1", "all"), _container("1", "0,1"))


@pytest.mark.parametrize("declared", ["1_0m", "\uff11\uff10m", "0x10", " 10m"])
def test_kubernetes_equal_containers_refuses_a_non_ascii_quantity(declared):
    # Kubernetes accepts ASCII digits only, so a declaration Python would read
    # as a number must not pass as an unchanged one.
    assert not equal_containers(_container("1", "10m"), _container("1", declared))
