# The cases below drive the deployers' own log path, which is where the log
# options become observable without a live Docker daemon, Podman socket or
# Kubernetes cluster. `since` is an absolute epoch in seconds for every
# deployer; only Kubernetes has to translate it, because its API takes a
# relative duration. `tail` has to survive its own declared default.
# ruff: noqa: SLF001

from types import SimpleNamespace

import kubernetes.client
import pytest

from gpustack_runtime.deployer.docker import (
    _LABEL_COMPONENT_INDEX as _DOCKER_LABEL_COMPONENT_INDEX,
)
from gpustack_runtime.deployer.docker import (
    DockerDeployer,
)
from gpustack_runtime.deployer.kuberentes import (
    KubernetesDeployer,
    to_since_seconds,
)
from gpustack_runtime.deployer.podman import (
    _LABEL_COMPONENT_INDEX as _PODMAN_LABEL_COMPONENT_INDEX,
)
from gpustack_runtime.deployer.podman import (
    PodmanDeployer,
)

_COMPONENT_INDEX_LABELS = {
    DockerDeployer: _DOCKER_LABEL_COMPONENT_INDEX,
    PodmanDeployer: _PODMAN_LABEL_COMPONENT_INDEX,
}

NOW = 1_700_000_000


@pytest.fixture
def frozen_now(monkeypatch):
    monkeypatch.setattr(
        "gpustack_runtime.deployer.kuberentes.time.time",
        lambda: float(NOW),
    )


class _FakeCoreV1LogApi:
    """
    Stand-in for the Kubernetes core API, recording the log calls it receives.
    """

    def __init__(self, journal: list):
        self.journal = journal

    def __call__(self, client=None):
        return self

    def read_namespaced_pod_log(self, **kwargs):
        self.journal.append(kwargs)
        return b""


def _kubernetes_deployer(monkeypatch, journal: list) -> SimpleNamespace:
    monkeypatch.setattr(
        kubernetes.client,
        "CoreV1Api",
        _FakeCoreV1LogApi(journal),
    )
    pod = kubernetes.client.V1Pod(
        metadata=kubernetes.client.V1ObjectMeta(name="test", namespace="default"),
        spec=kubernetes.client.V1PodSpec(
            containers=[kubernetes.client.V1Container(name="run")],
        ),
    )
    return SimpleNamespace(
        is_supported=lambda: True,
        get=lambda **_kwargs: SimpleNamespace(_k_pod=pod),
        _client=None,
        _find_self_pod_for_endoscopy=lambda: pod,
    )


class _FakeContainer:
    """
    Stand-in for a Docker or Podman container, recording its log calls.
    """

    def __init__(self, journal: list):
        self.journal = journal
        self.labels = {}
        self.id = "cid"
        self.short_id = "cid"

    def logs(self, **kwargs):
        self.journal.append(kwargs)
        return b""


def _container_deployer(journal: list, deployer) -> SimpleNamespace:
    container = _FakeContainer(journal)
    # The component index label is how both deployers pick the loggable container.
    container.labels = {_COMPONENT_INDEX_LABELS[deployer]: "0"}
    return SimpleNamespace(
        is_supported=lambda: True,
        get=lambda **_kwargs: SimpleNamespace(_d_containers=[container]),
        _find_self_container_for_endoscopy=lambda: container,
    )


@pytest.mark.parametrize(
    "deployer",
    [DockerDeployer, PodmanDeployer],
)
def test_container_logs_forward_the_epoch_verbatim(deployer):
    # Docker and Podman both take an absolute epoch, so `since` passes through.
    journal = []
    dep = _container_deployer(journal, deployer)

    deployer._logs(dep, name="test", tail=-1, since=NOW - 90)

    assert journal[0]["since"] == NOW - 90


@pytest.mark.usefixtures("frozen_now")
def test_kubernetes_logs_convert_the_epoch_to_a_relative_duration(monkeypatch):
    # The Kubernetes API counts back from now, so the same absolute epoch has to
    # become a duration. Rounding up and adding a second keeps the requested
    # instant inside the window rather than just outside it.
    journal = []
    dep = _kubernetes_deployer(monkeypatch, journal)

    KubernetesDeployer._logs(dep, name="test", tail=-1, since=NOW - 90)

    assert journal[0]["since_seconds"] == 91


@pytest.mark.usefixtures("frozen_now")
def test_kubernetes_endoscopic_logs_convert_the_epoch_too(monkeypatch):
    # The deployer's own logs travel the same API and must not diverge.
    journal = []
    dep = _kubernetes_deployer(monkeypatch, journal)

    KubernetesDeployer._endoscopic_logs(dep, tail=-1, since=NOW - 90)

    assert journal[0]["since_seconds"] == 91


# Every way into the log path: each deployer serves both a workload and, in
# mirrored deployment, itself.
_ENTRY_POINTS = [
    (DockerDeployer, "_logs", {"name": "test"}),
    (DockerDeployer, "_endoscopic_logs", {}),
    (PodmanDeployer, "_logs", {"name": "test"}),
    (PodmanDeployer, "_endoscopic_logs", {}),
    (KubernetesDeployer, "_logs", {"name": "test"}),
    (KubernetesDeployer, "_endoscopic_logs", {}),
]


def _entry_deployer(monkeypatch, journal: list, deployer) -> SimpleNamespace:
    if deployer is KubernetesDeployer:
        return _kubernetes_deployer(monkeypatch, journal)
    return _container_deployer(journal, deployer)


@pytest.mark.parametrize("deployer, method, kwargs", _ENTRY_POINTS)
def test_an_unset_since_stays_unset(monkeypatch, deployer, method, kwargs):
    # No lower bound must reach the API as no lower bound, not as "now".
    journal = []
    dep = _entry_deployer(monkeypatch, journal, deployer)

    getattr(deployer, method)(dep, tail=-1, since=None, **kwargs)

    key = "since_seconds" if deployer is KubernetesDeployer else "since"
    assert journal[0][key] is None


@pytest.mark.parametrize("deployer, method, kwargs", _ENTRY_POINTS)
def test_the_declared_tail_default_reaches_the_api(
    monkeypatch,
    deployer,
    method,
    kwargs,
):
    # `tail` defaults to None, which has to mean "all lines" rather than blow up
    # on a comparison against an integer.
    journal = []
    dep = _entry_deployer(monkeypatch, journal, deployer)

    getattr(deployer, method)(dep, since=None, **kwargs)

    key = "tail_lines" if deployer is KubernetesDeployer else "tail"
    assert journal[0][key] is None


@pytest.mark.parametrize(
    "since, expected",
    [
        # A whole minute back is a whole minute plus the safety second.
        (NOW - 60, 61),
        # The current second still has to ask for a positive duration.
        (NOW, 1),
        # A cursor ahead of the node's clock must not become a negative or zero
        # duration, which the API rejects.
        (NOW + 30, 1),
        (None, None),
    ],
)
@pytest.mark.usefixtures("frozen_now")
def test_to_since_seconds_never_returns_a_non_positive_duration(since, expected):
    assert to_since_seconds(since) == expected
