# The deployer cases below drive its own probe and policy resolution, which are
# internal by design -- the API traffic they guard is not observable from the
# public surface.
# ruff: noqa: SLF001

from types import SimpleNamespace

import kubernetes.client
import pytest

from gpustack_runtime.deployer.k8s.devicemanager import (
    get_resource_injection_policy,
    node_has_device_plugin_resources,
)
from gpustack_runtime.deployer.kuberentes import KubernetesDeployer

# Allocatable of a node whose accelerators are advertised by a device plugin
# allocating them the way the GPUStack Operator does.
_OPERATOR_ALLOCATABLE = {
    "cpu": "32",
    "memory": "128Gi",
    "nvidia.com/gpu": "1",
    "nvidia.com/gpu.shared": "10",
    "nvidia.com/gpu.sliced": "256",
    "nvidia.com/gpu.sliced.units": "3200k",
    "device.gpustack.ai/nvidia.visibility": "1024",
}

# Allocatable of a node running a stock vendor device plugin: the bare CDI kind
# and nothing else.
_STOCK_ALLOCATABLE = {
    "cpu": "32",
    "memory": "128Gi",
    "nvidia.com/gpu": "2",
}


@pytest.mark.parametrize(
    "name, allocatable, expected",
    [
        ("operator families", _OPERATOR_ALLOCATABLE, True),
        ("bare CDI kind only", _STOCK_ALLOCATABLE, False),
        ("no accelerator at all", {"cpu": "32", "memory": "128Gi"}, False),
        ("nothing allocatable", {}, False),
        # The visibility resource is deliberately outside the families, so on
        # its own it must not pass for one.
        (
            "visibility resource only",
            {"device.gpustack.ai/nvidia.visibility": "1024"},
            False,
        ),
        # A partitioned family carries the profile after the family segment.
        (
            "partitioned profile key",
            {"nvidia.com/gpu.partitioned.mig-1g.20gb": "7"},
            True,
        ),
    ],
)
def test_node_has_device_plugin_resources(name, allocatable, expected):
    actual = node_has_device_plugin_resources(allocatable)
    assert actual == expected, f"case {name} expected {expected}, but got {actual}"


@pytest.mark.parametrize(
    "name, configured, probe, expected",
    [
        # An explicit policy is never second-guessed, and never probes.
        ("explicit env", "Env", None, "env"),
        ("explicit kdp", "KDP", None, "kdp"),
        (
            "explicit env wins over an operator node",
            "Env",
            lambda: _OPERATOR_ALLOCATABLE,
            "env",
        ),
        # Auto decides from the target node.
        ("auto on an operator node", "Auto", lambda: _OPERATOR_ALLOCATABLE, "kdp"),
        ("auto on a stock node", "Auto", lambda: _STOCK_ALLOCATABLE, "env"),
        # A probe that cannot answer falls back to KDP: env injection hands the
        # container every device of the host and leaves the allocation off the
        # plugin's ledger, so guessing it wrong that way is the costlier miss.
        ("auto with a failing probe", "Auto", lambda: None, "kdp"),
        ("auto with no probe", "Auto", None, "kdp"),
    ],
)
def test_get_resource_injection_policy(name, configured, probe, expected, monkeypatch):
    monkeypatch.setattr(
        "gpustack_runtime.envs.GPUSTACK_RUNTIME_KUBERNETES_RESOURCE_INJECTION_POLICY",
        configured,
    )
    actual = get_resource_injection_policy(probe)
    assert actual == expected, f"case {name} expected {expected}, but got {actual}"


def test_explicit_policy_does_not_probe(monkeypatch):
    """An explicit policy must not spend an API call on the node."""
    monkeypatch.setattr(
        "gpustack_runtime.envs.GPUSTACK_RUNTIME_KUBERNETES_RESOURCE_INJECTION_POLICY",
        "KDP",
    )

    calls = []

    def _probe():
        calls.append(1)
        return _STOCK_ALLOCATABLE

    assert get_resource_injection_policy(_probe) == "kdp"
    assert calls == [], "the probe must not run under an explicit policy"


class _FakeNode:
    def __init__(self, name, allocatable):
        self.metadata = SimpleNamespace(name=name)
        self.status = SimpleNamespace(allocatable=allocatable)


def _kubernetes_deployer(monkeypatch, nodes, node_name=None):
    """A KubernetesDeployer whose only live dependency is list_node, counted."""
    calls = []

    class _FakeCoreV1Api:
        def __init__(self, client=None):
            pass

        def list_node(self, field_selector=None, limit=None):
            calls.append(field_selector)
            items = nodes
            if field_selector:
                wanted = field_selector.split("=", 1)[1]
                items = [n for n in nodes if n.metadata.name == wanted]
            return SimpleNamespace(items=items[:limit] if limit else items)

    monkeypatch.setattr(kubernetes.client, "CoreV1Api", _FakeCoreV1Api)

    deployer = object.__new__(KubernetesDeployer)
    deployer._client = None
    deployer._node_name = node_name
    deployer._runtime_uuid_values_allowed = None
    return deployer, calls


def test_probe_remembers_the_node_it_resolved(monkeypatch):
    """
    With no node configured the probe reads the same first node
    `_get_default_node_name` would, so it remembers it rather than leaving the
    next caller to pay for the lookup again.
    """
    monkeypatch.setattr(
        "gpustack_runtime.envs.GPUSTACK_RUNTIME_KUBERNETES_RESOURCE_INJECTION_POLICY",
        "Auto",
    )
    deployer, calls = _kubernetes_deployer(
        monkeypatch,
        [_FakeNode("node-a", _OPERATOR_ALLOCATABLE)],
    )

    assert deployer._probe_node_allocatable() == _OPERATOR_ALLOCATABLE
    assert deployer._node_name == "node-a"
    # The next probe addresses that node by name instead of scanning again.
    deployer._probe_node_allocatable()
    assert calls == [None, "metadata.name=node-a"]


def test_probe_reports_none_when_the_node_cannot_be_read(monkeypatch):
    deployer, _ = _kubernetes_deployer(monkeypatch, [])
    assert deployer._probe_node_allocatable() is None
    assert deployer._node_name is None


def test_allowed_runtime_uuid_values_probes_once(monkeypatch):
    """
    `_prepare` reads this once per manufacturer while building the device
    materials, so it must not spend an API call per read.
    """
    monkeypatch.setattr(
        "gpustack_runtime.envs.GPUSTACK_RUNTIME_KUBERNETES_RESOURCE_INJECTION_POLICY",
        "Auto",
    )
    deployer, calls = _kubernetes_deployer(
        monkeypatch,
        [_FakeNode("node-a", _OPERATOR_ALLOCATABLE)],
        node_name="node-a",
    )

    assert deployer.allowed_runtime_uuid_values is False  # operator node -> kdp
    assert deployer.allowed_runtime_uuid_values is False
    assert deployer.allowed_runtime_uuid_values is False
    assert len(calls) == 1, f"probed {len(calls)} times, expected 1"


def test_creation_path_still_probes_per_pod(monkeypatch):
    """
    The per-Pod resolution stays live: a node that gains (or loses) a device
    plugin must be seen by the next deployment, not only by the next process.
    """
    monkeypatch.setattr(
        "gpustack_runtime.envs.GPUSTACK_RUNTIME_KUBERNETES_RESOURCE_INJECTION_POLICY",
        "Auto",
    )
    nodes = [_FakeNode("node-a", dict(_STOCK_ALLOCATABLE))]
    deployer, calls = _kubernetes_deployer(monkeypatch, nodes, node_name="node-a")

    assert deployer._resolve_resource_injection_policy() == "env"
    nodes[0].status.allocatable = _OPERATOR_ALLOCATABLE
    assert deployer._resolve_resource_injection_policy() == "kdp"
    assert len(calls) == 2
