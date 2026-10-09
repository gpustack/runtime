from __future__ import annotations as __future_annotations__

import stat
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from gpustack_runtime import envs

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


class KubernetesResourceInjectionPolicyEnum(str, Enum):
    """
    Device injection policies for Kubernetes workloads.
    """

    AUTO = "Auto"
    """
    Detect the policy from the target node's allocatable resources.
    """
    ENV = "Env"
    """
    Inject runtime-visible device environment variables without device-plugin requests.
    """
    KDP = "KDP"
    """
    Request device-plugin resources.
    """

    @classmethod
    def _missing_(cls, value: object) -> KubernetesResourceInjectionPolicyEnum | None:
        if isinstance(value, str):
            for member in cls:
                if member.value.lower() == value.lower():
                    return member
        return None

    def __str__(self) -> str:
        return self.value


def is_kubelet_socket_accessible(
    kubelet_endpoint: Path | None = None,
) -> bool:
    """
    Check if the kubelet socket is accessible.

    Args:
        kubelet_endpoint:
            The path to the kubelet endpoint.

    Returns:
        True if the socket is accessible, False otherwise.

    """
    if not kubelet_endpoint:
        kubelet_endpoint = Path("/var/lib/kubelet/device-plugins/kubelet.sock")

    if kubelet_endpoint.exists():
        path_stat = kubelet_endpoint.lstat()
        if path_stat and stat.S_ISSOCK(path_stat.st_mode):
            return True
    return False


_DEVICE_PLUGIN_RESOURCE_FAMILIES = (
    "shared",
    "sliced",
    "partitioned",
)
"""
Resource-name families a device plugin advertises on top of a plain CDI kind,
mirroring the GPUStack Operator's own families
(`gpustack-operator pkg/nodefeature`): "nvidia.com/gpu.shared",
"nvidia.com/gpu.sliced.units", "nvidia.com/gpu.partitioned.mig-1g.20gb", ...
A stock vendor plugin advertises only the bare kind ("nvidia.com/gpu"), so the
family segment is what tells the two apart.
"""


def node_has_device_plugin_resources(
    node_allocatable: Mapping[str, Any],
) -> bool:
    """
    Report whether a node advertises accelerators through a device plugin that
    allocates them the way the GPUStack Operator does.

    Only the suffixed families count (see
    :data:`_DEVICE_PLUGIN_RESOURCE_FAMILIES`): a bare CDI kind on its own is
    what a stock vendor plugin advertises, and requesting a device from it
    yields none of the operator's accounting.

    Args:
        node_allocatable:
            The allocatable resources of a node, keyed by resource name.

    Returns:
        True if any allocatable resource name carries a family segment.

    """
    return any(
        family in name.split(".")
        for name in node_allocatable
        for family in _DEVICE_PLUGIN_RESOURCE_FAMILIES
    )


def get_resource_injection_policy(
    probe_node_allocatable: Callable[[], Mapping[str, Any] | None] | None = None,
    *,
    policy: KubernetesResourceInjectionPolicyEnum | None = None,
) -> Literal["env", "kdp"]:
    """
    Get the resource injection policy (in lowercase) for the deployer.

    A workload policy overrides the environment setting. Under "auto" the
    decision belongs to the cluster, not to the process doing the deploying:
    the Kubernetes deployer orchestrates remotely, so whether *it* can reach
    a kubelet socket says
    nothing about whether the *target* node runs a device plugin. So the probe
    reads that node's allocatable resources and looks for a device-plugin
    resource family there.

    A probe that cannot answer -- absent, failing, or unauthorized -- falls
    back to KDP rather than to env: env injection hands the container every
    device of the host and leaves the allocation off the plugin's ledger, so
    guessing it wrong is the more damaging of the two.

    Args:
        probe_node_allocatable:
            Called only under the "auto" policy, to read the target node's
            allocatable resources. Returns None when the node cannot be read.
        policy:
            Workload policy override. None inherits the environment setting;
            Auto probes the node even if the environment selects Env or KDP.

    Raises:
        ValueError:
            If the policy is not Auto, Env or KDP.

    Returns:
        The resource injection policy.

    """
    if policy is None:
        policy = envs.GPUSTACK_RUNTIME_KUBERNETES_RESOURCE_INJECTION_POLICY
    try:
        policy = KubernetesResourceInjectionPolicyEnum(policy)
    except ValueError as e:
        msg = f"Invalid resource injection policy '{policy}'"
        raise ValueError(msg) from e
    if policy == KubernetesResourceInjectionPolicyEnum.ENV:
        return "env"
    if policy == KubernetesResourceInjectionPolicyEnum.KDP:
        return "kdp"

    if probe_node_allocatable is None:
        return "kdp"

    node_allocatable = probe_node_allocatable()
    if node_allocatable is None:
        return "kdp"

    return "kdp" if node_has_device_plugin_resources(node_allocatable) else "env"


@lru_cache
def cdi_kind_to_kdp_resource(
    cdi_kind: str,
    mode: Literal["shared", "sliced"] = "shared",
):
    """
    Convert a CDI kind to a KDP resource name.

    Args:
        cdi_kind:
            The CDI kind to convert.
        mode:
            The mode of the resource, either "shared" or "sliced".

    Returns:
        The corresponding KDP resource name.

    """
    if mode == "shared":
        return f"{cdi_kind}.shared"
    return f"{cdi_kind}.sliced.units"


__all__ = [
    "KubernetesResourceInjectionPolicyEnum",
    "cdi_kind_to_kdp_resource",
    "get_resource_injection_policy",
    "is_kubelet_socket_accessible",
    "node_has_device_plugin_resources",
]
