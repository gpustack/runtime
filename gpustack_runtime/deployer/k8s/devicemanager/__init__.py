from __future__ import annotations as __future_annotations__

import stat
from functools import lru_cache
from pathlib import Path
from typing import Literal

from gpustack_runtime import envs


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


@lru_cache
def get_resource_injection_policy() -> Literal["env", "kdp"]:
    """
    Get the resource injection policy (in lowercase) for the deployer.

    Returns:
        The resource injection policy.

    """
    policy = envs.GPUSTACK_RUNTIME_KUBERNETES_RESOURCE_INJECTION_POLICY.lower()
    if policy != "auto":
        return policy

    return "kdp" if is_kubelet_socket_accessible() else "env"


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
    "cdi_kind_to_kdp_resource",
    "get_resource_injection_policy",
    "is_kubelet_socket_accessible",
]
