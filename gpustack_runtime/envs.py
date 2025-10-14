from __future__ import annotations

from functools import lru_cache
from os import getenv
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    # Global

    GPUSTACK_RUNTIME_LOG_LEVEL: str | None = None
    """
    Log level for the gpustack-runtime.
    """
    GPUSTACK_RUNTIME_LOG_TO_FILE: Path | None = None
    """
    Log to file instead of stdout.
    """

    ## Detector
    GPUSTACK_RUNTIME_DETECT: str | None = None
    """
    Detector to use (e.g., Auto, NVIDIA, AMD, Ascend, .etc).
    """
    GPUSTACK_RUNTIME_DETECT_BACKEND_MAP_RESOURCE_KEY: dict[str, str] | None = None
    """
    The detected backend mapping to resource keys,
    e.g `{"cuda": "nvidia.com/devices", "rocm": "amd.com/devices"}`.
    """
    ## Deployer
    GPUSTACK_RUNTIME_DEPLOY: str | None = None
    """
    Deployer to use (e.g., Auto, Docker, Kubernetes).
    """
    GPUSTACK_RUNTIME_DEPLOY_MIRRORED_NAME: str | None = None
    """
    The name of the deployer.
    Works with `GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT`.
    In some senses, the deployer needs to know its own name to execute mirrored deployment,
    e.g., when the deployer is a Kubernetes Pod, it need to know its own Pod name.
    """
    GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT: bool = False
    """
    Enable mirrored deployment mode.
    During deployment mirroring, when deployer deploys a workload,
    it will configure the workload with the same following settings as the deployer:
        - Container Runtime(e.g., nvidia, amd, .etc),
        - Customized environment variables,
        - Customized volume mounts.
    To be noted, without `GPUSTACK_RUNTIME_DEPLOY_MIRRORED_NAME` configured,
    if the deployer failed to retrieve its own settings, it will skip mirrored deployment.
    """
    GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_ENVIRONMENTS: set[str] | None = (
        None
    )
    """
    The environment variable names to ignore during mirrored deployment.
    Works only when `GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT` is enabled.
    """
    GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_VOLUMES: set[str] | None = None
    """
    The volume mount destinations to ignore during mirrored deployment.
    Works only when `GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT` is enabled.
    """
    GPUSTACK_RUNTIME_DEPLOY_CORRECT_RUNNER_IMAGE: bool = True
    """
    Correct the gpustack-runner image by rendering it with the host's detection.
    """
    GPUSTACK_RUNTIME_DEPLOY_LABEL_PREFIX: str | None = None
    """
    Label prefix for the deployer.
    """
    GPUSTACK_RUNTIME_DEPLOY_AUTOMAP_RESOURCE_KEY: str | None = None
    """
    The resource key to use for automatic mapping of container backend visible devices environment variables,
    which is used to tell deployer do a device detection and get the corresponding resource key before mapping.
    e.g., "gpustack.ai/devices".
    """
    GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_RUNTIME_VISIBLE_DEVICES: (
        dict[str, str] | None
    ) = None
    """
    Manual mapping of runtime visible devices environment variables,
    which is used to tell the Container Runtime which GPUs to mount into the container,
    e.g., `{"nvidia.com/devices": "NVIDIA_VISIBLE_DEVICES", "amd.com/devices": "AMD_VISIBLE_DEVICES"}`.
    The key is the resource key, and the value is the environment variable name.
    """
    GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_BACKEND_VISIBLE_DEVICES: (
        dict[str, list[str]] | None
    ) = None
    """
    Manual mapping of backend visible devices environment variables,
    which is used to tell the Device Runtime (e.g., ROCm, CUDA, OneAPI) which GPUs to use inside the container,
    e.g., `{"nvidia.com/devices": ["CUDA_VISIBLE_DEVICES"], "amd.com/devices": ["ROCR_VISIBLE_DEVICES"]}`.
    The key is the resource key, and the value is a list of environment variable names.
    """

    # Detector

    GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY: bool = True
    """
    Use physical index priority at detecting devices.
    """

    # Deployer

    ## Docker
    GPUSTACK_RUNTIME_DOCKER_PAUSE_IMAGE: str | None = None
    """
    Docker image used for the pause container.
    """
    GPUSTACK_RUNTIME_DOCKER_UNHEALTHY_RESTART_IMAGE: str | None = None
    """
    Docker image used for unhealthy restart container.
    """
    GPUSTACK_RUNTIME_DOCKER_EPHEMERAL_FILES_DIR: Path | None = None
    """
    Directory for storing ephemeral files for Docker.
    """
    ## Kubernetes
    GPUSTACK_RUNTIME_KUBERNETES_NODE_NAME: str | None = None
    """
    Name of the Kubernetes Node to deploy workloads to,
    if not set, take the first Node name from the cluster.
    """
    GPUSTACK_RUNTIME_KUBERNETES_NAMESPACE: str | None = None
    """
    Namespace of the Kubernetes to deploy workloads to.
    """
    GPUSTACK_RUNTIME_KUBERNETES_DOMAIN_SUFFIX: str | None = None
    """
    Domain suffix for Kubernetes services.
    """
    GPUSTACK_RUNTIME_KUBERNETES_SERVICE_TYPE: str | None = None
    """
    Service type for Kubernetes services (e.g., ClusterIP, NodePort, LoadBalancer).
    """
    GPUSTACK_RUNTIME_KUBERNETES_QUORUM_READ: bool = False
    """
    Whether to use quorum read for Kubernetes services.
    """

# --8<-- [start:env-vars-definition]

variables: dict[str, Callable[[], Any]] = {
    # Global
    "GPUSTACK_RUNTIME_LOG_LEVEL": lambda: getenv(
        "GPUSTACK_RUNTIME_LOG_LEVEL",
        "",
    ),
    "GPUSTACK_RUNTIME_LOG_TO_FILE": lambda: mkdir_path(
        getenv("GPUSTACK_RUNTIME_LOG_TO_FILE", None),
    ),
    "GPUSTACK_RUNTIME_DETECT": lambda: getenv(
        "GPUSTACK_RUNTIME_DETECT",
        "Auto",
    ),
    "GPUSTACK_RUNTIME_DETECT_BACKEND_MAP_RESOURCE_KEY": lambda: to_dict(
        getenv(
            "GPUSTACK_RUNTIME_DETECT_BACKEND_MAP_RESOURCE_KEY",
            "rocm=amd.com/devices;"
            "cann=huawei.com/devices;"
            "neuware=cambricon.com/devices;"
            "dtk=hygon.com/devices;"
            "corex=iluvatar.ai/devices;"
            "maca=metax-tech.com/devices;"
            "musa=mthreads.com/devices;"
            "cuda=nvidia.com/devices;",
        ),
    ),
    "GPUSTACK_RUNTIME_DEPLOY": lambda: getenv(
        "GPUSTACK_RUNTIME_DEPLOY",
        "Auto",
    ),
    "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_NAME": lambda: getenv(
        "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_NAME",
    ),
    "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT", "0"),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_ENVIRONMENTS": lambda: to_set(
        getenv(
            "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_ENVIRONMENTS",
        ),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_VOLUMES": lambda: to_set(
        getenv(
            "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_VOLUMES",
        ),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_CORRECT_RUNNER_IMAGE": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DEPLOY_CORRECT_RUNNER_IMAGE", "1"),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_LABEL_PREFIX": lambda: getenv(
        "GPUSTACK_RUNTIME_DEPLOY_LABEL_PREFIX",
        "runtime.gpustack.ai",
    ),
    "GPUSTACK_RUNTIME_DEPLOY_AUTOMAP_RESOURCE_KEY": lambda: getenv(
        "GPUSTACK_RUNTIME_DEPLOY_AUTOMAP_RESOURCE_KEY",
        "gpustack.ai/devices",
    ),
    "GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_RUNTIME_VISIBLE_DEVICES": lambda: to_dict(
        getenv(
            "GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_RUNTIME_VISIBLE_DEVICES",
            "amd.com/devices=AMD_VISIBLE_DEVICES;"
            "huawei.com/devices=ASCEND_VISIBLE_DEVICES;"
            "cambricon.com/devices=CAMBRICON_VISIBLE_DEVICES;"
            "hygon.com/devices=HYGON_VISIBLE_DEVICES;"
            "iluvatar.ai/devices=ILUVATAR_VISIBLE_DEVICES;"
            "metax-tech.com/devices=CUDA_VISIBLE_DEVICES;"
            "mthreads.com/devices=METHERDS_VISIBLE_DEVICES;"
            "nvidia.com/devices=NVIDIA_VISIBLE_DEVICES;",
        ),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_BACKEND_VISIBLE_DEVICES": lambda: to_dict(
        getenv(
            "GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_BACKEND_VISIBLE_DEVICES",
            "amd.com/devices=ROCR_VISIBLE_DEVICES;"
            "huawei.com/devices=ASCEND_RT_VISIBLE_DEVICES,NPU_VISIBLE_DEVICES;"
            "cambricon.com/devices=MLU_VISIBLE_DEVICES;"
            "hygon.com/devices=HIP_VISIBLE_DEVICES;"
            "iluvatar.ai/devices=CUDA_VISIBLE_DEVICES;"
            "metax-tech.com/devices=CUDA_VISIBLE_DEVICES;"
            "mthreads.com/devices=CUDA_VISIBLE_DEVICES;"
            "nvidia.com/devices=CUDA_VISIBLE_DEVICES;",
        ),
        list_sep=",",
    ),
    # Detector
    "GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY", "1"),
    ),
    # Deployer
    "GPUSTACK_RUNTIME_DOCKER_PAUSE_IMAGE": lambda: getenv(
        "GPUSTACK_RUNTIME_DOCKER_PAUSE_IMAGE",
        "rancher/mirrored-pause:3.10",
    ),
    "GPUSTACK_RUNTIME_DOCKER_UNHEALTHY_RESTART_IMAGE": lambda: getenv(
        "GPUSTACK_RUNTIME_DOCKER_UNHEALTHY_RESTART_IMAGE",
        "willfarrell/autoheal:latest",
    ),
    "GPUSTACK_RUNTIME_DOCKER_EPHEMERAL_FILES_DIR": lambda: mkdir_path(
        getenv(
            "GPUSTACK_RUNTIME_DOCKER_EPHEMERAL_FILES_DIR",
            expand_path("~/.cache/gpustack-runtime"),
        ),
    ),
    "GPUSTACK_RUNTIME_KUBERNETES_NODE_NAME": lambda: getenv(
        "GPUSTACK_RUNTIME_KUBERNETES_NODE_NAME",
        None,
    ),
    "GPUSTACK_RUNTIME_KUBERNETES_NAMESPACE": lambda: getenv(
        "GPUSTACK_RUNTIME_KUBERNETES_NAMESPACE",
        "default",
    ),
    "GPUSTACK_RUNTIME_KUBERNETES_DOMAIN_SUFFIX": lambda: getenv(
        "GPUSTACK_RUNTIME_KUBERNETES_DOMAIN_SUFFIX",
        "cluster.local",
    ),
    "GPUSTACK_RUNTIME_KUBERNETES_SERVICE_TYPE": lambda: choice(
        getenv(
            "GPUSTACK_RUNTIME_KUBERNETES_SERVICE_TYPE",
            "ClusterIP",
        ),
        options=["ClusterIP", "NodePort", "LoadBalancer"],
        default="ClusterIP",
    ),
    "GPUSTACK_RUNTIME_KUBERNETES_QUORUM_READ": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_KUBERNETES_QUORUM_READ", "0"),
    ),
}


# --8<-- [end:env-vars-definition]


@lru_cache
def __getattr__(name: str):
    # lazy evaluation of environment variables
    if name in variables:
        return variables[name]()
    msg = f"module {__name__} has no attribute {name}"
    raise AttributeError(msg)


def __dir__():
    return list(variables.keys())


def expand_path(path: Path | str) -> Path | str:
    """
    Expand a path, resolving `~` and environment variables.

    Args:
        path (str | Path): The path to expand.

    Returns:
        str | Path: The expanded path.

    """
    if isinstance(path, str):
        return str(Path(path).expanduser().resolve())
    return path.expanduser().resolve()


def mkdir_path(path: Path | str | None) -> Path | None:
    """
    Create a directory if it does not exist.

    Args:
        path (str | Path): The path to the directory.

    """
    if not path:
        return None
    if isinstance(path, str):
        path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def to_bool(value: str) -> bool:
    """
    Check if a value is considered true.

    Args:
        value (str): The value to check.

    Returns:
        bool: True if the value is considered true, False otherwise.

    """
    return value.lower() in ("1", "true", "yes", "on")


def to_dict(
    value: str,
    sep: str = ";",
    list_sep: str | None = None,
) -> dict[str, str] | dict[str, list[str]]:
    """
    Convert a (sep)-separated string to a dictionary.
    If list_sep is provided, values containing list_sep will be split into lists.

    Args:
        value:
            The (sep)-separated string.
        sep:
            The separator used in the string.
        list_sep:
            Separator for splitting values into lists.

    Returns:
        The resulting dictionary.

    """
    if not value:
        return {}

    result = {}
    for item in value.split(sep):
        if "=" in item:
            key, val = item.split("=", 1)
            key = key.strip()
            val = val.strip()
            if list_sep:
                val = to_list(value, sep=list_sep)
        else:
            key = item.strip()
            val = ""
            if list_sep:
                val = []

        if key:
            result[key] = val
    return result


def to_list(value: str | None, sep: str = ",") -> list[str]:
    """
    Convert a (sep)-separated string to a list.

    Args:
        value:
            The (sep)-separated string.
        sep:
            The separator used in the string.

    Returns:
        The resulting list.

    """
    if not value:
        return []
    return [item.strip() for item in value.split(sep) if item.strip()]


def to_set(value: str | None, sep: str = ",") -> set[str]:
    """
    Convert a (sep)-separated string to a set.

    Args:
        value:
            The (sep)-separated string.
        sep:
            The separator used in the string.

    Returns:
        The resulting set.

    """
    if not value:
        return set()
    return {item.strip() for item in value.split(sep) if item.strip()}


def choice(value: str, options: list[str], default: str = "") -> str:
    """
    Check if a value is one of the given options.

    Args:
        value (str): The value to check.
        options (list[str]): The list of options.
        default (str): The default value if the value is not in the options.

    Returns:
        The value if it is in the options, otherwise the default value.

    """
    if value in options:
        return value
    return default
