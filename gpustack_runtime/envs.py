from __future__ import annotations

import contextlib
import os
from functools import lru_cache
from os import getenv as sys_getenv
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
    GPUSTACK_RUNTIME_LOG_WARNING: bool = False
    """
    Enable logging warnings when GPUSTACK_RUNTIME_LOG_LEVEL=DEBUG.
    """
    GPUSTACK_RUNTIME_LOG_EXCEPTION: bool = True
    """
    Enable logging exceptions when GPUSTACK_RUNTIME_LOG_LEVEL=DEBUG.
    """

    ## Detector
    GPUSTACK_RUNTIME_DETECT: str | None = None
    """
    Detector to use (options: Auto, AMD, ASCEND, CAMBRICON, HYGON, ILUVATAR, METAX, MTHREADS, NVIDIA).
    """
    GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK: bool = False
    """
    Enable no PCI check during detection.
    Useful for WSL environments, where PCI information may not be available.
    """
    GPUSTACK_RUNTIME_DETECT_NO_TOOLKIT_CALL: bool = False
    """
    Enable only using management libraries calls during detection.
    Device detection typically involves calling platform-side management libraries and platform-side toolkit to retrieve extra information.
    For example, during NVIDIA detection, the NVML and CUDA are called, with CUDA used to retrieve GPU cores.
    However, if certain toolchains are not correctly installed in the environment,
    such as the Nvidia Fabric Manager being missing, calling the CUDA can cause blocking.
    Enabling this parameter can prevent blocking events.
    """
    GPUSTACK_RUNTIME_DETECT_BACKEND_MAP_RESOURCE_KEY: dict[str, str] | None = None
    """
    The detected backend mapping to resource keys,
    e.g `{"cuda": "nvidia.com/devices", "rocm": "amd.com/devices"}`.
    """
    GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY: bool = True
    """
    Use physical index priority at detecting devices.
    """
    ## Deployer
    GPUSTACK_RUNTIME_DEPLOY: str | None = None
    """
    Deployer to use (options: Auto, Docker, Kubernetes).
    """
    GPUSTACK_RUNTIME_DEPLOY_DEFAULT_REGISTRY: str | None = None
    """
    Default container registry for deployer to pull images from.
    If not set, it should be "docker.io".
    If the image name already contains a registry, this setting will be ignored.
    """
    GPUSTACK_RUNTIME_DEPLOY_DEFAULT_NAMESPACE: str | None = None
    """
    Namespace for default runner images.
    If not set, it should be "gpustack".
    """
    GPUSTACK_RUNTIME_DEPLOY_DEFAULT_REGISTRY_USERNAME: str | None = None
    """
    Username for the default container registry.
    """
    GPUSTACK_RUNTIME_DEPLOY_DEFAULT_REGISTRY_PASSWORD: str | None = None
    """
    Password for the default container registry.
    """
    GPUSTACK_RUNTIME_DEPLOY_API_CALL_ERROR_DETAIL: bool = True
    """
    Enable detailing the API call error during deployment.
    """
    GPUSTACK_RUNTIME_DEPLOY_PRINT_CONVERSION: bool = False
    """
    Enable printing the conversion during deployment.
    GPUStack Runtime provides a unified Workload definition API,
    which will be converted to the specific Container Runtime API calls(e.g., Docker SDK, Kubernetes API).
    Enabling this option will print the final converted API calls in INFO log for debugging purposes.
    """
    GPUSTACK_RUNTIME_DEPLOY_ASYNC: bool = True
    """
    Enable asynchronous deployment.
    """
    GPUSTACK_RUNTIME_DEPLOY_ASYNC_THREADS: int | None = None
    """
    The number of threads in the threadpool,
    which is used for asynchronous deployment operations.
    If not set, it should be min(32, system cpu cores + 4)
    """
    GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT: bool = False
    """
    Enable mirrored deployment mode.
    During deployment mirroring, when deployer deploys a workload,
    it will configure the workload with the same following settings as the deployer:
        - Container Runtime(e.g., nvidia, amd, .etc),
        - Customized environment variables,
        - Customized volume mounts,
        - Customized device or device requests,
        - Customized capabilities.
    To be noted, without `GPUSTACK_RUNTIME_DEPLOY_MIRRORED_NAME` configured,
    if the deployer failed to retrieve its own settings, it will skip mirrored deployment.
    """
    GPUSTACK_RUNTIME_DEPLOY_MIRRORED_NAME: str | None = None
    """
    The name of the deployer.
    Works with `GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT`.
    In some senses, the deployer needs to know its own name to execute mirrored deployment,
    e.g., when the deployer is a Kubernetes Pod, it need to know its own Pod name.
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
    GPUSTACK_RUNTIME_DEPLOY_IMAGE_PULL_POLICY: str | None = None
    """
    Image pull policy for the deployer (e.g., Always, IfNotPresent, Never).
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
    GPUSTACK_RUNTIME_DEPLOY_RUNTIME_VISIBLE_DEVICES_VALUE_UUID: set[str] | None = None
    """
    Use UUIDs for the given runtime visible devices environment variables.
    """
    GPUSTACK_RUNTIME_DEPLOY_BACKEND_VISIBLE_DEVICES_VALUE_ALIGNMENT: set[str] | None = (
        None
    )
    """
    Enable value alignment for the given backend visible devices environment variables.
    When detected devices are considered to be partially mapped (starting from a non-zero value or not contiguous),
    alignment is performed to ensure they are correctly identified.
    """
    GPUSTACK_RUNTIME_DEPLOY_CPU_AFFINITY: bool = False
    """
    Enable CPU affinity for deployed workloads.
    """
    GPUSTACK_RUNTIME_DEPLOY_NUMA_AFFINITY: bool = False
    """
    Enable NUMA affinity for deployed workloads.
    When enabled, `GPUSTACK_RUNTIME_DEPLOY_CPU_AFFINITY` is also implied.
    """

    # Deployer

    ## Docker
    GPUSTACK_RUNTIME_DOCKER_MIRRORED_NAME_FILTER_LABELS: dict[str, str] | None = None
    """
    Filter labels for selecting the mirrored deployer container in Docker.
    Only works when `GPUSTACK_RUNTIME_DEPLOY_MIRRORED_NAME` is not set.
    Normally, it should be injected automatically via CI without any manual configuration.
    """
    GPUSTACK_RUNTIME_DOCKER_IMAGE_NO_PULL_VISUALIZATION: bool = False
    """
    Disable image pull visualization in Docker deployer.
    """
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
    GPUSTACK_RUNTIME_DOCKER_MUTE_ORIGINAL_HEALTHCHECK: bool = True
    """
    Mute the original healthcheck of the container.
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
    GPUSTACK_RUNTIME_KUBERNETES_DELETE_PROPAGATION_POLICY: str | None = None
    """
    Deletion propagation policy for Kubernetes resources (e.g., Foreground, Background, Orphan).
    """

# --8<-- [start:env-vars-definition]

variables: dict[str, Callable[[], Any]] = {
    # Global
    "GPUSTACK_RUNTIME_LOG_LEVEL": lambda: getenv(
        "GPUSTACK_RUNTIME_LOG_LEVEL",
        "INFO",
    ),
    "GPUSTACK_RUNTIME_LOG_TO_FILE": lambda: mkdir_path(
        getenv("GPUSTACK_RUNTIME_LOG_TO_FILE", None),
    ),
    "GPUSTACK_RUNTIME_LOG_WARNING": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_LOG_WARNING", "0"),
    ),
    "GPUSTACK_RUNTIME_LOG_EXCEPTION": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_LOG_EXCEPTION", "1"),
    ),
    ## Detector
    "GPUSTACK_RUNTIME_DETECT": lambda: getenv(
        "GPUSTACK_RUNTIME_DETECT",
        "Auto",
    ),
    "GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK": lambda: ternary(
        lambda: getenv("GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK") is not None,
        lambda: to_bool(getenv("GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK")),
        lambda: any(x in get_os_release() for x in ("microsoft", "wsl")),
    ),
    "GPUSTACK_RUNTIME_DETECT_NO_TOOLKIT_CALL": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DETECT_NO_TOOLKIT_CALL", "0"),
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
    "GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY", "1"),
    ),
    ## Deployer
    "GPUSTACK_RUNTIME_DEPLOY": lambda: getenv(
        "GPUSTACK_RUNTIME_DEPLOY",
        "Auto",
    ),
    "GPUSTACK_RUNTIME_DEPLOY_DEFAULT_REGISTRY": lambda: trim_str(
        getenvs(
            keys=[
                "GPUSTACK_RUNTIME_DEPLOY_DEFAULT_REGISTRY",
                "GPUSTACK_SYSTEM_DEFAULT_CONTAINER_REGISTRY",  # Compatible with gpustack/gpustack.
            ],
        ),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_DEFAULT_NAMESPACE": lambda: trim_str(
        getenv("GPUSTACK_RUNTIME_DEPLOY_DEFAULT_NAMESPACE"),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_DEFAULT_REGISTRY_USERNAME": lambda: trim_str(
        getenv("GPUSTACK_RUNTIME_DEPLOY_DEFAULT_REGISTRY_USERNAME"),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_DEFAULT_REGISTRY_PASSWORD": lambda: getenv(
        "GPUSTACK_RUNTIME_DEPLOY_DEFAULT_REGISTRY_PASSWORD",
    ),
    "GPUSTACK_RUNTIME_DEPLOY_API_CALL_ERROR_DETAIL": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DEPLOY_API_CALL_ERROR_DETAIL", "1"),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_PRINT_CONVERSION": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DEPLOY_PRINT_CONVERSION", "0"),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_ASYNC": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DEPLOY_ASYNC", "1"),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_ASYNC_THREADS": lambda: to_int(
        getenv("GPUSTACK_RUNTIME_DEPLOY_ASYNC_THREADS"),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT", "0"),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_NAME": lambda: getenv(
        "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_NAME",
    ),
    "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_ENVIRONMENTS": lambda: to_set(
        getenv(
            "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_ENVIRONMENTS",
        ),
        sep=";",
    ),
    "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_VOLUMES": lambda: to_set(
        getenv(
            "GPUSTACK_RUNTIME_DEPLOY_MIRRORED_DEPLOYMENT_IGNORE_VOLUMES",
        ),
        sep=";",
    ),
    "GPUSTACK_RUNTIME_DEPLOY_CORRECT_RUNNER_IMAGE": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DEPLOY_CORRECT_RUNNER_IMAGE", "1"),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_IMAGE_PULL_POLICY": lambda: choice(
        getenv(
            "GPUSTACK_RUNTIME_DEPLOY_IMAGE_PULL_POLICY",
            "IfNotPresent",
        ),
        options=["Always", "IfNotPresent", "Never"],
        default="IfNotPresent",
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
            "iluvatar.ai/devices=IX_VISIBLE_DEVICES;"
            "metax-tech.com/devices=CUDA_VISIBLE_DEVICES;"
            "mthreads.com/devices=METHERDS_VISIBLE_DEVICES;"
            "nvidia.com/devices=NVIDIA_VISIBLE_DEVICES;",
        ),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_BACKEND_VISIBLE_DEVICES": lambda: to_dict(
        getenv(
            "GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_BACKEND_VISIBLE_DEVICES",
            "amd.com/devices=HIP_VISIBLE_DEVICES,ROCR_VISIBLE_DEVICES;"
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
    "GPUSTACK_RUNTIME_DEPLOY_RUNTIME_VISIBLE_DEVICES_VALUE_UUID": lambda: to_set(
        getenv(
            "GPUSTACK_RUNTIME_DEPLOY_RUNTIME_VISIBLE_DEVICES_VALUE_UUID",
        ),
        sep=",",
    ),
    "GPUSTACK_RUNTIME_DEPLOY_BACKEND_VISIBLE_DEVICES_VALUE_ALIGNMENT": lambda: to_set(
        getenv(
            "GPUSTACK_RUNTIME_DEPLOY_BACKEND_VISIBLE_DEVICES_VALUE_ALIGNMENT",
            "ASCEND_RT_VISIBLE_DEVICES,NPU_VISIBLE_DEVICES",
        ),
        sep=",",
    ),
    "GPUSTACK_RUNTIME_DEPLOY_CPU_AFFINITY": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DEPLOY_CPU_AFFINITY", "0"),
    ),
    "GPUSTACK_RUNTIME_DEPLOY_NUMA_AFFINITY": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DEPLOY_NUMA_AFFINITY", "0"),
    ),
    # Deployer
    ## Docker
    "GPUSTACK_RUNTIME_DOCKER_MIRRORED_NAME_FILTER_LABELS": lambda: to_dict(
        getenv(
            "GPUSTACK_RUNTIME_DOCKER_MIRRORED_NAME_FILTER_LABELS",
        ),
        sep=";",
    ),
    "GPUSTACK_RUNTIME_DOCKER_PAUSE_IMAGE": lambda: getenv(
        "GPUSTACK_RUNTIME_DOCKER_PAUSE_IMAGE",
        "gpustack/runtime:pause",
    ),
    "GPUSTACK_RUNTIME_DOCKER_UNHEALTHY_RESTART_IMAGE": lambda: getenv(
        "GPUSTACK_RUNTIME_DOCKER_UNHEALTHY_RESTART_IMAGE",
        "gpustack/runtime:health",
    ),
    "GPUSTACK_RUNTIME_DOCKER_EPHEMERAL_FILES_DIR": lambda: mkdir_path(
        getenv(
            "GPUSTACK_RUNTIME_DOCKER_EPHEMERAL_FILES_DIR",
            expand_path("~/.cache/gpustack-runtime"),
        ),
    ),
    "GPUSTACK_RUNTIME_DOCKER_MUTE_ORIGINAL_HEALTHCHECK": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DOCKER_MUTE_ORIGINAL_HEALTHCHECK", "1"),
    ),
    "GPUSTACK_RUNTIME_DOCKER_IMAGE_NO_PULL_VISUALIZATION": lambda: to_bool(
        getenv("GPUSTACK_RUNTIME_DOCKER_IMAGE_NO_PULL_VISUALIZATION", "0"),
    ),
    ## Kubernetes
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
    "GPUSTACK_RUNTIME_KUBERNETES_DELETE_PROPAGATION_POLICY": lambda: choice(
        getenv(
            "GPUSTACK_RUNTIME_KUBERNETES_DELETE_PROPAGATION_POLICY",
            "Foreground",
        ),
        options=["Foreground", "Background", "Orphan"],
        default="Foreground",
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


def trim_str(value: str | None) -> str | None:
    """
    Trim leading and trailing whitespace from a string.

    Args:
        value:
            The string to trim.

    Returns:
        The trimmed string, or None if the input is None.

    """
    if value is not None:
        return value.strip()
    return None


def to_bool(value: str | None) -> bool:
    """
    Convert a string to a boolean.

    Args:
        value:
            The value to check.

    Returns:
        bool: True if the value is considered true, False otherwise.

    """
    if value:
        return value.lower() in ("1", "true", "yes", "on")
    return False


def to_int(value: str | None) -> int | None:
    """
    Convert a string to an integer.

    Args:
        value:
            The string to convert.

    Returns:
        The converted integer, or None if the input is None.

    """
    if value:
        try:
            return int(value)
        except (ValueError, TypeError):
            pass
    return None


def to_float(value: str | None) -> float | None:
    """
    Convert a string to a float.

    Args:
        value:
            The string to convert.

    Returns:
        The converted float, or None if the input is None.

    """
    if value:
        try:
            return float(value)
        except (ValueError, TypeError):
            pass
    return None


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
                val = to_list(val, sep=list_sep)
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


def ternary(
    condition: callable,
    true_callback: callable,
    false_callback: callable,
) -> Any:
    """
    A ternary function that evaluates a condition and returns the result of one of two callbacks.

    Args:
        condition (callable): A callable that returns a boolean value.
        true_callback (callable): A callable to be executed if the condition is True.
        false_callback (callable): A callable to be executed if the condition is False.

    Returns:
        The result of true_callback if condition is True, otherwise the result of false_callback.

    """
    if condition():
        return true_callback()
    return false_callback()


_ENV_PREFIX = "GPUSTACK_RUNTIME_"


def getenv(key: str, default=None) -> any | None:
    """
    Get the value of an environment variable.
    Try headless module variable if the key starts with "GPUSTACK_RUNTIME_".

    Args:
        key:
            The environment variable key.
        default:
            The default value if the key is not found.

    Returns:
        The value of the environment variable if it exists, otherwise None.

    """
    value = sys_getenv(key)
    if value is not None:
        return value
    if key.startswith(_ENV_PREFIX):
        headless_key = key.removeprefix(_ENV_PREFIX)
        return sys_getenv(headless_key, default)
    return default


def getenvs(keys: list[str], default=None) -> any | None:
    """
    Get the value of an environment variable.
    Return the first found value among the provided keys.

    Args:
        keys:
            The environment variable key(s).
        default:
            The default value if none of the keys are found.

    Returns:
        The value of the environment variable if it exists, otherwise None.

    """
    for key in keys:
        value = getenv(key)
        if value is not None:
            return value
    return default


def get_os_release() -> str:
    """
    Get the operating system release information.

    Returns:
        The OS release string if available, otherwise "unknown".

    """
    release = "unknown"
    with contextlib.suppress(Exception):
        r = os.uname()
        if hasattr(r, "release"):
            release = r.release.lower()
    return release
