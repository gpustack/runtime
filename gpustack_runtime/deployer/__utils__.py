from __future__ import annotations

import enum
import json
from functools import lru_cache
from typing import Any

import yaml
from gpustack_runner import DockerImage, list_backend_runners

from ..detector import backend_to_manufacturer, detect_backend, detect_devices
from ..detector.ascend import get_ascend_cann_variant


@lru_cache
def correct_runner_image(
    image: str,
    backend: str = "",
    backend_version: str = "",
) -> str:
    """
    Correct the gpustack-runner image by rendering it with the host.

    Generally, the image is in the format of:
    `[prefix/]gpustack-runner:{backend}{backend_version}[-backend_variant]-{service}{service_version}[-suffix]`

    - If the given image's backend is "Host", this function will detect host's backend,
      select the closest backend_version available in gpustack-runner,
      and fill in the backend, backend_version and backend_variant fields.
      If failed to detect host's backend, return the image as is.

      E.g. `HostX.Y-vllm0.10.0`, if the host is using NVIDIA CUDA 12.8,
      at the same time, if the gpustack-runner support NVIDIA CUDA 12.8 and vLLM 0.10.0,
      the image will be rendered to `gpustack/runner:cuda12.8-vllm0.10.0`.

    - If the given image's backend is not "Host", this function will detect version and variant of the given backend,
      select the closest backend_version available in gpustack-runner,
      and fill in the backend_version and backend_variant fields.
      If failed to detect version of the given backend or no available item matched the given backend version,
      use the default version in gpustack-runner.

      E.g. `cuda12.5-vllm0.10.0`, if the host is using NVIDIA CUDA 12.4,
      at the same time, if the gpustack-runner support NVIDIA CUDA 12.4 and vLLM 0.10.0,
      the image will be corrected to `gpustack/runner:cuda12.4-vllm0.10.0`.

    - If no runner is found for the given backend, service and service_version,
      return the image as is.

      E.g. `cuda12.5-vllm0.10.1`, if the gpustack-runner only support vLLM 0.10.0,
      the image will be returned as is.

    Args:
        image:
            The gpustack-runner image to correct.
        backend:
            The backend to use if the given image's backend is "Host",
            which is used fot mocking detected backend in testing.
        backend_version:
            The backend_version to use if the given image's backend is "Host",
            which is used fot mocking detected backend_version in testing.

    Returns:
        The corrected gpustack-runner image.

    """
    docker_image = DockerImage.from_string(image)
    if not docker_image:
        return image

    # If the image's backend is "Host", detect host's backend.
    if docker_image.backend == "Host":
        if not backend:
            backend = _get_backend()
            if not backend:
                return image
        if not backend_version:
            backend_version, _ = _get_backend_version_and_variant(backend)
        docker_image.backend = backend
        docker_image.backend_version = backend_version

    # Detect version and variant of the given backend.
    backend_version, backend_variant = _get_backend_version_and_variant(
        docker_image.backend,
    )
    # Update backend_variant if detected.
    if not backend_variant and docker_image.backend == "cann":
        # Default backend_variant for CANN if not detected.
        backend_variant = "910b"
    docker_image.backend_variant = backend_variant
    # Only update backend_version if detected version is less than or equal to the one in image.
    if (
        backend_version
        and compare_versions(backend_version, docker_image.backend_version) < 0
    ):
        docker_image.backend_version = backend_version

    # Get the closest backend_version available in gpustack-runner.
    backend_version_closest = _get_runner_closest_backend_version(
        docker_image.backend,
        docker_image.service,
        docker_image.service_version,
        docker_image.backend_version,
    )
    if backend_version_closest is None:
        return image
    # Update backend_version to the closest one.
    docker_image.backend_version = backend_version_closest

    return str(docker_image)


@lru_cache
def _get_backend() -> str:
    """
    Get the detected backend name.

    Returns:
        The name of the backend.
        If no backend is detected, return an empty string.

    """
    backend = detect_backend()
    return backend if backend else ""


@lru_cache
def _get_backend_version_and_variant(backend: str) -> (str, str):
    """
    Get the version and variant for the specified backend name.

    Args:
        backend:
            The name of the backend.

    Returns:
        A tuple of (backend_version, backend_variant).
        If the backend is not recognized or no devices are detected, return ("", "").

    """
    version = ""
    variant = ""

    devices = detect_devices(fast=False)
    if not devices:
        return version, variant

    manufacturer = backend_to_manufacturer(backend)

    for device in devices:
        if device.manufacturer != manufacturer:
            continue
        if device.runtime_version:
            version = device.runtime_version or ""
        if backend == "cann":
            soc_name = device.appendix.get("arch_family", "")
            variant = get_ascend_cann_variant(soc_name) or ""
        break

    return version, variant


@lru_cache
def _get_runner_closest_backend_version(
    backend: str,
    service: str,
    service_version: str,
    backend_version: str = "",
) -> str | None:
    """
    Get the closest backend version that is less than or equal to the specified version in gpustack-runner.

    Args:
        backend:
            The name of the backend.
        service:
            The name of the service.
        service_version:
            The version of the service.
        backend_version:
            The version of the backend.

    Returns:
        The closest backend version.
        If not found any backend runners, return None.
        If no backend version matched, return the default backend version.

    """
    runners = list_backend_runners(
        backend=backend,
        service=service,
        service_version=service_version,
    )
    if not runners:
        return None

    default_backend_version = runners[0].versions[0].version

    if backend_version:
        for v in runners[0].versions:
            if compare_versions(v.version, backend_version) > 0:
                continue
            return v.version

    return default_backend_version


def safe_dict(obj: Any) -> Any:
    """
    Filter out None from a dictionary or list recursively.

    Args:
        obj:
            The dictionary or list to filter.

    Returns:
        The filtered dictionary or list.

    """
    if isinstance(obj, dict):
        return {
            k: safe_dict(v)
            for k, v in obj.items()
            if v is not None and v not in ({}, [])
        }
    if isinstance(obj, list):
        return [safe_dict(i) for i in obj if i is not None and i != {}]
    if isinstance(obj, enum.Enum):
        return obj.value
    if hasattr(obj, "to_dict") and callable(obj.to_dict):
        d = obj.to_dict()
        return {
            k: safe_dict(v) for k, v in d.items() if v is not None and v not in ({}, [])
        }
    return obj


def safe_json(obj: Any, **kwargs) -> str:
    """
    Safely convert an object to a JSON string.

    Args:
        obj:
            The object to convert.
        **kwargs:
            Additional keyword arguments to pass to json.dumps.


    Returns:
        The JSON string representation of the object.

    """
    dict_data = safe_dict(obj)
    return json.dumps(dict_data, **kwargs)


def safe_yaml(obj: Any, **kwargs) -> str:
    """
    Safely convert an object to a YAML string.

    Args:
        obj:
            The object to convert.
        **kwargs:
            Additional keyword arguments to pass to yaml.dump.

    Returns:
        The YAML string representation of the object.

    """
    dict_data = safe_dict(obj)
    return yaml.dump(dict_data, **kwargs)


@lru_cache
def compare_versions(v1: str, v2: str) -> int:
    """
    Compare two version strings.

    Args:
        v1:
            The first version string.
        v2:
            The second version string.

    Returns:
        -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2.

    """
    if v1 == v2:
        return 0

    parts1 = _explode(v1)
    parts2 = _explode(v2)
    diff = len(parts1) - len(parts2)
    if diff > 0:
        parts2.extend([0] * diff)
    elif diff < 0:
        parts1.extend([0] * (-diff))

    for p1, p2 in zip(parts1, parts2, strict=False):
        if isinstance(p1, list) != isinstance(p2, list):
            p1, p2 = (p1, [p2]) if isinstance(p1, list) else ([p1], p2)  # noqa: PLW2901
        if p1 < p2:
            return -1
        if p1 > p2:
            return 1
    return 0


@lru_cache
def _explode(v: str) -> list[int | list[int]]:
    """
    Explode a string into a list of integers and lists of codepoints.

    Args:
        v:
            The string to explode.

    Returns:
        A list of integers and lists of codepoints.

    """
    v = v.replace("-", ".")
    v = v.replace("_", ".")
    v = v.replace("+", ".")

    ret = []
    for x in v.split("."):
        if x.isdigit():
            ret.append(int(x))
            continue
        if x.startswith("alpha"):
            ret.append([-3, *[ord(c) for c in x[5:]]])
        elif x.startswith("beta"):
            ret.append([-2, *[ord(c) for c in x[4:]]])
        elif x.startswith("rc"):
            ret.append([-1, *[ord(c) for c in x[2:]]])
        else:
            ret.append([ord(c) for c in x])
    return ret
