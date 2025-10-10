from __future__ import annotations

import contextlib
import enum
import json
import platform
from functools import lru_cache
from typing import Any

import yaml
from gpustack_runner import list_backend_runners

from ..detector import backend_to_manufacturer, detect_backend, detect_devices
from ..detector.ascend import get_ascend_cann_variant


def render_image(
    image: str,
    os: str | None = None,
    arch: str | None = None,
    backend_name: str | None = None,
    backend_version: str | None = None,
    backend_variant: str | None = None,
) -> str:
    """
    Render the image string with the specified parameters.

    Args:
        image:
            The image string to render.
        os:
            The operating system to use. If None, the current OS will be used.
        arch:
            The architecture to use. If None, the current architecture will be used.
        backend_name:
            The backend name to use. If None, the detected backend will be used.
            If no backend is detected, "cuda" will be used.
        backend_version:
            The backend version to use. If None, the detected backend version will be used.
            If the detected backend version is not available, the closest version of gpustack-runner will be used.
        backend_variant:
            The backend variant to use. If None, the detected backend variant will be used.
            If the backend is "cann" and no variant is detected, "910b" will be used.

    Returns:
        The rendered image string.

    """
    if not image or "{" not in image:
        return image

    if os is None:
        os = get_os()
    if arch is None:
        arch = get_arch()
    if backend_name is None:
        backend_name = get_backend()
    if backend_version is None:
        backend_version, _ = get_backend_version_and_variant(backend_name)
    if backend_variant is None:
        _, backend_variant = get_backend_version_and_variant(backend_name)

    # Default to CUDA if no backend is detected.
    if not backend_name:
        backend_name = "cuda"
    # Default to 910b for CANN if no variant is detected.
    if backend_name == "cann" and not backend_variant:
        backend_variant = "910b"
    # Get the closest backend version based on backend version.
    backend_version_closest = get_closest_backend_version_in_runner(
        backend_name,
        backend_version,
    )

    backend = f"{backend_name}{backend_version}"
    backend_corrected = f"{backend_name}{backend_version_closest}"
    if backend_variant:
        backend += f"-{backend_variant}"
        backend_corrected += f"-{backend_variant}"

    with contextlib.suppress(KeyError):
        image = image.format_map(
            {
                "OS": os,
                "ARCH": arch,
                "BACKEND": backend,
                "BACKEND_CORRECTED": backend_corrected,
            },
        )

    return image


@lru_cache
def get_os() -> str:
    """
    Get the operating system of the current machine in lowercase.

    Returns:
        The operating system of the current machine.

    """
    return platform.system().lower()


@lru_cache
def get_arch() -> str:
    """
    Get the architecture of the current machine in lowercase.

    Returns:
        The architecture of the current machine.

    """
    arch = platform.machine().lower()
    if arch == "x86_64":
        arch = "amd64"
    elif arch == "aarch64":
        arch = "arm64"
    return arch


@lru_cache
def get_backend() -> str:
    """
    Get the detected backend name.
    If no backend is detected, return an empty string.

    Returns:
        The name of the backend.

    """
    backend = detect_backend()
    return backend if backend else ""


@lru_cache
def get_backend_version_and_variant(backend_name: str) -> (str, str):
    """
    Get the backend version and variant for the specified backend name.

    Args:
        backend_name:
            The name of the backend.

    Returns:
        A tuple of (backend_version, backend_variant).

    """
    manufacturer = backend_to_manufacturer(backend_name)
    if not manufacturer:
        return "", ""

    devices = detect_devices()
    if not devices:
        return "", ""

    version = None
    variant = None
    for device in devices:
        if device.manufacturer != manufacturer:
            continue
        if device.runtime_version:
            version = device.runtime_version
        if backend_name == "cann":
            soc_name = device.appendix.get("arch_family", "")
            variant = get_ascend_cann_variant(soc_name)
        break

    return version if version else "", variant if variant else ""


@lru_cache
def get_closest_backend_version_in_runner(
    backend_name: str,
    backend_version: str,
) -> str:
    """
    Get the closest backend version that is less than or equal to the specified version.
    If no such version is found, return the default version of the backend in gpustack-runner.

    Args:
        backend_name:
            The name of the backend.
        backend_version:
            The version of the backend.

    Returns:
        The closest backend version.

    """
    runners = list_backend_runners(
        backend=backend_name,
    )
    if not runners:
        return backend_version

    default_backend_version = runners[0].versions[0].version

    if backend_version:
        for v in runners[0].versions:
            if v.version > backend_version:
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
