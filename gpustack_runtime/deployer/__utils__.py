from __future__ import annotations

import base64
import enum
import json
import platform
import re
from functools import lru_cache
from typing import Any

import yaml
from gpustack_runner import DockerImage, list_backend_runners

from ..detector import backend_to_manufacturer, detect_backend, detect_devices
from ..detector.ascend import get_ascend_cann_variant

_RE_RFC1123_DOMAIN_NAME = re.compile(r"(?!-)[a-z0-9-]{1,63}(?<!-)")
"""
Regex for RFC1123 domain name, which must:
    - contain no more than 63 characters
    - contain only lowercase alphanumeric characters or '-'
    - start with an alphanumeric character
    - end with an alphanumeric character
"""

_RE_RFC1123_SUBDOMAIN_NAME = re.compile(
    r"(?!-)[a-z0-9-]{1,63}(?<!-)(\.(?!-)[a-z0-9-]{1,63}(?<!-))*",
)
"""
Regex for RFC1123 subdomain name, which must:
    - contain no more than 253 characters
    - contain only lowercase alphanumeric characters, '-' or '.'
    - start with an alphanumeric character
    - end with an alphanumeric character
"""

_RE_RFC1035_DOMAIN_NAME = re.compile(r"(?![0-9-])[a-zA-Z0-9_.-]{1,63}(?<![.-])")
"""
Regex for RFC1035 domain name, which must:
    - contain no more than 63 characters
    - contain only lowercase alphanumeric characters or '-'
    - start with an alphabetic character
    - end with an alphanumeric character
"""


@lru_cache
def correct_runner_image(
    image: str,
    backend: str = "",
    backend_version: str = "",
) -> (str, bool):
    """
    Correct the gpustack-runner image by rendering it with the host.
    Noted, this function only corrects the backend, backend_version and backend_variant fields of the image.
    If no correction is applied, the original image will be returned.

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
      use the oldest backend version in gpustack-runner.

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
        The corrected gpustack-runner image,
        and a boolean indicating whether correction was applied.

    """
    docker_image = DockerImage.from_string(image)
    if not docker_image:
        return image, False

    # If the image's backend is "Host", detect host's backend.
    # It's worth noting that we only take the first detected backend here,
    # so, if there are multiple backends in host, the other backends will be ignored.
    if docker_image.backend == "Host":
        if not backend:
            backend = _get_backend()
            if not backend:
                return image, False
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
    if not backend_version_closest:
        return image, False
    # Update backend_version to the closest one.
    docker_image.backend_version = backend_version_closest

    corrected_image = str(docker_image)
    if corrected_image == image:
        return image, False

    return corrected_image, True


@lru_cache
def _get_backend() -> str:
    """
    Get the first detected backend name.

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
        If no backend version matched, return the oldest backend version.

    """
    arch = platform.machine().lower()
    if arch == "x86_64":
        arch = "amd64"
    elif arch == "aarch64":
        arch = "arm64"

    runners = list_backend_runners(
        backend=backend,
        service=service,
        service_version=service_version,
        platform=f"linux/{arch}",
    )
    if not runners:
        return None

    default_backend_version = runners[0].versions[-1].version

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
def compare_versions(v1: str | None, v2: str | None) -> int:
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
    if v1 is None:
        v1 = ""
    if v2 is None:
        v2 = ""

    v1 = v1.removeprefix("v")
    v2 = v2.removeprefix("v")

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
def _explode(v: str | None) -> list[int | list[int]]:
    """
    Explode a string into a list of integers and lists of codepoints.

    Args:
        v:
            The string to explode.

    Returns:
        A list of integers and lists of codepoints.

    """
    if v is None:
        return []

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


def is_rfc1123_domain_name(name: str) -> bool:
    """
    Check if the given name is a valid RFC 1123 domain name.

    Args:
        name:
            The domain name to check.

    Returns:
        True if the name is a valid RFC 1123 domain name, False otherwise.

    """
    return bool(_RE_RFC1123_DOMAIN_NAME.fullmatch(name))


def validate_rfc1123_domain_name(name: str):
    """
    Validate that the given name is a valid RFC 1123 domain name.

    Args:
        name:
            The domain name to validate.

    Raises:
        ValueError:
            If the name is not a valid RFC 1123 domain name.

    """
    if not is_rfc1123_domain_name(name):
        msg = (
            f"Invalid RFC 1123 domain name: '{name}'. "
            "It must contain no more than 63 characters, "
            "contain only lowercase alphanumeric characters or '-', "
            "start with an alphanumeric character, "
            "and end with an alphanumeric character."
        )
        raise ValueError(
            msg,
        )


def is_rfc1123_subdomain_name(name: str) -> bool:
    """
    Check if the given name is a valid RFC 1123 subdomain name.

    Args:
        name:
            The subdomain name to check.

    Returns:
        True if the name is a valid RFC 1123 subdomain name, False otherwise.

    """
    return bool(_RE_RFC1123_SUBDOMAIN_NAME.fullmatch(name))


def validate_rfc1123_subdomain_name(name: str):
    """
    Validate that the given name is a valid RFC 1123 subdomain name.

    Args:
        name:
            The subdomain name to validate.

    Raises:
        ValueError:
            If the name is not a valid RFC 1123 subdomain name.

    """
    if not is_rfc1123_subdomain_name(name):
        msg = (
            f"Invalid RFC 1123 subdomain name: '{name}'. "
            "It must contain no more than 253 characters, "
            "contain only lowercase alphanumeric characters, '-' or '.', "
            "start with an alphanumeric character, "
            "and end with an alphanumeric character."
        )
        raise ValueError(
            msg,
        )


def is_rfc1035_domain_name(name: str) -> bool:
    """
    Check if the given name is a valid RFC 1035 domain name.

    Args:
        name:
            The domain name to check.

    Returns:
        True if the name is a valid RFC 1035 domain name, False otherwise:

    """
    return bool(_RE_RFC1035_DOMAIN_NAME.fullmatch(name))


def validate_rfc1035_domain_name(name: str):
    """
    Validate that the given name is a valid RFC 1035 domain name.

    Args:
        name:
            The domain name to validate.

    Raises:
        ValueError:
            If the name is not a valid RFC 1035 domain name.

    """
    if not is_rfc1035_domain_name(name):
        msg = (
            f"Invalid RFC 1035 domain name: '{name}'. "
            "It must contain no more than 63 characters, "
            "contain only alphanumeric characters, '_', '.' or '-', "
            "start with an alphabetic character, "
            "and end with an alphanumeric character."
        )
        raise ValueError(
            msg,
        )


def fnv1a_32(data: bytes | str) -> int:
    """
    Computes the FNV-1a 32-bit hash for the given bytes or str.

    Args:
        data:
            The input bytes or str to hash.

    Returns:
        The FNV-1a 32-bit hash as an integer.

    """
    if isinstance(data, str):
        data = data.encode("utf-8")

    hash_value = 0x811C9DC5

    for byte in data:
        hash_value ^= byte
        hash_value *= 0x01000193
        hash_value &= 0xFFFFFFFF

    return hash_value


def fnv1a_32_hex(data: bytes | str) -> str:
    """
    Computes the FNV-1a 32-bit hash for the given bytes or str,
    and returns it as a hexadecimal string.

    Args:
        data:
            The input bytes or str to hash.

    Returns:
        The FNV-1a 32-bit hash as a hexadecimal string.

    """
    hash_value = fnv1a_32(data)
    return f"{hash_value:08x}"


def fnv1a_64(data: bytes | str) -> int:
    """
    Computes the FNV-1a 64-bit hash for the given bytes or str.

    Args:
        data:
            The input bytes or str to hash.

    Returns:
        The FNV-1a 64-bit hash as an integer.

    """
    if isinstance(data, str):
        data = data.encode("utf-8")

    hash_value = 0xCBF29CE484222325

    for byte in data:
        hash_value ^= byte
        hash_value *= 0x100000001B3
        hash_value &= 0xFFFFFFFFFFFFFFFF

    return hash_value


def fnv1a_64_hex(data: bytes | str) -> str:
    """
    Computes the FNV-1a 64-bit hash for the given bytes or str,
    and returns it as a hexadecimal string.

    Args:
        data:
            The input bytes or str to hash.

    Returns:
        The FNV-1a 64-bit hash as a hexadecimal string.

    """
    hash_value = fnv1a_64(data)
    return f"{hash_value:016x}"


def base64_encode(data: bytes | str) -> str:
    """
    Encode bytes or str to a base64 string.

    Args:
        data:
            The input bytes or str to encode.

    Returns:
        The base64 encoded string.

    """
    if isinstance(data, str):
        data = data.encode("utf-8")

    return base64.b64encode(data).decode("ascii")


_KiB = 1 << 10
_MiB = 1 << 20
_GiB = 1 << 30
_TiB = 1 << 40


def bytes_to_human_readable(size_in_bytes: int) -> str:
    """
    Convert bytes to a human-readable string.

    Args:
        size_in_bytes:
            The size in bytes.

    Returns:
        The human-readable string.

    """
    if size_in_bytes >= _TiB:
        return f"{size_in_bytes / _TiB:.2f} TiB"
    if size_in_bytes >= _GiB:
        return f"{size_in_bytes / _GiB:.2f} GiB"
    if size_in_bytes >= _MiB:
        return f"{size_in_bytes / _MiB:.2f} MiB"
    if size_in_bytes >= _KiB:
        return f"{size_in_bytes / _KiB:.2f} KiB"
    return f"{size_in_bytes} B"


def replace_image_with(
    image: str,
    registry: str | None = None,
    namespace: str | None = None,
    repository: str | None = None,
) -> str:
    """
    Replace the registry, namespace, and repository of a Docker image string.

    The given image string is parsed into its components (registry, namespace, repository, tag),
    and the specified components are replaced with the provided values.

    The format of a Docker image string is:
    [registry/][namespace/]repository[:tag|@digest]

    Args:
        image:
            The original Docker image string.
        registry:
            The new registry to use. If None, keep the original registry.
        namespace:
            The new namespace to use. If None, keep the original namespace.
        repository:
            The new repository to use. If None, keep the original repository.

    Returns:
        The modified Docker image string.

    """
    if not image or (not registry and not namespace and not repository):
        return image

    registry = registry.strip() if registry else None
    namespace = namespace.strip() if namespace else None
    repository = repository.strip() if repository else None

    image_reg, image_ns, image_repo, image_tag = (
        None,
        None,
        None,
        None,
    )
    image_rest = image.strip()

    # Get tag.
    parts = image_rest.rsplit("@", maxsplit=1)
    if len(parts) == 2:
        image_rest, image_tag = parts
    else:
        parts = image_rest.rsplit(":", maxsplit=1)
        if len(parts) == 2 and "/" not in parts[1]:
            image_rest, image_tag = parts
    if not image_rest:
        return image

    # Get repository.
    parts = image_rest.rsplit("/", maxsplit=1)
    if len(parts) == 2:
        image_rest, image_repo = parts
    else:
        image_rest, image_repo = None, image_rest

    # Get namespace.
    if image_rest:
        parts = image_rest.rsplit("/", maxsplit=1)
        if len(parts) == 2:
            image_reg, image_ns = parts
        else:
            image_reg, image_ns = None, image_rest

    return make_image_with(
        repository=repository or image_repo,
        registry=registry or image_reg,
        namespace=namespace or image_ns,
        tag=image_tag,
    )


def make_image_with(
    repository: str,
    registry: str | None = None,
    namespace: str | None = None,
    tag: str | None = None,
) -> str:
    """
    Make a Docker image string from the given registry, namespace, repository, and tag.

    The format of a Docker image string is:
    [registry/][namespace/]repository[:tag|@digest]

    Args:
        repository:
            The repository name.
        registry:
            The registry to use. If None, no registry will be used.
        namespace:
            The namespace to use. If None, no namespace will be used.
        tag:
            The tag to use. If None, no tag will be used.

    Returns:
        The Docker image string.

    """
    if not repository or (not registry and not namespace and not tag):
        return repository

    image = ""
    if registry:
        image += f"{registry}/"
    if namespace:
        image += f"{namespace}/"
    elif registry:
        image += "library/"
    image += repository
    if not tag:
        return image
    if tag.startswith("sha256:"):
        image += f"@{tag}"
    else:
        image += f":{tag}"
    return image
