from __future__ import annotations as __future_annotations__

from typing import TYPE_CHECKING, Literal

from ...detector import Device, manufacturer_to_backend
from ...detector import supported_manufacturers as detector_supported_manufacturers
from .__types__ import Config, manufacturer_to_cdi_kind, manufacturer_to_runtime_env
from .amd import AMDGenerator
from .ascend import AscendGenerator
from .hygon import HygonGenerator
from .iluvatar import IluvatarGenerator
from .metax import MetaXGenerator
from .thead import THeadGenerator

if TYPE_CHECKING:
    from pathlib import Path

    from ...detector import ManufacturerEnum
    from .__types__ import Generator

_GENERATORS: list[Generator] = [
    AMDGenerator(),
    AscendGenerator(),
    HygonGenerator(),
    IluvatarGenerator(),
    MetaXGenerator(),
    THeadGenerator(),
]
"""
List of all CDI generators.
"""

_GENERATORS_MAP: dict[ManufacturerEnum, Generator] = {
    gen.manufacturer: gen for gen in _GENERATORS
}
"""
Mapping from manufacturer to CDI generator.
"""


def dump_config(
    manufacturer: ManufacturerEnum,
    output: Path | None = None,
    _format: Literal["yaml", "json"] = "yaml",
) -> tuple[str | None, str | None]:
    """
    Dump the CDI configuration.

    Args:
        manufacturer:
            Manufacturer to filter the generation.
        output:
            The directory to store CDI files.
            If None, CDI configuration is not stored.
        _format:
            The format of the CDI configuration.
            Either "yaml" or "json". Default is "yaml".

    Returns:
        A tuple containing:
        - CDI configuration string, or None if not supported.
        - Path to the output CDI file, or None if not stored.

    Raises:
        Exception if failed to write to the output file.

    """
    gen = _GENERATORS_MAP.get(manufacturer)
    if not gen:
        return None, None

    cfg = gen.generate()
    if not cfg:
        return None, None

    expected = cfg.stringify(_format)
    if not output:
        return expected, None

    cdi_file = cfg.kind.replace("/", "-") + f".{_format}"
    cdi_path = output / cdi_file
    if cdi_path.exists():
        actual = cdi_path.read_text(encoding="utf-8")
        if actual == expected:
            return expected, str(cdi_path)

    cdi_path.write_text(expected, encoding="utf-8")
    return expected, str(cdi_path)


def generate_config(
    device: Device,
) -> Config | None:
    """
    Generate the CDI configuration for the given devices.

    Args:
        device:
            The detected device.

    Returns:
        The Config object, or None if not supported.

    """
    gen = _GENERATORS_MAP.get(device.manufacturer)
    if not gen:
        return None

    cfg = gen.generate(devices=[device])
    return cfg


def available_manufacturers() -> list[ManufacturerEnum]:
    """
    Get a list of available manufacturers,
    which allow CDI generation,
    regardless of whether they are supported or not.

    Returns:
        A list of available manufacturers.

    """
    return list(_GENERATORS_MAP.keys())


def supported_manufacturers() -> list[ManufacturerEnum]:
    """
    Get a list of supported manufacturers,
    which allow CDI generation,
    and must be supported in the current environment.

    Returns:
        A list of supported manufacturers.

    """
    manus = detector_supported_manufacturers()
    return [manu for manu in manus if manu in _GENERATORS_MAP]


def available_backends() -> list[str]:
    """
    Get a list of available backends,
    which allow CDI generation,
    regardless of whether they are supported or not.

    Returns:
        A list of available backends.

    """
    return [manufacturer_to_backend(manu) for manu in _GENERATORS_MAP]


__all__ = [
    "Config",
    "available_backends",
    "available_manufacturers",
    "dump_config",
    "generate_config",
    "manufacturer_to_cdi_kind",
    "manufacturer_to_runtime_env",
    "supported_manufacturers",
]
