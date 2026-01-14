from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from .thead import THeadGenerator

if TYPE_CHECKING:
    from pathlib import Path

    from ...detector import ManufacturerEnum
    from .__types__ import Generator


_GENERATORS: list[Generator] = [
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


def generate_config(
    manufacturer: ManufacturerEnum | str,
    output: Path | None = None,
    _format: Literal["yaml", "json"] = "yaml",
) -> tuple[str | None, str | None]:
    """
    Generate the CDI configuration.

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


__all__ = [
    "generate_config",
]
