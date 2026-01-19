from __future__ import annotations as __future_annotations__

import json
from importlib import resources

__all__ = ["load"]


def load(filename: str, callback: callable | None = None) -> list[tuple]:
    """
    Load a fixture file and return its content as a list.

    Args:
        filename:
            The name of the fixture file to load.
        callback:
            A callback function to process the loaded data.

    Returns:
        The content of the fixture file as a list.
    """
    data_path = resources.files(__package__).joinpath(filename)
    with data_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if callback:
        return callback(data)
    return data


def resolve_load_path(filename: str) -> str:
    """
    Resolve the absolute path of a fixture file.

    Args:
        filename:
            The name of the fixture file.

    Returns:
        The absolute path of the fixture file as a string.
    """
    data_path = resources.files(__package__).joinpath(filename)
    return str(data_path)
