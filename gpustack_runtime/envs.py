from __future__ import annotations

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

    # Docker

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

    # Detect

    GPUSTACK_RUNTIME_DETECT_INDEX_IN_BUS_INDEX: bool | None = None
    """
    Whether to detect GPU index in bus index.
    """

# --8<-- [start:env-vars-definition]

variables: dict[str, Callable[[], Any]] = {
    "GPUSTACK_RUNTIME_LOG_LEVEL": lambda: getenv(
        "GPUSTACK_RUNTIME_LOG_LEVEL",
        "",
    ),
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
    "GPUSTACK_RUNTIME_DETECT_INDEX_IN_BUS_INDEX": lambda: (
        getenv("GPUSTACK_RUNTIME_DETECT_INDEX_IN_BUS_INDEX", "1")
        in ("1", "true", "True")
    ),
}


# --8<-- [end:env-vars-definition]


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


def mkdir_path(path: Path | str) -> Path:
    """
    Create a directory if it does not exist.

    Args:
        path (str | Path): The path to the directory.

    """
    if isinstance(path, str):
        path = Path(path)
    path.mkdir(exist_ok=True)
    return path
