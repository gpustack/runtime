from __future__ import annotations as __future_annotations__

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncio
    from pathlib import Path


class GroupedError(Exception):
    """
    Exception to encapsulate multiple errors.

    Args:
        errors:
            A list of exceptions that occurred.

    """

    _errors: list[BaseException | Exception]
    """
    Errors that occurred.
    """

    def __init__(self, errors: list[BaseException | Exception] | None = None):
        """
        Initialize the GroupedError exception.

        Args:
            errors:
                A list of exceptions that occurred.

        """
        self._errors = errors or []

    def append(self, error: BaseException | Exception):
        """
        Append an error to the GroupedError.

        Args:
            error:
                The exception to append.

        """
        self._errors.append(error)

    def extend(self, errors: list[BaseException | Exception]):
        """
        Extend the GroupedError with multiple errors.

        Args:
            errors:
                A list of exceptions to extend with.

        """
        self._errors.extend(errors)

    def __str__(self) -> str:
        """
        Get the string representation of the GroupedError exception.

        Returns:
            A string representation of the GroupedError exception.

        """
        error_messages = "\n".join(
            f"{i + 1}. {error!s}" for i, error in enumerate(self._errors)
        )
        return f"{len(self._errors)} errors occurred:\n{error_messages}"


class PluginServer(ABC):
    """
    Base class for Kubernetes device plugins.

    """

    _name: str
    """
    Name of the device plugin.
    """

    def __init__(self, name: str):
        """
        Initialize the device plugin.

        Args:
            name:
                The name of the device plugin.

        """
        self._name = name

    @property
    def name(self) -> str:
        """
        Get the name of the device plugin.

        Returns:
            The name of the device plugin.

        """
        return self._name

    @abstractmethod
    async def serve(
        self,
        stop_event: asyncio.Event,
        kubelet_endpoint: Path | None = None,
        start_timeout: int = 5,
        register_timeout: int = 5,
    ):
        """
        Serve the device plugin asynchronously.

        Args:
            stop_event:
                An asyncio event to signal stopping the server.
            kubelet_endpoint:
                The path to the kubelet endpoint.
                Default is None, which uses the default kubelet socket path.
            start_timeout:
                The timeout in seconds for starting the device plugin server.
                Default is 5 seconds.
            register_timeout:
                The timeout in seconds for registering the device plugin.
                Default is 5 seconds.

        """
        raise NotImplementedError
