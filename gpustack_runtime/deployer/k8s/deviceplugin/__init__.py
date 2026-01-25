from __future__ import annotations as __future_annotations__

import asyncio
import contextlib
import logging
import signal
import stat
import threading
from functools import lru_cache
from typing import TYPE_CHECKING, Literal

from .... import envs
from ....deployer.cdi import dump_config as cdi_dump_config
from ....detector import ManufacturerEnum, detect_devices, supported_manufacturers
from ...cdi import Config, manufacturer_to_cdi_kind
from ..types.kubelet.deviceplugin.v1beta1 import KubeletSocket
from .__types__ import GroupedError, PluginServer
from .plugin import SharableDevicePlugin, cdi_kind_to_kdp_resource

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__package__)


async def serve_async(
    stop_event: asyncio.Event,
    manufacturer: ManufacturerEnum | None = None,
    cdi_generation_output: Path | None = None,
    kubelet_endpoint: Path | None = None,
    start_timeout: int = 5,
    register_timeout: int = 5,
):
    """
    Serve device plugins for all detected devices asynchronously.

    Args:
        stop_event:
            An asyncio event to signal stopping the server.
        manufacturer:
            Manufacturer for serving.
            If None, detected from current environment.
        cdi_generation_output:
            A directory to store generated CDI configuration files.
            If None, CDI configuration is not stored.
        kubelet_endpoint:
            The path to the kubelet endpoint.
        start_timeout:
            The timeout in seconds for starting the device plugin.
        register_timeout:
            The timeout in seconds for registering the device plugin.

    Raises:
        If any device plugin fails during serving, the exception is raised.

    """
    manufacturers = [manufacturer] if manufacturer else supported_manufacturers()

    # Create servers for all detected devices.
    servers: list[PluginServer] = []
    for manu in manufacturers:
        devices = detect_devices(
            manufacturer=manu,
        )
        if not devices:
            continue

        allocation_policy = _get_device_allocation_policy(manu)
        logger.info(
            "Using device allocation policy '%s' for manufacturer '%s'",
            allocation_policy,
            manu,
        )

        # Also works if the manufacturer does not have a CDI generator,
        # which means we are relying on other tools to generate CDI specs.
        if (
            allocation_policy == "cdi"
            and envs.GPUSTACK_RUNTIME_KUBERNETES_KDP_CDI_SPECS_GENERATE
        ):
            generated_content, generated_path = cdi_dump_config(
                manufacturer=manu,
                output=cdi_generation_output,
            )
            if generated_content:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "Generated CDI configuration for '%s' at '%s':\n%s",
                        manu,
                        generated_path,
                        generated_content,
                    )
                else:
                    logger.info(
                        "Generated CDI configuration for '%s' at '%s'",
                        manu,
                        generated_path,
                    )
            else:
                logger.warning(
                    "Delegated CDI configuration by other tools for manufacturer '%s', "
                    "e.g. NVIDIA Container Toolkit Manual CDI Specification Generation, "
                    "see https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/cdi-support.html#manual-cdi-specification-generation",
                    manu,
                )

        for dev in devices:
            servers.append(
                SharableDevicePlugin(
                    device=dev,
                    id_by="index" if manu == ManufacturerEnum.ASCEND else "uuid",
                    allocation_policy=allocation_policy,
                ),
            )

    if not servers:
        logger.warning("No supported devices found. Waiting for stop event...")
        await stop_event.wait()
        logger.info("Stop event triggered, shutting down...")
        return

    if not kubelet_endpoint:
        kubelet_endpoint = KubeletSocket

    # Create tasks to start all servers.
    serve_tasks = [
        asyncio.create_task(
            server.serve(
                stop_event=stop_event,
                kubelet_endpoint=kubelet_endpoint,
                start_timeout=start_timeout,
                register_timeout=register_timeout,
            ),
            name=f"serve-{server.name}",
        )
        for server in servers
    ]

    # Create a task to wait for the stop event.
    stop_task = asyncio.create_task(
        stop_event.wait(),
        name="stop",
    )

    try:
        # Wait for stop event or any server to fail.
        done, pending = await asyncio.wait(
            [*serve_tasks, stop_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Check if stop event was triggered.
        if stop_task in done:
            logger.info("Stop event triggered, shutting down servers...")
            for task in serve_tasks:
                if not task.done():
                    with contextlib.suppress(asyncio.CancelledError):
                        task.cancel()
            await asyncio.gather(*serve_tasks, return_exceptions=True)
            logger.info("All servers shut down gracefully")
            return

        # Otherwise, one or more servers have failed.
        errs = []
        for task in pending:
            with contextlib.suppress(asyncio.CancelledError):
                task.cancel()
        for task in done:
            if err := task.exception():
                errs.append(err)
        if errs:
            logger.error(
                "One or more servers have failed, shutting down remaining servers...",
            )
            raise GroupedError(errs)
    finally:
        # Ensure all tasks are cleaned up.
        for task in serve_tasks:
            if not task.done():
                with contextlib.suppress(asyncio.CancelledError):
                    task.cancel()


def serve(
    manufacturer: ManufacturerEnum | None = None,
    cdi_generation_output: Path | None = None,
    kubelet_endpoint: Path | None = None,
    start_timeout: int = 5,
    register_timeout: int = 5,
):
    """
    Serve device plugins for all detected devices.

    Args:
        manufacturer:
            Manufacturer for serving.
            If None, detected from current environment.
        cdi_generation_output:
            A directory to store generated CDI configuration files.
            If None, CDI configuration is not stored.
        kubelet_endpoint:
            The path to the kubelet endpoint.
        start_timeout:
            The timeout in seconds for starting the device plugin.
        register_timeout:
            The timeout in seconds for registering the device plugin.

    Raises:
        If any device plugin fails during serving, the exception is raised.

    """
    # Ensure we're running in the main thread.
    if threading.current_thread() != threading.main_thread():
        logger.warning("Serve should be called from main thread")

    # Create a stop event and register signal handlers.
    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def handle_signal(_s, _f):
        logger.info("Received termination signal, stopping servers...")
        stop_event.set()

    # Register signal handlers for graceful shutdown.
    signals = [signal.SIGINT, signal.SIGTERM]
    try:
        for sig in signals:
            loop.add_signal_handler(sig, handle_signal, sig, None)
    except (NotImplementedError, RuntimeError):
        for sig in signals:
            signal.signal(sig, handle_signal)

    # Run the asynchronous server.
    try:
        loop.run_until_complete(
            serve_async(
                stop_event=stop_event,
                manufacturer=manufacturer,
                cdi_generation_output=cdi_generation_output,
                kubelet_endpoint=kubelet_endpoint,
                start_timeout=start_timeout,
                register_timeout=register_timeout,
            ),
        )
    finally:
        # Remove signal handlers to avoid side effects.
        try:
            for sig in signals:
                loop.remove_signal_handler(sig)
        except (NotImplementedError, RuntimeError):
            pass


def is_kubelet_socket_accessible(
    kubelet_endpoint: Path | None = None,
) -> bool:
    """
    Check if the kubelet socket is accessible.

    Args:
        kubelet_endpoint:
            The path to the kubelet endpoint.

    Returns:
        True if the socket is accessible, False otherwise.

    """
    if not kubelet_endpoint:
        kubelet_endpoint = KubeletSocket

    if kubelet_endpoint.exists():
        path_stat = kubelet_endpoint.lstat()
        if path_stat and stat.S_ISSOCK(path_stat.st_mode):
            return True
    return False


@lru_cache
def _get_device_allocation_policy(
    manufacturer: ManufacturerEnum,
) -> Literal["env", "cdi", "opaque"]:
    """
    Get the device allocation policy (in lowercase) for the device plugin.

    Args:
        manufacturer:
            The manufacturer of the device.

    Returns:
        The device allocation policy.

    """
    policy = envs.GPUSTACK_RUNTIME_KUBERNETES_KDP_DEVICE_ALLOCATION_POLICY.lower()
    if policy != "auto":
        return policy

    cdi_kind = manufacturer_to_cdi_kind(manufacturer)

    cdi_dir = envs.GPUSTACK_RUNTIME_DEPLOY_CDI_SPECS_DIRECTORY
    for suffix in ["*.yaml", "*.yml", "*.json"]:
        for file in cdi_dir.glob(suffix):
            with contextlib.suppress(Exception):
                config = Config.from_file(file)
                if config and config.kind == cdi_kind:
                    return "cdi"

    if manufacturer in [
        ManufacturerEnum.AMD,
        # ManufacturerEnum.ASCEND, # Prioritize using Env policy for Ascend.
        ManufacturerEnum.HYGON,
        ManufacturerEnum.ILUVATAR,
        ManufacturerEnum.METAX,
        ManufacturerEnum.THEAD,
    ]:
        return "opaque"

    return "env"


__all__ = [
    "cdi_kind_to_kdp_resource",
    "is_kubelet_socket_accessible",
    "serve",
    "serve_async",
]
