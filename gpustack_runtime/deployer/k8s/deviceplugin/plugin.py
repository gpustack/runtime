from __future__ import annotations as __future_annotations__

import asyncio
import contextlib
import logging
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import grpc
from grpc_interceptor import AsyncServerInterceptor
from grpc_interceptor.exceptions import GrpcException

from ....detector import Device, DeviceMemoryStatusEnum, str_range_to_list
from ...cdi import (
    generate_config,
    manufacturer_to_cdi_kind,
    manufacturer_to_runtime_env,
)
from ..types.kubelet.deviceplugin.v1beta1 import (
    AllocateRequest,
    AllocateResponse,
    CDIDevice,
    ContainerAllocateRequest,
    ContainerAllocateResponse,
    ContainerPreferredAllocationRequest,
    ContainerPreferredAllocationResponse,
    DevicePluginOptions,
    DevicePluginServicer,
    DeviceSpec,
    Empty,
    Healthy,
    ListAndWatchResponse,
    Mount,
    NUMANode,
    PreferredAllocationRequest,
    PreferredAllocationResponse,
    PreStartContainerRequest,
    PreStartContainerResponse,
    RegisterRequest,
    RegistrationStub,
    TopologyInfo,
    Unhealthy,
    Version,
    add_DevicePluginServicer_to_server,
)
from ..types.kubelet.deviceplugin.v1beta1 import (
    Device as DevicePluginDevice,
)
from .__types__ import PluginServer

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

logger = logging.getLogger(__name__)

_ID_SPLIT = "::"


def _to_device_plugin_device_id(device_id: str, shard: int) -> str:
    """
    Converts a device ID and shard number to a Kubernetes Device Plugin device ID.

    Args:
        device_id:
            The base device ID.
        shard:
            The shard number.

    Returns:
        The combined device plugin device ID.

    """
    return f"{device_id}{_ID_SPLIT}{shard}"


def _from_device_plugin_device_id(device_plugin_device_id: str) -> tuple[str, int]:
    """
    Converts a Kubernetes Device Plugin device ID to its base device ID and shard number.

    Args:
        device_plugin_device_id:
            The combined device plugin device ID.

    Returns:
        A tuple containing the base device ID and shard number.

    """
    parts = device_plugin_device_id.split(_ID_SPLIT)
    device_id = parts[0]
    shard = int(parts[1])
    return device_id, shard


class SharableDevicePlugin(PluginServer, DevicePluginServicer):
    """
    SharableDevicePlugin is a Kubernetes Device Plugin that supports device sharing.

    It allows multiple containers to share the same underlying device by
    creating multiple device IDs (shards) for each physical device.

    """

    _device: Device
    """
    The underlying device to be managed by this device plugin.
    """
    _id_by: Literal["uuid", "index"]
    """
    Controls how the device IDs of the Kubernetes Device Plugin are generated.
    """
    _allocation_policy: Literal["env", "cdi", "opaque"]
    """
    Controls the device allocation policy.
    """
    _max_allocations: int
    """
    Controls the maximum shards per underlying device.
    """
    _cdi_kind: str
    """
    The CDI kind associated with the `_device`.
    """
    _runtime_env: str
    """
    The runtime environment associated with the `_device`.
    """
    _kdp_resource: str
    """
    The device plugin resource name associated with the `_device`.
    """

    def __init__(
        self,
        device: Device,
        id_by: Literal["uuid", "index"] = "uuid",
        allocation_policy: Literal["env", "cdi", "opaque"] = "cdi",
        max_allocations: int | None = 10,
    ):
        """
        Initializes the SharableDevicePlugin.

        Args:
            device:
                The underlying device to be managed by this device plugin.
            id_by:
                Controls how the device IDs of the Kubernetes Device Plugin are generated.
                Either "uuid" or "index". Default is "uuid".
            allocation_policy:
                Controls the device allocation policy.
            max_allocations:
                Controls the maximum allocations per underlying device.

        """
        self._device = device
        self._id_by = id_by
        self._allocation_policy = allocation_policy
        self._max_allocations = max(max_allocations, 1)
        self._cdi_kind = manufacturer_to_cdi_kind(device.manufacturer)
        self._runtime_env = manufacturer_to_runtime_env(device.manufacturer)
        self._kdp_resource = cdi_kind_to_kdp_resource(
            cdi_kind=self._cdi_kind,
            device_index=str(device.index),
        )

        super().__init__(self._kdp_resource)

    @contextlib.asynccontextmanager
    async def _serve_device_plugin(
        self,
        endpoint: Path,
        timeout: int = 5,
    ) -> AsyncIterator[None]:
        """
        Serve the device plugin asynchronously.

        Args:
            endpoint:
                The path to the device plugin server endpoint.
            timeout:
                The timeout in seconds for starting the device plugin.

        """
        endpoint.unlink(missing_ok=True)

        # Create the device plugin server.
        server = grpc.aio.server(
            interceptors=[
                _LoggingInterceptor(
                    name=self._kdp_resource,
                ),
            ],
        )
        server.add_insecure_port(
            address=f"unix://{endpoint}",
        )

        # Start the device plugin server.
        add_DevicePluginServicer_to_server(self, server)
        server_task = asyncio.create_task(server.start())

        # Wait for the server to be ready,
        # then yield for use within the context,
        # and ensure proper shutdown afterward.
        try:
            loop = asyncio.get_event_loop()
            start_time = loop.time()
            while (loop.time() - start_time) < timeout:
                if endpoint.exists():
                    with contextlib.suppress(ConnectionError, asyncio.TimeoutError):
                        _, writer = await asyncio.wait_for(
                            asyncio.open_unix_connection(endpoint),
                            timeout=0.1,
                        )
                        writer.close()
                        await writer.wait_closed()
                        break

                await asyncio.sleep(0.05)
            else:
                msg = f"Failed to start device plugin server within {timeout} seconds"
                raise TimeoutError(msg)

            yield
        finally:
            await server.stop(grace=3.0)
            await server_task
            endpoint.unlink(missing_ok=True)

    async def serve(
        self,
        stop_event: asyncio.Event,
        kubelet_endpoint: Path,
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
            start_timeout:
                The timeout in seconds for starting the device plugin.
            register_timeout:
                The timeout in seconds for registering the device plugin.

        """
        resource_name = self._kdp_resource
        endpoint = kubelet_endpoint.parent / f"{resource_name.replace('/', '.')}.sock"

        async with self._serve_device_plugin(
            endpoint=endpoint,
            timeout=start_timeout,
        ):
            request = RegisterRequest(
                version=Version,
                endpoint=str(endpoint.name),
                resource_name=resource_name,
                options=self._get_device_plugin_options(),
            )
            async with grpc.aio.insecure_channel(
                target=f"unix://{kubelet_endpoint}",
            ) as channel:
                stub = RegistrationStub(channel)
                try:
                    await stub.Register(
                        request=request,
                        timeout=register_timeout,
                    )
                except Exception:
                    logger.exception(
                        f"Failed to register device plugin for resource '{resource_name}' "
                        f"at endpoint '{endpoint}'",
                    )
                    raise

                logger.info(
                    f"Serving device plugin for resource '{resource_name}' "
                    f"at endpoint '{endpoint}'",
                )
                await stop_event.wait()

    @staticmethod
    def _get_device_plugin_options() -> DevicePluginOptions:
        """
        Returns the device plugin options.

        Returns:
            The device plugin options.

        """
        return DevicePluginOptions(
            pre_start_required=False,
            get_preferred_allocation_available=True,
        )

    async def GetDevicePluginOptions(  # noqa: N802
        self,
        req: Empty,
        ctx: grpc.aio.ServicerContext,
    ) -> DevicePluginOptions:
        """
        Returns the device plugin options.

        Args:
            req:
                An empty request message.
            ctx:
                The request context.

        Returns:
            The device plugin options.

        """
        return self._get_device_plugin_options()

    async def ListAndWatch(  # noqa: N802
        self,
        req: Empty,
        ctx: grpc.aio.ServicerContext,
    ) -> AsyncIterator[ListAndWatchResponse]:
        """
        List and watch for device changes.

        Args:
            req:
                An empty request message.
            ctx:
                The request context.

        Yields:
            The response containing the list of devices.

        """
        dp_devices: list[DevicePluginDevice] = []
        dp_device_health = (
            Healthy
            if self._device.memory_status == DeviceMemoryStatusEnum.HEALTHY
            else Unhealthy
        )
        dp_device_topo = TopologyInfo(
            nodes=[
                NUMANode(
                    ID=node_id,
                )
                for node_id in str_range_to_list(
                    self._device.appendix.get("numa", "0"),
                )
            ],
        )

        for device_replica in range(1, self._max_allocations + 1):
            dp_device_id = _to_device_plugin_device_id(
                str(self._device.index),
                device_replica,
            )
            dp_devices.append(
                DevicePluginDevice(
                    ID=dp_device_id,
                    health=dp_device_health,
                    topology=dp_device_topo,
                ),
            )

        yield ListAndWatchResponse(
            devices=dp_devices,
        )

        # TODO(thxCode): implement health check

        while not ctx.done():  # noqa: ASYNC110
            await asyncio.sleep(15)

    @staticmethod
    def _get_preferred_allocation(
        req: ContainerPreferredAllocationRequest,
    ) -> ContainerPreferredAllocationResponse:
        available_dp_device_ids = req.available_deviceIDs
        required_dp_device_ids = req.must_include_deviceIDs
        allocation_size = req.allocation_size

        if len(available_dp_device_ids) == allocation_size:
            return ContainerPreferredAllocationResponse(
                deviceIDs=available_dp_device_ids,
            )

        if len(required_dp_device_ids) == allocation_size:
            return ContainerPreferredAllocationResponse(
                deviceIDs=required_dp_device_ids,
            )

        selected_dp_device_ids = list(required_dp_device_ids)
        for dp_device_id in available_dp_device_ids:
            if dp_device_id not in selected_dp_device_ids:
                selected_dp_device_ids.append(dp_device_id)
            if len(selected_dp_device_ids) == allocation_size:
                break

        return ContainerPreferredAllocationResponse(
            deviceIDs=selected_dp_device_ids,
        )

    async def GetPreferredAllocation(  # noqa: N802
        self,
        req: PreferredAllocationRequest,
        ctx: grpc.aio.ServicerContext,
    ) -> PreferredAllocationResponse:
        allocation_responses = []
        for request in req.container_requests:
            allocation_responses.append(
                self._get_preferred_allocation(request),
            )

        return PreferredAllocationResponse(
            container_responses=allocation_responses,
        )

    def _allocate(
        self,
        req: ContainerAllocateRequest,
    ) -> ContainerAllocateResponse:
        policy = self._allocation_policy
        device_id = self._device.uuid
        if self._id_by == "index":
            device_id = str(self._device.index)

        # CDI device allocation.
        if policy == "cdi":
            return ContainerAllocateResponse(
                cdi_devices=[
                    CDIDevice(
                        name=f"{self._cdi_kind}={device_id}",
                    ),
                ],
            )

        # Environment variable device allocation.
        if policy == "env":
            return ContainerAllocateResponse(
                envs={
                    self._runtime_env: device_id,
                },
            )

        # Opaque device allocation.
        if cdi_cfg := generate_config(self._device):
            dev_envs: dict[str, str] = {}
            dev_mounts: list[Mount] = []
            dev_devices: list[DeviceSpec] = []

            container_edits = cdi_cfg.container_edits or []
            for cdi_dev in cdi_cfg.devices:
                container_edits.append(cdi_dev.container_edits)

            for edit in container_edits:
                for e in edit.env or []:
                    k, v = e.split("=", 1)
                    dev_envs[k] = v
                for m in edit.mounts or []:
                    dev_mounts.append(
                        Mount(
                            container_path=m.container_path,
                            host_path=m.host_path,
                            read_only="ro" in (m.options or []),
                        ),
                    )
                for d in edit.device_nodes or []:
                    dev_devices.append(
                        DeviceSpec(
                            container_path=d.path,
                            host_path=d.host_path or d.path,
                            permissions=d.permissions,
                        ),
                    )

            return ContainerAllocateResponse(
                envs=dev_envs,
                mounts=dev_mounts,
                devices=dev_devices,
            )

        return ContainerAllocateResponse()

    async def Allocate(  # noqa: N802
        self,
        req: AllocateRequest,
        ctx: grpc.aio.ServicerContext,
    ) -> AllocateResponse:
        allocation_response = []
        for request in req.container_requests:
            allocation_response.append(
                self._allocate(request),
            )

        return AllocateResponse(
            container_responses=allocation_response,
        )

    async def PreStartContainer(  # noqa: N802
        self,
        req: PreStartContainerRequest,
        ctx: grpc.aio.ServicerContext,
    ) -> PreStartContainerResponse:
        return PreStartContainerResponse()


@lru_cache
def cdi_kind_to_kdp_resource(
    cdi_kind: str,
    device_index: str,
) -> str:
    """
    Map CDI kind and device index to a Kubernetes Device Plugin resource name.

    Args:
        cdi_kind:
            The CDI kind.
        device_index:
            The index of the device.

    Returns:
        The resource name for the device plugin.

    """
    return f"{cdi_kind}{device_index}"


class _LoggingInterceptor(AsyncServerInterceptor):
    _name: str
    """
    Name of the server.
    """

    def __init__(self, name: str):
        self._name = name

    def log_debug(self, msg, *args, **kwargs):
        logger.debug(f"[{self._name}] {msg}", *args, **kwargs)

    def log_exception(self, msg, *args, **kwargs):
        logger.exception(f"[{self._name}] {msg}", *args, **kwargs)

    @staticmethod
    @lru_cache
    def simplify_method_name(method_name: str) -> str:
        return Path(method_name).name

    async def intercept(
        self,
        method: Callable,
        request: Any,
        context: grpc.aio.ServicerContext,
        method_name: str,
    ) -> object:
        method_name = self.simplify_method_name(method_name)

        if hasattr(request, "__aiter__"):
            self.log_debug(f"{method_name} received streaming request")
        elif isinstance(request, Empty):
            self.log_debug(f"{method_name} received empty request")
        else:
            self.log_debug(f"{method_name} received request:\n{request}")
        try:
            response = method(request, context)
            if hasattr(response, "__aiter__"):
                self.log_debug(f"{method_name} returning streaming response")
            else:
                response = await response
                if isinstance(response, Empty):
                    self.log_debug(f"{method_name} returned empty response")
                else:
                    self.log_debug(f"{method_name} returned response:\n{response}")
        except GrpcException:
            self.log_exception(f"{method_name} raised grpc exception")
            raise
        except Exception:
            self.log_exception(f"{method_name} raised unexpected exception")
            raise
        else:
            return response
