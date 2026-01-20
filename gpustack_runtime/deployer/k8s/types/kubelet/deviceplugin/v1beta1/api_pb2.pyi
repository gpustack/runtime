from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DevicePluginOptions(_message.Message):
    __slots__ = ("pre_start_required", "get_preferred_allocation_available")
    PRE_START_REQUIRED_FIELD_NUMBER: _ClassVar[int]
    GET_PREFERRED_ALLOCATION_AVAILABLE_FIELD_NUMBER: _ClassVar[int]
    pre_start_required: bool
    get_preferred_allocation_available: bool
    def __init__(self, pre_start_required: bool = ..., get_preferred_allocation_available: bool = ...) -> None: ...

class RegisterRequest(_message.Message):
    __slots__ = ("version", "endpoint", "resource_name", "options")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_NAME_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    version: str
    endpoint: str
    resource_name: str
    options: DevicePluginOptions
    def __init__(self, version: _Optional[str] = ..., endpoint: _Optional[str] = ..., resource_name: _Optional[str] = ..., options: _Optional[_Union[DevicePluginOptions, _Mapping]] = ...) -> None: ...

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListAndWatchResponse(_message.Message):
    __slots__ = ("devices",)
    DEVICES_FIELD_NUMBER: _ClassVar[int]
    devices: _containers.RepeatedCompositeFieldContainer[Device]
    def __init__(self, devices: _Optional[_Iterable[_Union[Device, _Mapping]]] = ...) -> None: ...

class TopologyInfo(_message.Message):
    __slots__ = ("nodes",)
    NODES_FIELD_NUMBER: _ClassVar[int]
    nodes: _containers.RepeatedCompositeFieldContainer[NUMANode]
    def __init__(self, nodes: _Optional[_Iterable[_Union[NUMANode, _Mapping]]] = ...) -> None: ...

class NUMANode(_message.Message):
    __slots__ = ("ID",)
    ID_FIELD_NUMBER: _ClassVar[int]
    ID: int
    def __init__(self, ID: _Optional[int] = ...) -> None: ...

class Device(_message.Message):
    __slots__ = ("ID", "health", "topology")
    ID_FIELD_NUMBER: _ClassVar[int]
    HEALTH_FIELD_NUMBER: _ClassVar[int]
    TOPOLOGY_FIELD_NUMBER: _ClassVar[int]
    ID: str
    health: str
    topology: TopologyInfo
    def __init__(self, ID: _Optional[str] = ..., health: _Optional[str] = ..., topology: _Optional[_Union[TopologyInfo, _Mapping]] = ...) -> None: ...

class PreStartContainerRequest(_message.Message):
    __slots__ = ("devices_ids",)
    DEVICES_IDS_FIELD_NUMBER: _ClassVar[int]
    devices_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, devices_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class PreStartContainerResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PreferredAllocationRequest(_message.Message):
    __slots__ = ("container_requests",)
    CONTAINER_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    container_requests: _containers.RepeatedCompositeFieldContainer[ContainerPreferredAllocationRequest]
    def __init__(self, container_requests: _Optional[_Iterable[_Union[ContainerPreferredAllocationRequest, _Mapping]]] = ...) -> None: ...

class ContainerPreferredAllocationRequest(_message.Message):
    __slots__ = ("available_deviceIDs", "must_include_deviceIDs", "allocation_size")
    AVAILABLE_DEVICEIDS_FIELD_NUMBER: _ClassVar[int]
    MUST_INCLUDE_DEVICEIDS_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_SIZE_FIELD_NUMBER: _ClassVar[int]
    available_deviceIDs: _containers.RepeatedScalarFieldContainer[str]
    must_include_deviceIDs: _containers.RepeatedScalarFieldContainer[str]
    allocation_size: int
    def __init__(self, available_deviceIDs: _Optional[_Iterable[str]] = ..., must_include_deviceIDs: _Optional[_Iterable[str]] = ..., allocation_size: _Optional[int] = ...) -> None: ...

class PreferredAllocationResponse(_message.Message):
    __slots__ = ("container_responses",)
    CONTAINER_RESPONSES_FIELD_NUMBER: _ClassVar[int]
    container_responses: _containers.RepeatedCompositeFieldContainer[ContainerPreferredAllocationResponse]
    def __init__(self, container_responses: _Optional[_Iterable[_Union[ContainerPreferredAllocationResponse, _Mapping]]] = ...) -> None: ...

class ContainerPreferredAllocationResponse(_message.Message):
    __slots__ = ("deviceIDs",)
    DEVICEIDS_FIELD_NUMBER: _ClassVar[int]
    deviceIDs: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, deviceIDs: _Optional[_Iterable[str]] = ...) -> None: ...

class AllocateRequest(_message.Message):
    __slots__ = ("container_requests",)
    CONTAINER_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    container_requests: _containers.RepeatedCompositeFieldContainer[ContainerAllocateRequest]
    def __init__(self, container_requests: _Optional[_Iterable[_Union[ContainerAllocateRequest, _Mapping]]] = ...) -> None: ...

class ContainerAllocateRequest(_message.Message):
    __slots__ = ("devices_ids",)
    DEVICES_IDS_FIELD_NUMBER: _ClassVar[int]
    devices_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, devices_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class CDIDevice(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class AllocateResponse(_message.Message):
    __slots__ = ("container_responses",)
    CONTAINER_RESPONSES_FIELD_NUMBER: _ClassVar[int]
    container_responses: _containers.RepeatedCompositeFieldContainer[ContainerAllocateResponse]
    def __init__(self, container_responses: _Optional[_Iterable[_Union[ContainerAllocateResponse, _Mapping]]] = ...) -> None: ...

class ContainerAllocateResponse(_message.Message):
    __slots__ = ("envs", "mounts", "devices", "annotations", "cdi_devices")
    class EnvsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class AnnotationsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ENVS_FIELD_NUMBER: _ClassVar[int]
    MOUNTS_FIELD_NUMBER: _ClassVar[int]
    DEVICES_FIELD_NUMBER: _ClassVar[int]
    ANNOTATIONS_FIELD_NUMBER: _ClassVar[int]
    CDI_DEVICES_FIELD_NUMBER: _ClassVar[int]
    envs: _containers.ScalarMap[str, str]
    mounts: _containers.RepeatedCompositeFieldContainer[Mount]
    devices: _containers.RepeatedCompositeFieldContainer[DeviceSpec]
    annotations: _containers.ScalarMap[str, str]
    cdi_devices: _containers.RepeatedCompositeFieldContainer[CDIDevice]
    def __init__(self, envs: _Optional[_Mapping[str, str]] = ..., mounts: _Optional[_Iterable[_Union[Mount, _Mapping]]] = ..., devices: _Optional[_Iterable[_Union[DeviceSpec, _Mapping]]] = ..., annotations: _Optional[_Mapping[str, str]] = ..., cdi_devices: _Optional[_Iterable[_Union[CDIDevice, _Mapping]]] = ...) -> None: ...

class Mount(_message.Message):
    __slots__ = ("container_path", "host_path", "read_only")
    CONTAINER_PATH_FIELD_NUMBER: _ClassVar[int]
    HOST_PATH_FIELD_NUMBER: _ClassVar[int]
    READ_ONLY_FIELD_NUMBER: _ClassVar[int]
    container_path: str
    host_path: str
    read_only: bool
    def __init__(self, container_path: _Optional[str] = ..., host_path: _Optional[str] = ..., read_only: bool = ...) -> None: ...

class DeviceSpec(_message.Message):
    __slots__ = ("container_path", "host_path", "permissions")
    CONTAINER_PATH_FIELD_NUMBER: _ClassVar[int]
    HOST_PATH_FIELD_NUMBER: _ClassVar[int]
    PERMISSIONS_FIELD_NUMBER: _ClassVar[int]
    container_path: str
    host_path: str
    permissions: str
    def __init__(self, container_path: _Optional[str] = ..., host_path: _Optional[str] = ..., permissions: _Optional[str] = ...) -> None: ...
