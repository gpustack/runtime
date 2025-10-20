##
# Python bindings for the HSA library
##
from __future__ import annotations

import os
import string
import sys
import threading
from ctypes import *
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import ClassVar

## C Type mappings ##

## Enums ##
HSA_AGENT_INFO_NAME = 0
HSA_AGENT_INFO_VENDOR_NAME = 1
HSA_AGENT_INFO_FEATURE = 2
HSA_AGENT_INFO_MACHINE_MODEL = 3
HSA_AGENT_INFO_PROFILE = 4
HSA_AGENT_INFO_DEFAULT_FLOAT_ROUNDING_MODE = 5
HSA_AGENT_INFO_BASE_PROFILE_DEFAULT_FLOAT_ROUNDING_MODES = 23
HSA_AGENT_INFO_FAST_F16_OPERATION = 24
HSA_AGENT_INFO_WAVEFRONT_SIZE = 6
HSA_AGENT_INFO_WORKGROUP_MAX_DIM = 7
HSA_AGENT_INFO_WORKGROUP_MAX_SIZE = 8
HSA_AGENT_INFO_GRID_MAX_DIM = 9
HSA_AGENT_INFO_GRID_MAX_SIZE = 10
HSA_AGENT_INFO_FBARRIER_MAX_SIZE = 11
HSA_AGENT_INFO_QUEUES_MAX = 12
HSA_AGENT_INFO_QUEUE_MIN_SIZE = 13
HSA_AGENT_INFO_QUEUE_MAX_SIZE = 14
HSA_AGENT_INFO_QUEUE_TYPE = 15
HSA_AGENT_INFO_NODE = 16
HSA_AGENT_INFO_DEVICE = 17
HSA_AGENT_INFO_CACHE_SIZE = 18
HSA_AGENT_INFO_ISA = 19
HSA_AGENT_INFO_EXTENSIONS = 20
HSA_AGENT_INFO_VERSION_MAJOR = 21
HSA_AGENT_INFO_VERSION_MINOR = 22
HSA_AGENT_INFO_IS_DP4X_ENABLE = 101

## Enums ##
HSA_AMD_AGENT_INFO_CHIP_ID = 0xA000
HSA_AMD_AGENT_INFO_CACHELINE_SIZE = 0xA001
HSA_AMD_AGENT_INFO_COMPUTE_UNIT_COUNT = 0xA002
HSA_AMD_AGENT_INFO_MAX_CLOCK_FREQUENCY = 0xA003
HSA_AMD_AGENT_INFO_DRIVER_NODE_ID = 0xA004
HSA_AMD_AGENT_INFO_MAX_ADDRESS_WATCH_POINTS = 0xA005
HSA_AMD_AGENT_INFO_BDFID = 0xA006
HSA_AMD_AGENT_INFO_MEMORY_WIDTH = 0xA007
HSA_AMD_AGENT_INFO_MEMORY_MAX_FREQUENCY = 0xA008
HSA_AMD_AGENT_INFO_PRODUCT_NAME = 0xA009
HSA_AMD_AGENT_INFO_MAX_WAVES_PER_CU = 0xA00A
HSA_AMD_AGENT_INFO_NUM_SIMDS_PER_CU = 0xA00B
HSA_AMD_AGENT_INFO_NUM_SHADER_ENGINES = 0xA00C
HSA_AMD_AGENT_INFO_NUM_SHADER_ARRAYS_PER_SE = 0xA00D
HSA_AMD_AGENT_INFO_HDP_FLUSH = 0xA00E
HSA_AMD_AGENT_INFO_DOMAIN = 0xA00F
HSA_AMD_AGENT_INFO_COOPERATIVE_QUEUES = 0xA010
HSA_AMD_AGENT_INFO_UUID = 0xA011
HSA_AMD_AGENT_INFO_ASIC_REVISION = 0xA012
HSA_AMD_AGENT_INFO_SVM_DIRECT_HOST_ACCESS = 0xA013
HSA_AMD_AGENT_INFO_COOPERATIVE_COMPUTE_UNIT_COUNT = 0xA014
HSA_AMD_AGENT_INFO_MEMORY_AVAIL = 0xA015
HSA_AMD_AGENT_INFO_TIMESTAMP_FREQUENCY = 0xA016
HSA_AMD_AGENT_INFO_ASIC_FAMILY_ID = 0xA107
HSA_AMD_AGENT_INFO_UCODE_VERSION = 0xA108
HSA_AMD_AGENT_INFO_SDMA_UCODE_VERSION = 0xA109
HSA_AMD_AGENT_INFO_NUM_SDMA_ENG = 0xA10A
HSA_AMD_AGENT_INFO_NUM_SDMA_XGMI_ENG = 0xA10B
HSA_AMD_AGENT_INFO_IOMMU_SUPPORT = 0xA110
HSA_AMD_AGENT_INFO_NUM_XCC = 0xA111
HSA_AMD_AGENT_INFO_DRIVER_UID = 0xA112
HSA_AMD_AGENT_INFO_NEAREST_CPU = 0xA113
HSA_AMD_AGENT_INFO_MEMORY_PROPERTIES = 0xA114
HSA_AMD_AGENT_INFO_AQL_EXTENSIONS = 0xA115

## Enums ##
HSA_DEVICE_TYPE_CPU = 0
HSA_DEVICE_TYPE_GPU = 1
HSA_DEVICE_TYPE_DSP = 2
HSA_DEVICE_TYPE_AIE = 3

## Error Codes ##
HSA_STATUS_SUCCESS = 0x0
HSA_STATUS_INFO_BREAK = 0x1
HSA_STATUS_ERROR = 0x1000
HSA_STATUS_ERROR_INVALID_ARGUMENT = 0x1001
HSA_STATUS_ERROR_INVALID_QUEUE_CREATION = 0x1002
HSA_STATUS_ERROR_INVALID_ALLOCATION = 0x1003
HSA_STATUS_ERROR_INVALID_AGENT = 0x1004
HSA_STATUS_ERROR_INVALID_REGION = 0x1005
HSA_STATUS_ERROR_INVALID_SIGNAL = 0x1006
HSA_STATUS_ERROR_INVALID_QUEUE = 0x1007
HSA_STATUS_ERROR_OUT_OF_RESOURCES = 0x1008
HSA_STATUS_ERROR_INVALID_PACKET_FORMAT = 0x1009
HSA_STATUS_ERROR_RESOURCE_FREE = 0x100A
HSA_STATUS_ERROR_NOT_INITIALIZED = 0x100B
HSA_STATUS_ERROR_REFCOUNT_OVERFLOW = 0x100C
HSA_STATUS_ERROR_INCOMPATIBLE_ARGUMENTS = 0x100D
HSA_STATUS_ERROR_INVALID_INDEX = 0x100E
HSA_STATUS_ERROR_INVALID_ISA = 0x100F
HSA_STATUS_ERROR_INVALID_ISA_NAME = 0x1017
HSA_STATUS_ERROR_INVALID_CODE_OBJECT = 0x1010
HSA_STATUS_ERROR_INVALID_EXECUTABLE = 0x1011
HSA_STATUS_ERROR_FROZEN_EXECUTABLE = 0x1012
HSA_STATUS_ERROR_INVALID_SYMBOL_NAME = 0x1013
HSA_STATUS_ERROR_VARIABLE_ALREADY_DEFINED = 0x1014
HSA_STATUS_ERROR_VARIABLE_UNDEFINED = 0x1015
HSA_STATUS_ERROR_EXCEPTION = 0x1016
HSA_STATUS_ERROR_INVALID_CODE_SYMBOL = 0x1018
HSA_STATUS_ERROR_INVALID_EXECUTABLE_SYMBOL = 0x1019
HSA_STATUS_ERROR_INVALID_FILE = 0x1020
HSA_STATUS_ERROR_INVALID_CODE_OBJECT_READER = 0x1021
HSA_STATUS_ERROR_INVALID_CACHE = 0x1022
HSA_STATUS_ERROR_INVALID_WAVEFRONT = 0x1023
HSA_STATUS_ERROR_INVALID_SIGNAL_GROUP = 0x1024
HSA_STATUS_ERROR_INVALID_RUNTIME_STATE = 0x1025
HSA_STATUS_ERROR_FATAL = 0x1026
HSA_STATUS_ERROR_UNINITIALIZED = -99997
HSA_STATUS_ERROR_FUNCTION_NOT_FOUND = -99998
HSA_STATUS_ERROR_LIBRARY_NOT_FOUND = -99999

## Lib loading ##
hsaLib = None
libLoadLock = threading.Lock()


## Error Checking ##
class HSAError(Exception):
    _valClassMapping: ClassVar[dict] = {}

    _errcode_to_string: ClassVar[dict] = {}

    def __new__(cls, value):
        """
        Maps value to a proper subclass of HSAError.
        See _extractHSAErrorsAsClasses function for more details.
        """
        if cls == HSAError:
            cls = HSAError._valClassMapping.get(value, cls)
        obj = Exception.__new__(cls)
        obj.value = value
        return obj

    def __str__(self):
        try:
            if self.value not in HSAError._errcode_to_string:
                HSAError._errcode_to_string[self.value] = (
                    f"Unknown HSA Error {self.value}"
                )
            return HSAError._errcode_to_string[self.value]
        except HSAError:
            return f"HSA Error with code {self.value}"

    def __eq__(self, other):
        if isinstance(other, HSAError):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return False


def hsaExceptionClass(hsaErrorCode):
    if hsaErrorCode not in HSAError._valClassMapping:
        msg = f"HSA error code {hsaErrorCode} is not valid"
        raise ValueError(msg)
    return HSAError._valClassMapping[hsaErrorCode]


def _extractHSAErrorsAsClasses():
    """
    Generates a hierarchy of classes on top of HSAError class.

    Each HSA Error gets a new HSAError subclass. This way try,except blocks can filter appropriate
    exceptions more easily.

    HSAError is a parent class. Each HSA_STATUS_ERROR_* gets it's own subclass.
    e.g. HSA_STATUS_ERROR_INVALID_ARGUMENT will be turned into HSAERROR_InvalidArgument.
    """
    this_module = sys.modules[__name__]
    hsaErrorsNames = [x for x in dir(this_module) if x.startswith("HSA_STATUS_ERROR_")]
    for err_name in hsaErrorsNames:
        # e.g. Turn HSA_STATUS_ERROR_INVALID_ARGUMENT into HSAERROR_InvalidArgument
        class_name = "HSAERROR_" + string.capwords(
            err_name.replace("HSA_STATUS_ERROR_", ""),
            "_",
        ).replace("_", "")
        err_val = getattr(this_module, err_name)

        def gen_new(val):
            def new(typ, *args):
                obj = HSAError.__new__(typ, val)
                return obj

            return new

        new_error_class = type(class_name, (HSAError,), {"__new__": gen_new(err_val)})
        new_error_class.__module__ = __name__
        setattr(this_module, class_name, new_error_class)
        HSAError._valClassMapping[err_val] = new_error_class


_extractHSAErrorsAsClasses()


def _hsaCheckReturn(ret):
    if ret != HSA_STATUS_SUCCESS:
        raise HSAError(ret)
    return ret


## Function access ##
_hsaGetFunctionPointer_cache = {}


def _hsaGetFunctionPointer(name):
    global hsaLib

    if name in _hsaGetFunctionPointer_cache:
        return _hsaGetFunctionPointer_cache[name]

    libLoadLock.acquire()
    try:
        if hsaLib is None:
            raise HSAError(HSA_STATUS_ERROR_UNINITIALIZED)
        try:
            _hsaGetFunctionPointer_cache[name] = getattr(hsaLib, name)
            return _hsaGetFunctionPointer_cache[name]
        except AttributeError:
            raise HSAError(HSA_STATUS_ERROR_FUNCTION_NOT_FOUND)
    finally:
        libLoadLock.release()


## Alternative object
# Allows the object to be printed
# Allows mismatched types to be assigned
#  - like None when the Structure variant requires c_uint
class hsaFriendlyObject:
    def __init__(self, dictionary):
        for x in dictionary:
            setattr(self, x, dictionary[x])

    def __str__(self):
        return self.__dict__.__str__()


def hsaStructToFriendlyObject(struct):
    d = {}
    for x in struct._fields_:
        key = x[0]
        value = getattr(struct, key)
        # only need to convert from bytes if bytes, no need to check python version.
        d[key] = value.decode() if isinstance(value, bytes) else value
    obj = hsaFriendlyObject(d)
    return obj


# pack the object so it can be passed to the HSA library
def hsaFriendlyObjectToStruct(obj, model):
    for x in model._fields_:
        key = x[0]
        value = obj.__dict__[key]
        # any c_char_p in python3 needs to be bytes, default encoding works fine.
        setattr(model, key, value.encode())
    return model


## Structure definitions ##
class _PrintableStructure(Structure):
    """
    Abstract class that produces nicer __str__ output than ctypes.Structure.
    """

    _fmt_ = {}

    def __str__(self):
        result = []
        for x in self._fields_:
            key = x[0]
            value = getattr(self, key)
            fmt = "%s"
            if key in self._fmt_:
                fmt = self._fmt_[key]
            elif "<default>" in self._fmt_:
                fmt = self._fmt_["<default>"]
            result.append(("%s: " + fmt) % (key, value))
        return self.__class__.__name__ + "(" + ", ".join(result) + ")"

    def __getattribute__(self, name):
        res = super().__getattribute__(name)
        if isinstance(res, bytes):
            return res.decode()
        return res

    def __setattr__(self, name, value):
        if isinstance(value, str):
            value = value.encode()
        super().__setattr__(name, value)


class c_hsa_agent_t(Structure):
    _fields_: ClassVar = [
        ("handle", c_uint64),
    ]


## string/bytes conversion for ease of use
def convertStrBytes(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # encoding a str returns bytes in python 2 and 3
        args = [arg.encode() if isinstance(arg, str) else arg for arg in args]
        res = func(*args, **kwargs)
        # In python 2, str and bytes are the same
        # In python 3, str is unicode and should be decoded.
        # Ctypes handles most conversions, this only effects c_char and char arrays.
        if isinstance(res, bytes):
            if isinstance(res, str):
                return res
            return res.decode()
        return res

    return wrapper


def _LoadHsaLibrary():
    global hsaLib
    if hsaLib is None:
        libLoadLock.acquire()
        try:
            if hsaLib is None:
                if sys.platform.startswith("win"):
                    # Do not support Windows yet.
                    raise HSAError(HSA_STATUS_ERROR_LIBRARY_NOT_FOUND)
                # Linux path
                locs = [
                    "libhsa-runtime64.so.1",
                    "libhsa-runtime64.so",
                ]
                rocm_path = Path(
                    os.getenv("ROCM_HOME", os.getenv("ROCM_PATH") or "/opt/rocm"),
                )
                if rocm_path.exists():
                    locs.extend(
                        [
                            str(rocm_path / "lib" / "libhsa-runtime64.so.1"),
                            str(rocm_path / "lib" / "libhsa-runtime64.so"),
                        ]
                    )
                for loc in locs:
                    try:
                        hsaLib = CDLL(loc)
                        break
                    except OSError:
                        pass
        finally:
            libLoadLock.release()


## C function wrappers ##
def hsa_init():
    _LoadHsaLibrary()

    # Initialize the library
    fn = _hsaGetFunctionPointer("hsa_init")
    ret = fn()
    _hsaCheckReturn(ret)


def hsa_iterate_agents(callback):
    def w_callback(agent, _):
        return callback(agent)

    c_callback = CFUNCTYPE(c_int, c_hsa_agent_t, c_void_p)(w_callback)

    func = _hsaGetFunctionPointer("hsa_iterate_agents")
    ret = func(c_callback, None)
    _hsaCheckReturn(ret)


def hsa_agent_get_info(agent, attribute, value):
    func = _hsaGetFunctionPointer("hsa_agent_get_info")
    ret = func(agent, attribute, value)
    _hsaCheckReturn(ret)


def hsa_agent_get_info_device(agent):
    c_device = c_uint32()
    hsa_agent_get_info(agent, HSA_AGENT_INFO_DEVICE, byref(c_device))
    return c_device.value


@convertStrBytes
def hsa_agent_get_info_name(agent):
    c_name = create_string_buffer(64)
    hsa_agent_get_info(agent, HSA_AGENT_INFO_NAME, byref(c_name))
    return c_name.value


@convertStrBytes
def hsa_agent_get_info_product_name(agent):
    c_name = create_string_buffer(64)
    hsa_agent_get_info(agent, HSA_AMD_AGENT_INFO_PRODUCT_NAME, byref(c_name))
    return c_name.value


def hsa_agent_get_info_chip_id(agent):
    c_chip_id = c_uint32()
    hsa_agent_get_info(agent, HSA_AMD_AGENT_INFO_CHIP_ID, byref(c_chip_id))
    return c_chip_id.value


def hsa_agent_get_info_compute_unit_count(agent):
    c_cu_count = c_uint32()
    hsa_agent_get_info(agent, HSA_AMD_AGENT_INFO_COMPUTE_UNIT_COUNT, byref(c_cu_count))
    return c_cu_count.value


@convertStrBytes
def hsa_agent_get_info_uuid(agent):
    c_uuid = create_string_buffer(21)
    hsa_agent_get_info(agent, HSA_AMD_AGENT_INFO_UUID, byref(c_uuid))
    return c_uuid.value


def hsa_agent_get_info_driver_node_id(agent):
    c_driver_node_id = c_uint32()
    hsa_agent_get_info(
        agent, HSA_AMD_AGENT_INFO_DRIVER_NODE_ID, byref(c_driver_node_id)
    )
    return c_driver_node_id.value


@dataclass
class Agent:
    device_type: int
    device_id: str
    uuid: str
    name: str
    compute_capability: str
    compute_units: int
    driver_node_id: int


def get_agents() -> list[Agent]:
    agents: list[Agent] = []

    try:
        hsa_init()

        def has_agent_callback(agent) -> int:
            agent_device_type = hsa_agent_get_info_device(agent)
            if agent_device_type != HSA_DEVICE_TYPE_GPU:
                return 0

            agent_device_id = hex(hsa_agent_get_info_chip_id(agent))
            agent_uuid = hsa_agent_get_info_uuid(agent)
            agent_name = hsa_agent_get_info_product_name(agent)
            agent_compute_capability = hsa_agent_get_info_name(agent)
            agent_compute_units = hsa_agent_get_info_compute_unit_count(agent)
            agent_driver_node_id = hsa_agent_get_info_driver_node_id(agent)

            agents.append(
                Agent(
                    device_type=agent_device_type,
                    device_id=agent_device_id,
                    uuid=agent_uuid,
                    name=agent_name,
                    compute_capability=agent_compute_capability,
                    compute_units=agent_compute_units,
                    driver_node_id=agent_driver_node_id,
                )
            )

            return 0

        hsa_iterate_agents(has_agent_callback)

    except HSAError:
        pass

    return agents
