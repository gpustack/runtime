##
# Python bindings for the MTML library
##
from __future__ import annotations

import string
import sys
import threading
from ctypes import *
from functools import wraps
from typing import ClassVar

## C Type mappings ##
## Constants ##
MTML_LIBRARY_VERSION_BUFFER_SIZE = 32
MTML_DRIVER_VERSION_BUFFER_SIZE = 80
MTML_DEVICE_NAME_BUFFER_SIZE = 32
MTML_DEVICE_UUID_BUFFER_SIZE = 48
MTML_DEVICE_MTBIOS_VERSION_BUFFER_SIZE = 64
MTML_DEVICE_VBIOS_VERSION_BUFFER_SIZE = MTML_DEVICE_MTBIOS_VERSION_BUFFER_SIZE
MTML_DEVICE_PATH_BUFFER_SIZE = 64
MTML_DEVICE_PCI_SBDF_BUFFER_SIZE = 32
MTML_VIRT_TYPE_ID_BUFFER_SIZE = 16
MTML_VIRT_TYPE_CLASS_BUFFER_SIZE = 32
MTML_VIRT_TYPE_NAME_BUFFER_SIZE = 32
MTML_VIRT_TYPE_API_BUFFER_SIZE = 16
MTML_DEVICE_PCI_BUS_ID_FMT = "%08X:%02X:%02X.0"
MTML_LOG_FILE_PATH_BUFFER_SIZE = 200
MTML_MPC_PROFILE_NAME_BUFFER_SIZE = 32
MTML_MPC_CONF_NAME_BUFFER_SIZE = 32
MTML_MPC_CONF_MAX_PROF_NUM = 16
MTML_DEVICE_SLOT_NAME_BUFFER_SIZE = 32
MTML_MEMORY_VENDOR_BUFFER_SIZE = 64
MTML_DEVICE_SERIAL_NUMBER_BUFFER_SIZE = 64

## Enums ##
MTML_BRAND_MTT = 0
MTML_BRAND_UNKNOWN = 1

## Enums ##
MTML_DEVICE_NOT_SUPPORT_VIRTUALIZATION = 0
MTML_DEVICE_SUPPORT_VIRTUALIZATION = 1

## Enums ##
MTML_VIRT_ROLE_NONE = 0
MTML_VIRT_ROLE_HOST_VIRTDEVICE = 1

## Enums ##
MTML_MPC_TYPE_NONE = 0
MTML_MPC_TYPE_PARENT = 1
MTML_MPC_TYPE_INSTANCE = 2

## Enums ##
MTML_DEVICE_NOT_SUPPORT_MPC = 0
MTML_DEVICE_SUPPORT_MPC = 1

## Enums ##
MTML_DEVICE_NOT_SUPPORT_MTLINK = 0
MTML_DEVICE_SUPPORT_MTLINK = 1

## Enums ##
MTML_GPU_ENGINE_GEOMETRY = 0
MTML_GPU_ENGINE_2D = 1
MTML_GPU_ENGINE_3D = 2
MTML_GPU_ENGINE_COMPUTE = 3

## Enums ##
MTML_DEVICE_MPC_DISABLE = 0
MTML_DEVICE_MPC_ENABLE = 1

## Enums ##
MTML_TOPOLOGY_INTERNAL = 0
MTML_TOPOLOGY_SINGLE = 1
MTML_TOPOLOGY_MULTIPLE = 2
MTML_TOPOLOGY_HOSTBRIDGE = 3
MTML_TOPOLOGY_NODE = 4
MTML_TOPOLOGY_SYSTEM = 5

## Error Codes ##
MTML_SUCCESS = 0
MTML_ERROR_DRIVER_NOT_LOADED = 1
MTML_ERROR_DRIVER_FAILURE = 2
MTML_ERROR_INVALID_ARGUMENT = 3
MTML_ERROR_NOT_SUPPORTED = 4
MTML_ERROR_NO_PERMISSION = 5
MTML_ERROR_INSUFFICIENT_SIZE = 6
MTML_ERROR_NOT_FOUND = 7
MTML_ERROR_INSUFFICIENT_MEMORY = 8
MTML_ERROR_DRIVER_TOO_OLD = 9
MTML_ERROR_DRIVER_TOO_NEW = 10
MTML_ERROR_TIMEOUT = 11
MTML_ERROR_UNKNOWN = 999
MTML_ERROR_UNINITIALIZED = -99997
MTML_ERROR_FUNCTION_NOT_FOUND = -99998
MTML_ERROR_LIBRARY_NOT_FOUND = -99999

## Lib loading ##
mtmlLib = None
libLoadLock = threading.Lock()
_mtmlLib_refcount = (
    0  # Incremented on each mtmlLibraryInit and decremented on mtmlLibraryShutdown
)
_mtmlLib = None  # Hold the loaded library, release when refcount goes to 0


## Error Checking ##
class MTMLError(Exception):
    _valClassMapping: ClassVar[dict] = {}

    _errcode_to_string: ClassVar[dict] = {
        MTML_ERROR_DRIVER_NOT_LOADED: "Driver Not Loaded",
        MTML_ERROR_DRIVER_FAILURE: "Driver Failure",
        MTML_ERROR_INVALID_ARGUMENT: "Invalid Argument",
        MTML_ERROR_NOT_SUPPORTED: "Not Supported",
        MTML_ERROR_NO_PERMISSION: "No Permission",
        MTML_ERROR_INSUFFICIENT_SIZE: "Insufficient Size",
        MTML_ERROR_NOT_FOUND: "Not Found",
        MTML_ERROR_INSUFFICIENT_MEMORY: "Insufficient Memory",
        MTML_ERROR_DRIVER_TOO_OLD: "Driver Too Old",
        MTML_ERROR_DRIVER_TOO_NEW: "Driver too New",
        MTML_ERROR_TIMEOUT: "Timeout",
        MTML_ERROR_UNKNOWN: "Unknown Error",
        MTML_ERROR_UNINITIALIZED: "Library Not Initialized",
        MTML_ERROR_FUNCTION_NOT_FOUND: "Function Not Found",
        MTML_ERROR_LIBRARY_NOT_FOUND: "Library Not Found",
    }

    def __new__(cls, value):
        """
        Maps value to a proper subclass of MTMLError.
        See _extractMTMLErrorsAsClasses function for more details.
        """
        if cls == MTMLError:
            cls = MTMLError._valClassMapping.get(value, cls)
        obj = Exception.__new__(cls)
        obj.value = value
        return obj

    def __str__(self):
        try:
            if self.value not in MTMLError._errcode_to_string:
                MTMLError._errcode_to_string[self.value] = (
                    f"Unknown MTML Error {self.value}"
                )
            return MTMLError._errcode_to_string[self.value]
        except MTMLError:
            return f"MTML Error with code {self.value}"

    def __eq__(self, other):
        if isinstance(other, MTMLError):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return False


def mtmlExceptionClass(mtmlErrorCode):
    if mtmlErrorCode not in MTMLError._valClassMapping:
        msg = f"MTML error code {mtmlErrorCode} is not valid"
        raise ValueError(msg)
    return MTMLError._valClassMapping[mtmlErrorCode]


def _extractMTMLErrorsAsClasses():
    """
    Generates a hierarchy of classes on top of MTMLError class.

    Each MTML Error gets a new MTMLError subclass. This way try,except blocks can filter appropriate
    exceptions more easily.

    MTMLError is a parent class. Each MTML_ERROR_* gets it's own subclass.
    e.g. MTML_ERROR_INVALID_ARGUMENT will be turned into MTMLError_InvalidArgument.
    """
    this_module = sys.modules[__name__]
    mtmlErrorsNames = [x for x in dir(this_module) if x.startswith("MTML_ERROR_")]
    for err_name in mtmlErrorsNames:
        # e.g. Turn MTML_ERROR_INVALID_ARGUMENT into MTMLError_InvalidArgument
        class_name = "MTMLError_" + string.capwords(
            err_name.replace("MTML_ERROR_", ""),
            "_",
        ).replace("_", "")
        err_val = getattr(this_module, err_name)

        def gen_new(val):
            def new(typ, *args):
                obj = MTMLError.__new__(typ, val)
                return obj

            return new

        new_error_class = type(class_name, (MTMLError,), {"__new__": gen_new(err_val)})
        new_error_class.__module__ = __name__
        setattr(this_module, class_name, new_error_class)
        MTMLError._valClassMapping[err_val] = new_error_class


_extractMTMLErrorsAsClasses()


def _mtmlCheckReturn(ret):
    if ret != MTML_SUCCESS:
        raise MTMLError(ret)
    return ret


## Function access ##
_mtmlGetFunctionPointer_cache = {}


def _mtmlGetFunctionPointer(name):
    global mtmlLib

    if name in _mtmlGetFunctionPointer_cache:
        return _mtmlGetFunctionPointer_cache[name]

    libLoadLock.acquire()
    try:
        if mtmlLib is None:
            raise MTMLError(MTML_ERROR_UNINITIALIZED)
        try:
            _mtmlGetFunctionPointer_cache[name] = getattr(mtmlLib, name)
            return _mtmlGetFunctionPointer_cache[name]
        except AttributeError:
            raise MTMLError(MTML_ERROR_FUNCTION_NOT_FOUND)
    finally:
        libLoadLock.release()


## Alternative object
# Allows the object to be printed
# Allows mismatched types to be assigned
#  - like None when the Structure variant requires c_uint
class mtmlFriendlyObject:
    def __init__(self, dictionary):
        for x in dictionary:
            setattr(self, x, dictionary[x])

    def __str__(self):
        return self.__dict__.__str__()


def mtmlStructToFriendlyObject(struct):
    d = {}
    for x in struct._fields_:
        key = x[0]
        value = getattr(struct, key)
        # only need to convert from bytes if bytes, no need to check python version.
        d[key] = value.decode() if isinstance(value, bytes) else value
    obj = mtmlFriendlyObject(d)
    return obj


# pack the object so it can be passed to the MTML library
def mtmlFriendlyObjectToStruct(obj, model):
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


## Device structures
class struct_c_MtmlDevice_t(Structure):
    pass  # opaque handle


c_MtmlDevice_t = POINTER(struct_c_MtmlDevice_t)


class struct_c_MtmlGpu_t(Structure):
    pass  # opaque handle


c_MtmlGpu_t = POINTER(struct_c_MtmlGpu_t)


class struct_c_MtmlLibrary_t(Structure):
    pass  # opaque handle


c_MtmlLibrary_t = POINTER(struct_c_MtmlLibrary_t)


class struct_c_MtmlMemory_t(Structure):
    pass  # opaque handle


c_MtmlMemory_t = POINTER(struct_c_MtmlMemory_t)


class struct_c_MtmlSystem_t(Structure):
    pass  # opaque handle


c_MtmlSystem_t = POINTER(struct_c_MtmlSystem_t)


class struct_c_MtmlVpu_t(Structure):
    pass  # opaque handle


c_MtmlVpu_t = POINTER(struct_c_MtmlVpu_t)


class c_MtmlPciInfo_t(_PrintableStructure):
    _fields_: ClassVar = [
        ("sbdf", c_char * MTML_DEVICE_PCI_SBDF_BUFFER_SIZE),
        ("segment", c_uint),
        ("bus", c_uint),
        ("device", c_uint),
        ("pciDeviceId", c_uint),
        ("pciSubsystemId", c_uint),
        ("busWidth", c_uint),
        ("pciMaxSpeed", c_float),
        ("pciCurSpeed", c_float),
        ("pciMaxWidth", c_uint),
        ("pciCurWidth", c_uint),
        ("pciMaxGen", c_uint),
        ("pciCurGen", c_uint),
        ("rsvd", c_int * 6),
    ]


class c_MtmlDeviceProperty_t(_PrintableStructure):
    _fields_: ClassVar = [
        ("virtCap", c_uint),  # 0 = no virt, 1 = virt supported
        ("virtRole", c_uint),  # 0 = none, 1 = host virt device
        ("mpcCap", c_uint),  # 0 = no mpc, 1 = mpc parent, 2 = mpc instance
        ("mpcType", c_uint),  # 0 = no mpc, 1 = supported
        ("mtLinkCap", c_uint),  # 0 = no mtlink, 1 = mtlink supported
        ("rsvd", c_uint),
        ("rsvd2", c_uint),
    ]


class c_MtmlVirtType_t(_PrintableStructure):
    _fields_: ClassVar = [
        ("id", c_char * MTML_VIRT_TYPE_ID_BUFFER_SIZE),
        ("name", c_char * MTML_VIRT_TYPE_NAME_BUFFER_SIZE),
        ("api", c_char * MTML_VIRT_TYPE_API_BUFFER_SIZE),
        ("horizontalResolution", c_uint),
        ("verticalResolution", c_uint),
        ("frameBuffer", c_uint),
        ("maxEncodeNum", c_uint),
        ("maxDecodeNum", c_uint),
        ("maxInstances", c_uint),
        ("maxVirtualDisplay", c_uint),
        ("rsvd", c_int * 11),
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


def _LoadMtmlLibrary():
    global mtmlLib
    if mtmlLib is None:
        libLoadLock.acquire()
        try:
            if mtmlLib is None:
                if sys.platform.startswith("win"):
                    # MTML is typically used on Linux, but for completeness,
                    # Windows support would require different path handling.
                    raise MTMLError(MTML_ERROR_LIBRARY_NOT_FOUND)
                # Linux path
                locs = [
                    "libmtml.so",
                ]
                for loc in locs:
                    try:
                        mtmlLib = CDLL(loc)
                        break
                    except OSError:
                        pass
                if mtmlLib is None:
                    raise MTMLError(MTML_ERROR_LIBRARY_NOT_FOUND)
        finally:
            libLoadLock.release()


## C function wrappers ##
def mtmlLibraryInit():
    _LoadMtmlLibrary()

    global _mtmlLib
    global _mtmlLib_refcount

    fn = _mtmlGetFunctionPointer("mtmlLibraryInit")

    libLoadLock.acquire()

    if _mtmlLib is None:
        __mtmlLib = c_MtmlLibrary_t()
        ret = fn(byref(__mtmlLib))
        _mtmlCheckReturn(ret)
        _mtmlLib = __mtmlLib

    _mtmlLib_refcount += 1
    libLoadLock.release()


def mtmlLibraryShutDown():
    global _mtmlLib
    global _mtmlLib_refcount

    fn = _mtmlGetFunctionPointer("mtmlLibraryShutDown")

    libLoadLock.acquire()
    if _mtmlLib_refcount > 0:
        _mtmlLib_refcount -= 1
    if _mtmlLib_refcount == 0 and _mtmlLib is not None:
        ret = fn(_mtmlLib)
        _mtmlCheckReturn(ret)
        _mtmlLib = None
    libLoadLock.release()


@convertStrBytes
def mtmlLibraryGetVersion():
    c_version = create_string_buffer(MTML_LIBRARY_VERSION_BUFFER_SIZE)
    fn = _mtmlGetFunctionPointer("mtmlLibraryGetVersion")
    ret = fn(_mtmlLib, c_version, MTML_LIBRARY_VERSION_BUFFER_SIZE)
    _mtmlCheckReturn(ret)
    return c_version.value


def mtmlLibraryInitSystem():
    system = c_MtmlSystem_t()
    fn = _mtmlGetFunctionPointer("mtmlLibraryInitSystem")
    ret = fn(_mtmlLib, byref(system))
    _mtmlCheckReturn(ret)
    return system


def mtmlLibraryFreeSystem(system):
    fn = _mtmlGetFunctionPointer("mtmlLibraryFreeSystem")
    ret = fn(_mtmlLib, system)
    _mtmlCheckReturn(ret)


def mtmlLibraryCountDevice():
    c_count = c_uint32()
    fn = _mtmlGetFunctionPointer("mtmlLibraryCountDevice")
    ret = fn(_mtmlLib, byref(c_count))
    _mtmlCheckReturn(ret)
    return c_count.value


def mtmlLibraryInitDeviceByIndex(index):
    c_index = c_uint(index)
    device = c_MtmlDevice_t()
    fn = _mtmlGetFunctionPointer("mtmlLibraryInitDeviceByIndex")
    ret = fn(_mtmlLib, c_index, byref(device))
    _mtmlCheckReturn(ret)
    return device


def mtmlLibraryFreeDevice(device):
    fn = _mtmlGetFunctionPointer("mtmlLibraryFreeDevice")
    ret = fn(_mtmlLib, device)
    _mtmlCheckReturn(ret)


@convertStrBytes
def mtmlSystemGetDriverVersion():
    system = mtmlLibraryInitSystem()
    try:
        c_version = create_string_buffer(MTML_DRIVER_VERSION_BUFFER_SIZE)
        fn = _mtmlGetFunctionPointer("mtmlSystemGetDriverVersion")
        ret = fn(system, c_version, MTML_DRIVER_VERSION_BUFFER_SIZE)
        _mtmlCheckReturn(ret)
        return c_version.value
    finally:
        mtmlLibraryFreeSystem(system)


def mtmlDeviceInitGpu(device):
    gpu = c_MtmlGpu_t()
    fn = _mtmlGetFunctionPointer("mtmlDeviceInitGpu")
    ret = fn(device, byref(gpu))
    _mtmlCheckReturn(ret)
    return gpu


def mtmlDeviceFreeGpu(gpu):
    fn = _mtmlGetFunctionPointer("mtmlDeviceFreeGpu")
    ret = fn(gpu)
    _mtmlCheckReturn(ret)


def mtmlDeviceInitMemory(device):
    memory = c_MtmlMemory_t()
    fn = _mtmlGetFunctionPointer("mtmlDeviceInitMemory")
    ret = fn(device, byref(memory))
    _mtmlCheckReturn(ret)
    return memory


def mtmlDeviceFreeMemory(memory):
    fn = _mtmlGetFunctionPointer("mtmlDeviceFreeMemory")
    ret = fn(memory)
    _mtmlCheckReturn(ret)


def mtmlDeviceInitVpu(device):
    vpu = c_MtmlVpu_t()
    fn = _mtmlGetFunctionPointer("mtmlDeviceInitVpu")
    ret = fn(device, byref(vpu))
    _mtmlCheckReturn(ret)
    return vpu


def mtmlDeviceFreeVpu(vpu):
    fn = _mtmlGetFunctionPointer("mtmlDeviceFreeVpu")
    ret = fn(vpu)
    _mtmlCheckReturn(ret)


def mtmlDeviceGetIndex(device):
    c_index = c_uint32()
    fn = _mtmlGetFunctionPointer("mtmlDeviceGetIndex")
    ret = fn(device, byref(c_index))
    _mtmlCheckReturn(ret)
    return c_index.value


@convertStrBytes
def mtmlDeviceGetUUID(device):
    c_uuid = create_string_buffer(MTML_DEVICE_UUID_BUFFER_SIZE)
    fn = _mtmlGetFunctionPointer("mtmlDeviceGetUUID")
    ret = fn(device, c_uuid, MTML_DEVICE_UUID_BUFFER_SIZE)
    _mtmlCheckReturn(ret)
    return c_uuid.value


def mtmlDeviceGetBrand(device):
    c_brand = c_uint32()
    fn = _mtmlGetFunctionPointer("mtmlDeviceGetBrand")
    ret = fn(device, byref(c_brand))
    _mtmlCheckReturn(ret)
    return c_brand.value


@convertStrBytes
def mtmlDeviceGetName(device):
    c_name = create_string_buffer(MTML_DEVICE_NAME_BUFFER_SIZE)
    fn = _mtmlGetFunctionPointer("mtmlDeviceGetName")
    ret = fn(device, c_name, MTML_DEVICE_NAME_BUFFER_SIZE)
    _mtmlCheckReturn(ret)
    return c_name.value


def mtmlDeviceGetPciInfo(device):
    pciInfo = c_MtmlPciInfo_t()
    fn = _mtmlGetFunctionPointer("mtmlDeviceGetPciInfo")
    ret = fn(device, byref(pciInfo))
    _mtmlCheckReturn(ret)
    return mtmlStructToFriendlyObject(pciInfo)


def mtmlDeviceGetPowerUsage(device):
    c_power_usage = c_uint32()
    fn = _mtmlGetFunctionPointer("mtmlDeviceGetPowerUsage")
    ret = fn(device, byref(c_power_usage))
    _mtmlCheckReturn(ret)
    return c_power_usage.value


def mtmlDeviceGetProperty(device):
    c_property = c_MtmlDeviceProperty_t()
    fn = _mtmlGetFunctionPointer("mtmlDeviceGetProperty")
    ret = fn(device, byref(c_property))
    _mtmlCheckReturn(ret)
    return mtmlStructToFriendlyObject(c_property)


@convertStrBytes
def mtmlDeviceGetSerialNumber(device):
    c_serial = create_string_buffer(MTML_DEVICE_SERIAL_NUMBER_BUFFER_SIZE)
    fn = _mtmlGetFunctionPointer("mtmlDeviceGetSerialNumber")
    ret = fn(device, MTML_DEVICE_SERIAL_NUMBER_BUFFER_SIZE, c_serial)
    _mtmlCheckReturn(ret)
    return c_serial.value


def mtmlDeviceCountGpuCores(device):
    c_count = c_uint32()
    fn = _mtmlGetFunctionPointer("mtmlDeviceCountGpuCores")
    ret = fn(device, byref(c_count))
    _mtmlCheckReturn(ret)
    return c_count.value


def mtmlDeviceCountAvailVirtTypes(device):
    c_count = c_uint32()
    fn = _mtmlGetFunctionPointer("mtmlDeviceCountAvailVirtTypes")
    ret = fn(device, byref(c_count))
    _mtmlCheckReturn(ret)
    return c_count.value


def mtmlDeviceCountActiveVirtDevices(device):
    c_count = c_uint32()
    fn = _mtmlGetFunctionPointer("mtmlDeviceCountActiveVirtDevices")
    ret = fn(device, byref(c_count))
    _mtmlCheckReturn(ret)
    return c_count.value


def mtmlDeviceGetActiveVirtDeviceUuids(device):
    entryCount = mtmlDeviceCountActiveVirtDevices(device)
    entryLength = MTML_DEVICE_UUID_BUFFER_SIZE
    uuids = (c_char * (entryLength * entryCount))()
    fn = _mtmlGetFunctionPointer("mtmlDeviceGetActiveVirtDeviceUuids")
    ret = fn(device, uuids, entryLength, entryCount)
    _mtmlCheckReturn(ret)
    uuid_list = []
    for i in range(entryCount):
        start = i * entryLength
        end = start + entryLength
        uuid = bytes(uuids[start:end]).split(b"\x00", 1)[0].decode()
        uuid_list.append(uuid)
    return uuid_list


def mtmlDeviceInitVirtDevice(device, uuid):
    c_uuid = c_char_p(uuid.encode())
    virtDevice = c_MtmlDevice_t()
    fn = _mtmlGetFunctionPointer("mtmlDeviceInitVirtDevice")
    ret = fn(device, c_uuid, byref(virtDevice))
    _mtmlCheckReturn(ret)
    return virtDevice


def mtmlDeviceFreeVirtDevice(virtDevice):
    fn = _mtmlGetFunctionPointer("mtmlDeviceFreeVirtDevice")
    ret = fn(virtDevice)
    _mtmlCheckReturn(ret)


def mtmlDeviceGetVirtType(virtDevice):
    fn = _mtmlGetFunctionPointer("mtmlDeviceGetVirtType")
    c_virtType = c_MtmlVirtType_t()
    ret = fn(virtDevice, byref(c_virtType))
    _mtmlCheckReturn(ret)
    return c_virtType


def mtmlGpuGetUtilization(deviceGpu):
    c_util = c_uint32()
    fn = _mtmlGetFunctionPointer("mtmlGpuGetUtilization")
    ret = fn(deviceGpu, byref(c_util))
    _mtmlCheckReturn(ret)
    return c_util.value


def mtmlGpuGetTemperature(deviceGpu):
    c_temp = c_int()
    fn = _mtmlGetFunctionPointer("mtmlGpuGetTemperature")
    ret = fn(deviceGpu, byref(c_temp))
    _mtmlCheckReturn(ret)
    return c_temp.value


def mtmlGpuGetEngineUtilization(deviceGpu, engineType=MTML_GPU_ENGINE_COMPUTE):
    c_util = c_uint32()
    c_engineType = c_uint32(engineType)
    fn = _mtmlGetFunctionPointer("mtmlGpuGetEngineUtilization")
    ret = fn(deviceGpu, c_engineType, byref(c_util))
    _mtmlCheckReturn(ret)
    return c_util.value


def mtmlMemoryGetTotal(deviceMemory):
    c_total = c_uint64()
    fn = _mtmlGetFunctionPointer("mtmlMemoryGetTotal")
    ret = fn(deviceMemory, byref(c_total))
    _mtmlCheckReturn(ret)
    return c_total.value


def mtmlMemoryGetUsed(deviceMemory):
    c_used = c_uint64()
    fn = _mtmlGetFunctionPointer("mtmlMemoryGetUsed")
    ret = fn(deviceMemory, byref(c_used))
    _mtmlCheckReturn(ret)
    return c_used.value


def mtmlMemoryGetUtilization(deviceMemory):
    c_util = c_uint32()
    fn = _mtmlGetFunctionPointer("mtmlMemoryGetUtilization")
    ret = fn(deviceMemory, byref(c_util))
    _mtmlCheckReturn(ret)
    return c_util.value


def mtmlDeviceGetMpcMode(device):
    c_mode = c_uint32()
    fn = _mtmlGetFunctionPointer("mtmlDeviceGetMpcMode")
    ret = fn(device, byref(c_mode))
    _mtmlCheckReturn(ret)
    return c_mode.value


def mtmlDeviceGetMemoryAffinityWithinNode(device, nodeSetSize):
    affinity_array = c_ulonglong * nodeSetSize
    c_affinity = affinity_array()
    fn = _mtmlGetFunctionPointer("mtmlDeviceGetMemoryAffinityWithinNode")
    ret = fn(device, nodeSetSize, byref(c_affinity))
    _mtmlCheckReturn(ret)
    return c_affinity


def mtmlDeviceGetTopologyLevel(device1, device2):
    c_level = c_uint()
    fn = _mtmlGetFunctionPointer("mtmlDeviceGetTopologyLevel")
    ret = fn(device1, device2, byref(c_level))
    _mtmlCheckReturn(ret)
    return c_level.value
