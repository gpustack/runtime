"""
Copyright © 2024 MetaX Integrated Circuits (Shanghai) Co., Ltd. All Rights Reserved.

This software and associated documentation files (hereinafter collectively referred to as
"Software") is a proprietary commercial software developed by MetaX Integrated Circuits
(Shanghai) Co., Ltd. and/or its affiliates (hereinafter collectively referred to as “MetaX”).
The information presented in the Software belongs to MetaX. Without prior written permission
from MetaX, no entity or individual has the right to obtain a copy of the Software to deal in
the Software, including but not limited to use, copy, modify, merge, disclose, publish,
distribute, sublicense, and/or sell copies of the Software or substantial portions of the Software.

The Software is provided for reference only, without warranty of any kind, either express or
implied, including but not limited to the warranty of merchantability, fitness for any purpose
and/or noninfringement. In no case shall MetaX be liable for any claim, damage or other liability
arising from, out of or in connection with the Software.

If the Software need to be used in conjunction with any third-party software or open source
software, the rights to the third-party software or open source software still belong to the
copyright owners. For details, please refer to the respective notices or licenses. Please comply
with the provisions of the relevant notices or licenses. If the open source software licenses
additionally require the disposal of rights related to this Software, please contact MetaX
immediately and obtain MetaX 's written consent.

MetaX reserves the right, at its sole discretion, to change, modify, add or remove portions of the
Software, at any time. MetaX reserves all the right for the final explanation.
"""

# Python bindings for the MXSML library
from ctypes import *
import sys
import string
from .mxsml import convertStrBytes, _searchMxsmlLibrary
from .mxsml import _MxsmlBaseStructure as _MxsmlExBaseStructure
from .mxsml import mxsmlFriendlyObject as mxSmlExFriendlyOnject
from .mxsml import mxsmlStructToFriendlyObject as mxSmlExStructToFriendlyObject
from .mxsml import mxsmlFriendlyObjectToStruct as mxSmlExFriendlyObjectToStruct

# C Type mappings
# Enums
_mxSmlExEnableState_t = c_uint
MXSMLEX_FEATURE_DISABLED = 0
MXSMLEX_FEATURE_ENABLED = 1

_mxSmlExTemperatureSensors_t = c_uint
MXSMLEX_TEMPERATURE_GPU = 0
MXSMLEX_TEMPERATURE_COUNT = 1

_mxSmlExPcieUtilCounter_t = c_uint
MXSMLEX_PCIE_UTIL_TX_BYTES = 0
MXSMLEX_PCIE_UTIL_RX_BYTES = 1
MXSMLEX_PCIE_UTIL_COUNT = 2

_mxSmlExTemperatureThresholds_t = c_uint
MXSMLEX_TEMPERATURE_THRESHOLD_SHUTDOWN = 0
MXSMLEX_TEMPERATURE_THRESHOLD_SLOWDOWN = 1
MXSMLEX_TEMPERATURE_THRESHOLD_MEM_MAX = 2
MXSMLEX_TEMPERATURE_THRESHOLD_GPU_MAX = 3
MXSMLEX_TEMPERATURE_THRESHOLD_ACOUSTIC_MIN = 4
MXSMLEX_TEMPERATURE_THRESHOLD_ACOUSTIC_CURR = 5
MXSMLEX_TEMPERATURE_THRESHOLD_ACOUSTIC_MAX = 6
MXSMLEX_TEMPERATURE_THRESHOLD_COUNT = 7

_mxSmlExClockType_t = c_uint
MXSMLEX_CLOCK_GRAPHICS = 0
MXSMLEX_CLOCK_SM = 1
MXSMLEX_CLOCK_MEM = 2
MXSMLEX_CLOCK_VIDEO = 3
MXSMLEX_CLOCK_COUNT = 4

_mxSmlExGpuP2PStatus_t = c_uint
MXSMLEX_P2P_STATUS_OK = 0
MXSMLEX_P2P_STATUS_CHIPSET_NOT_SUPPORTED = 1
MXSMLEX_P2P_STATUS_GPU_NOT_SUPPORTED = 2
MXSMLEX_P2P_STATUS_IOH_TOPOLOGY_NOT_SUPPORTED = 3
MXSMLEX_P2P_STATUS_DISABLED_BY_REGKEY = 4
MXSMLEX_P2P_STATUS_NOT_SUPPORTED = 5
MXSMLEX_P2P_STATUS_UNKNOWN = 6

_mxSmlExGpuP2PCapsIndex_t = c_uint
MXSMLEX_P2P_CAPS_INDEX_READ = 0
MXSMLEX_P2P_CAPS_INDEX_WRITE = 1
MXSMLEX_P2P_CAPS_INDEX_MXLINK = 2
MXSMLEX_P2P_CAPS_INDEX_ATOMICS = 3
MXSMLEX_P2P_CAPS_INDEX_PROP = 4
MXSMLEX_P2P_CAPS_INDEX_UNKNOWN = 5

_mxSmlExPstates_t = c_uint
MXSMLEX_PSTATE_0 = 0  # Performance state 0 -- Maximum Performance.
MXSMLEX_PSTATE_1 = 1
MXSMLEX_PSTATE_2 = 2
MXSMLEX_PSTATE_3 = 3
MXSMLEX_PSTATE_4 = 4
MXSMLEX_PSTATE_5 = 5
MXSMLEX_PSTATE_6 = 6
MXSMLEX_PSTATE_7 = 7
MXSMLEX_PSTATE_8 = 8
MXSMLEX_PSTATE_9 = 9
MXSMLEX_PSTATE_10 = 10
MXSMLEX_PSTATE_11 = 11
MXSMLEX_PSTATE_12 = 12
MXSMLEX_PSTATE_13 = 13
MXSMLEX_PSTATE_14 = 14
MXSMLEX_PSTATE_15 = 15  # Performance state 15 -- Minimum Performance.
MXSMLEX_PSTATE_UNKNOWN = 32  # Unknown performance state.

_mxSmlExGpuTopologyLevel_t = c_uint
MXSMLEX_TOPOLOGY_INTERNAL = 0
MXSMLEX_TOPOLOGY_SINGLE = 10
MXSMLEX_TOPOLOGY_MULTIPLE = 20
MXSMLEX_TOPOLOGY_HOSTBRIDGE = 30
MXSMLEX_TOPOLOGY_NODE = 40
MXSMLEX_TOPOLOGY_SYSTEM = 50

_mxSmlExMetaXLinkDeviceType_t = c_uint
MXSMLEX_METAXLINK_DEVICE_TYPE_GPU = 0x00
MXSMLEX_METAXLINK_DEVICE_TYPE_NPU = 0x01
MXSMLEX_METAXLINK_DEVICE_TYPE_SWITCH = 0x02
MXSMLEX_METAXLINK_DEVICE_TYPE_UNKNOWN = 0xFF

_mxSmlExValueType_t = c_uint
MXSMLEX_VALUE_TYPE_DOUBLE = 0
MXSMLEX_VALUE_TYPE_UNSIGNED_INT = 1
MXSMLEX_VALUE_TYPE_UNSIGNED_LONG = 2
MXSMLEX_VALUE_TYPE_UNSIGNED_LONG_LONG = 3
MXSMLEX_VALUE_TYPE_SIGNED_LONG_LONG = 4
MXSMLEX_VALUE_TYPE_SIGNED_INT = 5
MXSMLEX_VALUE_TYPE_COUNT = 6

_mxSmlExReturn_t = c_uint
MXSMLEX_SUCCESS = 0
MXSMLEX_ERROR_UNINITIALIZED = 1
MXSMLEX_ERROR_INVALID_ARGUMENT = 2
MXSMLEX_ERROR_NOT_SUPPORTED = 3
MXSMLEX_ERROR_NO_PERMISSION = 4
MXSMLEX_ERROR_ALREADY_INITIALIZED = 5
MXSMLEX_ERROR_NOT_FOUND = 6
MXSMLEX_ERROR_INSUFFICIENT_SIZE = 7
MXSMLEX_ERROR_INSUFFICIENT_POWER = 8
MXSMLEX_ERROR_DRIVER_NOT_LOADED = 9
MXSMLEX_ERROR_TIMEOUT = 10
MXSMLEX_ERROR_IRQ_ISSUE = 11
MXSMLEX_ERROR_LIBRARY_NOT_FOUND = 12
MXSMLEX_ERROR_FUNCTION_NOT_FOUND = 13
MXSMLEX_ERROR_CORRUPTED_INFOROM = 14
MXSMLEX_ERROR_GPU_IS_LOST = 15
MXSMLEX_ERROR_RESET_REQUIRED = 16
MXSMLEX_ERROR_OPERATING_SYSTEM = 17
MXSMLEX_ERROR_LIB_RM_VERSION_MISMATCH = 18
MXSMLEX_ERROR_IN_USE = 19
MXSMLEX_ERROR_MEMORY = 20
MXSMLEX_ERROR_NO_DATA = 21
MXSMLEX_ERROR_VGPU_ECC_NOT_SUPPORTED = 22
MXSMLEX_ERROR_INSUFFICIENT_RESOURCES = 23
MXSMLEX_ERROR_FREQ_NOT_SUPPORTED = 24
MXSMLEX_ERROR_ARGUMENT_VERSION_MISMATCH = 25
MXSMLEX_ERROR_DEPRECATED = 26
MXSMLEX_ERROR_UNKNOWN = 999

# buffer size
MXSMLEX_DEVICE_UUID_BUFFER_SIZE = 80  # Guaranteed maximum possible size for device UUID
MXSMLEX_DEVICE_UUID_V2_BUFFER_SIZE = 96
MXSMLEX_NAME_V2_BUFFER_SIZE = 96
MXSMLEX_DRIVER_VERSION_BUFFER_SIZE = (
    80  # Guaranteed maximum possible size for driver version
)
MXSMLEX_SYSTEM_MXSMLEX_VERSION_BUFFER_SIZE = 80
MXSMLEX_DBDF_ID_BUFFER_V2_SIZE = (
    16  # Guaranteed maximum possible size for BDF ID legacy
)
MXSMLEX_DBDF_ID_BUFFER_SIZE = 32  # Guaranteed maximum possible size for BDF ID

MXSMLEX_FI_DEV_METAXLINK_LINK_COUNT = (
    91  # Field value enums used to query MetaXLink number
)


## Error Checking ##
class MXSMLEXError(Exception):
    _valClassMapping = dict()
    _errcode_to_string = {
        MXSMLEX_ERROR_UNINITIALIZED: "Uninitialized",
        MXSMLEX_ERROR_INVALID_ARGUMENT: "Invalid Argument",
        MXSMLEX_ERROR_NOT_SUPPORTED: "Not Supported",
        MXSMLEX_ERROR_NO_PERMISSION: "Insufficient Permissions",
        MXSMLEX_ERROR_ALREADY_INITIALIZED: "Already Initialized",
        MXSMLEX_ERROR_NOT_FOUND: "Not Found",
        MXSMLEX_ERROR_INSUFFICIENT_SIZE: "Insufficient Size",
        MXSMLEX_ERROR_INSUFFICIENT_POWER: "Insufficient External Power",
        MXSMLEX_ERROR_DRIVER_NOT_LOADED: "Driver Not Loaded",
        MXSMLEX_ERROR_TIMEOUT: "Timeout",
        MXSMLEX_ERROR_IRQ_ISSUE: "Interrupt Request Issue",
        MXSMLEX_ERROR_LIBRARY_NOT_FOUND: "MXSML Shared Library Not Found",
        MXSMLEX_ERROR_FUNCTION_NOT_FOUND: "Function Not Found",
        MXSMLEX_ERROR_CORRUPTED_INFOROM: "Corrupted infoROM",
        MXSMLEX_ERROR_GPU_IS_LOST: "GPU is lost",
        MXSMLEX_ERROR_RESET_REQUIRED: "GPU requires restart",
        MXSMLEX_ERROR_OPERATING_SYSTEM: "The operating system has blocked the request.",
        MXSMLEX_ERROR_LIB_RM_VERSION_MISMATCH: "RM has detected an MXSML/RM version mismatch.",
        MXSMLEX_ERROR_MEMORY: "Insufficient Memory",
        MXSMLEX_ERROR_UNKNOWN: "Unknown Error",
    }

    def __new__(typ, value):
        if typ == MXSMLEXError:
            typ = MXSMLEXError._valClassMapping.get(value, typ)
        obj = Exception.__new__(typ)
        obj.value = value
        return obj

    def __str__(self):
        try:
            if self.value not in MXSMLEXError._errcode_to_string:
                MXSMLEXError._errcode_to_string[self.value] = str(
                    mxSmlExErrorString(self.value)
                )
            return MXSMLEXError._errcode_to_string[self.value]
        except MXSMLEXError:
            return "MXSMLEX Error with code %d" % self.value

    def __eq__(self, other):
        return self.value == other.value


def _generateErrorSubclass():
    # Create execption subclass for each MXSMLEX Error
    def generate_new_func(val):
        def new(typ):
            obj = MXSMLEXError.__new__(typ, val)
            return obj

        return new

    this_module = sys.modules[__name__]
    mxsmlErrorsNames = [x for x in dir(this_module) if x.startswith("MXSMLEXError_")]
    for err_name in mxsmlErrorsNames:
        class_name = "MXSMLEXError_" + string.capwords(
            err_name.replace("MXSMLEXError_", ""), "_"
        ).replace("_", "")
        err_val = getattr(this_module, err_name)
        new_error_class = type(
            class_name, (MXSMLEXError,), {"__new__": generate_new_func(err_val)}
        )
        new_error_class.__module__ = __name__
        setattr(this_module, class_name, new_error_class)
        MXSMLEXError._valClassMapping[err_val] = new_error_class


_generateErrorSubclass()


## Device structures
class struct_c_mxSmlExDevice_t(Structure):
    pass  # opaque handle


c_mxSmlExDevice_t = POINTER(struct_c_mxSmlExDevice_t)


class c_mxSmlExMemory_t(_MxsmlExBaseStructure):
    _fields_ = [
        ("total", c_ulonglong),
        ("free", c_ulonglong),
        ("used", c_ulonglong),
    ]
    _default_format_ = "%d B"


class c_mxSmlExPciInfo_t(_MxsmlExBaseStructure):
    _fields_ = [
        (
            "busIdLegacy",
            c_char * MXSMLEX_DBDF_ID_BUFFER_V2_SIZE,
        ),  # The legacy tuple domain:bus:device.function PCI identifier
        (
            "domain",
            c_uint,
        ),  # The PCI domain on which the device's bus resides, 0 to 0xffffffff
        ("bus", c_uint),  # The bus on which the device resides, 0 to 0xff
        ("device", c_uint),  # The device's id on the bus, 0 to 31
        ("pciDeviceId", c_uint),  # The combined 16-bit device id and 16-bit vendor id
        ("pciSubSystemId", c_uint),  # The 32-bit Sub System Device ID
        (
            "busId",
            c_char * MXSMLEX_DBDF_ID_BUFFER_SIZE,
        ),  # The tuple domain:bus:device.function PCI identifier
    ]
    _format_ = {
        "domain": "0x%08X",
        "bus": "0x%02X",
        "device": "0x%02X",
        "pciDeviceId": "0x%08X",
        "pciSubSystemId": "0x%08X",
    }


class c_mxSmlExUtilization_t(_MxsmlExBaseStructure):
    _fields_ = [
        ("gpu", c_uint),
        ("memory", c_uint),
    ]


class c_mxsmlExProcessInfo_t(_MxsmlExBaseStructure):
    _fields_ = [
        ("computeInstanceId", c_uint),
        ("gpuInstanceId", c_uint),
        ("pid", c_uint),
        ("usedGpuMemory", c_ulonglong),
    ]
    _default_format_ = "%dxxx"


class c_mxSmlExExcludedDeviceInfo_t(_MxsmlExBaseStructure):
    _fields_ = [
        ("pciInfo", c_mxSmlExPciInfo_t),
        ("uuid", c_char * MXSMLEX_DEVICE_UUID_BUFFER_SIZE),
    ]


class c_mxSmlExValue_t(Union):
    _fields_ = [
        ("dVal", c_double),
        ("siVal", c_int),
        ("sllVal", c_longlong),
        ("uiVal", c_uint),
        ("ulVal", c_ulong),
        ("ullVal", c_ulonglong),
    ]


class c_mxSmlExFieldValue_t(_MxsmlExBaseStructure):
    _fields_ = [
        ("fieldId", c_uint),
        ("latencyUsec", c_longlong),
        ("mxSmlExReturn", _mxSmlExReturn_t),
        ("scopeId", c_uint),
        ("timestamp", c_longlong),
        ("value", c_mxSmlExValue_t),
        ("valueType", _mxSmlExValueType_t),
    ]


class c_mxSmlExSample_t(_MxsmlExBaseStructure):
    _fields_ = [("timeStamp", c_ulonglong), ("sampleValue", c_mxSmlExValue_t)]


mxSmlExClocksThrottleReasonAll = 0x00000000000001FF
mxSmlExClocksThrottleReasonApplicationsClocksSetting = 0x0000000000000002
mxSmlExClocksThrottleReasonDisplayClockSetting = 0x0000000000000100
mxSmlExClocksThrottleReasonGpuIdle = 0x0000000000000001
mxSmlExClocksThrottleReasonHwPowerBrakeSlowdown = 0x0000000000000080
mxSmlExClocksThrottleReasonHwSlowdown = 0x0000000000000008
mxSmlExClocksThrottleReasonHwThermalSlowdown = 0x0000000000000040
mxSmlExClocksThrottleReasonNone = 0x0000000000000000
mxSmlExClocksThrottleReasonSwPowerCap = 0x0000000000000004
mxSmlExClocksThrottleReasonSwThermalSlowdown = 0x0000000000000020
mxSmlExClocksThrottleReasonSyncBoost = 0x0000000000000010

## Lib loading ##
mxSmlExLib = None


def _mxsmlExCheckReturn(ret):
    if ret != MXSMLEX_SUCCESS:
        raise MXSMLEXError(ret)
    return ret


## Function access ##
_mxsmlExFunctionPointerCache = (
    dict()
)  # function pointers are cached to prevent unnecessary libLoadLock locking


def _mxsmlExGetFunctionPointer(name):
    if name in _mxsmlExFunctionPointerCache:
        return _mxsmlExFunctionPointerCache[name]

    global mxSmlExLib
    if mxSmlExLib == None:
        raise MXSMLEXError(MXSMLEX_ERROR_UNINITIALIZED)
    try:
        _mxsmlExFunctionPointerCache[name] = getattr(mxSmlExLib, name)
        return _mxsmlExFunctionPointerCache[name]
    except AttributeError:
        raise MXSMLEXError(MXSMLEX_ERROR_FUNCTION_NOT_FOUND)


def _loadMxsmlLibrary():
    global mxSmlExLib
    if mxSmlExLib != None:
        return

    try:
        if sys.platform[:3] == "win":
            _mxsmlExCheckReturn(MXSMLEX_ERROR_NOT_SUPPORTED)
        else:
            path_libmxsml = _searchMxsmlLibrary()
            if not path_libmxsml:
                _mxsmlExCheckReturn(MXSMLEX_ERROR_LIBRARY_NOT_FOUND)
            else:
                mxSmlExLib = CDLL(path_libmxsml)
    except OSError as ose:
        _mxsmlExCheckReturn(MXSMLEX_ERROR_LIBRARY_NOT_FOUND)
    if mxSmlExLib == None:
        _mxsmlExCheckReturn(MXSMLEX_ERROR_LIBRARY_NOT_FOUND)


## C function wrappers ##
def mxSmlExInit():
    _loadMxsmlLibrary()
    fn = _mxsmlExGetFunctionPointer("mxSmlExInit")
    ret = fn()
    _mxsmlExCheckReturn(ret)
    return None


def mxSmlExShutdown():
    fn = _mxsmlExGetFunctionPointer("mxSmlExShutdown")
    ret = fn()
    _mxsmlExCheckReturn(ret)
    return None


@convertStrBytes
def mxSmlExErrorString(result):
    fn = _mxsmlExGetFunctionPointer("mxSmlExErrorString")
    fn.restype = c_char_p  # otherwise return is an int
    ret = fn(_mxSmlExReturn_t(result))
    return ret


## System get functions
@convertStrBytes
def mxSmlExSystemGetDriverVersion():
    version = create_string_buffer(MXSMLEX_DRIVER_VERSION_BUFFER_SIZE)
    fn = _mxsmlExGetFunctionPointer("mxSmlExSystemGetDriverVersion")
    ret = fn(version, c_uint(MXSMLEX_DRIVER_VERSION_BUFFER_SIZE))
    _mxsmlExCheckReturn(ret)
    return version.value


@convertStrBytes
def mxSmlExSystemGetMXSMLEXVersion():
    version = create_string_buffer(MXSMLEX_SYSTEM_MXSMLEX_VERSION_BUFFER_SIZE)
    fn = _mxsmlExGetFunctionPointer("mxSmlExSystemGetMxsmlVersion")
    ret = fn(version, c_uint(MXSMLEX_SYSTEM_MXSMLEX_VERSION_BUFFER_SIZE))
    _mxsmlExCheckReturn(ret)
    return version.value


## Device get functions
def mxSmlExDeviceGetCount():
    device_count = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetCount")
    ret = fn(byref(device_count))
    _mxsmlExCheckReturn(ret)
    return device_count.value


def mxSmlExMacaDeviceGetCount():
    device_count = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExMacaDeviceGetCount")
    ret = fn(byref(device_count))
    _mxsmlExCheckReturn(ret)
    return device_count.value


def mxSmlExDeviceGetHandleByIndex(index):
    device_index = c_uint(index)
    device = c_mxSmlExDevice_t()
    fn = _mxsmlExGetFunctionPointer("mxSmlExGetDeviceHandleByIndex")
    ret = fn(device_index, byref(device))
    _mxsmlExCheckReturn(ret)
    return device


@convertStrBytes
def mxSmlExDeviceGetHandleByUUID(uuid):
    handle = c_mxSmlExDevice_t()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetHandleByUUID")
    ret = fn(uuid, byref(handle))
    _mxsmlExCheckReturn(ret)
    return handle


@convertStrBytes
def mxSmlExDeviceGetHandleByPciBusId(pci_bus_id):
    busId = c_char_p(pci_bus_id)
    device = c_mxSmlExDevice_t()
    fn = _mxsmlExGetFunctionPointer("mxSmlExGetDeviceHandleByPciBusId")
    ret = fn(busId, byref(device))
    _mxsmlExCheckReturn(ret)
    return device


@convertStrBytes
def mxSmlExDeviceGetName(handle):
    device_name = create_string_buffer(MXSMLEX_NAME_V2_BUFFER_SIZE)
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetName")
    ret = fn(handle, device_name, c_uint(MXSMLEX_NAME_V2_BUFFER_SIZE))
    _mxsmlExCheckReturn(ret)
    return device_name.value


@convertStrBytes
def mxSmlExDeviceGetUUID(handle):
    uuid = create_string_buffer(MXSMLEX_DEVICE_UUID_V2_BUFFER_SIZE)
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetUUID")
    ret = fn(handle, uuid, c_uint(MXSMLEX_DEVICE_UUID_V2_BUFFER_SIZE))
    _mxsmlExCheckReturn(ret)
    return uuid.value


def mxSmlExDeviceGetPciInfo(handle):
    pci_info = c_mxSmlExPciInfo_t()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetPciInfo")
    ret = fn(handle, byref(pci_info))
    _mxsmlExCheckReturn(ret)
    return pci_info


def mxSmlExDeviceGetMemoryInfo(handle, version=None):
    memory_info = c_mxSmlExMemory_t()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetMemoryInfo")
    ret = fn(handle, byref(memory_info))
    _mxsmlExCheckReturn(ret)
    return memory_info


def mxSmlExDeviceGetFanSpeed(handle):
    fan_speed = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetFanSpeed")
    ret = fn(handle, byref(fan_speed))
    _mxsmlExCheckReturn(ret)
    return fan_speed.value


def mxSmlExDeviceGetFanSpeed_v2(handle, fan):
    fan_speed = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetFanSpeed_v2")
    ret = fn(handle, c_uint(fan), byref(fan_speed))
    _mxsmlExCheckReturn(ret)
    return fan_speed.value


def mxSmlExDeviceGetUtilizationRates(handle):
    util = c_mxSmlExUtilization_t()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetUtilization")
    ret = fn(handle, byref(util))
    _mxsmlExCheckReturn(ret)
    return util


def mxSmlExDeviceGetMinorNumber(handle):
    minor_number = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetMinorNumber")
    ret = fn(handle, byref(minor_number))
    _mxsmlExCheckReturn(ret)
    return minor_number.value


def mxSmlExDeviceGetMetaXLinkState(handle, link):
    is_active = _mxSmlExEnableState_t()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetMetaXLinkState")
    ret = fn(handle, link, byref(is_active))
    _mxsmlExCheckReturn(ret)
    return is_active.value


def mxSmlExDeviceGetMetaXLinkRemotePciInfo(device, link):
    pci_info = c_mxSmlExPciInfo_t()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetMetaXLinkRemotePciInfo_v2")
    ret = fn(device, link, byref(pci_info))
    _mxsmlExCheckReturn(ret)
    return pci_info


def mxSmlExDeviceGetTemperature(handle, sensor):
    temp = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetTemperature")
    ret = fn(handle, _mxSmlExTemperatureSensors_t(sensor), byref(temp))
    _mxsmlExCheckReturn(ret)
    return temp.value


def mxSmlExDeviceGetCurrPcieLinkWidth(handle):
    width = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetCurrPcieLinkWidth")
    ret = fn(handle, byref(width))
    _mxsmlExCheckReturn(ret)
    return width.value


def mxSmlExDeviceGetComputeRunningProcesses(handle):
    count = c_uint(0)
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetComputeRunningProcesses")
    ret = fn(handle, byref(count), (c_mxsmlExProcessInfo_t * 1)())

    if ret == MXSMLEX_SUCCESS:
        return []
    elif ret == MXSMLEX_ERROR_INSUFFICIENT_SIZE:
        # oversize the array incase more processes are created
        count.value = count.value * 2 + 5
        processes = (c_mxsmlExProcessInfo_t * count.value)()
        ret = fn(handle, byref(count), byref(processes))
        _mxsmlExCheckReturn(ret)

        process_array = []
        for i in range(count.value):
            process_array.append(mxSmlExStructToFriendlyObject(processes[i]))
        return process_array
    else:
        # error case
        raise MXSMLEXError(ret)


def mxSmlExDeviceGetComputeCapability(handle):
    major = c_int()
    minor = c_int()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetComputeCapability")
    ret = fn(handle, byref(major), byref(minor))
    _mxsmlExCheckReturn(ret)
    return (major.value, minor.value)


def mxSmlExDeviceGetPowerUsage(handle):
    watts = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExGetPowerUsage")
    ret = fn(handle, byref(watts))
    _mxsmlExCheckReturn(ret)
    return watts.value


def mxSmlExDeviceGetPowerManagementLimit(handle):
    limit = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExGetPowerManagementLimit")
    ret = fn(handle, byref(limit))
    _mxsmlExCheckReturn(ret)
    return limit.value


def mxSmlExDeviceGetMaxPcieLinkWidth(handle):
    width = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExGetMaxPcieLinkWidth")
    ret = fn(handle, byref(width))
    _mxsmlExCheckReturn(ret)
    return width.value


def mxSmlExGetPcieThroughput(handle, counter):
    throughput = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExGetPcieThroughput")
    ret = fn(handle, _mxSmlExPcieUtilCounter_t(counter), byref(throughput))
    _mxsmlExCheckReturn(ret)
    return throughput.value


def mxSmlExDeviceGetTemperatureThreshold(handle, threshold):
    temp = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetTemperatureThreshold")
    ret = fn(handle, _mxSmlExTemperatureThresholds_t(threshold), byref(temp))
    _mxsmlExCheckReturn(ret)
    return temp.value


def mxSmlExDeviceGetClockInfo(handle, clock_type):
    clock = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetClockInfo")
    ret = fn(handle, _mxSmlExClockType_t(clock_type), byref(clock))
    _mxsmlExCheckReturn(ret)
    return clock.value


def mxSmlExDeviceGetCurrentClocksThrottleReasons(handle):
    reasons = c_ulonglong()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetCurrentClocksThrottleReasons")
    ret = fn(handle, byref(reasons))
    _mxsmlExCheckReturn(ret)
    return reasons.value


def mxSmlExDeviceGetSupportedClocksThrottleReasons(handle):
    reasons = c_ulonglong()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetSupportedClocksThrottleReasons")
    ret = fn(handle, byref(reasons))
    _mxsmlExCheckReturn(ret)
    return reasons.value


def mxSmlExDeviceGetCpuAffinity(handle, cpu_set_size):
    affinity_array = c_ulonglong * cpu_set_size
    affinity = affinity_array()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetCpuAffinity")
    ret = fn(handle, c_uint(cpu_set_size), byref(affinity))
    _mxsmlExCheckReturn(ret)
    return affinity


def mxSmlExDeviceGetSupportedMemoryClocks(handle):
    count = c_uint(0)
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetSupportedMemoryClocks")
    # Get count
    ret = fn(handle, byref(count), (c_uint * 1)())

    if ret == MXSMLEX_SUCCESS:
        return []
    elif ret == MXSMLEX_ERROR_INSUFFICIENT_SIZE:
        clocks = (c_uint * count.value)()
        ret = fn(handle, byref(count), clocks)
        _mxsmlExCheckReturn(ret)
        return [clk for clk in clocks]
    else:
        raise MXSMLEXError(ret)


def mxSmlExDeviceGetSupportedGraphicsClocks(handle, clock_MHz):
    count = c_uint(0)
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetSupportedGraphicsClocks")
    # Get count
    ret = fn(handle, clock_MHz, byref(count), (c_uint * 1)())

    if ret == MXSMLEX_SUCCESS:
        return []
    elif ret == MXSMLEX_ERROR_INSUFFICIENT_SIZE:
        clocks = (c_uint * count.value)()
        ret = fn(handle, clock_MHz, byref(count), clocks)
        _mxsmlExCheckReturn(ret)
        return [clk for clk in clocks]
    else:
        raise MXSMLEXError(ret)


def mxSmlExDeviceSetApplicationsClocks(handle, mem_clock, graphics_clock):
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceSetApplicationsClocks")
    ret = fn(handle, c_uint(mem_clock), c_uint(graphics_clock))
    _mxsmlExCheckReturn(ret)
    return


def mxSmlExDeviceResetApplicationsClocks(handle):
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceResetApplicationsClocks")
    ret = fn(handle)
    _mxsmlExCheckReturn(ret)
    return


def mxSmlExDeviceGetApplicationsClock(handle, clock_type):
    clock = c_uint()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetApplicationsClock")
    ret = fn(handle, _mxSmlExClockType_t(clock_type), byref(clock))
    _mxsmlExCheckReturn(ret)
    return clock.value


def mxSmlExDeviceGetP2PStatus(handle1, handle2, p2p_index):
    status = _mxSmlExGpuP2PStatus_t()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetP2PStatus")
    ret = fn(handle1, handle2, _mxSmlExGpuP2PCapsIndex_t(p2p_index), byref(status))
    _mxsmlExCheckReturn(ret)
    return status.value


def mxSmlExDeviceGetPerformanceState(handle):
    state = _mxSmlExPstates_t()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetPerformanceState")
    ret = fn(handle, byref(state))
    _mxsmlExCheckReturn(ret)
    return state.value


def mxSmlExDeviceSetAutoBoostedClocksEnabled(handle, enabled):
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceSetAutoBoostedClocksEnabled")
    ret = fn(handle, _mxSmlExEnableState_t(enabled))
    _mxsmlExCheckReturn(ret)
    return


def mxSmlExDeviceGetAutoBoostedClocksEnabled(handle):
    is_enabled = _mxSmlExEnableState_t()
    is_enabled_default = _mxSmlExEnableState_t()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetAutoBoostedClocksEnabled")
    ret = fn(handle, byref(is_enabled), byref(is_enabled_default))
    _mxsmlExCheckReturn(ret)
    return [is_enabled.value, is_enabled_default.value]


def mxSmlExDeviceGetTopologyCommonAncestor(handle1, handle2):
    level = _mxSmlExGpuTopologyLevel_t()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetTopologyCommonAncestor")
    ret = fn(handle1, handle2, byref(level))
    _mxsmlExCheckReturn(ret)
    return level.value


def mxSmlExDeviceGetMetaXLinkRemoteDeviceType(handle, link):
    mxlk_type = _mxSmlExMetaXLinkDeviceType_t()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetMetaXLinkRemoteDeviceType")
    ret = fn(handle, c_uint(link), byref(mxlk_type))
    _mxsmlExCheckReturn(ret)
    return mxlk_type.value


def mxSmlExDeviceGetFieldValues(handle, field_ids):
    values = (c_mxSmlExFieldValue_t * len(field_ids))()
    fn = _mxsmlExGetFunctionPointer("mxSmlExDeviceGetFieldValues")

    for i, field_id in enumerate(field_ids):
        try:
            (values[i].fieldId, values[i].scopeId) = field_id
        except TypeError:
            values[i].fieldId = field_id

    ret = fn(handle, c_int(len(field_ids)), byref(values))
    _mxsmlExCheckReturn(ret)
    return values
