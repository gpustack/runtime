##
# Python bindings for the CNDev library
#
# Derived from the Cambricon MLU driver's cndev.h, as vendored by the operator in
# binding/cndev -- the header of record. Only what the Cambricon detector needs is
# bound: device enumeration, identity, inventory and the usage metrics. MLU-Link,
# sMLU/MIM partitioning, per-process accounting and topology are deliberately absent.
##
from __future__ import annotations as __future_annotations__

import os
import string
import sys
import threading
from ctypes import *
from functools import wraps
from typing import ClassVar

## C Type mappings ##
# cndevDevice_t is an __int32_t handle, not an opaque pointer.
cndevDevice_t = c_int32

## Constants ##
CNDEV_VERSION_1 = 1
CNDEV_VERSION_2 = 2
CNDEV_VERSION_3 = 3
CNDEV_VERSION_4 = 4
CNDEV_VERSION_5 = 5
CNDEV_VERSION_6 = 6
CNDEV_UUID_SIZE = 37
CNDEV_ERR_MSG_LENGTH = 512
CNDEV_HEALTH_SYSTEM_MAX_INCIDENTS = 64
# The header hardcodes these array lengths in the struct declarations themselves.
CNDEV_TEMPERATURE_CLUSTER_COUNT = 20
CNDEV_TEMPERATURE_MEMORY_DIE_COUNT = 8
CNDEV_UTILIZATION_CORE_COUNT = 80

## Enums ##
# A C enum here is int-sized: every enumerator in cndev.h is a small non-negative
# value, so the compiler lays them out as a 4-byte signed int.
_cndevNameEnum_t = c_int32
CNDEV_DEVICE_TYPE_MLU100 = 0
CNDEV_DEVICE_TYPE_MLU270 = 1
CNDEV_DEVICE_TYPE_MLU220_M2 = 16
CNDEV_DEVICE_TYPE_MLU220_EDGE = 17
CNDEV_DEVICE_TYPE_MLU220_EVB = 18
CNDEV_DEVICE_TYPE_MLU220_M2i = 19
CNDEV_DEVICE_TYPE_MLU290 = 20
CNDEV_DEVICE_TYPE_MLU370 = 23
CNDEV_DEVICE_TYPE_MLU365 = 24
CNDEV_DEVICE_TYPE_CE3226 = 25
CNDEV_DEVICE_TYPE_MLU590 = 26
CNDEV_DEVICE_TYPE_MLU585 = 27
CNDEV_DEVICE_TYPE_1V_2201 = 29
CNDEV_DEVICE_TYPE_MLU580 = 30
CNDEV_DEVICE_TYPE_MLU570 = 31
CNDEV_DEVICE_TYPE_1V_2202 = 32

## Enums ##
_cndevEnableStatusEnum_t = c_int32
CNDEV_FEATURE_DISABLED = 0
CNDEV_FEATURE_ENABLED = 1

## Enums ##
_cndevHealthStateEnum_t = c_int32
CNDEV_HEALTH_STATE_DEVICE_IN_PROBLEM = 0
CNDEV_HEALTH_STATE_DEVICE_GOOD = 1

## Enums ##
_cndevDriverHealthStateEnum_t = c_int32
CNDEV_HEALTH_STATE_DRIVER_EARLY_INITED = 0
CNDEV_HEALTH_STATE_DRIVER_BRING_UP = 1
CNDEV_HEALTH_STATE_DRIVER_BOOTING = 2
CNDEV_HEALTH_STATE_DRIVER_LATEINIT = 3
CNDEV_HEALTH_STATE_DRIVER_RUNNING = 4
CNDEV_HEALTH_STATE_DRIVER_BOOT_ERROR = 5
CNDEV_HEALTH_STATE_DRIVER_RESET = 6
CNDEV_HEALTH_STATE_DRIVER_RESET_ERROR = 7
CNDEV_HEALTH_STATE_DRIVER_UNKNOWN = 8

## Enums ##
_cndevHealthResult_t = c_int32
CNDEV_HEALTH_RESULT_PASS = 0
CNDEV_HEALTH_RESULT_WARN = 10
CNDEV_HEALTH_RESULT_FAIL = 20

## Enums ##
# The incident systems (cndevHealthSystem_t) and diagnosis codes (cndevHealthError_t)
# are only needed as field widths: nothing on the detect path names an incident, so
# their enumerators are left unbound rather than copied and left to rot.
_cndevHealthSystem_t = c_int32
_cndevHealthError_t = c_int32


## Error Codes ##
CNDEV_SUCCESS = 0
CNDEV_ERROR_NO_DRIVER = 1
CNDEV_ERROR_LOW_DRIVER_VERSION = 2
CNDEV_ERROR_UNSUPPORTED_API_VERSION = 3
CNDEV_ERROR_UNINITIALIZED = 4
CNDEV_ERROR_INVALID_ARGUMENT = 5
CNDEV_ERROR_INVALID_DEVICE_ID = 6
CNDEV_ERROR_UNKNOWN = 7
CNDEV_ERROR_MALLOC = 8
CNDEV_ERROR_INSUFFICIENT_SPACE = 9
CNDEV_ERROR_NOT_SUPPORTED = 10
CNDEV_ERROR_INVALID_LINK = 11
CNDEV_ERROR_NO_DEVICES = 12
CNDEV_ERROR_NO_PERMISSION = 13
CNDEV_ERROR_NOT_FOUND = 14
CNDEV_ERROR_IN_USE = 15
CNDEV_ERROR_DUPLICATE = 16
CNDEV_ERROR_TIMEOUT = 17
CNDEV_ERROR_IN_PROBLEM = 18
# Not in the header: the operator's binding/cndev/library.go defines the same two codes,
# at the same values, for the failures that happen before any driver call -- the shared
# object is absent, or the symbol is missing from it.
CNDEV_ERROR_FUNCTION_NOT_FOUND = -99998
CNDEV_ERROR_LIBRARY_NOT_FOUND = -99999

## Lib loading ##
cndevLib = None
# Reentrant, unlike the sibling bindings' lock: cndevInit holds it across the whole
# check-then-initialize sequence, and _cndevGetFunctionPointer takes it as well.
libLoadLock = threading.RLock()
_libInitialized = False
_libInitializedException = None


## Error Checking ##
class CNDevError(Exception):
    _valClassMapping: ClassVar[dict] = {}

    _errcode_to_string: ClassVar[dict] = {
        CNDEV_ERROR_NO_DRIVER: "No Driver",
        CNDEV_ERROR_LOW_DRIVER_VERSION: "Low Driver Version",
        CNDEV_ERROR_UNSUPPORTED_API_VERSION: "Unsupported API Version",
        CNDEV_ERROR_UNINITIALIZED: "Library Not Initialized",
        CNDEV_ERROR_INVALID_ARGUMENT: "Invalid Argument",
        CNDEV_ERROR_INVALID_DEVICE_ID: "Invalid Device ID",
        CNDEV_ERROR_UNKNOWN: "Unknown Error",
        CNDEV_ERROR_MALLOC: "Memory Allocation Failed",
        CNDEV_ERROR_INSUFFICIENT_SPACE: "Insufficient Space",
        CNDEV_ERROR_NOT_SUPPORTED: "Not Supported",
        CNDEV_ERROR_INVALID_LINK: "Invalid Link",
        CNDEV_ERROR_NO_DEVICES: "No Devices",
        CNDEV_ERROR_NO_PERMISSION: "No Permission",
        CNDEV_ERROR_NOT_FOUND: "Not Found",
        CNDEV_ERROR_IN_USE: "In Use",
        CNDEV_ERROR_DUPLICATE: "Duplicate",
        CNDEV_ERROR_TIMEOUT: "Time Out",
        CNDEV_ERROR_IN_PROBLEM: "In Problem",
        CNDEV_ERROR_FUNCTION_NOT_FOUND: "Function Not Found",
        CNDEV_ERROR_LIBRARY_NOT_FOUND: "Library Not Found",
    }

    def __new__(cls, value):
        """
        Maps value to a proper subclass of CNDevError.
        See _extractCNDevErrorsAsClasses function for more details.
        """
        if cls == CNDevError:
            cls = CNDevError._valClassMapping.get(value, cls)
        obj = Exception.__new__(cls)
        obj.value = value
        return obj

    def __str__(self):
        return CNDevError._errcode_to_string.get(
            self.value,
            f"Unknown CNDev Error {self.value}",
        )

    def __eq__(self, other):
        if isinstance(other, CNDevError):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return False


def cndevExceptionClass(cndevErrorCode):
    if cndevErrorCode not in CNDevError._valClassMapping:
        msg = f"CNDev error code {cndevErrorCode} is not valid"
        raise ValueError(msg)
    return CNDevError._valClassMapping[cndevErrorCode]


def _extractCNDevErrorsAsClasses():
    """
    Generates a hierarchy of classes on top of CNDevError class.

    Each CNDev Error gets a new CNDevError subclass. This way try,except blocks can
    filter appropriate exceptions more easily.

    CNDevError is a parent class. Each CNDEV_ERROR_* gets it's own subclass.
    e.g. CNDEV_ERROR_INVALID_ARGUMENT will be turned into CNDevError_InvalidArgument.
    """
    this_module = sys.modules[__name__]
    cndevErrorsNames = [x for x in dir(this_module) if x.startswith("CNDEV_ERROR_")]
    for err_name in cndevErrorsNames:
        # e.g. Turn CNDEV_ERROR_INVALID_ARGUMENT into CNDevError_InvalidArgument
        class_name = "CNDevError_" + string.capwords(
            err_name.replace("CNDEV_ERROR_", ""),
            "_",
        ).replace("_", "")
        err_val = getattr(this_module, err_name)

        def gen_new(val):
            def new(typ, *args):
                obj = CNDevError.__new__(typ, val)
                return obj

            return new

        new_error_class = type(class_name, (CNDevError,), {"__new__": gen_new(err_val)})
        new_error_class.__module__ = __name__
        setattr(this_module, class_name, new_error_class)
        CNDevError._valClassMapping[err_val] = new_error_class


_extractCNDevErrorsAsClasses()


def _cndevCheckReturn(ret):
    if ret != CNDEV_SUCCESS:
        raise CNDevError(ret)
    return ret


## Function access ##
_cndevGetFunctionPointer_cache = {}


def _cndevGetFunctionPointer(name):
    global cndevLib

    if name in _cndevGetFunctionPointer_cache:
        return _cndevGetFunctionPointer_cache[name]

    libLoadLock.acquire()
    try:
        if cndevLib is None:
            raise CNDevError(CNDEV_ERROR_UNINITIALIZED)
        try:
            _cndevGetFunctionPointer_cache[name] = getattr(cndevLib, name)
            return _cndevGetFunctionPointer_cache[name]
        except AttributeError:
            raise CNDevError(CNDEV_ERROR_FUNCTION_NOT_FOUND)
    finally:
        libLoadLock.release()


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


class c_cndevCardInfo_t(_PrintableStructure):
    _fields_: ClassVar = [
        ("version", c_int32),
        ("number", c_uint32),
    ]


class c_cndevUUID_t(_PrintableStructure):
    # uuid is a NUL-terminated __uint8_t array in the header; c_char keeps the same
    # layout and reads back as the string the driver wrote.
    _fields_: ClassVar = [
        ("version", c_int32),
        ("uuid", c_char * CNDEV_UUID_SIZE),
        ("ncsUUID64", c_uint64),
    ]


class c_cndevMemoryInfoV2_t(_PrintableStructure):
    # Sizes are MB, and this struct carries no version field.
    _fields_: ClassVar = [
        ("physicalMemoryTotal", c_int64),
        ("physicalMemoryUsed", c_int64),
        ("reservedMemory", c_int64),
        ("virtualMemoryTotal", c_int64),
        ("virtualMemoryUsed", c_int64),
        ("globalMemory", c_uint64),
        ("reserved", c_uint64 * 16),
    ]


class c_cndevVersionInfo_t(_PrintableStructure):
    _fields_: ClassVar = [
        ("version", c_int32),
        ("mcuMajorVersion", c_uint32),
        ("mcuMinorVersion", c_uint32),
        ("mcuBuildVersion", c_uint32),
        ("driverMajorVersion", c_uint32),
        ("driverMinorVersion", c_uint32),
        ("driverBuildVersion", c_uint32),
    ]


class c_cndevECCInfo_t(_PrintableStructure):
    _fields_: ClassVar = [
        ("version", c_int32),
        ("oneBitError", c_uint64),
        ("multipleOneError", c_uint64),
        ("multipleError", c_uint64),
        ("multipleMultipleError", c_uint64),
        ("correctedError", c_uint64),
        ("uncorrectedError", c_uint64),
        ("totalError", c_uint64),
        ("addressForbiddenError", c_uint64),
    ]


class c_cndevDevicePowerInfo_t(_PrintableStructure):
    # Watts, and this struct carries no version field.
    _fields_: ClassVar = [
        ("usage", c_int32),
        ("cap", c_int32),
        ("machine", c_int32),
        ("tdp", c_int32),
        ("maxPower", c_int32),
        ("reserved", c_int32 * 16),
    ]


class c_cndevTemperatureInfo_t(_PrintableStructure):
    # Degrees Celsius.
    _fields_: ClassVar = [
        ("version", c_int32),
        ("board", c_int32),
        ("cluster", c_int32 * CNDEV_TEMPERATURE_CLUSTER_COUNT),
        ("memoryDie", c_int32 * CNDEV_TEMPERATURE_MEMORY_DIE_COUNT),
        ("chip", c_int32),
        ("airInlet", c_int32),
        ("airOutlet", c_int32),
        ("memory", c_int32),
        ("videoInput", c_int32),
        ("cpu", c_int32),
        ("isp", c_int32),
    ]


class c_cndevUtilizationInfo_t(_PrintableStructure):
    _fields_: ClassVar = [
        ("version", c_int32),
        ("averageCoreUtilization", c_int32),
        ("coreUtilization", c_int32 * CNDEV_UTILIZATION_CORE_COUNT),
    ]


class c_cndevCardName_t(_PrintableStructure):
    _fields_: ClassVar = [
        ("version", c_int32),
        ("id", _cndevNameEnum_t),
    ]


class c_cndevPCIeInfoV2_t(_PrintableStructure):
    # This struct carries no version field.
    _fields_: ClassVar = [
        ("subsystemId", c_uint32),
        ("deviceId", c_uint32),
        ("vendor", c_uint16),
        ("subsystemVendor", c_uint16),
        ("domain", c_uint32),
        ("bus", c_uint32),
        ("device", c_uint32),
        ("function", c_uint32),
        ("moduleId", c_uint16),
        ("slotId", c_uint16),
        ("reserved", c_uint32 * 8),
    ]


class c_cndevCardHealthState_t(_PrintableStructure):
    _fields_: ClassVar = [
        ("version", c_int32),
        ("health", c_int32),
        ("deviceState", _cndevHealthStateEnum_t),
        ("driverState", _cndevDriverHealthStateEnum_t),
    ]


class c_cndevDiagErrorDetail_t(_PrintableStructure):
    _fields_: ClassVar = [
        ("msg", c_char * CNDEV_ERR_MSG_LENGTH),
        ("device_id", c_uint32),
        ("bdf", c_uint32),
        ("code", _cndevHealthError_t),
        ("category", c_uint32),
        ("severity", c_uint32),
    ]


class c_cndevIncidentInfo_t(_PrintableStructure):
    _fields_: ClassVar = [
        ("system", _cndevHealthSystem_t),
        ("health", _cndevHealthResult_t),
        ("error", c_cndevDiagErrorDetail_t),
    ]


class c_cndevCardHealthStateV2_t(_PrintableStructure):
    # This struct carries no version field, and the driver fills all 64 incident slots
    # in place: a short definition here is a buffer overrun, not a missing field.
    _fields_: ClassVar = [
        ("health", c_int32),
        ("deviceState", _cndevHealthStateEnum_t),
        ("driverState", _cndevDriverHealthStateEnum_t),
        ("overallHealth", _cndevHealthResult_t),
        ("incident_count", c_uint32),
        ("incidents", c_cndevIncidentInfo_t * CNDEV_HEALTH_SYSTEM_MAX_INCIDENTS),
        ("reserved", c_uint32 * 8),
    ]


class c_cndevNUMANodeId_t(_PrintableStructure):
    _fields_: ClassVar = [
        ("version", c_int32),
        ("nodeId", c_int32),
    ]


## Device name fallback table ##
# Mirrors the switch in the operator's binding/cndev/library_device.go, used only when
# the driver exports neither cndevGetCardNameStringByDevId nor cndevGetCardNameString.
# The MLU220 variants all report as the family, and anything unlisted reports as "MLU".
_cndevCardNames = {
    CNDEV_DEVICE_TYPE_MLU100: "MLU100",
    CNDEV_DEVICE_TYPE_MLU270: "MLU270",
    CNDEV_DEVICE_TYPE_MLU220_M2: "MLU220",
    CNDEV_DEVICE_TYPE_MLU220_EDGE: "MLU220",
    CNDEV_DEVICE_TYPE_MLU220_EVB: "MLU220",
    CNDEV_DEVICE_TYPE_MLU220_M2i: "MLU220",
    CNDEV_DEVICE_TYPE_MLU290: "MLU290",
    CNDEV_DEVICE_TYPE_MLU370: "MLU370",
    CNDEV_DEVICE_TYPE_MLU365: "MLU365",
    CNDEV_DEVICE_TYPE_CE3226: "CE3226",
    CNDEV_DEVICE_TYPE_MLU590: "MLU590",
    CNDEV_DEVICE_TYPE_MLU585: "MLU585",
    CNDEV_DEVICE_TYPE_MLU580: "MLU580",
    CNDEV_DEVICE_TYPE_MLU570: "MLU570",
}


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


def _LoadCndevLibrary():
    """
    Load the library if it isn't loaded already.
    """
    global cndevLib

    if cndevLib is None:
        # lock to ensure only one caller loads the library
        libLoadLock.acquire()

        try:
            # ensure the library still isn't loaded
            if cndevLib is None:
                if sys.platform.startswith("win"):
                    # Do not support Windows yet.
                    raise CNDevError(CNDEV_ERROR_LIBRARY_NOT_FOUND)
                # Linux path,
                # mirroring the search order of the operator's binding/cndev/library.go:
                # the bare soname first, so a driver on the loader's search path wins,
                # then the Neuware home, where the CNToolkit package installs it.
                neuware_home = os.getenv("NEUWARE_HOME") or "/usr/local/neuware"
                locs = [
                    "libcndev.so",
                    os.path.join(neuware_home, "lib64", "libcndev.so"),
                    os.path.join(neuware_home, "lib", "libcndev.so"),
                ]
                load_error = None
                for loc in locs:
                    try:
                        cndevLib = CDLL(loc)
                        break
                    except OSError as e:
                        load_error = e
                if cndevLib is None:
                    # Chain the loader's own complaint: a library that is present
                    # but unloadable -- wrong architecture, a missing dependency,
                    # no permission -- is otherwise indistinguishable from one
                    # that is absent, and is_supported() only logs what it caught.
                    raise CNDevError(CNDEV_ERROR_LIBRARY_NOT_FOUND) from load_error
        finally:
            # lock is always freed
            libLoadLock.release()


## C function wrappers ##
def cndevInit():
    _LoadCndevLibrary()

    # Initialize the library
    global _libInitialized, _libInitializedException

    # Checking the flag outside the lock lets two concurrent first callers both reach
    # the driver, and the one that loses is answered with an already-initialized error
    # that is then cached as the library's permanent state, disabling detection until
    # the process restarts. The operator's binding uses sync.Once for the same reason,
    # so the whole sequence is held here.
    with libLoadLock:
        if _libInitialized:
            if _libInitializedException is not None:
                # Re-raise a fresh copy: re-raising the same cached exception object
                # appends a traceback frame on every call, and those frames retain the
                # caller's locals, leaking memory over time. See gpustack/gpustack#5342.
                from ..__utils__ import clone_exception

                raise clone_exception(_libInitializedException) from None
            return

        try:
            fn = _cndevGetFunctionPointer("cndevInit")
            # The header names the parameter "reserved" and the operator passes 0.
            # Version negotiation happens per struct, not here.
            ret = fn(c_int32(0))
            _cndevCheckReturn(ret)
        except Exception as e:
            _libInitializedException = e
            raise
        finally:
            _libInitialized = True


def cndevRelease():
    global _libInitialized, _libInitializedException

    with libLoadLock:
        if not _libInitialized:
            return

        # Initialization that never reached the driver left nothing to release.
        if _libInitializedException is None:
            # Unlike the sibling bindings' shutdown, this reaches the driver:
            # cndevRelease frees what cndevInit allocated, as the operator's binding
            # does. A driver too old to export it has nothing to free.
            try:
                fn = _cndevGetFunctionPointer("cndevRelease")
            except CNDevError as e:
                if e.value != CNDEV_ERROR_FUNCTION_NOT_FOUND:
                    raise
            else:
                # Clear the flags only once the driver has let go. Clearing them
                # first would report the library uninitialized while it still
                # holds what cndevInit allocated, and the next cndevInit would
                # then initialize on top of it.
                _cndevCheckReturn(fn())

        _libInitialized = False
        _libInitializedException = None


def cndevGetDeviceCount():
    c_card_info = c_cndevCardInfo_t()
    c_card_info.version = CNDEV_VERSION_6
    fn = _cndevGetFunctionPointer("cndevGetDeviceCount")
    ret = fn(byref(c_card_info))
    _cndevCheckReturn(ret)
    return c_card_info.number


def cndevGetDeviceHandleByIndex(index):
    c_handle = cndevDevice_t()
    fn = _cndevGetFunctionPointer("cndevGetDeviceHandleByIndex")
    ret = fn(index, byref(c_handle))
    _cndevCheckReturn(ret)
    return c_handle.value


@convertStrBytes
def cndevGetDeviceHandleByUUID(uuid):
    c_handle = cndevDevice_t()
    fn = _cndevGetFunctionPointer("cndevGetDeviceHandleByUUID")
    ret = fn(uuid, byref(c_handle))
    _cndevCheckReturn(ret)
    return c_handle.value


@convertStrBytes
def cndevGetDeviceHandleByPciBusId(pciBusId):
    c_handle = cndevDevice_t()
    fn = _cndevGetFunctionPointer("cndevGetDeviceHandleByPciBusId")
    ret = fn(pciBusId, byref(c_handle))
    _cndevCheckReturn(ret)
    return c_handle.value


def cndevGetUUID(device):
    c_uuid_info = c_cndevUUID_t()
    c_uuid_info.version = CNDEV_VERSION_6
    fn = _cndevGetFunctionPointer("cndevGetUUID")
    ret = fn(byref(c_uuid_info), device)
    _cndevCheckReturn(ret)
    # An empty field is not an identity: prefixing it would yield a bare "MLU-" that
    # reads as a valid id and is the same for every card, so the usage join would write
    # one card's metrics onto all of them. The caller's contract is that a card whose
    # identity cannot be read is skipped, so say so rather than manufacture one.
    if not c_uuid_info.uuid:
        raise CNDevError(CNDEV_ERROR_NOT_FOUND)
    # The driver reports the bare UUID. Every Cambricon tool, and the operator's
    # UUID.String(), names the device with the "MLU-" prefix, so the prefix belongs here
    # rather than in each caller.
    return "MLU-" + c_uuid_info.uuid


def cndevGetPCIeInfoV2(device):
    c_pcie_info = c_cndevPCIeInfoV2_t()
    fn = _cndevGetFunctionPointer("cndevGetPCIeInfoV2")
    ret = fn(byref(c_pcie_info), device)
    _cndevCheckReturn(ret)
    return c_pcie_info


def cndevGetPCIeBusId(device):
    c_pcie_info = cndevGetPCIeInfoV2(device)

    domain = c_pcie_info.domain
    bus = c_pcie_info.bus
    dev = c_pcie_info.device
    function = c_pcie_info.function
    return f"{domain:04x}:{bus:02x}:{dev:02x}.{function:d}"


def cndevGetCardName(device):
    c_card_name = c_cndevCardName_t()
    c_card_name.version = CNDEV_VERSION_6
    fn = _cndevGetFunctionPointer("cndevGetCardName")
    ret = fn(byref(c_card_name), device)
    _cndevCheckReturn(ret)
    return c_card_name


@convertStrBytes
def cndevGetCardNameString(cardName):
    fn = _cndevGetFunctionPointer("cndevGetCardNameString")
    fn.restype = c_char_p
    return fn(cardName)


@convertStrBytes
def cndevGetCardNameStringByDevId(device):
    fn = _cndevGetFunctionPointer("cndevGetCardNameStringByDevId")
    fn.restype = c_char_p
    return fn(device)


def cndevGetCardNameByDevId(device):
    """
    Resolve the device's marketing name, whatever the driver offers.

    Not a CNDev entry point: it applies the precedence of the operator's
    Device.GetCardName -- the name string by device, then the name enum resolved by the
    library, then the enum resolved locally -- so the fallback lives in one place
    instead of in every caller.

    Args:
        device:
            The device handle.

    Returns:
        The device name, "MLU" when the driver reports an unknown name enum.

    Raises:
        CNDevError: If the driver fails for a reason other than a missing symbol.

    """
    name = None
    try:
        name = cndevGetCardNameStringByDevId(device)
    except CNDevError as e:
        if e.value != CNDEV_ERROR_FUNCTION_NOT_FOUND:
            raise
    if name:
        return name

    c_card_name = cndevGetCardName(device)

    try:
        name = cndevGetCardNameString(c_card_name.id)
    except CNDevError as e:
        if e.value != CNDEV_ERROR_FUNCTION_NOT_FOUND:
            raise
    if name:
        return name

    return _cndevCardNames.get(c_card_name.id, "MLU")


def cndevGetMemoryUsageV2(device):
    c_memory_info = c_cndevMemoryInfoV2_t()
    fn = _cndevGetFunctionPointer("cndevGetMemoryUsageV2")
    ret = fn(byref(c_memory_info), device)
    _cndevCheckReturn(ret)
    return c_memory_info


def cndevGetCardHealthState(device):
    c_health_state = c_cndevCardHealthState_t()
    c_health_state.version = CNDEV_VERSION_6
    fn = _cndevGetFunctionPointer("cndevGetCardHealthState")
    ret = fn(byref(c_health_state), device)
    _cndevCheckReturn(ret)
    return c_health_state


def cndevGetCardHealthStateV2(device):
    c_health_state = c_cndevCardHealthStateV2_t()
    try:
        fn = _cndevGetFunctionPointer("cndevGetCardHealthStateV2")
    except CNDevError as e:
        if e.value != CNDEV_ERROR_FUNCTION_NOT_FOUND:
            raise
        # Mirror the operator's CardHealthStateHandler.V1: an older driver reports the
        # same three fields through the V1 struct, so map them into the V2 shape and
        # leave the incident report empty rather than failing the whole detection.
        c_health_state_v1 = cndevGetCardHealthState(device)
        c_health_state.health = c_health_state_v1.health
        c_health_state.deviceState = c_health_state_v1.deviceState
        c_health_state.driverState = c_health_state_v1.driverState
        # V1 carries no overall verdict, and leaving the field at zero would read
        # as CNDEV_HEALTH_RESULT_PASS: a false pass on every old driver, for any
        # caller that reaches for it instead of the health bit. Derive it.
        c_health_state.overallHealth = (
            CNDEV_HEALTH_RESULT_PASS
            if c_health_state_v1.health == CNDEV_HEALTH_STATE_DEVICE_GOOD
            else CNDEV_HEALTH_RESULT_FAIL
        )
        return c_health_state
    ret = fn(byref(c_health_state), device)
    _cndevCheckReturn(ret)
    return c_health_state


def cndevGetVersionInfo(device):
    c_version_info = c_cndevVersionInfo_t()
    c_version_info.version = CNDEV_VERSION_6
    fn = _cndevGetFunctionPointer("cndevGetVersionInfo")
    ret = fn(byref(c_version_info), device)
    _cndevCheckReturn(ret)
    return c_version_info


def cndevGetDeviceUtilizationInfo(device):
    c_util_info = c_cndevUtilizationInfo_t()
    c_util_info.version = CNDEV_VERSION_6
    fn = _cndevGetFunctionPointer("cndevGetDeviceUtilizationInfo")
    ret = fn(byref(c_util_info), device)
    _cndevCheckReturn(ret)
    return c_util_info


def cndevGetTemperatureInfo(device):
    c_temperature_info = c_cndevTemperatureInfo_t()
    # cndev.h declares this struct's version as IN, so it is stamped like every other
    # versioned struct. The operator's GetTemperatureInfo is the one wrapper that leaves
    # it zero, which its own GetVersionInfo comment suggests is an oversight surviving on
    # its driver: an unstamped struct is answered with ERROR_UNSUPPORTED_API_VERSION.
    # Temperature is a usage field the detector reads under suppression, so that error
    # would read as no temperature rather than fail loudly. Confirmed on real hardware at
    # checkpoint C2.
    c_temperature_info.version = CNDEV_VERSION_6
    fn = _cndevGetFunctionPointer("cndevGetTemperatureInfo")
    ret = fn(byref(c_temperature_info), device)
    _cndevCheckReturn(ret)
    return c_temperature_info


def cndevGetDevicePowerInfo(device):
    c_power_info = c_cndevDevicePowerInfo_t()
    fn = _cndevGetFunctionPointer("cndevGetDevicePowerInfo")
    ret = fn(byref(c_power_info), device)
    _cndevCheckReturn(ret)
    return c_power_info


def cndevGetNUMANodeIdByDevId(device):
    c_numa_node_id = c_cndevNUMANodeId_t()
    c_numa_node_id.version = CNDEV_VERSION_6
    fn = _cndevGetFunctionPointer("cndevGetNUMANodeIdByDevId")
    ret = fn(byref(c_numa_node_id), device)
    _cndevCheckReturn(ret)
    return c_numa_node_id


def cndevGetECCInfo(device):
    c_ecc_info = c_cndevECCInfo_t()
    c_ecc_info.version = CNDEV_VERSION_6
    fn = _cndevGetFunctionPointer("cndevGetECCInfo")
    ret = fn(byref(c_ecc_info), device)
    _cndevCheckReturn(ret)
    return c_ecc_info
