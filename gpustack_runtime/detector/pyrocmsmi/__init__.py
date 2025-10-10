# Refer to
# https://github.com/ROCm/rocm_smi_lib/blob/rocm-6.2.4/python_smi_tools/rocm_smi.py,
# https://github.com/ROCm/rocm_smi_lib/blob/rocm-6.2.4/python_smi_tools/rsmiBindings.py,
# https://rocm.docs.amd.com/projects/rocm_smi_lib/en/latest/doxygen/html/rocm__smi_8h_source.html,
# https://rocm.docs.amd.com/projects/rocm_smi_lib/en/latest/doxygen/html/rocm__smi_8h.html.
from __future__ import annotations

import sys
import threading
from ctypes import *
from functools import wraps
from pathlib import Path

## Error Codes ##
ROCMSMI_ERROR_UNINITIALIZED = -99997

## Lib loading ##
rocmsmiLib = None
libLoadLock = threading.Lock()

if rocmsmiLib is None:
    for p in [
        "/opt/rocm/libexec/rocm_smi/",
        "/opt/dtk/rocm_smi/bindings/",
    ]:
        if p in sys.path:
            continue
        if Path(p).exists():
            sys.path.append(p)

    libLoadLock.acquire()

    try:
        # Refer to https://github.com/ROCm/rocm_smi_lib/blob/amd-staging_deprecated/python_smi_tools/rsmiBindings.py.
        from rsmiBindings import *

        rocmsmiLib = initRsmiBindings()
    except ImportError:
        rocmsmiLib = None
    finally:
        libLoadLock.release()


class ROCMSMIError(Exception):
    _extend_errcode_to_string: ClassVar[dict[int, str]] = {
        ROCMSMI_ERROR_UNINITIALIZED: "Library Not Initialized",
    }

    def __init__(self, value):
        self.value = value

    def __str__(self):
        if self.value in ROCMSMIError._extend_errcode_to_string:
            return f"ROCMSMI error {self.value}: {ROCMSMIError._extend_errcode_to_string[self.value]}"
        if self.value not in rsmi_status_verbose_err_out:
            return f"Unknown ROCMSMI error {self.value}"
        return f"ROCMSMI error {self.value}: {rsmi_status_verbose_err_out[self.value]}"


def _rocmsmiCheckReturn(ret):
    if ret != rsmi_status_t.RSMI_STATUS_SUCCESS:
        raise ROCMSMIError(ret)
    return ret


## string/bytes conversion for ease of use
def convertStrBytes(func):
    """
    In python 3, strings are unicode instead of bytes, and need to be converted for ctypes
    Args from caller: (1, 'string', <__main__.c_nvmlDevice_t at 0xFFFFFFFF>)
    Args passed to function: (1, b'string', <__main__.c_nvmlDevice_t at 0xFFFFFFFF)>.
    ----
    Returned from function: b'returned string'
    Returned to caller: 'returned string'
    """

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


## C function wrappers ##
def rsmi_init(flags=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    ret = rocmsmiLib.rsmi_init(flags)
    _rocmsmiCheckReturn(ret)


@convertStrBytes
def rsmi_driver_version_get():
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    component = rsmi_sw_component_t.RSMI_SW_COMP_DRIVER
    c_version = create_string_buffer(256)
    ret = rocmsmiLib.rsmi_version_str_get(component, c_version, 256)
    _rocmsmiCheckReturn(ret)
    return c_version.value


def rsmi_num_monitor_devices():
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_num_devices = c_uint32()
    ret = rocmsmiLib.rsmi_num_monitor_devices(byref(c_num_devices))
    _rocmsmiCheckReturn(ret)
    return c_num_devices.value


@convertStrBytes
def rsmi_dev_name_get(device=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_name = create_string_buffer(256)
    ret = rocmsmiLib.rsmi_dev_name_get(device, c_name, 256)
    _rocmsmiCheckReturn(ret)
    return c_name.value


@convertStrBytes
def rsmi_dev_serial_number_get(device=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_serial = create_string_buffer(256)
    ret = rocmsmiLib.rsmi_dev_serial_number_get(device, c_serial, 256)
    _rocmsmiCheckReturn(ret)
    return c_serial.value


def rsmi_dev_unique_id_get(device=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_uid = c_uint64()
    ret = rocmsmiLib.rsmi_dev_unique_id_get(device, byref(c_uid))
    _rocmsmiCheckReturn(ret)
    return hex(c_uid.value)


def rsmi_dev_busy_percent_get(device=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_percent = c_uint32()
    ret = rocmsmiLib.rsmi_dev_busy_percent_get(device, byref(c_percent))
    _rocmsmiCheckReturn(ret)
    return c_percent.value


def rsmi_dev_memory_usage_get(device=0, memory_type=None):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    if memory_type is None:
        memory_type = rsmi_memory_type_t.RSMI_MEM_TYPE_VRAM
    c_used = c_uint64()
    ret = rocmsmiLib.rsmi_dev_memory_usage_get(device, memory_type, byref(c_used))
    _rocmsmiCheckReturn(ret)
    return c_used.value


def rsmi_dev_memory_total_get(device=0, memory_type=None):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    if memory_type is None:
        memory_type = rsmi_memory_type_t.RSMI_MEM_TYPE_VRAM
    c_total = c_uint64()
    ret = rocmsmiLib.rsmi_dev_memory_total_get(device, memory_type, byref(c_total))
    _rocmsmiCheckReturn(ret)
    return c_total.value


def rsmi_dev_target_graphics_version_get(device=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_version = c_uint64()
    ret = rocmsmiLib.rsmi_dev_target_graphics_version_get(device, byref(c_version))
    _rocmsmiCheckReturn(ret)
    version = str(c_version.value)
    if len(version) == 4:
        version = hex(c_version.value)[2:]
    return "gfx" + version


def rsmi_dev_temp_metric_get(device=0, sensor=None, metric=None):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    if sensor is None:
        sensor = rsmi_temperature_type_t.RSMI_TEMP_TYPE_JUNCTION
    if metric is None:
        metric = rsmi_temperature_metric_t.RSMI_TEMP_CURRENT
    c_temp = c_int64(0)
    ret = rocmsmiLib.rsmi_dev_temp_metric_get(
        c_uint32(device),
        sensor,
        metric,
        byref(c_temp),
    )
    _rocmsmiCheckReturn(ret)
    return c_temp.value // 1000


def rsmi_dev_power_cap_get(device=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_power_cap = c_uint64()
    ret = rocmsmiLib.rsmi_dev_power_cap_get(device, 0, byref(c_power_cap))
    _rocmsmiCheckReturn(ret)
    return c_power_cap.value // 1000000


def rsmi_dev_power_get(device=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_power = c_uint64()
    c_power_type = rsmi_power_type_t()
    ret = rocmsmiLib.rsmi_dev_power_get(device, byref(c_power), byref(c_power_type))
    _rocmsmiCheckReturn(ret)
    return c_power.value // 1000000
