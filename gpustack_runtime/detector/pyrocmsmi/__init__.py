# Refer to
# https://github.com/ROCm/rocm_smi_lib/blob/rocm-6.2.4/python_smi_tools/rocm_smi.py,
# https://github.com/ROCm/rocm_smi_lib/blob/rocm-6.2.4/python_smi_tools/rsmiBindings.py,
# https://rocm.docs.amd.com/projects/rocm_smi_lib/en/latest/doxygen/html/rocm__smi_8h_source.html,
# https://rocm.docs.amd.com/projects/rocm_smi_lib/en/latest/doxygen/html/rocm__smi_8h.html.
from __future__ import annotations

import os
import sys
import threading
from ctypes import *
from functools import wraps
from pathlib import Path

## Enums ##
ROCMSMI_IOLINK_TYPE_UNDEFINED = 0
ROCMSMI_IOLINK_TYPE_PCIE = 1
ROCMSMI_IOLINK_TYPE_XGMI = 2
ROCMSMI_IOLINK_TYPE_NUMIOLINKTYPES = 3

## Error Codes ##
ROCMSMI_ERROR_UNINITIALIZED = -99997

## Lib loading ##
rocmsmiLib = None
libLoadLock = threading.Lock()

if rocmsmiLib is None:
    # Example ROCM_SMI_LIB_PATH
    # - /opt/hyhal/lib
    # - /opt/rocm/rocm_smi/lib
    # Example ROCM_PATH/ROCM_HOME
    # - /opt/dtk-24.04.3
    # - /opt/dtk
    # - /opt/rocm
    rocmsmi_lib_path = os.getenv("ROCM_SMI_LIB_PATH")
    if not rocmsmi_lib_path:
        rocm_path = Path(os.getenv("ROCM_HOME", os.getenv("ROCM_PATH") or "/opt/rocm"))
        rocmsmi_lib_path = str(rocm_path / "lib")
        if not Path(rocmsmi_lib_path).exists():
            rocmsmi_lib_path = str(rocm_path / "rocm_smi" / "lib")
    else:
        rocm_path = Path(
            os.getenv(
                "ROCM_HOME",
                os.getenv("ROCM_PATH") or str(Path(rocmsmi_lib_path).parent.parent),
            )
        )

    rocmsmi_lib_loc = Path(rocmsmi_lib_path) / "librocm_smi64.so"
    if rocmsmi_lib_loc.exists():
        rocmsmi_bindings_paths = [
            (rocm_path / "rocm_smi" / "bindings"),
            (rocm_path / "libexec" / "rocm_smi"),
        ]
        rocmsmi_bindings_path = None
        for p in rocmsmi_bindings_paths:
            if p.exists():
                rocmsmi_bindings_path = p
                break

        if rocmsmi_bindings_path and rocmsmi_bindings_path.exists():
            if str(rocmsmi_bindings_path) not in sys.path:
                sys.path.append(str(rocmsmi_bindings_path))

            libLoadLock.acquire()

            try:
                # Refer to https://github.com/ROCm/rocm_smi_lib/blob/amd-staging_deprecated/python_smi_tools/rsmiBindings.py.
                from rsmiBindings import *

                if not rocmsmiLib:
                    rocmsmiLib = CDLL(rocmsmi_lib_loc)
            except OSError:
                pass
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

    try:
        c_version = c_uint64()
        ret = rocmsmiLib.rsmi_dev_target_graphics_version_get(device, byref(c_version))
        _rocmsmiCheckReturn(ret)
        version = str(c_version.value)
        if len(version) == 4:
            dev_name = rsmi_dev_name_get(device)
            if "Instinct MI2" in dev_name:
                hex_part = str(hex(int(version[2:]))).replace("0x", "")
                version = version[:2] + hex_part
        else:
            version = str(c_version.value // 10 + c_version.value % 10)
        return "gfx" + version
    except AttributeError:
        return None


def rsmi_dev_temp_metric_get(device=0, sensor=None, metric=None):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    if metric is None:
        metric = rsmi_temperature_metric_t.RSMI_TEMP_CURRENT

    if sensor is not None:
        c_temp = c_int64(0)
        ret = rocmsmiLib.rsmi_dev_temp_metric_get(
            c_uint32(device),
            sensor,
            metric,
            byref(c_temp),
        )
        _rocmsmiCheckReturn(ret)
        return c_temp.value // 1000

    # If no sensor specified,
    # try all sensors and return the first valid temperature.
    for sensor_i in range(7):
        c_temp = c_int64(0)
        ret = rocmsmiLib.rsmi_dev_temp_metric_get(
            c_uint32(device),
            sensor_i,
            metric,
            byref(c_temp),
        )
        if ret == rsmi_status_t.RSMI_STATUS_SUCCESS:
            return c_temp.value // 1000

    return None


def rsmi_dev_power_cap_get(device=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_power_cap = c_uint64(0)
    ret = rocmsmiLib.rsmi_dev_power_cap_get(device, 0, byref(c_power_cap))
    _rocmsmiCheckReturn(ret)
    return c_power_cap.value // 1000000


def rsmi_dev_power_ave_get(device=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_device_chip = c_uint32(0)
    c_power = c_uint64(0)
    ret = rocmsmiLib.rsmi_dev_power_ave_get(device, c_device_chip, byref(c_power))
    _rocmsmiCheckReturn(ret)
    return c_power.value // 1000000


def rsmi_dev_power_get(device=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    try:
        c_power = c_uint64(0)
        c_power_type = rsmi_power_type_t()
        ret = rocmsmiLib.rsmi_dev_power_get(device, byref(c_power), byref(c_power_type))
        _rocmsmiCheckReturn(ret)
        return c_power.value // 1000000
    except NameError:
        # Fallback for older versions without rsmi_dev_power_get
        return rsmi_dev_power_ave_get(device)


def rsmi_dev_node_id_get(device=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_node_id = c_uint32()
    ret = rocmsmiLib.rsmi_dev_node_id_get(device, byref(c_node_id))
    _rocmsmiCheckReturn(ret)
    return c_node_id.value


def rsmi_dev_pci_id_get(device=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_pci_id = c_uint64()
    ret = rocmsmiLib.rsmi_dev_pci_id_get(device, byref(c_pci_id))
    _rocmsmiCheckReturn(ret)

    v_pci_id = c_pci_id.value
    # BDFID = ((DOMAIN & 0xFFFFFFFF) << 32) | ((Partition & 0xF) << 28)
    #         | ((BUS & 0xFF) << 8) | ((DEVICE & 0x1F) <<3 )
    #         | (FUNCTION & 0x7)
    # Extract domain, bus, device, function
    domain = (v_pci_id >> 32) & 0xFFFFFFFF
    bus = (v_pci_id >> 8) & 0xFF
    device_id = (v_pci_id >> 3) & 0x1F
    function = v_pci_id & 0x7
    return f"{domain:04x}:{bus:02x}:{device_id:02x}.{function:x}"


def rsmi_topo_get_numa_node_number(device=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_numa_node = c_uint32()
    ret = rocmsmiLib.rsmi_topo_get_numa_node_number(device, byref(c_numa_node))
    _rocmsmiCheckReturn(ret)
    return c_numa_node.value


def rsmi_topo_get_link_type(device_a=0, device_b=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_hops = c_uint64()
    c_type = c_uint32()
    ret = rocmsmiLib.rsmi_topo_get_link_type(
        device_a,
        device_b,
        byref(c_hops),
        byref(c_type),
    )
    _rocmsmiCheckReturn(ret)
    return {"hops": c_hops.value, "type": c_type.value}


def rsmi_topo_get_link_weight(device_a=0, device_b=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_weight = c_uint64()
    ret = rocmsmiLib.rsmi_topo_get_link_weight(
        device_a,
        device_b,
        byref(c_weight),
    )
    _rocmsmiCheckReturn(ret)
    return c_weight.value


def rsmi_is_p2p_accessible(device_a=0, device_b=0):
    if not rocmsmiLib:
        raise ROCMSMIError(ROCMSMI_ERROR_UNINITIALIZED)

    c_accessible = c_bool()
    ret = rocmsmiLib.rsmi_is_P2P_accessible(
        device_a,
        device_b,
        byref(c_accessible),
    )
    _rocmsmiCheckReturn(ret)
    return c_accessible.value
