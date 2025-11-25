##
# Python bindings for the DCMI library
##
from __future__ import annotations

import errno
import os
import sys
import threading
from ctypes import *
from typing import ClassVar

## C Type mappings ##
## Enums ##
## /opt/amdgpu/include/libdrm/amdgpu_drm.h
AMDGPU_FAMILY_UNKNOWN = 0
AMDGPU_FAMILY_SI = 110  # Hainan, Oland, Verde, Pitcairn, Tahiti
AMDGPU_FAMILY_CI = 120  # Bonaire, Hawaii
AMDGPU_FAMILY_KV = 125  # Kaveri, Kabini, Mullins
AMDGPU_FAMILY_VI = 130  # Iceland, Tonga
AMDGPU_FAMILY_CZ = 135  # Carrizo, Stoney
AMDGPU_FAMILY_AI = 141  # Vega10
AMDGPU_FAMILY_RV = 142  # Raven
AMDGPU_FAMILY_NV = 143  # Navi10
AMDGPU_FAMILY_VGH = 144  # Van Gogh
AMDGPU_FAMILY_GC_11_0_0 = 145  # GC 11.0.0
AMDGPU_FAMILY_YC = 146  # Yellow Carp
AMDGPU_FAMILY_GC_11_0_1 = 148  # GC 11.0.1
AMDGPU_FAMILY_GC_10_3_6 = 149  # GC 10.3.6
AMDGPU_FAMILY_GC_10_3_7 = 151  # GC 10.3.7
AMDGPU_FAMILY_GC_11_5_0 = 150  # GC 11.5.0
AMDGPU_FAMILY_GC_12_0_0 = 152  # GC 12.0.0

## Error Codes ##
AMDGPU_SUCCESS = 0
AMDGPU_ERROR_CARD_NOTFOUND = -99996
AMDGPU_ERROR_UNINITIALIZED = -99997
AMDGPU_ERROR_FUNCTION_NOT_FOUND = -99998
AMDGPU_ERROR_LIBRARY_NOT_FOUND = -99999

## Lib loading ##
amdgpuLib = None
libLoadLock = threading.Lock()


class AMDGPUError(Exception):
    _extend_errcode_to_string: ClassVar[dict[int, str]] = {
        AMDGPU_ERROR_CARD_NOTFOUND: "Card Not Found",
        AMDGPU_ERROR_UNINITIALIZED: "Library Not Initialized",
        AMDGPU_ERROR_FUNCTION_NOT_FOUND: "Function Not Found",
        AMDGPU_ERROR_LIBRARY_NOT_FOUND: "Library Not Found",
    }

    def __init__(self, value):
        self.value = (
            -value if value not in AMDGPUError._extend_errcode_to_string else value
        )

    def __str__(self):
        if self.value in AMDGPUError._extend_errcode_to_string:
            return f"AMDGPU error {self.value}: {AMDGPUError._extend_errcode_to_string[self.value]}"
        if self.value not in errno.errorcode:
            return f"Unknown AMDGPU error {self.value}"
        return f"AMDGPU error {self.value}: {errno.errorcode[self.value]}"

    def __eq__(self, other):
        if isinstance(other, AMDGPUError):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return False


def _amdgpuCheckReturn(ret):
    if ret != AMDGPU_SUCCESS:
        raise AMDGPUError(ret)
    return ret


## Function access ##
_amdgpuGetFunctionPointer_cache = {}


def _amdgpuGetFunctionPointer(name):
    global amdgpuLib

    if name in _amdgpuGetFunctionPointer_cache:
        return _amdgpuGetFunctionPointer_cache[name]

    libLoadLock.acquire()
    try:
        if amdgpuLib is None:
            raise AMDGPUError(AMDGPU_ERROR_UNINITIALIZED)
        try:
            _amdgpuGetFunctionPointer_cache[name] = getattr(amdgpuLib, name)
            return _amdgpuGetFunctionPointer_cache[name]
        except AttributeError:
            raise AMDGPUError(AMDGPU_ERROR_FUNCTION_NOT_FOUND)
    finally:
        libLoadLock.release()


## Alternative object
# Allows the object to be printed
# Allows mismatched types to be assigned
#  - like None when the Structure variant requires c_uint
class amdgpuFriendlyObject:
    def __init__(self, dictionary):
        for x in dictionary:
            setattr(self, x, dictionary[x])

    def __str__(self):
        return self.__dict__.__str__()


def amdgpuStructToFriendlyObject(struct):
    d = {}
    for x in struct._fields_:
        key = x[0]
        value = getattr(struct, key)
        # only need to convert from bytes if bytes, no need to check python version.
        d[key] = value.decode() if isinstance(value, bytes) else value
    obj = amdgpuFriendlyObject(d)
    return obj


# pack the object so it can be passed to the AMDGPU library
def amdgpuFriendlyObjectToStruct(obj, model):
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
class struct_c_amdgpu_device_t(Structure):
    pass  # opaque handle


c_amdgpu_device_t = POINTER(struct_c_amdgpu_device_t)


class c_amdgpu_gpu_info(_PrintableStructure):
    _fields_: ClassVar = [
        ("asic_id", c_uint),
        ("chip_rev", c_uint),
        ("chip_external_rev", c_uint),
        ("family_id", c_uint),
        ("ids_flags", c_ulonglong),
        ("max_engine_clk", c_ulonglong),
        ("max_memory_clk", c_ulonglong),
        ("num_shader_engines", c_uint),
        ("num_shader_arrays_per_engine", c_uint),
        ("avail_quad_shader_pipes", c_uint),
        ("max_quad_shader_pipes", c_uint),
        ("cache_entries_per_quad_pipe", c_uint),
        ("num_hw_gfx_contexts", c_uint),
        ("rb_pipes", c_uint),
        ("enabled_rb_pipes_mask", c_uint),
        ("gpu_counter_freq", c_uint),
        ("backend_disable", c_uint * 4),
        ("mc_arb_ramcfg", c_uint),
        ("gb_addr_cfg", c_uint),
        ("gb_tile_mode", c_uint * 32),
        ("gb_macro_tile_mode", c_uint * 16),
        ("pa_sc_raster_cfg", c_uint * 4),
        ("pa_sc_raster_cfg1", c_uint * 4),
        ("cu_active_number", c_uint),
        ("cu_ao_mask", c_uint),
        ("cu_bitmap", (c_uint * 4) * 4),
        ("vram_type", c_uint),
        ("vram_bit_width", c_uint),
        ("ce_ram_size", c_uint),
        ("vce_harvest_config", c_uint),
        ("pci_rev_id", c_uint),
    ]


def _LoadAMDGPULibrary():
    global amdgpuLib
    if amdgpuLib is None:
        libLoadLock.acquire()
        try:
            if amdgpuLib is None:
                if sys.platform.startswith("win"):
                    # Do not support Windows yet.
                    raise AMDGPUError(AMDGPU_ERROR_LIBRARY_NOT_FOUND)
                # Linux path
                locs = [
                    "libdrm_amdgpu.so.1.0.0",
                    "libdrm_amdgpu.so.1",
                    "libdrm_amdgpu.so",
                ]
                for loc in locs:
                    try:
                        amdgpuLib = CDLL(loc)
                        break
                    except OSError:
                        pass
                if amdgpuLib is None:
                    raise AMDGPUError(AMDGPU_ERROR_LIBRARY_NOT_FOUND)
        finally:
            libLoadLock.release()


## C function wrappers ##
def amdgpu_device_initialize(card=1):
    _LoadAMDGPULibrary()

    try:
        fd = os.open(f"/dev/dri/card{card}", os.O_RDONLY)
    except FileNotFoundError:
        raise AMDGPUError(AMDGPU_ERROR_CARD_NOTFOUND)

    c_major = c_uint32()
    c_minor = c_uint32()
    device = c_amdgpu_device_t()
    fn = _amdgpuGetFunctionPointer("amdgpu_device_initialize")
    # If receive an error print here, try
    # sudo vim /etc/default/grub
    # and add "amdgpu.dc=0" to GRUB_CMDLINE_LINUX_DEFAULT
    # then run "sudo update-grub" and reboot.
    ret = fn(fd, byref(c_major), byref(c_minor), byref(device))
    _amdgpuCheckReturn(ret)
    return c_major.value, c_minor.value, device


def amdgpu_device_deinitialize(device):
    fn = _amdgpuGetFunctionPointer("amdgpu_device_deinitialize")
    ret = fn(device)
    _amdgpuCheckReturn(ret)


def amdgpu_query_gpu_info(device):
    c_info = c_amdgpu_gpu_info()
    fn = _amdgpuGetFunctionPointer("amdgpu_query_gpu_info")
    ret = fn(device, byref(c_info))
    _amdgpuCheckReturn(ret)
    return c_info
