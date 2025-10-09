# Bridge amdsmi module to avoid import errors when amdsmi is not installed
# This module raises an exception when amdsmi_init is called
# and does nothing when amdsmi_shut_down is called.
from __future__ import annotations

import contextlib
import os
from ctypes import *
from pathlib import Path

try:
    with contextlib.redirect_stdout(Path(os.devnull).open("w")):
        from amdsmi import *
except (ImportError, KeyError, OSError):

    class AmdSmiException(Exception):
        pass

    def amdsmi_init(*_):
        msg = (
            "amdsmi module is not installed, please install it via 'pip install amdsmi'"
        )
        raise AmdSmiException(msg)

    def amdsmi_get_processor_handles():
        return []

    def amdsmi_shut_down():
        pass


def amdsmi_get_rocm_version2() -> str | None:
    possible_locations = ["librocm-core.so"]
    rocm_path = os.getenv("ROCM_HOME", os.getenv("ROCM_PATH"))
    if rocm_path:
        possible_locations.append(os.path.join(rocm_path, "lib/librocm-core.so"))
    if Path("/opt/rocm/lib/librocm-core.so").exists():
        possible_locations.append("/opt/rocm/lib/librocm-core.so")

    for possible_location in possible_locations:
        try:
            rocmcore = CDLL(possible_location)
            get_rocm_core_version = rocmcore.getROCmVersion
            get_rocm_core_version.restype = c_uint32
            get_rocm_core_version.argtypes = [
                POINTER(c_uint32),
                POINTER(c_uint32),
                POINTER(c_uint32),
            ]

            major = c_uint32()
            minor = c_uint32()
            patch = c_uint32()
            ret = get_rocm_core_version(byref(major), byref(minor), byref(patch))
        except (OSError, AttributeError):
            continue
        else:
            if ret != 0:
                return None
            return f"{major.value}.{minor.value}.{patch.value}"

    return None
