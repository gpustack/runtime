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


def amdsmi_get_rocm_original_version() -> str | None:
    locs = [
        "librocm-core.so",
    ]
    rocm_path = Path(os.getenv("ROCM_HOME", os.getenv("ROCM_PATH") or "/opt/rocm"))
    if rocm_path.exists():
        locs.append(str(rocm_path / "lib/librocm-core.so"))
    for loc in locs:
        try:
            clib = CDLL(loc)
            major = c_uint32()
            minor = c_uint32()
            patch = c_uint32()
            ret = clib.getROCmVersion(byref(major), byref(minor), byref(patch))
        except (OSError, AttributeError):
            continue
        else:
            if ret != 0:
                return None
            return f"{major.value}.{minor.value}.{patch.value}"

    return None
