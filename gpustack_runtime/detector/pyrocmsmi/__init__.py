# Refer to https://github.com/ROCm/rocm_smi_lib/blob/amd-staging_deprecated/python_smi_tools/rocm_smi.py.
from __future__ import annotations

import sys
import threading
from ctypes import *
from pathlib import Path

## Lib loading ##
rocmsmiLib = None
libLoadLock = threading.Lock()

if rocmsmiLib is None and Path("/opt/rocm/libexec/rocm_smi").exists():
    libLoadLock.acquire()

    sys.path.append("/opt/rocm/libexec/rocm_smi/")
    try:
        # Refer to https://github.com/ROCm/rocm_smi_lib/blob/amd-staging_deprecated/python_smi_tools/rsmiBindings.py.
        from rsmiBindings import *

        rocmsmiLib = initRsmiBindings()
        rocmsmiLib.rsmi_init(0)
    except ImportError:
        rocmsmiLib = None
    finally:
        libLoadLock.release()


class ROCMSMIError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        if self.value not in rsmi_status_verbose_err_out:
            return f"Unknown ROCMSMI error {self.value}"
        return f"ROCMSMI error {self.value}: {rsmi_status_verbose_err_out[self.value]}"


def _rocmsmiCheckReturn(ret):
    if ret != rsmi_status_t.RSMI_STATUS_SUCCESS:
        raise ROCMSMIError(ret)
    return ret


## C function wrappers ##
def rsmi_dev_target_graphics_version_get(device=0):
    if not rocmsmiLib:
        return None

    c_version = c_uint64()
    ret = rocmsmiLib.rsmi_dev_target_graphics_version_get(device, byref(c_version))
    _rocmsmiCheckReturn(ret)
    version = str(c_version.value)
    if len(version) == 4:
        version = hex(c_version.value)[2:]
    return "gfx" + version
