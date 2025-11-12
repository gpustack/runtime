from __future__ import annotations

import os
import threading
from ctypes import *
from pathlib import Path

## Lib loading ##
rocmcoreLib = None
libLoadLock = threading.Lock()

if rocmcoreLib is None:
    # Example ROCM_SMI_LIB_PATH
    # - /opt/dtk-24.04.3/rocm_smi/lib
    # - /opt/rocm/rocm_smi/lib
    rocmcore_lib_path = os.getenv("ROCM_CORE_LIB_PATH")
    if not rocmcore_lib_path:
        # Example ROCM_PATH/ROCM_HOME
        # - /opt/dtk-24.04.3
        # - /opt/rocm
        rocm_path = Path(os.getenv("ROCM_HOME", os.getenv("ROCM_PATH") or "/opt/rocm"))
        rocmcore_lib_path = str(rocm_path / "lib")
    else:
        rocm_path = Path(
            os.getenv(
                "ROCM_HOME",
                os.getenv("ROCM_PATH") or str(Path(rocmcore_lib_path).parent.parent),
            )
        )

    rocmcore_lib_loc = Path(rocmcore_lib_path) / "librocm-core.so"
    if rocmcore_lib_loc.exists():
        libLoadLock.acquire()
        try:
            if not rocmcoreLib:
                rocmcoreLib = CDLL(rocmcore_lib_loc)
        except OSError:
            pass
        finally:
            libLoadLock.release()


def getROCmVersion() -> str | None:
    rocm_version_files = [
        rocm_path / ".info" / "version",
        rocm_path / ".info" / "version-rocm",
        rocm_path / ".info" / "version-dev",
        rocm_path / ".info" / "version-libs",
    ]
    for vf in rocm_version_files:
        if not vf.exists():
            continue

        try:
            with vf.open() as f:
                version = f.read().strip()
            if version:
                return version.split("-", 1)[0]
        except OSError:
            continue

    if rocmcoreLib:
        try:
            major = c_uint32()
            minor = c_uint32()
            patch = c_uint32()
            ret = rocmcoreLib.getROCmVersion(byref(major), byref(minor), byref(patch))
        except (OSError, AttributeError):
            pass
        else:
            if ret == 0:
                version = f"{major.value}.{minor.value}.{patch.value}"
                return version

    return None
