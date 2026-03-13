# Bridge amdsmi module to avoid import errors when amdsmi is not installed
# This module raises an exception when amdsmi_init is called
# and does nothing when amdsmi_shut_down is called.
from __future__ import annotations as __future_annotations__

import contextlib
import os
import threading
from pathlib import Path

## Enums ##
AMDSMI_LINK_TYPE_INTERNAL = 0
AMDSMI_LINK_TYPE_PCIE = 1
AMDSMI_LINK_TYPE_XGMI = 2
AMDSMI_LINK_TYPE_NOT_APPLICABLE = 3
AMDSMI_LINK_TYPE_UNKNOWN = 4

_libInitialized = False
_libInitializedException = None
libInitLock = threading.Lock()

try:
    with Path(os.devnull).open("w") as dev_null, contextlib.redirect_stdout(dev_null):
        from amdsmi import *

    _original_amdsmi_init = amdsmi_init
    _original_amdsmi_shut_down = amdsmi_shut_down

    def amdsmi_init(flag=AmdSmiInitFlags.INIT_AMD_GPUS):
        # Initialize the library
        global _libInitialized, _libInitializedException

        if _libInitialized:
            if _libInitializedException is not None:
                raise _libInitializedException
            return

        try:
            _original_amdsmi_init(flag)
        except Exception as e:
            with libInitLock:
                _libInitializedException = e
            raise
        finally:
            with libInitLock:
                _libInitialized = True

    def amdsmi_shut_down():
        global _libInitialized, _libInitializedException

        _original_amdsmi_shut_down()

        with libInitLock:
            if not _libInitialized:
                return

            _libInitialized = False
            _libInitializedException = None


except Exception:

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
