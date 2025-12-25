# Bridge amdsmi module to avoid import errors when amdsmi is not installed
# This module raises an exception when amdsmi_init is called
# and does nothing when amdsmi_shut_down is called.
from __future__ import annotations

import contextlib
import os
from pathlib import Path

## Enums ##
AMDSMI_LINK_TYPE_INTERNAL = 0
AMDSMI_LINK_TYPE_PCIE = 1
AMDSMI_LINK_TYPE_XGMI = 2
AMDSMI_LINK_TYPE_NOT_APPLICABLE = 3
AMDSMI_LINK_TYPE_UNKNOWN = 4

try:
    with Path(os.devnull).open("w") as dev_null, contextlib.redirect_stdout(dev_null):
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
