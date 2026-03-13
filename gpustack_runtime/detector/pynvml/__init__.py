from __future__ import annotations as __future_annotations__

from pynvml import *

_libInitialized = False
_libInitializedException = None

_original_nvmlInitWithFlags = nvmlInitWithFlags
_original_nvmlInit = nvmlInit
_original_nvmlShutdown = nvmlShutdown


def nvmlInitWithFlags(flags):
    # Initialize the library
    global _libInitialized, _libInitializedException

    if _libInitialized:
        if _libInitializedException is not None:
            raise _libInitializedException
        return

    try:
        _original_nvmlInitWithFlags(flags)
    except Exception as e:
        with libLoadLock:
            _libInitializedException = e
        raise
    finally:
        with libLoadLock:
            _libInitialized = True


def nvmlInit():
    nvmlInitWithFlags(0)


def nvmlShutdown():
    # Uninitialize the library
    global _libInitialized, _libInitializedException

    if not _libInitialized:
        return

    _original_nvmlShutdown()

    with libLoadLock:
        if not _libInitialized:
            return

        _libInitialized = False
        _libInitializedException = None
