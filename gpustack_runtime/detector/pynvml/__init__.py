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
            # Re-raise a fresh copy: re-raising the same cached exception object
            # appends a traceback frame on every call, and those frames retain the
            # caller's locals, leaking memory over time. See gpustack/gpustack#5342.
            from ..__utils__ import clone_exception

            raise clone_exception(_libInitializedException) from None
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
