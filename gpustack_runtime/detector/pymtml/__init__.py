from __future__ import annotations as __future_annotations__

from pymtml import *

_libInitialized = False
_libInitializedException = None

_original_mtmlLibraryInit = mtmlLibraryInit
_original_mtmlLibraryShutDown = mtmlLibraryShutDown


def mtmlLibraryInit():
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
        _original_mtmlLibraryInit()
    except Exception as e:
        with libLoadLock:
            _libInitializedException = e
        raise
    finally:
        with libLoadLock:
            _libInitialized = True


def mtmlLibraryShutDown():
    # Uninitialize the library
    global _libInitialized, _libInitializedException

    if not _libInitialized:
        return

    _original_mtmlLibraryShutDown()

    with libLoadLock:
        if not _libInitialized:
            return

        _libInitialized = False
        _libInitializedException = None
