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
            raise _libInitializedException
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
