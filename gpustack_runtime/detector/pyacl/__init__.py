##
# Python bindings for the ACL library
##
from __future__ import annotations

import contextlib
import os
import string
import sys
import threading
from ctypes import *
from functools import wraps
from pathlib import Path
from typing import ClassVar

## C Type mappings ##
## Constants ##
ACL_PKG_VERSION_MAX_SIZE = 128
ACL_PKG_VERSION_PARTS_MAX_SIZE = 64

## Enums ##
ACL_PKG_NAME_CANN = 0
ACL_PKG_NAME_RUNTIME = 1
ACL_PKG_NAME_COMPILER = 2
ACL_PKG_NAME_HCCL = 3
ACL_PKG_NAME_TOOLKIT = 4
ACL_PKG_NAME_OPP = 5
ACL_PKG_NAME_OPP_KERNEL = 6
ACL_PKG_NAME_DRIVER = 7

## Error Codes ##
ACL_SUCCESS = 0
ACL_ERROR_INVALID_PARAM = 100000
ACL_ERROR_UNINITIALIZE = 100001
ACL_ERROR_REPEAT_INITIALIZE = 100002
ACL_ERROR_INVALID_FILE = 100003
ACL_ERROR_WRITE_FILE = 100004
ACL_ERROR_INVALID_FILE_SIZE = 100005
ACL_ERROR_PARSE_FILE = 100006
ACL_ERROR_FILE_MISSING_ATTR = 100007
ACL_ERROR_FILE_ATTR_INVALID = 100008
ACL_ERROR_INVALID_DUMP_CONFIG = 100009
ACL_ERROR_INVALID_PROFILING_CONFIG = 100010
ACL_ERROR_INVALID_MODEL_ID = 100011
ACL_ERROR_DESERIALIZE_MODEL = 100012
ACL_ERROR_PARSE_MODEL = 100013
ACL_ERROR_READ_MODEL_FAILURE = 100014
ACL_ERROR_MODEL_SIZE_INVALID = 100015
ACL_ERROR_MODEL_MISSING_ATTR = 100016
ACL_ERROR_MODEL_INPUT_NOT_MATCH = 100017
ACL_ERROR_MODEL_OUTPUT_NOT_MATCH = 100018
ACL_ERROR_MODEL_NOT_DYNAMIC = 100019
ACL_ERROR_OP_TYPE_NOT_MATCH = 100020
ACL_ERROR_OP_INPUT_NOT_MATCH = 100021
ACL_ERROR_OP_OUTPUT_NOT_MATCH = 100022
ACL_ERROR_OP_ATTR_NOT_MATCH = 100023
ACL_ERROR_OP_NOT_FOUND = 100024
ACL_ERROR_OP_LOAD_FAILED = 100025
ACL_ERROR_UNSUPPORTED_DATA_TYPE = 100026
ACL_ERROR_FORMAT_NOT_MATCH = 100027
ACL_ERROR_BIN_SELECTOR_NOT_REGISTERED = 100028
ACL_ERROR_KERNEL_NOT_FOUND = 100029
ACL_ERROR_BIN_SELECTOR_ALREADY_REGISTERED = 100030
ACL_ERROR_KERNEL_ALREADY_REGISTERED = 100031
ACL_ERROR_INVALID_QUEUE_ID = 100032
ACL_ERROR_REPEAT_SUBSCRIBE = 100033
ACL_ERROR_STREAM_NOT_SUBSCRIBE = 100034
ACL_ERROR_THREAD_NOT_SUBSCRIBE = 100035
ACL_ERROR_WAIT_CALLBACK_TIMEOUT = 100036
ACL_ERROR_REPEAT_FINALIZE = 100037
ACL_ERROR_NOT_STATIC_AIPP = 100038
ACL_ERROR_COMPILING_STUB_MODE = 100039
ACL_ERROR_GROUP_NOT_SET = 100040
ACL_ERROR_GROUP_NOT_CREATE = 100041
ACL_ERROR_PROF_ALREADY_RUN = 100042
ACL_ERROR_PROF_NOT_RUN = 100043
ACL_ERROR_DUMP_ALREADY_RUN = 100044
ACL_ERROR_DUMP_NOT_RUN = 100045
ACL_ERROR_PROF_REPEAT_SUBSCRIBE = 148046
ACL_ERROR_PROF_API_CONFLICT = 148047
ACL_ERROR_INVALID_MAX_OPQUEUE_NUM_CONFIG = 148048
ACL_ERROR_INVALID_OPP_PATH = 148049
ACL_ERROR_OP_UNSUPPORTED_DYNAMIC = 148050
ACL_ERROR_RELATIVE_RESOURCE_NOT_CLEARED = 148051
ACL_ERROR_UNSUPPORTED_JPEG = 148052
ACL_ERROR_INVALID_BUNDLE_MODEL_ID = 148053
ACL_ERROR_BAD_ALLOC = 200000
ACL_ERROR_API_NOT_SUPPORT = 200001
ACL_ERROR_INVALID_DEVICE = 200002
ACL_ERROR_MEMORY_ADDRESS_UNALIGNED = 200003
ACL_ERROR_RESOURCE_NOT_MATCH = 200004
ACL_ERROR_INVALID_RESOURCE_HANDLE = 200005
ACL_ERROR_FEATURE_UNSUPPORTED = 200006
ACL_ERROR_PROF_MODULES_UNSUPPORTED = 200007
ACL_ERROR_STORAGE_OVER_LIMIT = 300000
ACL_ERROR_INTERNAL_ERROR = 500000
ACL_ERROR_FAILURE = 500001
ACL_ERROR_GE_FAILURE = 500002
ACL_ERROR_RT_FAILURE = 500003
ACL_ERROR_DRV_FAILURE = 500004
ACL_ERROR_PROFILING_FAILURE = 500005
ACL_ERROR_FUNCTION_NOT_FOUND = -99998
ACL_ERROR_LIBRARY_NOT_FOUND = -99999

## Lib loading ##
aclLib = None
libLoadLock = threading.Lock()


## Error Checking ##
class ACLError(Exception):
    _valClassMapping: ClassVar[dict] = {}

    _errcode_to_string: ClassVar[dict] = {
        ACL_ERROR_INVALID_PARAM: "Invalid Param",
        ACL_ERROR_UNINITIALIZE: "Uninitialized",
        ACL_ERROR_REPEAT_INITIALIZE: "Repeat Initialize",
        ACL_ERROR_INVALID_FILE: "Invalid File",
        ACL_ERROR_WRITE_FILE: "Write File Error",
        ACL_ERROR_INVALID_FILE_SIZE: "Invalid File Size",
        ACL_ERROR_PARSE_FILE: "Parse File Error",
        ACL_ERROR_FILE_MISSING_ATTR: "File Missing Attribute",
        ACL_ERROR_FILE_ATTR_INVALID: "File Attribute Invalid",
        ACL_ERROR_INVALID_DUMP_CONFIG: "Invalid Dump Config",
        ACL_ERROR_INVALID_PROFILING_CONFIG: "Invalid Profiling Config",
        ACL_ERROR_INVALID_MODEL_ID: "Invalid Model ID",
        ACL_ERROR_DESERIALIZE_MODEL: "Deserialize Model Error",
        ACL_ERROR_PARSE_MODEL: "Parse Model Error",
        ACL_ERROR_READ_MODEL_FAILURE: "Read Model Failure",
        ACL_ERROR_MODEL_SIZE_INVALID: "Model Size Invalid",
        ACL_ERROR_MODEL_MISSING_ATTR: "Model Missing Attribute",
        ACL_ERROR_MODEL_INPUT_NOT_MATCH: "Model Input Not Match",
        ACL_ERROR_MODEL_OUTPUT_NOT_MATCH: "Model Output Not Match",
        ACL_ERROR_MODEL_NOT_DYNAMIC: "Model Not Dynamic",
        ACL_ERROR_OP_TYPE_NOT_MATCH: "Op Type Not Match",
        ACL_ERROR_OP_INPUT_NOT_MATCH: "Op Input Not Match",
        ACL_ERROR_OP_OUTPUT_NOT_MATCH: "Op Output Not Match",
        ACL_ERROR_OP_ATTR_NOT_MATCH: "Op Attr Not Match",
        ACL_ERROR_OP_NOT_FOUND: "Op Not Found",
        ACL_ERROR_OP_LOAD_FAILED: "Op Load Failed",
        ACL_ERROR_UNSUPPORTED_DATA_TYPE: "Unsupported Data Type",
        ACL_ERROR_FORMAT_NOT_MATCH: "Format Not Match",
        ACL_ERROR_BIN_SELECTOR_NOT_REGISTERED: "Bin Selector Not Registered",
        ACL_ERROR_KERNEL_NOT_FOUND: "Kernel Not Found",
        ACL_ERROR_BIN_SELECTOR_ALREADY_REGISTERED: "Bin Selector Already Registered",
        ACL_ERROR_KERNEL_ALREADY_REGISTERED: "Kernel Already Registered",
        ACL_ERROR_INVALID_QUEUE_ID: "Invalid Queue ID",
        ACL_ERROR_REPEAT_SUBSCRIBE: "Repeat Subscribe",
        ACL_ERROR_STREAM_NOT_SUBSCRIBE: "Stream Not Subscribe",
        ACL_ERROR_THREAD_NOT_SUBSCRIBE: "Thread Not Subscribe",
        ACL_ERROR_WAIT_CALLBACK_TIMEOUT: "Wait Callback Timeout",
        ACL_ERROR_REPEAT_FINALIZE: "Repeat Finalize",
        ACL_ERROR_NOT_STATIC_AIPP: "Not Static AIPP",
        ACL_ERROR_COMPILING_STUB_MODE: "Compiling Stub Mode",
        ACL_ERROR_GROUP_NOT_SET: "Group Not Set",
        ACL_ERROR_GROUP_NOT_CREATE: "Group Not Create",
        ACL_ERROR_PROF_ALREADY_RUN: "Profiling Already Run",
        ACL_ERROR_PROF_NOT_RUN: "Profiling Not Run",
        ACL_ERROR_DUMP_ALREADY_RUN: "Dump Already Run",
        ACL_ERROR_DUMP_NOT_RUN: "Dump Not Run",
        ACL_ERROR_PROF_REPEAT_SUBSCRIBE: "Profiling Repeat Subscribe",
        ACL_ERROR_PROF_API_CONFLICT: "Profiling API Conflict",
        ACL_ERROR_INVALID_MAX_OPQUEUE_NUM_CONFIG: "Invalid Max Opqueue Num Config",
        ACL_ERROR_INVALID_OPP_PATH: "Invalid OPP Path",
        ACL_ERROR_OP_UNSUPPORTED_DYNAMIC: "Op Unsupported Dynamic",
        ACL_ERROR_RELATIVE_RESOURCE_NOT_CLEARED: "Relative Resource Not Cleared",
        ACL_ERROR_UNSUPPORTED_JPEG: "Unsupported JPEG",
        ACL_ERROR_INVALID_BUNDLE_MODEL_ID: "Invalid Bundle Model ID",
        ACL_ERROR_BAD_ALLOC: "Bad Alloc",
        ACL_ERROR_API_NOT_SUPPORT: "API Not Support",
        ACL_ERROR_INVALID_DEVICE: "Invalid Device",
        ACL_ERROR_MEMORY_ADDRESS_UNALIGNED: "Memory Address Unaligned",
        ACL_ERROR_RESOURCE_NOT_MATCH: "Resource Not Match",
        ACL_ERROR_INVALID_RESOURCE_HANDLE: "Invalid Resource Handle",
        ACL_ERROR_FEATURE_UNSUPPORTED: "Feature Unsupported",
        ACL_ERROR_PROF_MODULES_UNSUPPORTED: "Profiling Modules Unsupported",
        ACL_ERROR_STORAGE_OVER_LIMIT: "Storage Over Limit",
        ACL_ERROR_INTERNAL_ERROR: "Internal Error",
        ACL_ERROR_FAILURE: "Failure",
        ACL_ERROR_GE_FAILURE: "GE Failure",
        ACL_ERROR_RT_FAILURE: "RT Failure",
        ACL_ERROR_DRV_FAILURE: "DRV Failure",
        ACL_ERROR_PROFILING_FAILURE: "Profiling Failure",
        ACL_ERROR_FUNCTION_NOT_FOUND: "Function Not Found",
        ACL_ERROR_LIBRARY_NOT_FOUND: "Library Not Found",
    }

    def __new__(cls, value):
        """
        Maps value to a proper subclass of ACLError.
        See _extractACLErrorsAsClasses function for more details.
        """
        if cls == ACLError:
            cls = ACLError._valClassMapping.get(value, cls)
        obj = Exception.__new__(cls)
        obj.value = value
        return obj

    def __str__(self):
        try:
            if self.value not in ACLError._errcode_to_string:
                ACLError._errcode_to_string[self.value] = (
                    f"Unknown ACL Error {self.value}"
                )
            return ACLError._errcode_to_string[self.value]
        except ACLError:
            return f"ACL Error with code {self.value}"

    def __eq__(self, other):
        if isinstance(other, ACLError):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return False


def aclExceptionClass(ascendCLErrorCode):
    if ascendCLErrorCode not in ACLError._valClassMapping:
        msg = f"ACL error code {ascendCLErrorCode} is not valid"
        raise ValueError(msg)
    return ACLError._valClassMapping[ascendCLErrorCode]


def _extractACLErrorsAsClasses():
    """
    Generates a hierarchy of classes on top of ACLError class.

    Each ACL Error gets a new ACLError subclass. This way try,except blocks can filter appropriate
    exceptions more easily.

    ACLError is a parent class. Each ACL_ERROR_* gets it's own subclass.
    e.g. ACL_ERROR_INVALID_PARAM will be turned into ACLError_InvalidParam.
    """
    this_module = sys.modules[__name__]
    aclErrorsNames = [x for x in dir(this_module) if x.startswith("ACL_ERROR_")]
    for err_name in aclErrorsNames:
        # e.g. Turn ACL_ERROR_INVALID_PARAM into ACLError_InvalidParam
        class_name = "ACLError_" + string.capwords(
            err_name.replace("ACL_ERROR_", ""),
            "_",
        ).replace("_", "")
        err_val = getattr(this_module, err_name)

        def gen_new(val):
            def new(typ, *args):
                obj = ACLError.__new__(typ, val)
                return obj

            return new

        new_error_class = type(class_name, (ACLError,), {"__new__": gen_new(err_val)})
        new_error_class.__module__ = __name__
        setattr(this_module, class_name, new_error_class)
        ACLError._valClassMapping[err_val] = new_error_class


_extractACLErrorsAsClasses()


def _aclCheckReturn(ret):
    if ret != ACL_SUCCESS:
        raise ACLError(ret)
    return ret


## Function access ##
_aclGetFunctionPointer_cache = {}


def _aclGetFunctionPointer(name):
    global aclLib

    if name in _aclGetFunctionPointer_cache:
        return _aclGetFunctionPointer_cache[name]

    libLoadLock.acquire()
    try:
        if aclLib is None:
            raise ACLError(ACL_ERROR_UNINITIALIZE)
        try:
            _aclGetFunctionPointer_cache[name] = getattr(aclLib, name)
            return _aclGetFunctionPointer_cache[name]
        except AttributeError:
            raise ACLError(ACL_ERROR_FUNCTION_NOT_FOUND)
    finally:
        libLoadLock.release()


## Alternative object
# Allows the object to be printed
# Allows mismatched types to be assigned
#  - like None when the Structure variant requires c_uint
class aclFriendlyObject:
    def __init__(self, dictionary):
        for x in dictionary:
            setattr(self, x, dictionary[x])

    def __str__(self):
        return self.__dict__.__str__()


def aclStructToFriendlyObject(struct):
    d = {}
    for x in struct._fields_:
        key = x[0]
        value = getattr(struct, key)
        # only need to convert from bytes if bytes, no need to check python version.
        d[key] = value.decode() if isinstance(value, bytes) else value
    obj = aclFriendlyObject(d)
    return obj


# pack the object so it can be passed to the ACL library
def aclFriendlyObjectToStruct(obj, model):
    for x in model._fields_:
        key = x[0]
        value = obj.__dict__[key]
        # any c_char_p in python3 needs to be bytes, default encoding works fine.
        setattr(model, key, value.encode())
    return model


## Structure definitions ##
class _PrintableStructure(Structure):
    """
    Abstract class that produces nicer __str__ output than ctypes.Structure.
    """

    _fmt_ = {}

    def __str__(self):
        result = []
        for x in self._fields_:
            key = x[0]
            value = getattr(self, key)
            fmt = "%s"
            if key in self._fmt_:
                fmt = self._fmt_[key]
            elif "<default>" in self._fmt_:
                fmt = self._fmt_["<default>"]
            result.append(("%s: " + fmt) % (key, value))
        return self.__class__.__name__ + "(" + ", ".join(result) + ")"

    def __getattribute__(self, name):
        res = super().__getattribute__(name)
        if isinstance(res, bytes):
            return res.decode()
        return res

    def __setattr__(self, name, value):
        if isinstance(value, str):
            value = value.encode()
        super().__setattr__(name, value)


class c_aclCANNPackageVersion(_PrintableStructure):
    _fields_: ClassVar = [
        ("version", c_char * ACL_PKG_VERSION_MAX_SIZE),
        ("majorVersion", c_char * ACL_PKG_VERSION_PARTS_MAX_SIZE),
        ("minorVersion", c_char * ACL_PKG_VERSION_PARTS_MAX_SIZE),
        ("releaseVersion", c_char * ACL_PKG_VERSION_PARTS_MAX_SIZE),
        ("patchVersion", c_char * ACL_PKG_VERSION_PARTS_MAX_SIZE),
        ("reserved", c_char * ACL_PKG_VERSION_MAX_SIZE),
    ]


## string/bytes conversion for ease of use
def convertStrBytes(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # encoding a str returns bytes in python 2 and 3
        args = [arg.encode() if isinstance(arg, str) else arg for arg in args]
        res = func(*args, **kwargs)
        # In python 2, str and bytes are the same
        # In python 3, str is unicode and should be decoded.
        # Ctypes handles most conversions, this only effects c_char and char arrays.
        if isinstance(res, bytes):
            if isinstance(res, str):
                return res
            return res.decode()
        return res

    return wrapper


def _LoadAclLibrary():
    global aclLib
    if aclLib is None:
        libLoadLock.acquire()
        try:
            if aclLib is None:
                if sys.platform.startswith("win"):
                    # ACL not supported on Windows
                    raise ACLError(ACL_ERROR_LIBRARY_NOT_FOUND)
                # Linux path
                locs = [
                    "libascendcl.so",
                ]
                ascend_path = Path(
                    os.getenv(
                        "ASCEND_HOME_PATH",
                        "/usr/local/Ascend/ascend-toolkit/latest",
                    ),
                )
                if ascend_path.exists():
                    locs.extend(
                        [
                            str(ascend_path / "runtime/lib64/libascendcl.so"),
                            str(ascend_path / "aarch64-linux/lib64/libascendcl.so"),
                            str(ascend_path / "x86_64-linux/lib64/libascendcl.so"),
                        ]
                    )
                for loc in locs:
                    try:
                        aclLib = CDLL(loc)
                        break
                    except OSError:
                        pass
                if aclLib is None:
                    raise ACLError(ACL_ERROR_LIBRARY_NOT_FOUND)
        finally:
            libLoadLock.release()


## C function wrappers ##
@convertStrBytes
def aclrtGetSocName():
    with contextlib.suppress(ACLError):
        _LoadAclLibrary()

        fn = _aclGetFunctionPointer("aclrtGetSocName")
        fn.restype = c_char_p
        c_version = fn()
        return c_version.decode()

    return None


def aclsysGetCANNVersion(package_name=ACL_PKG_NAME_CANN):
    with contextlib.suppress(ACLError):
        _LoadAclLibrary()

        c_version = c_aclCANNPackageVersion()
        package_name = c_int(package_name)
        fn = _aclGetFunctionPointer("aclsysGetCANNVersion")
        ret = fn(package_name, byref(c_version))
        _aclCheckReturn(ret)
        return f"{c_version.version}".lower()

    return None
