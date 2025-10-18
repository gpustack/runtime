from __future__ import annotations

import contextlib
import string
import sys
import threading
from ctypes import *
from typing import ClassVar

## C Type mappings ##
## Enums ##
## https://docs.nvidia.com/cuda/cuda-driver-api/group__CUDA__TYPES.html#CUdevice_attribute
CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_BLOCK = 1
CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_X = 2
CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_Y = 3
CU_DEVICE_ATTRIBUTE_MAX_BLOCK_DIM_Z = 4
CU_DEVICE_ATTRIBUTE_MAX_GRID_DIM_X = 5
CU_DEVICE_ATTRIBUTE_MAX_GRID_DIM_Y = 6
CU_DEVICE_ATTRIBUTE_MAX_GRID_DIM_Z = 7
CU_DEVICE_ATTRIBUTE_MAX_SHARED_MEMORY_PER_BLOCK = 8
CU_DEVICE_ATTRIBUTE_SHARED_MEMORY_PER_BLOCK = 8
CU_DEVICE_ATTRIBUTE_TOTAL_CONSTANT_MEMORY = 9
CU_DEVICE_ATTRIBUTE_WARP_SIZE = 10
CU_DEVICE_ATTRIBUTE_MAX_PITCH = 11
CU_DEVICE_ATTRIBUTE_MAX_REGISTERS_PER_BLOCK = 12
CU_DEVICE_ATTRIBUTE_REGISTERS_PER_BLOCK = 12
CU_DEVICE_ATTRIBUTE_CLOCK_RATE = 13
CU_DEVICE_ATTRIBUTE_TEXTURE_ALIGNMENT = 14
CU_DEVICE_ATTRIBUTE_GPU_OVERLAP = 15
CU_DEVICE_ATTRIBUTE_MULTIPROCESSOR_COUNT = 16
CU_DEVICE_ATTRIBUTE_KERNEL_EXEC_TIMEOUT = 17
CU_DEVICE_ATTRIBUTE_INTEGRATED = 18
CU_DEVICE_ATTRIBUTE_CAN_MAP_HOST_MEMORY = 19
CU_DEVICE_ATTRIBUTE_COMPUTE_MODE = 20
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE1D_WIDTH = 21
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_WIDTH = 22
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_HEIGHT = 23
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE3D_WIDTH = 24
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE3D_HEIGHT = 25
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE3D_DEPTH = 26
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_LAYERED_WIDTH = 27
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_LAYERED_HEIGHT = 28
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_LAYERED_LAYERS = 29
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_ARRAY_WIDTH = 27
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_ARRAY_HEIGHT = 28
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_ARRAY_NUMSLICES = 29
CU_DEVICE_ATTRIBUTE_SURFACE_ALIGNMENT = 30
CU_DEVICE_ATTRIBUTE_CONCURRENT_KERNELS = 31
CU_DEVICE_ATTRIBUTE_ECC_ENABLED = 32
CU_DEVICE_ATTRIBUTE_PCI_BUS_ID = 33
CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID = 34
CU_DEVICE_ATTRIBUTE_TCC_DRIVER = 35
CU_DEVICE_ATTRIBUTE_MEMORY_CLOCK_RATE = 36
CU_DEVICE_ATTRIBUTE_GLOBAL_MEMORY_BUS_WIDTH = 37
CU_DEVICE_ATTRIBUTE_L2_CACHE_SIZE = 38
CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_MULTIPROCESSOR = 39
CU_DEVICE_ATTRIBUTE_ASYNC_ENGINE_COUNT = 40
CU_DEVICE_ATTRIBUTE_UNIFIED_ADDRESSING = 41
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE1D_LAYERED_WIDTH = 42
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE1D_LAYERED_LAYERS = 43
CU_DEVICE_ATTRIBUTE_CAN_TEX2D_GATHER = 44
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_GATHER_WIDTH = 45
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_GATHER_HEIGHT = 46
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE3D_WIDTH_ALTERNATE = 47
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE3D_HEIGHT_ALTERNATE = 48
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE3D_DEPTH_ALTERNATE = 49
CU_DEVICE_ATTRIBUTE_PCI_DOMAIN_ID = 50
CU_DEVICE_ATTRIBUTE_TEXTURE_PITCH_ALIGNMENT = 51
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURECUBEMAP_WIDTH = 52
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURECUBEMAP_LAYERED_WIDTH = 53
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURECUBEMAP_LAYERED_LAYERS = 54
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACE1D_WIDTH = 55
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACE2D_WIDTH = 56
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACE2D_HEIGHT = 57
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACE3D_WIDTH = 58
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACE3D_HEIGHT = 59
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACE3D_DEPTH = 60
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACE1D_LAYERED_WIDTH = 61
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACE1D_LAYERED_LAYERS = 62
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACE2D_LAYERED_WIDTH = 63
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACE2D_LAYERED_HEIGHT = 64
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACE2D_LAYERED_LAYERS = 65
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACECUBEMAP_WIDTH = 66
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACECUBEMAP_LAYERED_WIDTH = 67
CU_DEVICE_ATTRIBUTE_MAXIMUM_SURFACECUBEMAP_LAYERED_LAYERS = 68
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE1D_LINEAR_WIDTH = 69
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_LINEAR_WIDTH = 70
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_LINEAR_HEIGHT = 71
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_LINEAR_PITCH = 72
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_MIPMAPPED_WIDTH = 73
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE2D_MIPMAPPED_HEIGHT = 74
CU_DEVICE_ATTRIBUTE_COMPUTE_CAPABILITY_MAJOR = 75
CU_DEVICE_ATTRIBUTE_COMPUTE_CAPABILITY_MINOR = 76
CU_DEVICE_ATTRIBUTE_MAXIMUM_TEXTURE1D_MIPMAPPED_WIDTH = 77
CU_DEVICE_ATTRIBUTE_STREAM_PRIORITIES_SUPPORTED = 78
CU_DEVICE_ATTRIBUTE_GLOBAL_L1_CACHE_SUPPORTED = 79
CU_DEVICE_ATTRIBUTE_LOCAL_L1_CACHE_SUPPORTED = 80
CU_DEVICE_ATTRIBUTE_MAX_SHARED_MEMORY_PER_MULTIPROCESSOR = 81
CU_DEVICE_ATTRIBUTE_MAX_REGISTERS_PER_MULTIPROCESSOR = 82
CU_DEVICE_ATTRIBUTE_MANAGED_MEMORY = 83
CU_DEVICE_ATTRIBUTE_MULTI_GPU_BOARD = 84
CU_DEVICE_ATTRIBUTE_MULTI_GPU_BOARD_GROUP_ID = 85
CU_DEVICE_ATTRIBUTE_HOST_NATIVE_ATOMIC_SUPPORTED = 86
CU_DEVICE_ATTRIBUTE_SINGLE_TO_DOUBLE_PRECISION_PERF_RATIO = 87
CU_DEVICE_ATTRIBUTE_PAGEABLE_MEMORY_ACCESS = 88
CU_DEVICE_ATTRIBUTE_CONCURRENT_MANAGED_ACCESS = 89
CU_DEVICE_ATTRIBUTE_COMPUTE_PREEMPTION_SUPPORTED = 90
CU_DEVICE_ATTRIBUTE_CAN_USE_HOST_POINTER_FOR_REGISTERED_MEM = 91
CU_DEVICE_ATTRIBUTE_CAN_USE_STREAM_MEM_OPS_V1 = 92
CU_DEVICE_ATTRIBUTE_CAN_USE_64_BIT_STREAM_MEM_OPS_V1 = 93
CU_DEVICE_ATTRIBUTE_CAN_USE_STREAM_WAIT_VALUE_NOR_V1 = 94
CU_DEVICE_ATTRIBUTE_COOPERATIVE_LAUNCH = 95
CU_DEVICE_ATTRIBUTE_COOPERATIVE_MULTI_DEVICE_LAUNCH = 96
CU_DEVICE_ATTRIBUTE_MAX_SHARED_MEMORY_PER_BLOCK_OPTIN = 97
CU_DEVICE_ATTRIBUTE_CAN_FLUSH_REMOTE_WRITES = 98
CU_DEVICE_ATTRIBUTE_HOST_REGISTER_SUPPORTED = 99
CU_DEVICE_ATTRIBUTE_PAGEABLE_MEMORY_ACCESS_USES_HOST_PAGE_TABLES = 100
CU_DEVICE_ATTRIBUTE_DIRECT_MANAGED_MEM_ACCESS_FROM_HOST = 101
CU_DEVICE_ATTRIBUTE_VIRTUAL_ADDRESS_MANAGEMENT_SUPPORTED = 102
CU_DEVICE_ATTRIBUTE_VIRTUAL_MEMORY_MANAGEMENT_SUPPORTED = 102
CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR_SUPPORTED = 103
CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_WIN32_HANDLE_SUPPORTED = 104
CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_WIN32_KMT_HANDLE_SUPPORTED = 105
CU_DEVICE_ATTRIBUTE_MAX_BLOCKS_PER_MULTIPROCESSOR = 106
CU_DEVICE_ATTRIBUTE_GENERIC_COMPRESSION_SUPPORTED = 107
CU_DEVICE_ATTRIBUTE_MAX_PERSISTING_L2_CACHE_SIZE = 108
CU_DEVICE_ATTRIBUTE_MAX_ACCESS_POLICY_WINDOW_SIZE = 109
CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED = 110
CU_DEVICE_ATTRIBUTE_RESERVED_SHARED_MEMORY_PER_BLOCK = 111
CU_DEVICE_ATTRIBUTE_SPARSE_CUDA_ARRAY_SUPPORTED = 112
CU_DEVICE_ATTRIBUTE_READ_ONLY_HOST_REGISTER_SUPPORTED = 113
CU_DEVICE_ATTRIBUTE_TIMELINE_SEMAPHORE_INTEROP_SUPPORTED = 114
CU_DEVICE_ATTRIBUTE_MEMORY_POOLS_SUPPORTED = 115
CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_SUPPORTED = 116
CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_FLUSH_WRITES_OPTIONS = 117
CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WRITES_ORDERING = 118
CU_DEVICE_ATTRIBUTE_MEMPOOL_SUPPORTED_HANDLE_TYPES = 119
CU_DEVICE_ATTRIBUTE_CLUSTER_LAUNCH = 120
CU_DEVICE_ATTRIBUTE_DEFERRED_MAPPING_CUDA_ARRAY_SUPPORTED = 121
CU_DEVICE_ATTRIBUTE_CAN_USE_64_BIT_STREAM_MEM_OPS = 122
CU_DEVICE_ATTRIBUTE_CAN_USE_STREAM_WAIT_VALUE_NOR = 123
CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED = 124
CU_DEVICE_ATTRIBUTE_IPC_EVENT_SUPPORTED = 125
CU_DEVICE_ATTRIBUTE_MEM_SYNC_DOMAIN_COUNT = 126
CU_DEVICE_ATTRIBUTE_TENSOR_MAP_ACCESS_SUPPORTED = 127
CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED = 128
CU_DEVICE_ATTRIBUTE_UNIFIED_FUNCTION_POINTERS = 129
CU_DEVICE_ATTRIBUTE_NUMA_CONFIG = 130
CU_DEVICE_ATTRIBUTE_NUMA_ID = 131
CU_DEVICE_ATTRIBUTE_MULTICAST_SUPPORTED = 132
CU_DEVICE_ATTRIBUTE_MPS_ENABLED = 133
CU_DEVICE_ATTRIBUTE_HOST_NUMA_ID = 134
CU_DEVICE_ATTRIBUTE_D3D12_CIG_SUPPORTED = 135
CU_DEVICE_ATTRIBUTE_MEM_DECOMPRESS_ALGORITHM_MASK = 136
CU_DEVICE_ATTRIBUTE_MEM_DECOMPRESS_MAXIMUM_LENGTH = 137
CU_DEVICE_ATTRIBUTE_VULKAN_CIG_SUPPORTED = 138
CU_DEVICE_ATTRIBUTE_GPU_PCI_DEVICE_ID = 139
CU_DEVICE_ATTRIBUTE_GPU_PCI_SUBSYSTEM_ID = 140
CU_DEVICE_ATTRIBUTE_HOST_NUMA_VIRTUAL_MEMORY_MANAGEMENT_SUPPORTED = 141
CU_DEVICE_ATTRIBUTE_HOST_NUMA_MEMORY_POOLS_SUPPORTED = 142
CU_DEVICE_ATTRIBUTE_HOST_NUMA_MULTINODE_IPC_SUPPORTED = 143
CU_DEVICE_ATTRIBUTE_HOST_MEMORY_POOLS_SUPPORTED = 144
CU_DEVICE_ATTRIBUTE_HOST_VIRTUAL_MEMORY_MANAGEMENT_SUPPORTED = 145
CU_DEVICE_ATTRIBUTE_HOST_ALLOC_DMA_BUF_SUPPORTED = 146
CU_DEVICE_ATTRIBUTE_ONLY_PARTIAL_HOST_NATIVE_ATOMIC_SUPPORTED = 147

## Error Codes ##
CUDA_SUCCESS = 0
CUDA_ERROR_INVALID_VALUE = 1
CUDA_ERROR_NOT_INITIALIZED = 3
CUDA_ERROR_DEINITIALIZED = 4
CUDA_ERROR_INVALID_DEVICE = 101
CUDA_ERROR_INVALID_CONTEXT = 201
CUDA_ERROR_SYSTEM_DRIVER_MISMATCH = 803
CUDA_ERROR_COMPAT_NOT_SUPPORTED_ON_DEVICE = 804
CUDA_ERROR_UNINITIALIZED = -99997
CUDA_ERROR_FUNCTION_NOT_FOUND = -99998
CUDA_ERROR_LIBRARY_NOT_FOUND = -99999

## Lib loading ##
cudaLib = None
libLoadLock = threading.Lock()


class CUDAError(Exception):
    _valClassMapping: ClassVar[dict] = {}

    _errcode_to_string: ClassVar[dict] = {
        CUDA_ERROR_INVALID_VALUE: "Invalid Value",
        CUDA_ERROR_NOT_INITIALIZED: "Not Initialized",
        CUDA_ERROR_DEINITIALIZED: "Deinitialized",
        CUDA_ERROR_INVALID_DEVICE: "Invalid Device",
        CUDA_ERROR_INVALID_CONTEXT: "Invalid Context",
        CUDA_ERROR_SYSTEM_DRIVER_MISMATCH: "System Driver Mismatch",
        CUDA_ERROR_COMPAT_NOT_SUPPORTED_ON_DEVICE: "Compatibility Not Supported on Device",
        CUDA_ERROR_UNINITIALIZED: "Uninitialized",
        CUDA_ERROR_FUNCTION_NOT_FOUND: "Function Not Found",
        CUDA_ERROR_LIBRARY_NOT_FOUND: "Library Not Found",
    }

    def __new__(cls, value):
        if cls == CUDAError:
            cls = CUDAError._valClassMapping.get(value, cls)
        obj = Exception.__new__(cls)
        obj.value = value
        return obj

    def __str__(self):
        try:
            if self.value not in CUDAError._errcode_to_string:
                CUDAError._errcode_to_string[self.value] = (
                    f"Unknown CUDA Error {self.value}"
                )
            return CUDAError._errcode_to_string[self.value]
        except CUDAError:
            return f"CUDA Error with code {self.value}"

    def __eq__(self, other):
        if isinstance(other, CUDAError):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return False


def cudaExceptionClass(cudaErrorCode):
    if cudaErrorCode not in CUDAError._valClassMapping:
        msg = f"CUDA error code {cudaErrorCode} is not valid"
        raise ValueError(msg)
    return CUDAError._valClassMapping[cudaErrorCode]


def _extractCUDAErrorAsClasses():
    """
    Generates a hierarchy of classes on top of CUDAError class.

    Each CUDA Error gets a new CUDAError subclass. This way try,except blocks can filter appropriate
    exceptions more easily.

    CUDAError is a parent class. Each CUDA_ERROR_* gets it's own subclass.
    e.g. CUDA_ERROR_INVALID_VALUE will be turned into CUDAError_InvalidValue.
    """
    this_module = sys.modules[__name__]
    cudaErrorsNames = [x for x in dir(this_module) if x.startswith("CUDA_ERROR_")]
    for err_name in cudaErrorsNames:
        # e.g. Turn CUDA_ERROR_INVALID_VALUE into CUDAError_InvalidValue
        class_name = "CUDAError_" + string.capwords(
            err_name.replace("CUDA_ERROR_", ""),
            "_",
        ).replace("_", "")
        err_val = getattr(this_module, err_name)

        def gen_new(val):
            def new(typ, *args):
                obj = CUDAError.__new__(typ, val)
                return obj

            return new

        new_error_class = type(class_name, (CUDAError,), {"__new__": gen_new(err_val)})
        new_error_class.__module__ = __name__
        setattr(this_module, class_name, new_error_class)
        CUDAError._valClassMapping[err_val] = new_error_class


_extractCUDAErrorAsClasses()


def _cudaCheckReturn(ret):
    if ret != CUDA_SUCCESS:
        raise CUDAError(ret)
    return ret


## Function access ##
_cudaGetFunctionPointer_cache = {}


def _cudaGetFunctionPointer(name):
    global cudaLib

    if name in _cudaGetFunctionPointer_cache:
        return _cudaGetFunctionPointer_cache[name]

    libLoadLock.acquire()
    try:
        if cudaLib is None:
            raise CUDAError(CUDA_ERROR_UNINITIALIZED)
        try:
            _cudaGetFunctionPointer_cache[name] = getattr(cudaLib, name)
            return _cudaGetFunctionPointer_cache[name]
        except AttributeError:
            raise CUDAError(CUDA_ERROR_FUNCTION_NOT_FOUND)
    finally:
        libLoadLock.release()


## Alternative object
# Allows the object to be printed
# Allows mismatched types to be assigned
#  - like None when the Structure variant requires c_uint
class cudaFriendlyObject:
    def __init__(self, dictionary):
        for x in dictionary:
            setattr(self, x, dictionary[x])

    def __str__(self):
        return self.__dict__.__str__()


def cudaStructToFriendlyObject(struct):
    d = {}
    for x in struct._fields_:
        key = x[0]
        value = getattr(struct, key)
        # only need to convert from bytes if bytes, no need to check python version.
        d[key] = value.decode() if isinstance(value, bytes) else value
    obj = cudaFriendlyObject(d)
    return obj


# pack the object so it can be passed to the CUDA library
def cudaFriendlyObjectToStruct(obj, model):
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


## Device structures
class struct_c_CUdevice_t(Structure):
    pass  # opaque handle


c_CUdevice_t = POINTER(struct_c_CUdevice_t)


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


def _LoadCudaLibrary():
    global cudaLib
    if cudaLib is None:
        libLoadLock.acquire()
        try:
            if cudaLib is None:
                if sys.platform.startswith("win"):
                    # Do not support Windows yet.
                    raise CUDAError(CUDA_ERROR_LIBRARY_NOT_FOUND)
                # Linux path
                locs = [
                    "libcuda.so",
                ]
                for loc in locs:
                    try:
                        cudaLib = CDLL(loc)
                        break
                    except OSError:
                        pass
                if cudaLib is None:
                    raise CUDAError(CUDA_ERROR_LIBRARY_NOT_FOUND)
        finally:
            libLoadLock.release()


## C function wrappers ##
def cuInit(flags=0):
    _LoadCudaLibrary()

    # Initialize the library
    fn = _cudaGetFunctionPointer("cuInit")
    ret = fn(flags)
    _cudaCheckReturn(ret)


def cuDeviceGet(ordinal=0):
    _LoadCudaLibrary()

    # Get handle for device with given ordinal
    fn = _cudaGetFunctionPointer("cuDeviceGet")
    device = c_CUdevice_t()
    ret = fn(byref(device), ordinal)
    _cudaCheckReturn(ret)
    return device


def cuDeviceGetAttribute(device, attrib=CU_DEVICE_ATTRIBUTE_MAX_THREADS_PER_BLOCK):
    _LoadCudaLibrary()

    # Get handle for device with given ordinal
    fn = _cudaGetFunctionPointer("cuDeviceGetAttribute")
    pi = c_int()
    ret = fn(byref(pi), attrib, device)
    _cudaCheckReturn(ret)
    return pi.value
