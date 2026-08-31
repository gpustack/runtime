##
# Python bindings for the Hygon DMI Multi-Instance library (libhydmi_mig.so).
#
# The vendor exports this API under NVML's symbol names while implementing
# something else, so every public wrapper here is dmi-prefixed and the library
# is loaded RTLD_LOCAL: loading it globally would let these names collide with
# libnvidia-ml.so's in a process that has both. Struct layouts follow the
# vendor's dmi_mig.h (v1.3.1), NOT pynvml, despite the shared names.
##
import string
import sys
import threading
from ctypes import *

## C Type mappings ##
_dmiReturn_t = c_int
DMI_SUCCESS = 0
DMI_ERROR_UNINITIALIZED = 1
DMI_ERROR_INVALID_ARGUMENT = 2
DMI_ERROR_NOT_SUPPORTED = 3
DMI_ERROR_NO_PERMISSION = 4
DMI_ERROR_ALREADY_INITIALIZED = 5
DMI_ERROR_NOT_FOUND = 6
DMI_ERROR_INSUFFICIENT_SIZE = 7
DMI_ERROR_INSUFFICIENT_POWER = 8
DMI_ERROR_DRIVER_NOT_LOADED = 9
DMI_ERROR_TIMEOUT = 10
DMI_ERROR_IRQ_ISSUE = 11
DMI_ERROR_LIBRARY_NOT_FOUND = 12
DMI_ERROR_FUNCTION_NOT_FOUND = 13
DMI_ERROR_CORRUPTED_INFOROM = 14
DMI_ERROR_GPU_IS_LOST = 15
DMI_ERROR_RESET_REQUIRED = 16
DMI_ERROR_OPERATING_SYSTEM = 17
DMI_ERROR_LIB_RM_VERSION_MISMATCH = 18
DMI_ERROR_IN_USE = 19
DMI_ERROR_MEMORY = 20
DMI_ERROR_NO_DATA = 21
DMI_ERROR_VGPU_ECC_NOT_SUPPORTED = 22
DMI_ERROR_INSUFFICIENT_RESOURCES = 23
DMI_ERROR_FREQ_NOT_SUPPORTED = 24
DMI_ERROR_ARGUMENT_VERSION_MISMATCH = 25
DMI_ERROR_DEPRECATED = 26
DMI_ERROR_NOT_READY = 27
DMI_ERROR_UNKNOWN = 999

DMI_DEVICE_PCI_BUS_ID_BUFFER_SIZE = 32
DMI_DEVICE_SERIAL_BUFFER_SIZE = 32
DMI_DEVICE_NAME_BUFFER_SIZE = 256
DMI_DEVICE_UUID_BUFFER_SIZE = 256

# Disable Multi Instance GPU mode.
DMI_DEVICE_MIG_DISABLE = 0x0
# Enable Multi Instance GPU mode.
DMI_DEVICE_MIG_ENABLE = 0x1

# GPU instance profiles.
# These are the values accepted by dmiDeviceGetGpuInstanceProfileInfo's
# `profile` argument: an index into a fixed slice-count enumeration, NOT a
# profile id -- 0 asks for the one-slice profile, 3 for the four-slice one.
DMI_GPU_INSTANCE_PROFILE_1_SLICE = 0x0
DMI_GPU_INSTANCE_PROFILE_2_SLICE = 0x1
DMI_GPU_INSTANCE_PROFILE_3_SLICE = 0x2
DMI_GPU_INSTANCE_PROFILE_4_SLICE = 0x3
DMI_GPU_INSTANCE_PROFILE_COUNT = 0x4

# Compute instance profiles, indexed the same way.
DMI_COMPUTE_INSTANCE_PROFILE_1_SLICE = 0x0
DMI_COMPUTE_INSTANCE_PROFILE_2_SLICE = 0x1
DMI_COMPUTE_INSTANCE_PROFILE_3_SLICE = 0x2
DMI_COMPUTE_INSTANCE_PROFILE_4_SLICE = 0x3
DMI_COMPUTE_INSTANCE_PROFILE_COUNT = 0x4

# Compute instance engine profiles.
DMI_COMPUTE_INSTANCE_ENGINE_PROFILE_SHARED = 0x0
DMI_COMPUTE_INSTANCE_ENGINE_PROFILE_COUNT = 0x1

## Opaque handles ##
# The vendor defines these as pointers to private structs; their only
# transferable property is identity, which is what attributes a MIG device to
# its card. Wrappers return c_void_p handles; struct fields of these types
# read back as plain ints, so compare a handle's `.value` against them.
_dmiDevice_t = c_void_p
_dmiGpuInstance_t = c_void_p
_dmiComputeInstance_t = c_void_p


## Structs ##
# Field layouts copied from dmi_mig.h v1.3.1. The vendor names fields in
# snake_case, which is kept.
class _dmiGpuInstanceProfileInfo_t(Structure):
    _fields_ = [
        ("id", c_uint),
        ("gi_count_max", c_uint),
        ("cu_count", c_uint),
        ("gpu_slice_count", c_uint),
        # Carries MiB despite the name.
        ("memory_size_MB", c_ulonglong),
        ("name", c_char * DMI_DEVICE_NAME_BUFFER_SIZE),
    ]


class _dmiComputeInstanceProfileInfo_t(Structure):
    _fields_ = [
        ("id", c_uint),
        ("ci_count_max", c_uint),
        ("cu_count", c_uint),
        ("gpu_slice_count", c_uint),
        ("name", c_char * DMI_DEVICE_NAME_BUFFER_SIZE),
    ]


class _dmiGpuInstancePlacement_t(Structure):
    _fields_ = [
        # Index of first occupied memory slice.
        ("start", c_uint),
        # Number of memory slices occupied.
        ("size", c_uint),
    ]


class _dmiComputeInstancePlacement_t(Structure):
    _fields_ = [
        # Index of first occupied compute slice.
        ("start", c_uint),
        # Number of compute slices occupied.
        ("size", c_uint),
    ]


class _dmiGpuInstanceInfo_t(Structure):
    _fields_ = [
        # Parent device handle.
        ("device", _dmiDevice_t),
        # Unique instance ID within the device.
        ("id", c_uint),
        # Unique profile ID within the device.
        ("profile_id", c_uint),
        ("placement", _dmiGpuInstancePlacement_t),
    ]


class _dmiComputeInstanceInfo_t(Structure):
    _fields_ = [
        # Parent device handle.
        ("device", _dmiDevice_t),
        # Parent GPU instance handle.
        ("gpu_instance", _dmiGpuInstance_t),
        # Unique instance ID within the GPU instance.
        ("id", c_uint),
        # Unique profile ID within the GPU instance.
        ("profile_id", c_uint),
        # Placement within the GPU instance's compute slice range.
        ("placement", _dmiComputeInstancePlacement_t),
    ]


class _dmiMemory_t(Structure):
    _fields_ = [
        ("total", c_ulonglong),
        ("free", c_ulonglong),
        ("used", c_ulonglong),
    ]


class _dmiUtilization_t(Structure):
    _fields_ = [
        # Compute core usage percent.
        ("gpu", c_uint),
        # Memory usage percent.
        ("memory", c_uint),
    ]


## Error Checking ##
class DMIError(Exception):
    _valClassMapping = {}
    # List of currently known error codes.
    # The library exports no error-string function, so the map is static.
    _errcode_to_string = {
        DMI_ERROR_UNINITIALIZED: "Uninitialized",
        DMI_ERROR_INVALID_ARGUMENT: "Invalid Argument",
        DMI_ERROR_NOT_SUPPORTED: "Not Supported",
        DMI_ERROR_NO_PERMISSION: "Insufficient Permissions",
        DMI_ERROR_ALREADY_INITIALIZED: "Already Initialized",
        DMI_ERROR_NOT_FOUND: "Not Found",
        DMI_ERROR_INSUFFICIENT_SIZE: "Insufficient Size",
        DMI_ERROR_INSUFFICIENT_POWER: "Insufficient External Power",
        DMI_ERROR_DRIVER_NOT_LOADED: "Driver Not Loaded",
        DMI_ERROR_TIMEOUT: "Timeout",
        DMI_ERROR_IRQ_ISSUE: "Interrupt Request Issue",
        DMI_ERROR_LIBRARY_NOT_FOUND: "DMI Shared Library Not Found",
        DMI_ERROR_FUNCTION_NOT_FOUND: "Function Not Found",
        DMI_ERROR_CORRUPTED_INFOROM: "Corrupted infoROM",
        DMI_ERROR_GPU_IS_LOST: "GPU is lost",
        DMI_ERROR_RESET_REQUIRED: "GPU requires restart",
        DMI_ERROR_OPERATING_SYSTEM: "The operating system has blocked the request.",
        DMI_ERROR_LIB_RM_VERSION_MISMATCH: "Driver/library version mismatch.",
        DMI_ERROR_IN_USE: "In Use",
        DMI_ERROR_MEMORY: "Insufficient Memory",
        DMI_ERROR_NO_DATA: "No Data",
        DMI_ERROR_VGPU_ECC_NOT_SUPPORTED: "VGPU ECC Not Supported",
        DMI_ERROR_INSUFFICIENT_RESOURCES: "Insufficient Resources",
        DMI_ERROR_FREQ_NOT_SUPPORTED: "Frequency Not Supported",
        DMI_ERROR_ARGUMENT_VERSION_MISMATCH: "Argument Version Mismatch",
        DMI_ERROR_DEPRECATED: "Deprecated",
        DMI_ERROR_NOT_READY: "Not Ready",
        DMI_ERROR_UNKNOWN: "Unknown Error",
    }

    def __new__(typ, value):
        """
        Maps value to a proper subclass of DMIError.
        See _extractDMIErrorsAsClasses function for more details.
        """
        if typ == DMIError:
            typ = DMIError._valClassMapping.get(value, typ)
        obj = Exception.__new__(typ)
        obj.value = value
        return obj

    def __str__(self):
        return DMIError._errcode_to_string.get(
            self.value,
            "DMI Error with code %d" % self.value,
        )

    def __eq__(self, other):
        return self.value == other.value


def dmiExceptionClass(dmiErrorCode):
    if dmiErrorCode not in DMIError._valClassMapping:
        msg = f"dmiErrorCode {dmiErrorCode} is not valid"
        raise ValueError(msg)
    return DMIError._valClassMapping[dmiErrorCode]


def _extractDMIErrorsAsClasses():
    """
    Generates a hierarchy of classes on top of DMIError class.

    Each DMI Error gets a new DMIError subclass. This way try,except blocks can
    filter appropriate exceptions more easily.
    """
    this_module = sys.modules[__name__]
    dmiErrorsNames = [x for x in dir(this_module) if x.startswith("DMI_ERROR_")]
    for err_name in dmiErrorsNames:
        # e.g. Turn DMI_ERROR_ALREADY_INITIALIZED into DMIError_AlreadyInitialized
        class_name = "DMIError_" + string.capwords(
            err_name.replace("DMI_ERROR_", ""), "_"
        ).replace("_", "")
        err_val = getattr(this_module, err_name)

        def gen_new(val):
            def new(typ, *args):
                obj = DMIError.__new__(typ, val)
                return obj

            return new

        new_error_class = type(class_name, (DMIError,), {"__new__": gen_new(err_val)})
        new_error_class.__module__ = __name__
        setattr(this_module, class_name, new_error_class)
        DMIError._valClassMapping[err_val] = new_error_class


_extractDMIErrorsAsClasses()


def _dmiCheckReturn(ret):
    if ret != DMI_SUCCESS:
        raise DMIError(ret)
    return ret


## Function access ##
libLoadLock = threading.Lock()
_dmiGetFunctionPointer_cache = {}  # function pointers are cached to prevent unnecessary libLoadLock locking
dmiLib = None


def _dmiGetFunctionPointer(name):
    global dmiLib

    if name in _dmiGetFunctionPointer_cache:
        return _dmiGetFunctionPointer_cache[name]

    libLoadLock.acquire()
    try:
        # ensure library was loaded
        if dmiLib is None:
            raise DMIError(DMI_ERROR_UNINITIALIZED)
        try:
            _dmiGetFunctionPointer_cache[name] = getattr(dmiLib, name)
            return _dmiGetFunctionPointer_cache[name]
        except AttributeError:
            raise DMIError(DMI_ERROR_FUNCTION_NOT_FOUND)
    finally:
        # lock is always freed
        libLoadLock.release()


def _LoadDmiLibrary():
    """
    Load the library if it isn't loaded already.

    The library ships in the hyhal tree, which is not on the dynamic linker's
    search path, so the absolute locations are tried as well. The versioned
    soname comes first because a host can carry it without the bare one.
    """
    global dmiLib

    if dmiLib is None:
        # lock to ensure only one caller loads the library
        libLoadLock.acquire()
        try:
            # ensure the library still isn't loaded
            if dmiLib is None:
                if not sys.platform.startswith("linux"):
                    # Do not support other platforms yet.
                    raise DMIError(DMI_ERROR_LIBRARY_NOT_FOUND)
                locs = [
                    "libhydmi_mig.so.1",
                    "libhydmi_mig.so",
                ]
                for loc_dir in ["/opt/hyhal/lib", "/opt/dtk/lib"]:
                    locs.append(loc_dir + "/libhydmi_mig.so.1")
                    locs.append(loc_dir + "/libhydmi_mig.so")
                for loc in locs:
                    try:
                        # RTLD_LOCAL, never GLOBAL: the vendor exports NVML's
                        # symbol names, and global linkage would collide with
                        # libnvidia-ml.so in a process that has both.
                        dmiLib = CDLL(loc, mode=RTLD_LOCAL)
                        break
                    except OSError:
                        pass
                if dmiLib is None:
                    raise DMIError(DMI_ERROR_LIBRARY_NOT_FOUND)
        finally:
            # lock is always freed
            libLoadLock.release()


def dmiInit():
    """
    Load and resolve the vendor library.

    The vendor API has no initialize/shutdown calls -- despite the NVML symbol
    names, there is no nvmlInit to forward to -- so "init" here means loading
    the library, after which every wrapper works.
    """
    _LoadDmiLibrary()


## System functions ##
def dmiGetSystemMigMode():
    """
    Report the node's Multi-Instance mode, current and pending.

    The mode is a property of the NODE, not of a card: this call takes no
    device, and every card of a host answers alike.
    """
    fn = _dmiGetFunctionPointer("nvmlGetSystemMigMode")
    c_current = c_uint()
    c_pending = c_uint()
    ret = fn(byref(c_current), byref(c_pending))
    _dmiCheckReturn(ret)
    return c_current.value, c_pending.value


## Device functions ##
def dmiDeviceGetCount():
    fn = _dmiGetFunctionPointer("nvmlDeviceGetCount")
    c_count = c_uint()
    ret = fn(byref(c_count))
    _dmiCheckReturn(ret)
    return c_count.value


def dmiDeviceGetHandleByIndex(index):
    fn = _dmiGetFunctionPointer("nvmlDeviceGetHandleByIndex")
    c_device = _dmiDevice_t()
    ret = fn(c_uint(index), byref(c_device))
    _dmiCheckReturn(ret)
    return c_device


def dmiDeviceGetHandleByPciBusId(pci_bus_id):
    """
    Return a handle for the physical DCU at a PCI address.

    This is the bridge from an identity another library (RSMI) enumerates by.
    The address must be domain-qualified, "0000:09:00.0" rather than "09:00.0";
    an address no card answers for returns DMI_ERROR_NOT_FOUND.
    """
    fn = _dmiGetFunctionPointer("nvmlDeviceGetHandleByPciBusId")
    c_device = _dmiDevice_t()
    ret = fn(c_char_p(pci_bus_id.encode()), byref(c_device))
    _dmiCheckReturn(ret)
    return c_device


def dmiDeviceGetIndex(device):
    fn = _dmiGetFunctionPointer("nvmlDeviceGetIndex")
    c_index = c_uint()
    ret = fn(device, byref(c_index))
    _dmiCheckReturn(ret)
    return c_index.value


def dmiDeviceGetMigMode(device):
    """
    Report the device's current and pending Multi-Instance mode.

    The mode is set for the whole node, so every card of a host answers alike;
    see dmiGetSystemMigMode.
    """
    fn = _dmiGetFunctionPointer("nvmlDeviceGetMigMode")
    c_current = c_uint()
    c_pending = c_uint()
    ret = fn(device, byref(c_current), byref(c_pending))
    _dmiCheckReturn(ret)
    return c_current.value, c_pending.value


def dmiDeviceGetMemoryInfo(device):
    """
    Report total, free and used memory in bytes.

    Asked of a MIG device handle it reports that instance's own memory rather
    than its card's, which is what makes it usable as a per-instance figure.
    """
    fn = _dmiGetFunctionPointer("nvmlDeviceGetMemoryInfo")
    c_memory = _dmiMemory_t()
    ret = fn(device, byref(c_memory))
    _dmiCheckReturn(ret)
    return c_memory


def dmiDeviceGetUtilizationRates(device):
    """
    Report compute and memory utilization as percentages.

    The vendor header does not declare this entry point, though the shared
    object exports it. Asked of a MIG device handle it reports that instance's
    own utilization; nothing else on this API measures per-instance compute.
    """
    fn = _dmiGetFunctionPointer("nvmlDeviceGetUtilizationRates")
    c_utilization = _dmiUtilization_t()
    ret = fn(device, byref(c_utilization))
    _dmiCheckReturn(ret)
    return c_utilization


## MIG functions ##
_DMI_MAX_INSTANCES_PER_QUERY = 32
"""
Bounds the buffers handed to the array-filling queries. A card carries four
GPU slices, so neither its GPU instances of one profile nor the compute
instances inside one of them can exceed that; the headroom is there so a
future card with a finer split does not silently truncate.
"""


def dmiDeviceGetGpuInstanceProfileInfo(device, profile):
    """
    Return the GPU-instance profile at `profile`, an index into the fixed
    slice-count enumeration -- 0 asks for the one-slice profile, 3 for the
    four-slice one -- NOT a profile id, which comes back inside the answer and
    bears no relation to the index.

    A card that offers no profile at this index answers DMI_ERROR_NOT_SUPPORTED,
    DMI_ERROR_NOT_FOUND or DMI_ERROR_INVALID_ARGUMENT; that is routine, not a
    fault.
    """
    fn = _dmiGetFunctionPointer("nvmlDeviceGetGpuInstanceProfileInfo")
    c_info = _dmiGpuInstanceProfileInfo_t()
    ret = fn(device, c_uint(profile), byref(c_info))
    _dmiCheckReturn(ret)
    return c_info


def dmiDeviceGetGpuInstancePossiblePlacements(device, profile_id):
    """
    Return every placement the profile may legally occupy on an empty card,
    as _dmiGpuInstancePlacement_t entries.
    """
    fn = _dmiGetFunctionPointer("nvmlDeviceGetGpuInstancePossiblePlacements")
    c_placements = (_dmiGpuInstancePlacement_t * _DMI_MAX_INSTANCES_PER_QUERY)()
    c_count = c_uint(_DMI_MAX_INSTANCES_PER_QUERY)
    ret = fn(device, c_uint(profile_id), c_placements, byref(c_count))
    _dmiCheckReturn(ret)
    if c_count.value > _DMI_MAX_INSTANCES_PER_QUERY:
        raise DMIError(DMI_ERROR_INSUFFICIENT_SIZE)
    return [c_placements[i] for i in range(c_count.value)]


def dmiDeviceGetGpuInstanceRemainingCapacity(device, profile_id):
    """
    Report how many more instances of the profile the card can still hold,
    accounting for what other profiles already occupy.
    """
    fn = _dmiGetFunctionPointer("nvmlDeviceGetGpuInstanceRemainingCapacity")
    c_count = c_uint()
    ret = fn(device, c_uint(profile_id), byref(c_count))
    _dmiCheckReturn(ret)
    return c_count.value


def dmiDeviceGetGpuInstances(device, profile_id):
    """
    Return the GPU instances of ONE profile that currently exist on the card.

    The query filters by profile id, so enumerating every instance on a card
    means asking once per profile the card offers.
    """
    fn = _dmiGetFunctionPointer("nvmlDeviceGetGpuInstances")
    c_instances = (_dmiGpuInstance_t * _DMI_MAX_INSTANCES_PER_QUERY)()
    c_count = c_uint(_DMI_MAX_INSTANCES_PER_QUERY)
    ret = fn(device, c_uint(profile_id), c_instances, byref(c_count))
    _dmiCheckReturn(ret)
    if c_count.value > _DMI_MAX_INSTANCES_PER_QUERY:
        raise DMIError(DMI_ERROR_INSUFFICIENT_SIZE)
    # Array elements read back as plain ints; re-wrap so a handle passed into a
    # later call keeps its full pointer width instead of ctypes' default c_int
    # conversion truncating it.
    return [_dmiGpuInstance_t(c_instances[i]) for i in range(c_count.value)]


def dmiDeviceGetGpuInstanceById(device, gpu_instance_id):
    fn = _dmiGetFunctionPointer("nvmlDeviceGetGpuInstanceById")
    c_gpu_instance = _dmiGpuInstance_t()
    ret = fn(device, c_uint(gpu_instance_id), byref(c_gpu_instance))
    _dmiCheckReturn(ret)
    return c_gpu_instance


def dmiGpuInstanceGetInfo(gpu_instance):
    """
    Report the instance's id, its profile id, its parent device and where it
    sits on the card.
    """
    fn = _dmiGetFunctionPointer("nvmlGpuInstanceGetInfo")
    c_info = _dmiGpuInstanceInfo_t()
    ret = fn(gpu_instance, byref(c_info))
    _dmiCheckReturn(ret)
    return c_info


def dmiGpuInstanceGetComputeInstanceProfileInfo(gpu_instance, profile, eng_profile):
    """
    Return the compute-instance profile at `profile`, the same slice-count
    indexing as its GPU-instance counterpart. `eng_profile` selects the engine
    profile, of which the vendor defines exactly one, SHARED.
    """
    fn = _dmiGetFunctionPointer("nvmlGpuInstanceGetComputeInstanceProfileInfo")
    c_info = _dmiComputeInstanceProfileInfo_t()
    ret = fn(gpu_instance, c_uint(profile), c_uint(eng_profile), byref(c_info))
    _dmiCheckReturn(ret)
    return c_info


def dmiGpuInstanceGetComputeInstances(gpu_instance, profile_id):
    """
    Return the compute instances of ONE profile inside this GPU instance.
    Like its GPU-instance counterpart it filters by profile id and must be
    asked once per profile.
    """
    fn = _dmiGetFunctionPointer("nvmlGpuInstanceGetComputeInstances")
    c_instances = (_dmiComputeInstance_t * _DMI_MAX_INSTANCES_PER_QUERY)()
    c_count = c_uint(_DMI_MAX_INSTANCES_PER_QUERY)
    ret = fn(gpu_instance, c_uint(profile_id), c_instances, byref(c_count))
    _dmiCheckReturn(ret)
    if c_count.value > _DMI_MAX_INSTANCES_PER_QUERY:
        raise DMIError(DMI_ERROR_INSUFFICIENT_SIZE)
    # Array elements read back as plain ints; re-wrap so a handle passed into a
    # later call keeps its full pointer width instead of ctypes' default c_int
    # conversion truncating it.
    return [_dmiComputeInstance_t(c_instances[i]) for i in range(c_count.value)]


def dmiGpuInstanceGetComputeInstanceById(gpu_instance, compute_instance_id):
    fn = _dmiGetFunctionPointer("nvmlGpuInstanceGetComputeInstanceById")
    c_compute_instance = _dmiComputeInstance_t()
    ret = fn(gpu_instance, c_uint(compute_instance_id), byref(c_compute_instance))
    _dmiCheckReturn(ret)
    return c_compute_instance


def dmiComputeInstanceGetInfo(compute_instance):
    """
    Report the compute instance's id, its profile id, its parent device and
    GPU instance, and its placement within the GPU instance's slice range.
    """
    fn = _dmiGetFunctionPointer("nvmlComputeInstanceGetInfo")
    c_info = _dmiComputeInstanceInfo_t()
    ret = fn(compute_instance, byref(c_info))
    _dmiCheckReturn(ret)
    return c_info


def dmiDeviceIsMigDeviceHandle(device):
    """
    Report whether this handle names a MIG instance rather than a physical card.
    """
    fn = _dmiGetFunctionPointer("nvmlDeviceIsMigDeviceHandle")
    c_is_mig = c_uint()
    ret = fn(device, byref(c_is_mig))
    _dmiCheckReturn(ret)
    return c_is_mig.value != 0


def dmiDeviceGetGpuInstanceId(device):
    """
    Report which GPU instance a MIG device handle belongs to.
    """
    fn = _dmiGetFunctionPointer("nvmlDeviceGetGpuInstanceId")
    c_id = c_uint()
    ret = fn(device, byref(c_id))
    _dmiCheckReturn(ret)
    return c_id.value


def dmiDeviceGetComputeInstanceId(device):
    """
    Report which compute instance a MIG device handle is.
    """
    fn = _dmiGetFunctionPointer("nvmlDeviceGetComputeInstanceId")
    c_id = c_uint()
    ret = fn(device, byref(c_id))
    _dmiCheckReturn(ret)
    return c_id.value


def dmiDeviceGetMaxMigDeviceCount(device):
    """
    Report how many MIG devices this card can hold at once.
    """
    fn = _dmiGetFunctionPointer("nvmlDeviceGetMaxMigDeviceCount")
    c_count = c_uint()
    ret = fn(device, byref(c_count))
    _dmiCheckReturn(ret)
    return c_count.value


def dmiDeviceGetMigDeviceHandleByIndex(device, index):
    """
    Return the MIG device at a GLOBAL index, if it belongs to this card.

    The index is not per-card, despite the call taking a card: it numbers
    every MIG device on the NODE, and an index belonging to another card
    answers DMI_ERROR_NOT_FOUND. A caller must bound and filter the sweep
    itself, attributing each handle to its card by the GI info's parent
    device, never by index ranges.
    """
    fn = _dmiGetFunctionPointer("nvmlDeviceGetMigDeviceHandleByIndex")
    c_mig_device = _dmiDevice_t()
    ret = fn(device, c_uint(index), byref(c_mig_device))
    _dmiCheckReturn(ret)
    return c_mig_device
