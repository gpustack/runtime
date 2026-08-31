import ctypes
import sys
from ctypes import RTLD_LOCAL

import pytest

from gpustack_runtime.detector import pydmi


@pytest.fixture(autouse=True)
def _reset_pydmi(monkeypatch):
    """
    Reset pydmi's module-level library state around every test,
    so each test controls its own fake CDLL.
    """
    monkeypatch.setattr(pydmi, "dmiLib", None)
    pydmi._dmiGetFunctionPointer_cache.clear()
    yield
    pydmi._dmiGetFunctionPointer_cache.clear()


class _FakeCDLL:
    """
    A CDLL stand-in recording every attempted load (path and mode),
    succeeding only for the paths in `accepted` with the given library object.
    """

    def __init__(self, lib, accepted: set[str]):
        self.lib = lib
        self.accepted = accepted
        self.calls: list[tuple[str, int | None]] = []

    def __call__(self, path, mode=None):
        self.calls.append((path, mode))
        if path not in self.accepted:
            msg = f"cannot open {path}"
            raise OSError(msg)
        return self.lib


def test_load_searches_default_and_vendor_paths_with_rtld_local(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    lib = object()
    fake = _FakeCDLL(lib, {"/opt/dtk/lib/libhydmi_mig.so"})
    monkeypatch.setattr(pydmi, "CDLL", fake)

    pydmi.dmiInit()

    tried = [path for path, _ in fake.calls]
    assert tried == [
        "libhydmi_mig.so.1",
        "libhydmi_mig.so",
        "/opt/hyhal/lib/libhydmi_mig.so.1",
        "/opt/hyhal/lib/libhydmi_mig.so",
        "/opt/dtk/lib/libhydmi_mig.so.1",
        "/opt/dtk/lib/libhydmi_mig.so",
    ]
    # The vendor exports NVML's symbol names, so the library must never be
    # loaded globally: every attempt carries RTLD_LOCAL.
    assert all(mode == RTLD_LOCAL for _, mode in fake.calls)
    assert pydmi.dmiLib is lib


def test_load_failure_raises_dmi_error_never_oserror(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(pydmi, "CDLL", _FakeCDLL(object(), set()))

    with pytest.raises(pydmi.DMIError_LibraryNotFound) as excinfo:
        pydmi.dmiInit()

    assert excinfo.value.value == pydmi.DMI_ERROR_LIBRARY_NOT_FOUND
    assert not isinstance(excinfo.value, OSError)


def test_error_codes_map_to_subclasses():
    assert isinstance(
        pydmi.DMIError(pydmi.DMI_ERROR_NOT_FOUND),
        pydmi.DMIError_NotFound,
    )
    assert isinstance(
        pydmi.DMIError(pydmi.DMI_ERROR_NOT_SUPPORTED),
        pydmi.DMIError_NotSupported,
    )
    assert isinstance(
        pydmi.DMIError(pydmi.DMI_ERROR_INVALID_ARGUMENT),
        pydmi.DMIError_InvalidArgument,
    )
    # An unknown code stays the base class and still stringifies.
    assert type(pydmi.DMIError(12345)) is pydmi.DMIError
    assert "12345" in str(pydmi.DMIError(12345))


def test_call_without_init_raises_uninitialized():
    with pytest.raises(pydmi.DMIError_Uninitialized):
        pydmi.dmiGetSystemMigMode()


def test_missing_symbol_raises_function_not_found(monkeypatch):
    monkeypatch.setattr(pydmi, "dmiLib", object())

    with pytest.raises(pydmi.DMIError_FunctionNotFound):
        pydmi.dmiGetSystemMigMode()


def test_wrapper_raises_the_mapped_error_on_non_success(monkeypatch):
    class Lib:
        def nvmlGetSystemMigMode(self, current, pending):
            return pydmi.DMI_ERROR_NOT_SUPPORTED

    monkeypatch.setattr(pydmi, "dmiLib", Lib())

    with pytest.raises(pydmi.DMIError_NotSupported):
        pydmi.dmiGetSystemMigMode()


def test_system_mig_mode_returns_current_and_pending(monkeypatch):
    class Lib:
        def nvmlGetSystemMigMode(self, current, pending):
            ctypes.cast(current, ctypes.POINTER(ctypes.c_uint)).contents.value = 1
            ctypes.cast(pending, ctypes.POINTER(ctypes.c_uint)).contents.value = 0
            return pydmi.DMI_SUCCESS

    monkeypatch.setattr(pydmi, "dmiLib", Lib())

    assert pydmi.dmiGetSystemMigMode() == (1, 0)


def test_handle_by_pci_bus_id_encodes_and_returns_handle(monkeypatch):
    seen = {}

    class Lib:
        def nvmlDeviceGetHandleByPciBusId(self, bus_id, device):
            seen["bus_id"] = bus_id.value
            ctypes.cast(device, ctypes.POINTER(ctypes.c_void_p)).contents.value = 0xDEAD
            return pydmi.DMI_SUCCESS

    monkeypatch.setattr(pydmi, "dmiLib", Lib())

    handle = pydmi.dmiDeviceGetHandleByPciBusId("0000:09:00.0")

    assert seen["bus_id"] == b"0000:09:00.0"
    assert handle.value == 0xDEAD


def test_gpu_instance_profile_info_marshals_the_vendor_struct(monkeypatch):
    class Lib:
        def nvmlDeviceGetGpuInstanceProfileInfo(self, device, profile, info):
            assert profile.value == 2  # raw slice-count-minus-one argument
            out = ctypes.cast(
                info,
                ctypes.POINTER(pydmi._dmiGpuInstanceProfileInfo_t),
            ).contents
            out.id = 7
            out.gi_count_max = 2
            out.cu_count = 30
            out.gpu_slice_count = 3
            out.memory_size_MB = 49152
            out.name = b"MIG 3g.48gb"
            return pydmi.DMI_SUCCESS

    monkeypatch.setattr(pydmi, "dmiLib", Lib())

    info = pydmi.dmiDeviceGetGpuInstanceProfileInfo(ctypes.c_void_p(1), 2)

    assert info.id == 7
    assert info.gi_count_max == 2
    assert info.cu_count == 30
    assert info.gpu_slice_count == 3
    assert info.memory_size_MB == 49152
    assert info.name == b"MIG 3g.48gb"


def test_gpu_instances_returns_only_the_reported_count(monkeypatch):
    # Handle values beyond 32 bits: array elements must survive the round trip
    # into later calls at full pointer width.
    hi, lo = 0x1_0000_0111, 0x1_0000_0222
    seen = {}

    class Lib:
        def nvmlDeviceGetGpuInstances(self, device, profile_id, instances, count):
            out = ctypes.cast(instances, ctypes.POINTER(ctypes.c_void_p))
            out[0] = hi
            out[1] = lo
            ctypes.cast(count, ctypes.POINTER(ctypes.c_uint)).contents.value = 2
            return pydmi.DMI_SUCCESS

        def nvmlGpuInstanceGetInfo(self, gpu_instance, info):
            seen["handle"] = gpu_instance
            return pydmi.DMI_SUCCESS

    monkeypatch.setattr(pydmi, "dmiLib", Lib())

    handles = pydmi.dmiDeviceGetGpuInstances(ctypes.c_void_p(1), 3)

    assert [h.value for h in handles] == [hi, lo]
    # The round trip: an enumerated handle passed back in keeps its full width.
    pydmi.dmiGpuInstanceGetInfo(handles[0])
    got = seen["handle"]
    assert (got.value if isinstance(got, ctypes.c_void_p) else got) == hi


def test_gpu_instances_overflow_reports_insufficient_size(monkeypatch):
    class Lib:
        def nvmlDeviceGetGpuInstances(self, device, profile_id, instances, count):
            ctypes.cast(count, ctypes.POINTER(ctypes.c_uint)).contents.value = (
                pydmi._DMI_MAX_INSTANCES_PER_QUERY + 1
            )
            return pydmi.DMI_SUCCESS

    monkeypatch.setattr(pydmi, "dmiLib", Lib())

    with pytest.raises(pydmi.DMIError_InsufficientSize):
        pydmi.dmiDeviceGetGpuInstances(ctypes.c_void_p(1), 3)


def test_gpu_instance_info_carries_parent_device_and_placement(monkeypatch):
    class Lib:
        def nvmlGpuInstanceGetInfo(self, gpu_instance, info):
            out = ctypes.cast(
                info,
                ctypes.POINTER(pydmi._dmiGpuInstanceInfo_t),
            ).contents
            out.device = 0x999
            out.id = 4
            out.profile_id = 1
            out.placement.start = 2
            out.placement.size = 2
            return pydmi.DMI_SUCCESS

    monkeypatch.setattr(pydmi, "dmiLib", Lib())

    info = pydmi.dmiGpuInstanceGetInfo(ctypes.c_void_p(0xABC))

    # The parent device is how a MIG device is attributed to its card,
    # so it must arrive as a comparable value.
    assert info.device == 0x999
    assert info.id == 4
    assert info.profile_id == 1
    assert (info.placement.start, info.placement.size) == (2, 2)


def test_compute_instance_info_marshals_the_vendor_struct(monkeypatch):
    class Lib:
        def nvmlComputeInstanceGetInfo(self, compute_instance, info):
            out = ctypes.cast(
                info,
                ctypes.POINTER(pydmi._dmiComputeInstanceInfo_t),
            ).contents
            out.device = 0x999
            out.gpu_instance = 0xABC
            out.id = 0
            out.profile_id = 0
            out.placement.start = 0
            out.placement.size = 1
            return pydmi.DMI_SUCCESS

    monkeypatch.setattr(pydmi, "dmiLib", Lib())

    info = pydmi.dmiComputeInstanceGetInfo(ctypes.c_void_p(0xDEF))

    assert info.device == 0x999
    assert info.gpu_instance == 0xABC
    assert info.id == 0
    assert (info.placement.start, info.placement.size) == (0, 1)


def test_mig_device_queries(monkeypatch):
    class Lib:
        def nvmlDeviceGetMaxMigDeviceCount(self, device, count):
            ctypes.cast(count, ctypes.POINTER(ctypes.c_uint)).contents.value = 4
            return pydmi.DMI_SUCCESS

        def nvmlDeviceGetMigDeviceHandleByIndex(self, device, index, mig_device):
            if index.value == 3:
                return pydmi.DMI_ERROR_NOT_FOUND
            ctypes.cast(
                mig_device,
                ctypes.POINTER(ctypes.c_void_p),
            ).contents.value = 0x1000 + index.value
            return pydmi.DMI_SUCCESS

        def nvmlDeviceIsMigDeviceHandle(self, device, is_mig):
            ctypes.cast(is_mig, ctypes.POINTER(ctypes.c_uint)).contents.value = 1
            return pydmi.DMI_SUCCESS

    monkeypatch.setattr(pydmi, "dmiLib", Lib())

    dev = ctypes.c_void_p(0x999)
    assert pydmi.dmiDeviceGetMaxMigDeviceCount(dev) == 4
    assert pydmi.dmiDeviceGetMigDeviceHandleByIndex(dev, 0).value == 0x1000
    with pytest.raises(pydmi.DMIError_NotFound):
        pydmi.dmiDeviceGetMigDeviceHandleByIndex(dev, 3)
    assert pydmi.dmiDeviceIsMigDeviceHandle(dev) is True


def test_struct_layouts_match_the_vendor_header():
    """
    Independent layout assertions (sizes/offsets computed by hand from
    dmi_mig.h v1.3.1, LP64), so a wrong field width or order cannot hide
    behind fakes that cast with the very declarations under test.
    """
    gi_prf = pydmi._dmiGpuInstanceProfileInfo_t
    assert ctypes.sizeof(gi_prf) == 280
    assert gi_prf.id.offset == 0
    assert gi_prf.memory_size_MB.offset == 16
    assert gi_prf.name.offset == 24

    ci_prf = pydmi._dmiComputeInstanceProfileInfo_t
    assert ctypes.sizeof(ci_prf) == 272
    assert ci_prf.name.offset == 16

    assert ctypes.sizeof(pydmi._dmiGpuInstancePlacement_t) == 8
    assert ctypes.sizeof(pydmi._dmiComputeInstancePlacement_t) == 8

    gi_info = pydmi._dmiGpuInstanceInfo_t
    assert ctypes.sizeof(gi_info) == 24
    assert gi_info.device.offset == 0
    assert gi_info.id.offset == 8
    assert gi_info.profile_id.offset == 12
    assert gi_info.placement.offset == 16

    ci_info = pydmi._dmiComputeInstanceInfo_t
    assert ctypes.sizeof(ci_info) == 32
    assert ci_info.device.offset == 0
    assert ci_info.gpu_instance.offset == 8
    assert ci_info.id.offset == 16
    assert ci_info.profile_id.offset == 20
    assert ci_info.placement.offset == 24

    assert ctypes.sizeof(pydmi._dmiMemory_t) == 24
    assert ctypes.sizeof(pydmi._dmiUtilization_t) == 8


def test_memory_and_utilization_read_through_a_mig_handle(monkeypatch):
    class Lib:
        def nvmlDeviceGetMemoryInfo(self, device, memory):
            out = ctypes.cast(
                memory,
                ctypes.POINTER(pydmi._dmiMemory_t),
            ).contents
            out.total = 16 * 1024**3
            out.free = 12 * 1024**3
            out.used = 4 * 1024**3
            return pydmi.DMI_SUCCESS

        def nvmlDeviceGetUtilizationRates(self, device, utilization):
            out = ctypes.cast(
                utilization,
                ctypes.POINTER(pydmi._dmiUtilization_t),
            ).contents
            out.gpu = 95
            out.memory = 12
            return pydmi.DMI_SUCCESS

    monkeypatch.setattr(pydmi, "dmiLib", Lib())

    mig = ctypes.c_void_p(0x1000)
    mem = pydmi.dmiDeviceGetMemoryInfo(mig)
    util = pydmi.dmiDeviceGetUtilizationRates(mig)

    assert (mem.total, mem.free, mem.used) == (
        16 * 1024**3,
        12 * 1024**3,
        4 * 1024**3,
    )
    assert (util.gpu, util.memory) == (95, 12)
