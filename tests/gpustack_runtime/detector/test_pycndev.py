from __future__ import annotations as __future_annotations__

import importlib.util
import sys
import threading
from ctypes import sizeof
from unittest import mock

import pytest

from gpustack_runtime.detector import pycndev

MODULE_NAME = "gpustack_runtime.detector.pycndev"

# Expected layout of every bound struct: total size, then (field, offset, size) in the
# header's own field order.
#
# Derived from the operator's binding/cndev/cndev.h -- the header of record -- by
# compiling a probe that prints sizeof() and offsetof() for each field, so the table is
# independent of the ctypes definitions it checks. That independence is the point: no
# Cambricon hardware exists in this suite, the driver fills these buffers by offset, and
# a wrong width or a missing field would corrupt memory rather than fail a test.
#
# Every struct here is plain fixed-width scalars and arrays -- no bitfields, no packing,
# no pointers -- so these numbers hold on Linux x86_64 and arm64 alike. They are not
# specific to the host that ran the probe.
EXPECTED_LAYOUTS = {
    "c_cndevCardInfo_t": (
        8,
        [("version", 0, 4), ("number", 4, 4)],
    ),
    "c_cndevUUID_t": (
        # uuid is 37 bytes at offset 4, so 7 bytes of padding precede the 8-aligned
        # ncsUUID64 at 48.
        56,
        [("version", 0, 4), ("uuid", 4, 37), ("ncsUUID64", 48, 8)],
    ),
    "c_cndevMemoryInfoV2_t": (
        176,
        [
            ("physicalMemoryTotal", 0, 8),
            ("physicalMemoryUsed", 8, 8),
            ("reservedMemory", 16, 8),
            ("virtualMemoryTotal", 24, 8),
            ("virtualMemoryUsed", 32, 8),
            ("globalMemory", 40, 8),
            ("reserved", 48, 128),
        ],
    ),
    "c_cndevVersionInfo_t": (
        28,
        [
            ("version", 0, 4),
            ("mcuMajorVersion", 4, 4),
            ("mcuMinorVersion", 8, 4),
            ("mcuBuildVersion", 12, 4),
            ("driverMajorVersion", 16, 4),
            ("driverMinorVersion", 20, 4),
            ("driverBuildVersion", 24, 4),
        ],
    ),
    "c_cndevECCInfo_t": (
        # 4 bytes of padding follow the version, so the first counter is 8-aligned.
        72,
        [
            ("version", 0, 4),
            ("oneBitError", 8, 8),
            ("multipleOneError", 16, 8),
            ("multipleError", 24, 8),
            ("multipleMultipleError", 32, 8),
            ("correctedError", 40, 8),
            ("uncorrectedError", 48, 8),
            ("totalError", 56, 8),
            ("addressForbiddenError", 64, 8),
        ],
    ),
    "c_cndevDevicePowerInfo_t": (
        84,
        [
            ("usage", 0, 4),
            ("cap", 4, 4),
            ("machine", 8, 4),
            ("tdp", 12, 4),
            ("maxPower", 16, 4),
            ("reserved", 20, 64),
        ],
    ),
    "c_cndevTemperatureInfo_t": (
        148,
        [
            ("version", 0, 4),
            ("board", 4, 4),
            ("cluster", 8, 80),
            ("memoryDie", 88, 32),
            ("chip", 120, 4),
            ("airInlet", 124, 4),
            ("airOutlet", 128, 4),
            ("memory", 132, 4),
            ("videoInput", 136, 4),
            ("cpu", 140, 4),
            ("isp", 144, 4),
        ],
    ),
    "c_cndevUtilizationInfo_t": (
        328,
        [
            ("version", 0, 4),
            ("averageCoreUtilization", 4, 4),
            ("coreUtilization", 8, 320),
        ],
    ),
    "c_cndevCardName_t": (
        8,
        [("version", 0, 4), ("id", 4, 4)],
    ),
    "c_cndevPCIeInfoV2_t": (
        64,
        [
            ("subsystemId", 0, 4),
            ("deviceId", 4, 4),
            ("vendor", 8, 2),
            ("subsystemVendor", 10, 2),
            ("domain", 12, 4),
            ("bus", 16, 4),
            ("device", 20, 4),
            ("function", 24, 4),
            ("moduleId", 28, 2),
            ("slotId", 30, 2),
            ("reserved", 32, 32),
        ],
    ),
    "c_cndevCardHealthState_t": (
        16,
        [
            ("version", 0, 4),
            ("health", 4, 4),
            ("deviceState", 8, 4),
            ("driverState", 12, 4),
        ],
    ),
    "c_cndevDiagErrorDetail_t": (
        532,
        [
            ("msg", 0, 512),
            ("device_id", 512, 4),
            ("bdf", 516, 4),
            ("code", 520, 4),
            ("category", 524, 4),
            ("severity", 528, 4),
        ],
    ),
    "c_cndevIncidentInfo_t": (
        540,
        [("system", 0, 4), ("health", 4, 4), ("error", 8, 532)],
    ),
    "c_cndevCardHealthStateV2_t": (
        # 64 incident slots, which the driver fills in place: a short struct here is a
        # buffer overrun, not a missing field.
        34612,
        [
            ("health", 0, 4),
            ("deviceState", 4, 4),
            ("driverState", 8, 4),
            ("overallHealth", 12, 4),
            ("incident_count", 16, 4),
            ("incidents", 20, 34560),
            ("reserved", 34580, 32),
        ],
    ),
    "c_cndevNUMANodeId_t": (
        8,
        [("version", 0, 4), ("nodeId", 4, 4)],
    ),
}

# Constants pinned to the literals in the operator's binding/cndev/cndev.h -- the header
# of record -- rather than to the module's own definitions.
#
# Asserting a constant against itself proves nothing: CNDEV_VERSION_6 defined as 7 would
# leave this suite green while every versioned call on an MLU host was answered with
# CNDEV_ERROR_UNSUPPORTED_API_VERSION. No Cambricon hardware exists here, so a mistyped
# constant has to fail below or it fails at a customer.
HEADER_API_VERSION = 6

# cndevRet_enum, in the header's own order.
HEADER_ERROR_CODES = {
    "CNDEV_SUCCESS": 0,
    "CNDEV_ERROR_NO_DRIVER": 1,
    "CNDEV_ERROR_LOW_DRIVER_VERSION": 2,
    "CNDEV_ERROR_UNSUPPORTED_API_VERSION": 3,
    "CNDEV_ERROR_UNINITIALIZED": 4,
    "CNDEV_ERROR_INVALID_ARGUMENT": 5,
    "CNDEV_ERROR_INVALID_DEVICE_ID": 6,
    "CNDEV_ERROR_UNKNOWN": 7,
    "CNDEV_ERROR_MALLOC": 8,
    "CNDEV_ERROR_INSUFFICIENT_SPACE": 9,
    "CNDEV_ERROR_NOT_SUPPORTED": 10,
    "CNDEV_ERROR_INVALID_LINK": 11,
    "CNDEV_ERROR_NO_DEVICES": 12,
    "CNDEV_ERROR_NO_PERMISSION": 13,
    "CNDEV_ERROR_NOT_FOUND": 14,
    "CNDEV_ERROR_IN_USE": 15,
    "CNDEV_ERROR_DUPLICATE": 16,
    "CNDEV_ERROR_TIMEOUT": 17,
    "CNDEV_ERROR_IN_PROBLEM": 18,
}

# cndevNameEnum_t, limited to the types the card-name fallback table answers for: a wrong
# value there names the wrong card on a driver that exports neither name string.
HEADER_DEVICE_TYPES = {
    "CNDEV_DEVICE_TYPE_MLU100": 0,
    "CNDEV_DEVICE_TYPE_MLU270": 1,
    "CNDEV_DEVICE_TYPE_MLU220_M2": 16,
    "CNDEV_DEVICE_TYPE_MLU220_EDGE": 17,
    "CNDEV_DEVICE_TYPE_MLU220_EVB": 18,
    "CNDEV_DEVICE_TYPE_MLU220_M2i": 19,
    "CNDEV_DEVICE_TYPE_MLU290": 20,
    "CNDEV_DEVICE_TYPE_MLU370": 23,
    "CNDEV_DEVICE_TYPE_MLU365": 24,
    "CNDEV_DEVICE_TYPE_CE3226": 25,
    "CNDEV_DEVICE_TYPE_MLU590": 26,
    "CNDEV_DEVICE_TYPE_MLU585": 27,
    "CNDEV_DEVICE_TYPE_MLU580": 30,
    "CNDEV_DEVICE_TYPE_MLU570": 31,
}

# What the fake library reports, so an assertion names a value rather than a literal.
FAKE_DEVICE_COUNT = 4
FAKE_HANDLE_BASE = 100
FAKE_UUID = "d4e5f60718293a4b"
FAKE_CARD_NAME = "MLU590-M9"
FAKE_MEMORY_TOTAL = 48000
FAKE_MEMORY_USED = 1024
FAKE_DRIVER_VERSION = (6, 10, 3)
FAKE_CORE_UTILIZATION = 37
FAKE_BOARD_TEMPERATURE = 45
FAKE_POWER_USAGE = 110
FAKE_POWER_CAP = 250
FAKE_NUMA_NODE = 1
FAKE_CORRECTED_ERRORS = 2
FAKE_PCIE_BUS_ID = "0000:3b:00.0"

# A device handle the fake library answers for.
DEVICE = FAKE_HANDLE_BASE


def _identity(obj):
    """
    Stand in for ctypes.byref.

    The fake library's entry points are plain Python callables, so a real byref() would
    hand them a CArgObject that cannot be read or written from Python. Passing the
    struct itself keeps what the wrapper sent -- and what it expects back -- visible.
    """
    return obj


def _default_handlers():
    """
    Build one handler per libcndev.so entry point the binding may call.

    Each handler fills the out-struct the way a healthy MLU590 card would and returns
    CNDEV_SUCCESS.
    """

    def init(reserved):
        assert reserved is not None
        return pycndev.CNDEV_SUCCESS

    def release():
        return pycndev.CNDEV_SUCCESS

    def get_device_count(card_info):
        card_info.number = FAKE_DEVICE_COUNT
        return pycndev.CNDEV_SUCCESS

    def get_device_handle_by_index(index, handle):
        handle.value = FAKE_HANDLE_BASE + index
        return pycndev.CNDEV_SUCCESS

    def get_device_handle_by_uuid(uuid, handle):
        assert isinstance(uuid, bytes)
        handle.value = FAKE_HANDLE_BASE
        return pycndev.CNDEV_SUCCESS

    def get_device_handle_by_pci_bus_id(pci_bus_id, handle):
        assert isinstance(pci_bus_id, bytes)
        handle.value = FAKE_HANDLE_BASE
        return pycndev.CNDEV_SUCCESS

    def get_uuid(uuid_info, device):
        assert device == DEVICE
        uuid_info.uuid = FAKE_UUID
        return pycndev.CNDEV_SUCCESS

    def get_pcie_info_v2(pcie_info, device):
        assert device == DEVICE
        pcie_info.domain = 0x0000
        pcie_info.bus = 0x3B
        pcie_info.device = 0x00
        pcie_info.function = 0
        return pycndev.CNDEV_SUCCESS

    def get_card_name(card_name, device):
        assert device == DEVICE
        card_name.id = pycndev.CNDEV_DEVICE_TYPE_MLU590
        return pycndev.CNDEV_SUCCESS

    def get_card_name_string(card_name_id):
        assert card_name_id == pycndev.CNDEV_DEVICE_TYPE_MLU590
        return b"MLU590"

    def get_card_name_string_by_dev_id(device):
        assert device == DEVICE
        return FAKE_CARD_NAME.encode()

    def get_memory_usage_v2(memory_info, device):
        assert device == DEVICE
        memory_info.physicalMemoryTotal = FAKE_MEMORY_TOTAL
        memory_info.physicalMemoryUsed = FAKE_MEMORY_USED
        return pycndev.CNDEV_SUCCESS

    def get_card_health_state(health_state, device):
        assert device == DEVICE
        health_state.health = 1
        health_state.deviceState = pycndev.CNDEV_HEALTH_STATE_DEVICE_GOOD
        health_state.driverState = pycndev.CNDEV_HEALTH_STATE_DRIVER_RUNNING
        return pycndev.CNDEV_SUCCESS

    def get_card_health_state_v2(health_state, device):
        assert device == DEVICE
        health_state.health = 1
        health_state.deviceState = pycndev.CNDEV_HEALTH_STATE_DEVICE_GOOD
        health_state.driverState = pycndev.CNDEV_HEALTH_STATE_DRIVER_RUNNING
        health_state.overallHealth = pycndev.CNDEV_HEALTH_RESULT_PASS
        return pycndev.CNDEV_SUCCESS

    def get_version_info(version_info, device):
        assert device == DEVICE
        major, minor, build = FAKE_DRIVER_VERSION
        version_info.driverMajorVersion = major
        version_info.driverMinorVersion = minor
        version_info.driverBuildVersion = build
        return pycndev.CNDEV_SUCCESS

    def get_device_utilization_info(util_info, device):
        assert device == DEVICE
        util_info.averageCoreUtilization = FAKE_CORE_UTILIZATION
        return pycndev.CNDEV_SUCCESS

    def get_temperature_info(temp_info, device):
        assert device == DEVICE
        temp_info.board = FAKE_BOARD_TEMPERATURE
        temp_info.chip = FAKE_BOARD_TEMPERATURE
        return pycndev.CNDEV_SUCCESS

    def get_device_power_info(power_info, device):
        assert device == DEVICE
        power_info.usage = FAKE_POWER_USAGE
        power_info.cap = FAKE_POWER_CAP
        return pycndev.CNDEV_SUCCESS

    def get_numa_node_id(numa_node_id, device):
        assert device == DEVICE
        numa_node_id.nodeId = FAKE_NUMA_NODE
        return pycndev.CNDEV_SUCCESS

    def get_ecc_info(ecc_info, device):
        assert device == DEVICE
        ecc_info.correctedError = FAKE_CORRECTED_ERRORS
        return pycndev.CNDEV_SUCCESS

    return {
        "cndevInit": init,
        "cndevRelease": release,
        "cndevGetDeviceCount": get_device_count,
        "cndevGetDeviceHandleByIndex": get_device_handle_by_index,
        "cndevGetDeviceHandleByUUID": get_device_handle_by_uuid,
        "cndevGetDeviceHandleByPciBusId": get_device_handle_by_pci_bus_id,
        "cndevGetUUID": get_uuid,
        "cndevGetPCIeInfoV2": get_pcie_info_v2,
        "cndevGetCardName": get_card_name,
        "cndevGetCardNameString": get_card_name_string,
        "cndevGetCardNameStringByDevId": get_card_name_string_by_dev_id,
        "cndevGetMemoryUsageV2": get_memory_usage_v2,
        "cndevGetCardHealthState": get_card_health_state,
        "cndevGetCardHealthStateV2": get_card_health_state_v2,
        "cndevGetVersionInfo": get_version_info,
        "cndevGetDeviceUtilizationInfo": get_device_utilization_info,
        "cndevGetTemperatureInfo": get_temperature_info,
        "cndevGetDevicePowerInfo": get_device_power_info,
        "cndevGetNUMANodeIdByDevId": get_numa_node_id,
        "cndevGetECCInfo": get_ecc_info,
    }


class FakeLibrary:
    """
    A stand-in for libcndev.so that records what the binding asked of it.

    A symbol outside its handler set raises AttributeError, which is how a real CDLL
    reports a driver too old to export a call.
    """

    def __init__(self, handlers: dict):
        self.handlers = handlers
        self.symbols: list[str] = []
        self.calls: list[tuple] = []

    def __getattr__(self, name: str):
        handler = self.handlers.get(name)
        if handler is None:
            raise AttributeError(name)

        self.symbols.append(name)

        def entry_point(*args):
            self.calls.append((name, args))
            return handler(*args)

        return entry_point


def install_fake_library(monkeypatch, missing: tuple[str, ...] = ()) -> FakeLibrary:
    """
    Put a FakeLibrary in place of the loaded libcndev.so, as if cndevInit() had run.

    Args:
        monkeypatch:
            The pytest monkeypatch fixture, which restores every global afterwards.
        missing:
            Symbols to withhold, standing in for a driver that does not export them.

    Returns:
        The installed FakeLibrary.

    """
    handlers = {
        name: handler
        for name, handler in _default_handlers().items()
        if name not in missing
    }
    lib = FakeLibrary(handlers)

    monkeypatch.setattr(pycndev, "cndevLib", lib)
    monkeypatch.setattr(pycndev, "_libInitialized", True)
    monkeypatch.setattr(pycndev, "_libInitializedException", None)
    monkeypatch.setattr(pycndev, "_cndevGetFunctionPointer_cache", {})
    monkeypatch.setattr(pycndev, "byref", _identity)

    return lib


@pytest.mark.parametrize("name", list(EXPECTED_LAYOUTS))
def test_struct_layout_matches_header(name):
    expected_size, expected_fields = EXPECTED_LAYOUTS[name]
    struct = getattr(pycndev, name)

    assert sizeof(struct) == expected_size

    # Field names as well as offsets: an extra field tucked into trailing padding leaves
    # every offset and the total size untouched.
    assert [field[0] for field in struct._fields_] == [
        field[0] for field in expected_fields
    ]

    for field_name, offset, size in expected_fields:
        descriptor = getattr(struct, field_name)
        assert descriptor.offset == offset, field_name
        assert descriptor.size == size, field_name


def test_api_version_matches_the_header():
    assert pycndev.CNDEV_VERSION_6 == HEADER_API_VERSION


@pytest.mark.parametrize("name, value", list(HEADER_ERROR_CODES.items()))
def test_error_code_matches_the_header(name, value):
    assert getattr(pycndev, name) == value


@pytest.mark.parametrize("name, value", list(HEADER_DEVICE_TYPES.items()))
def test_device_type_matches_the_header(name, value):
    assert getattr(pycndev, name) == value


def test_import_makes_no_library_call(monkeypatch):
    """
    Importing the module must not reach for libcndev.so.

    The detector package is imported on every host, and all but a few have no Cambricon
    driver at all.
    """
    # Execute a second, privately named copy: the module registers its error subclasses
    # on sys.modules[__name__], so re-executing it under the real name would rebind the
    # classes the rest of this file raises and compares.
    probe_name = f"{MODULE_NAME}_import_probe"
    spec = importlib.util.spec_from_file_location(probe_name, pycndev.__file__)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, probe_name, module)

    with mock.patch(
        "ctypes.CDLL",
        side_effect=AssertionError("the library was loaded at import time"),
    ):
        spec.loader.exec_module(module)

    assert module.cndevLib is None


def test_missing_library_raises_cndev_error(monkeypatch):
    """
    A host without libcndev.so must fail as CNDevError, not as a bare OSError.

    The detector catches the binding's own error type; an OSError escaping from here
    would take the whole detection down instead of skipping the vendor.
    """
    monkeypatch.setattr(pycndev, "cndevLib", None)
    monkeypatch.setattr(pycndev, "_libInitialized", False)
    monkeypatch.setattr(pycndev, "_libInitializedException", None)
    monkeypatch.setattr(
        pycndev,
        "CDLL",
        mock.Mock(side_effect=OSError("cannot open shared object file")),
    )

    with pytest.raises(pycndev.CNDevError) as excinfo:
        pycndev.cndevInit()

    assert excinfo.value == pycndev.CNDEV_ERROR_LIBRARY_NOT_FOUND
    assert not isinstance(excinfo.value, OSError)
    assert str(excinfo.value) == "Library Not Found"


def test_uninitialized_call_raises_cndev_error(monkeypatch):
    monkeypatch.setattr(pycndev, "cndevLib", None)
    monkeypatch.setattr(pycndev, "_cndevGetFunctionPointer_cache", {})

    with pytest.raises(pycndev.CNDevError) as excinfo:
        pycndev.cndevGetDeviceCount()

    assert excinfo.value == pycndev.CNDEV_ERROR_UNINITIALIZED


def test_error_maps_code_to_subclass():
    err = pycndev.CNDevError(pycndev.CNDEV_ERROR_NOT_SUPPORTED)

    assert isinstance(err, pycndev.CNDevError_NotSupported)
    assert err == pycndev.CNDEV_ERROR_NOT_SUPPORTED
    assert err == pycndev.CNDevError(pycndev.CNDEV_ERROR_NOT_SUPPORTED)
    assert err != pycndev.CNDevError(pycndev.CNDEV_ERROR_NO_DRIVER)
    assert str(err) == "Not Supported"

    assert (
        pycndev.cndevExceptionClass(pycndev.CNDEV_ERROR_NO_DRIVER)
        is pycndev.CNDevError_NoDriver
    )


def test_error_renders_an_unknown_code():
    assert str(pycndev.CNDevError(-12345)) == "Unknown CNDev Error -12345"


# One entry per wrapper that maps 1:1 onto a C entry point, so a wrapper reaching for
# the wrong symbol fails here rather than on a Cambricon host.
DIRECT_CALLS = [
    ("cndevGetDeviceCount", lambda: pycndev.cndevGetDeviceCount()),
    ("cndevGetDeviceHandleByIndex", lambda: pycndev.cndevGetDeviceHandleByIndex(0)),
    ("cndevGetDeviceHandleByUUID", lambda: pycndev.cndevGetDeviceHandleByUUID("MLU-x")),
    (
        "cndevGetDeviceHandleByPciBusId",
        lambda: pycndev.cndevGetDeviceHandleByPciBusId(FAKE_PCIE_BUS_ID),
    ),
    ("cndevGetUUID", lambda: pycndev.cndevGetUUID(DEVICE)),
    ("cndevGetPCIeInfoV2", lambda: pycndev.cndevGetPCIeInfoV2(DEVICE)),
    ("cndevGetCardName", lambda: pycndev.cndevGetCardName(DEVICE)),
    (
        "cndevGetCardNameString",
        lambda: pycndev.cndevGetCardNameString(pycndev.CNDEV_DEVICE_TYPE_MLU590),
    ),
    (
        "cndevGetCardNameStringByDevId",
        lambda: pycndev.cndevGetCardNameStringByDevId(DEVICE),
    ),
    ("cndevGetMemoryUsageV2", lambda: pycndev.cndevGetMemoryUsageV2(DEVICE)),
    ("cndevGetCardHealthState", lambda: pycndev.cndevGetCardHealthState(DEVICE)),
    ("cndevGetCardHealthStateV2", lambda: pycndev.cndevGetCardHealthStateV2(DEVICE)),
    ("cndevGetVersionInfo", lambda: pycndev.cndevGetVersionInfo(DEVICE)),
    (
        "cndevGetDeviceUtilizationInfo",
        lambda: pycndev.cndevGetDeviceUtilizationInfo(DEVICE),
    ),
    ("cndevGetTemperatureInfo", lambda: pycndev.cndevGetTemperatureInfo(DEVICE)),
    ("cndevGetDevicePowerInfo", lambda: pycndev.cndevGetDevicePowerInfo(DEVICE)),
    ("cndevGetNUMANodeIdByDevId", lambda: pycndev.cndevGetNUMANodeIdByDevId(DEVICE)),
    ("cndevGetECCInfo", lambda: pycndev.cndevGetECCInfo(DEVICE)),
]

# The wrappers whose out-struct carries the header's IN version field. cndev rejects a
# versioned struct that does not declare which layout the caller speaks, so an unstamped
# one is answered with CNDEV_ERROR_UNSUPPORTED_API_VERSION rather than data.
VERSIONED_CALLS = [
    entry
    for entry in DIRECT_CALLS
    if entry[0]
    in {
        "cndevGetDeviceCount",
        "cndevGetUUID",
        "cndevGetCardName",
        "cndevGetCardHealthState",
        "cndevGetVersionInfo",
        "cndevGetDeviceUtilizationInfo",
        "cndevGetTemperatureInfo",
        "cndevGetNUMANodeIdByDevId",
        "cndevGetECCInfo",
    }
]


@pytest.mark.parametrize("symbol, call", DIRECT_CALLS)
def test_wrapper_calls_expected_symbol(monkeypatch, symbol, call):
    lib = install_fake_library(monkeypatch)

    call()

    assert [name for name, _ in lib.calls] == [symbol]


@pytest.mark.parametrize("symbol, call", VERSIONED_CALLS)
def test_versioned_call_declares_the_api_version(monkeypatch, symbol, call):
    lib = install_fake_library(monkeypatch)

    call()

    _, args = lib.calls[-1]
    assert args[0].version == HEADER_API_VERSION, symbol


@pytest.mark.parametrize("symbol, call", DIRECT_CALLS)
def test_wrapper_raises_on_driver_failure(monkeypatch, symbol, call):
    if symbol in ("cndevGetCardNameString", "cndevGetCardNameStringByDevId"):
        pytest.skip("returns a string, not a cndevRet_t")

    lib = install_fake_library(monkeypatch)
    lib.handlers[symbol] = lambda *_: pycndev.CNDEV_ERROR_NOT_SUPPORTED

    with pytest.raises(pycndev.CNDevError_NotSupported):
        call()


def test_get_device_count_returns_the_reported_number(monkeypatch):
    install_fake_library(monkeypatch)

    assert pycndev.cndevGetDeviceCount() == FAKE_DEVICE_COUNT


def test_get_device_handle_by_index_returns_the_handle(monkeypatch):
    install_fake_library(monkeypatch)

    assert pycndev.cndevGetDeviceHandleByIndex(2) == FAKE_HANDLE_BASE + 2


def test_get_uuid_prefixes_the_manufacturer_tag(monkeypatch):
    """
    The UUID must read the way every Cambricon tool and the operator report it.
    """
    install_fake_library(monkeypatch)

    assert pycndev.cndevGetUUID(DEVICE) == "MLU-" + FAKE_UUID


def test_get_uuid_rejects_an_empty_field(monkeypatch):
    """
    A driver answering success with an empty UUID has reported no identity.

    Prefixing it would yield a bare "MLU-" that reads as a valid id and is the same
    for every card, so the usage join would write one card's metrics onto all of them.
    """
    lib = install_fake_library(monkeypatch)

    def _empty_uuid(uuid_info, _device):
        uuid_info.uuid = ""
        return pycndev.CNDEV_SUCCESS

    lib.handlers["cndevGetUUID"] = _empty_uuid

    with pytest.raises(pycndev.CNDevError) as raised:
        pycndev.cndevGetUUID(DEVICE)

    assert raised.value == pycndev.CNDEV_ERROR_NOT_FOUND


def test_get_pcie_bus_id_formats_the_bdf(monkeypatch):
    install_fake_library(monkeypatch)

    assert pycndev.cndevGetPCIeBusId(DEVICE) == FAKE_PCIE_BUS_ID


def test_get_card_name_prefers_the_driver_string(monkeypatch):
    lib = install_fake_library(monkeypatch)

    assert pycndev.cndevGetCardNameByDevId(DEVICE) == FAKE_CARD_NAME
    # The cheapest call answered, so the enum is never consulted.
    assert "cndevGetCardName" not in lib.symbols


def test_get_card_name_falls_back_to_the_name_enum(monkeypatch):
    lib = install_fake_library(monkeypatch, missing=("cndevGetCardNameStringByDevId",))

    assert pycndev.cndevGetCardNameByDevId(DEVICE) == "MLU590"
    assert "cndevGetCardNameString" in lib.symbols


def test_get_card_name_falls_back_to_the_name_table(monkeypatch):
    """
    A driver exporting neither name string still yields a card name.
    """
    lib = install_fake_library(
        monkeypatch,
        missing=("cndevGetCardNameStringByDevId", "cndevGetCardNameString"),
    )

    assert pycndev.cndevGetCardNameByDevId(DEVICE) == "MLU590"
    assert "cndevGetCardName" in lib.symbols


def test_get_card_name_falls_back_to_the_family(monkeypatch):
    lib = install_fake_library(
        monkeypatch,
        missing=("cndevGetCardNameStringByDevId", "cndevGetCardNameString"),
    )

    def unknown_card_name(card_name, _device):
        card_name.id = 0x7FFF
        return pycndev.CNDEV_SUCCESS

    lib.handlers["cndevGetCardName"] = unknown_card_name

    assert pycndev.cndevGetCardNameByDevId(DEVICE) == "MLU"


def test_get_memory_usage_reports_the_physical_totals(monkeypatch):
    install_fake_library(monkeypatch)

    memory_info = pycndev.cndevGetMemoryUsageV2(DEVICE)

    assert memory_info.physicalMemoryTotal == FAKE_MEMORY_TOTAL
    assert memory_info.physicalMemoryUsed == FAKE_MEMORY_USED


def test_get_card_health_state_v2_falls_back_to_v1(monkeypatch):
    """
    A driver without the V2 call still reports health, through the V1 struct.
    """
    lib = install_fake_library(monkeypatch, missing=("cndevGetCardHealthStateV2",))

    health_state = pycndev.cndevGetCardHealthStateV2(DEVICE)

    assert "cndevGetCardHealthState" in lib.symbols
    assert health_state.health == 1
    assert health_state.deviceState == pycndev.CNDEV_HEALTH_STATE_DEVICE_GOOD
    assert health_state.driverState == pycndev.CNDEV_HEALTH_STATE_DRIVER_RUNNING
    # V1 knows nothing about incidents, so the report stays empty rather than stale.
    assert health_state.incident_count == 0
    # And the overall verdict is derived rather than left at zero, which is
    # CNDEV_HEALTH_RESULT_PASS and would read as a pass on every old driver.
    assert health_state.overallHealth == pycndev.CNDEV_HEALTH_RESULT_PASS


def test_get_card_health_state_v2_derives_a_failing_v1_verdict(monkeypatch):
    lib = install_fake_library(monkeypatch, missing=("cndevGetCardHealthStateV2",))

    def _in_problem(health_state, _device):
        health_state.health = pycndev.CNDEV_HEALTH_STATE_DEVICE_IN_PROBLEM
        health_state.deviceState = pycndev.CNDEV_HEALTH_STATE_DEVICE_IN_PROBLEM
        health_state.driverState = pycndev.CNDEV_HEALTH_STATE_DRIVER_RUNNING
        return pycndev.CNDEV_SUCCESS

    lib.handlers["cndevGetCardHealthState"] = _in_problem

    health_state = pycndev.cndevGetCardHealthStateV2(DEVICE)

    assert health_state.overallHealth == pycndev.CNDEV_HEALTH_RESULT_FAIL


def test_get_version_info_reports_the_driver_version(monkeypatch):
    install_fake_library(monkeypatch)

    version_info = pycndev.cndevGetVersionInfo(DEVICE)

    assert (
        version_info.driverMajorVersion,
        version_info.driverMinorVersion,
        version_info.driverBuildVersion,
    ) == FAKE_DRIVER_VERSION


def test_usage_wrappers_report_their_metrics(monkeypatch):
    install_fake_library(monkeypatch)

    assert (
        pycndev.cndevGetDeviceUtilizationInfo(DEVICE).averageCoreUtilization
        == FAKE_CORE_UTILIZATION
    )
    assert pycndev.cndevGetTemperatureInfo(DEVICE).board == FAKE_BOARD_TEMPERATURE
    assert pycndev.cndevGetDevicePowerInfo(DEVICE).usage == FAKE_POWER_USAGE
    assert pycndev.cndevGetDevicePowerInfo(DEVICE).cap == FAKE_POWER_CAP


def test_get_numa_node_id_reports_the_node(monkeypatch):
    install_fake_library(monkeypatch)

    assert pycndev.cndevGetNUMANodeIdByDevId(DEVICE).nodeId == FAKE_NUMA_NODE


def test_get_ecc_info_reports_the_counters(monkeypatch):
    install_fake_library(monkeypatch)

    assert pycndev.cndevGetECCInfo(DEVICE).correctedError == FAKE_CORRECTED_ERRORS


def test_init_is_idempotent(monkeypatch):
    lib = install_fake_library(monkeypatch)
    monkeypatch.setattr(pycndev, "_libInitialized", False)

    pycndev.cndevInit()
    pycndev.cndevInit()

    assert [name for name, _ in lib.calls] == ["cndevInit"]


def test_init_replays_a_cached_failure(monkeypatch):
    """
    A failed init must keep failing, with a fresh exception each time.

    Re-raising the cached object appends a traceback frame per raise, and those frames
    retain their callers' locals. See gpustack/gpustack#5342.
    """
    lib = install_fake_library(monkeypatch)
    monkeypatch.setattr(pycndev, "_libInitialized", False)
    lib.handlers["cndevInit"] = lambda *_: pycndev.CNDEV_ERROR_NO_DRIVER

    with pytest.raises(pycndev.CNDevError) as first:
        pycndev.cndevInit()
    with pytest.raises(pycndev.CNDevError) as second:
        pycndev.cndevInit()

    assert first.value == pycndev.CNDEV_ERROR_NO_DRIVER
    assert second.value == pycndev.CNDEV_ERROR_NO_DRIVER
    assert first.value is not second.value
    # The driver was asked once; the second call replayed the cached failure.
    assert [name for name, _ in lib.calls] == ["cndevInit"]


def test_init_reaches_the_driver_once_when_two_callers_race(monkeypatch):
    """
    Two concurrent first callers must not both reach the driver.

    The loser is answered with an already-initialized error, and caching that as the
    library's permanent state disables Cambricon detection until the process restarts.
    The operator's binding uses sync.Once here.
    """
    lib = install_fake_library(monkeypatch)
    monkeypatch.setattr(pycndev, "_libInitialized", False)

    entered = threading.Event()
    entered_again = threading.Event()
    proceed = threading.Event()
    entries: list[int] = []

    def _blocking_init(*_args):
        entries.append(1)
        (entered if len(entries) == 1 else entered_again).set()
        # Stay inside the driver call until the second caller has had its chance.
        proceed.wait(timeout=5)
        return pycndev.CNDEV_SUCCESS

    lib.handlers["cndevInit"] = _blocking_init

    failures: list[BaseException] = []

    def _init():
        try:
            pycndev.cndevInit()
        except BaseException as e:
            failures.append(e)

    first = threading.Thread(target=_init)
    first.start()
    assert entered.wait(timeout=5)

    second = threading.Thread(target=_init)
    second.start()
    # The second caller has to wait for the lock the first one holds, so it must not
    # reach the driver while the first call is still open.
    assert not entered_again.wait(timeout=0.5)

    proceed.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not failures
    assert [name for name, _ in lib.calls] == ["cndevInit"]


def test_release_reaches_the_driver(monkeypatch):
    lib = install_fake_library(monkeypatch)

    pycndev.cndevRelease()

    assert [name for name, _ in lib.calls] == ["cndevRelease"]


def test_release_without_init_is_a_no_op(monkeypatch):
    lib = install_fake_library(monkeypatch)
    monkeypatch.setattr(pycndev, "_libInitialized", False)

    pycndev.cndevRelease()

    assert lib.calls == []


def test_release_tolerates_a_driver_without_the_symbol(monkeypatch):
    install_fake_library(monkeypatch, missing=("cndevRelease",))

    pycndev.cndevRelease()
