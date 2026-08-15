from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from gpustack_runtime import envs
from gpustack_runtime.detector import cambricon, pycndev
from gpustack_runtime.detector.__types__ import (
    DeviceMemoryStatusEnum,
    ManufacturerEnum,
)
from gpustack_runtime.detector.cambricon import CambriconDetector


@pytest.mark.skipif(
    not CambriconDetector.is_supported(),
    reason="Cambricon GPU not detected",
)
def test_detect():
    det = CambriconDetector()
    devs = det.detect()
    print(devs)


@pytest.mark.skipif(
    not CambriconDetector.is_supported(),
    reason="Cambricon GPU not detected",
)
def test_get_topology():
    det = CambriconDetector()
    topo = det.get_topology()
    print(topo)


# --------------------------------------------------------------------------- #
# A fake pycndev, so the split is provable on a host with no Cambricon driver. #
# --------------------------------------------------------------------------- #

_HANDLE_OFFSET = 100
"""
Distance between a card's index and the handle the fake driver hands out.
cndevDevice_t is an int32 handle, i.e. indistinguishable from an index at the
Python level, so the fake offsets it: a detector passing an index where a handle
belongs indexes out of range instead of quietly working.
"""


@dataclass
class _Card:
    """
    One card as the driver would report it, in the driver's own units.
    """

    uuid: str
    name: str = "MLU590-M9"
    memory_total: int = 49152
    memory_used: int = 1024
    driver_version: tuple[int, int, int] = (5, 10, 22)
    bus_id: str = "0000:1f:00.0"
    numa_node: int = 0
    # The header's `health` carries the same 0/1 convention as the device-state
    # enumerators, and the operator's verdict is a bare `Health == 0`.
    health: int = pycndev.CNDEV_HEALTH_STATE_DEVICE_GOOD
    core_utilization: int = 42
    temperature: int = 55
    power_usage: int = 150
    failing: tuple[str, ...] = ()
    """
    Entry points that raise for this card, so a faulty card is reproducible.
    """


@dataclass
class _MemoryInfo:
    """
    A c_cndevMemoryInfoV2_t stand-in. The header calls the unit MB; the operator,
    cnmon and this repo's Ascend detector all treat it as MiB.
    """

    physicalMemoryTotal: int  # noqa: N815
    physicalMemoryUsed: int  # noqa: N815


@dataclass
class _VersionInfo:
    """
    A c_cndevVersionInfo_t stand-in, of which only the driver triplet is read.
    """

    driverMajorVersion: int  # noqa: N815
    driverMinorVersion: int  # noqa: N815
    driverBuildVersion: int  # noqa: N815


@dataclass
class _NUMANodeId:
    """
    A c_cndevNUMANodeId_t stand-in.
    """

    nodeId: int  # noqa: N815


@dataclass
class _CardHealthState:
    """
    A c_cndevCardHealthStateV2_t stand-in, of which the detector reads the
    card-wide health bit alone, as the operator does.
    """

    health: int
    deviceState: int  # noqa: N815


@dataclass
class _UtilizationInfo:
    """
    A c_cndevUtilizationInfo_t stand-in.
    """

    averageCoreUtilization: int  # noqa: N815


@dataclass
class _TemperatureInfo:
    """
    A c_cndevTemperatureInfo_t stand-in, in degrees Celsius.
    """

    chip: int


@dataclass
class _PowerInfo:
    """
    A c_cndevDevicePowerInfo_t stand-in, in Watts.
    """

    usage: int


@dataclass
class _FakeCNDev:
    """
    A stand-in for the pycndev binding, recording every call the detector makes.

    The error type, the error codes and the health enumerators are the real
    module's, so a fake drifting from the binding's contract fails here rather
    than on hardware. Entry points are dispatched by name, as the sibling vendors'
    fakes do, which keeps the driver's camelCase out of the handlers' own names.

    cndevGetECCInfo is deliberately absent: the operator's Cambricon health
    verdict reads the card health state and nothing else, so an ECC read
    reintroduced here fails loudly instead of silently costing a driver call.
    """

    cards: list[_Card]
    calls: list[str] = field(default_factory=list)

    CNDevError = pycndev.CNDevError
    CNDEV_HEALTH_STATE_DEVICE_GOOD = pycndev.CNDEV_HEALTH_STATE_DEVICE_GOOD
    CNDEV_HEALTH_STATE_DEVICE_IN_PROBLEM = pycndev.CNDEV_HEALTH_STATE_DEVICE_IN_PROBLEM

    def __getattr__(self, name: str):
        handler = {
            "cndevInit": self._init,
            "cndevGetDeviceCount": self._get_device_count,
            "cndevGetDeviceHandleByIndex": self._get_device_handle_by_index,
            "cndevGetUUID": self._get_uuid,
            "cndevGetCardNameByDevId": self._get_card_name_by_dev_id,
            "cndevGetMemoryUsageV2": self._get_memory_usage_v2,
            "cndevGetVersionInfo": self._get_version_info,
            "cndevGetPCIeBusId": self._get_pcie_bus_id,
            "cndevGetNUMANodeIdByDevId": self._get_numa_node_id_by_dev_id,
            "cndevGetCardHealthStateV2": self._get_card_health_state_v2,
            "cndevGetDeviceUtilizationInfo": self._get_device_utilization_info,
            "cndevGetTemperatureInfo": self._get_temperature_info,
            "cndevGetDevicePowerInfo": self._get_device_power_info,
        }.get(name)
        if handler is None:
            msg = f"module pycndev has no attribute {name}"
            raise AttributeError(msg)

        def entry_point(*args):
            self.calls.append(name)
            return handler(*args)

        return entry_point

    def _card(self, handle: int, name: str) -> _Card:
        card = self.cards[handle - _HANDLE_OFFSET]
        if name in card.failing:
            raise pycndev.CNDevError(pycndev.CNDEV_ERROR_UNKNOWN)
        return card

    def _init(self) -> None:
        pass

    def _get_device_count(self) -> int:
        return len(self.cards)

    def _get_device_handle_by_index(self, index: int) -> int:
        return index + _HANDLE_OFFSET

    def _get_uuid(self, handle: int) -> str:
        # The binding prefixes the driver's bare UUID, as every Cambricon tool does.
        return "MLU-" + self._card(handle, "cndevGetUUID").uuid

    def _get_card_name_by_dev_id(self, handle: int) -> str:
        return self._card(handle, "cndevGetCardNameByDevId").name

    def _get_memory_usage_v2(self, handle: int) -> _MemoryInfo:
        card = self._card(handle, "cndevGetMemoryUsageV2")
        return _MemoryInfo(
            physicalMemoryTotal=card.memory_total,
            physicalMemoryUsed=card.memory_used,
        )

    def _get_version_info(self, handle: int) -> _VersionInfo:
        card = self._card(handle, "cndevGetVersionInfo")
        major, minor, build = card.driver_version
        return _VersionInfo(
            driverMajorVersion=major,
            driverMinorVersion=minor,
            driverBuildVersion=build,
        )

    def _get_pcie_bus_id(self, handle: int) -> str:
        return self._card(handle, "cndevGetPCIeBusId").bus_id

    def _get_numa_node_id_by_dev_id(self, handle: int) -> _NUMANodeId:
        card = self._card(handle, "cndevGetNUMANodeIdByDevId")
        return _NUMANodeId(nodeId=card.numa_node)

    def _get_card_health_state_v2(self, handle: int) -> _CardHealthState:
        card = self._card(handle, "cndevGetCardHealthStateV2")
        return _CardHealthState(health=card.health, deviceState=card.health)

    def _get_device_utilization_info(self, handle: int) -> _UtilizationInfo:
        card = self._card(handle, "cndevGetDeviceUtilizationInfo")
        return _UtilizationInfo(averageCoreUtilization=card.core_utilization)

    def _get_temperature_info(self, handle: int) -> _TemperatureInfo:
        card = self._card(handle, "cndevGetTemperatureInfo")
        return _TemperatureInfo(chip=card.temperature)

    def _get_device_power_info(self, handle: int) -> _PowerInfo:
        card = self._card(handle, "cndevGetDevicePowerInfo")
        return _PowerInfo(usage=card.power_usage)


_USAGE_CALLS = (
    "cndevGetDeviceUtilizationInfo",
    "cndevGetTemperatureInfo",
    "cndevGetDevicePowerInfo",
)
"""
The calls the usage query owns, i.e. the ones the information query must not
make. Deliberately not the whole metric-looking surface: cndevGetMemoryUsageV2
reports the memory *total* the inventory needs alongside the used amount, so
both queries read it, as the operator's DetectAccelerator and MonitorAccelerator
both do.
"""

_MEMORY_UTILIZATION = 2.08
"""
A default card's memory utilization: 1024 MiB used of 49152 MiB total, as
get_utilization rounds it.
"""


@pytest.fixture
def detector(monkeypatch, tmp_path):
    """
    Build a Cambricon detector talking to a fake driver reporting the given cards.
    """

    def _install(
        *cards: _Card,
        neuware_version: str | None = "Neuware Version: 1.2.3\n",
    ) -> tuple[CambriconDetector, _FakeCNDev]:
        fake = _FakeCNDev(cards=list(cards))
        monkeypatch.setattr(cambricon, "pycndev", fake)

        # The Neuware version is a file read, not a driver call, so it is faked
        # by pointing the module's path constant at a temporary file.
        version_path = tmp_path / "version.txt"
        if neuware_version is not None:
            version_path.write_text(neuware_version)
        monkeypatch.setattr(cambricon, "_NEUWARE_VERSION_PATH", version_path)

        det = CambriconDetector()
        # Shadowed on the instance, so the lru_cache'd static stays untouched.
        monkeypatch.setattr(det, "is_supported", lambda: True)
        return det, fake

    return _install


# --------------------------------------------------------------------------- #
# detect_info: inventory only, no vgpu, no usage call.                        #
# --------------------------------------------------------------------------- #


def test_detect_info_carries_the_inventory(detector):
    det, _ = detector(_Card(uuid="0123456789ab"))

    dev = det.detect_info()[0]

    assert dev.manufacturer == ManufacturerEnum.CAMBRICON
    assert dev.index == 0
    assert dev.name == "MLU590-M9"
    assert dev.uuid == "MLU-0123456789ab"
    # The operator formats major.minor only; the build number is free precision
    # from the same query, and it is the digit a driver bug is diagnosed by.
    assert dev.driver_version == "5.10.22"
    assert dev.runtime_version == "1.2"
    assert dev.runtime_version_original == "1.2.3"
    # The driver reports MB per the header and MiB in fact, so no conversion is
    # applied: converting would under-report ~4.8% against the operator.
    assert dev.memory == 49152
    assert dev.memory_status == DeviceMemoryStatusEnum.HEALTHY
    assert dev.appendix["bdf"] == "0000:1f:00.0"
    assert dev.appendix["numa"] == "0"
    # The usage fields keep their defaults: the information query does not fill
    # them, and must not invent a zero that reads like a measurement.
    assert dev.cores_utilization == 0
    assert dev.memory_used == 0
    assert dev.memory_utilization == 0
    assert dev.temperature is None
    assert dev.power_used is None
    # No power call is made, so the power limit stays unset.
    assert dev.power is None


def test_detect_info_issues_no_usage_call(detector):
    det, fake = detector(_Card(uuid="0123456789ab"))

    det.detect_info()

    assert [call for call in fake.calls if call in _USAGE_CALLS] == []
    assert "cndevGetMemoryUsageV2" in fake.calls
    assert "cndevGetVersionInfo" in fake.calls


def test_detect_info_enumerates_every_card(detector):
    det, _ = detector(_Card(uuid="card-0"), _Card(uuid="card-1"))

    devices = det.detect_info()

    assert [dev.uuid for dev in devices] == ["MLU-card-0", "MLU-card-1"]
    assert [dev.index for dev in devices] == [0, 1]


def test_no_appendix_carries_vgpu(detector):
    det, _ = detector(_Card(uuid="card-0"))

    for dev in det.detect():
        assert "vgpu" not in dev.appendix


def test_detect_info_without_the_neuware_version_file(detector):
    det, _ = detector(_Card(uuid="card-0"), neuware_version=None)

    dev = det.detect_info()[0]

    assert dev.runtime_version is None
    assert dev.runtime_version_original is None


def test_detect_info_ignores_an_unversioned_neuware_file(detector):
    det, _ = detector(_Card(uuid="card-0"), neuware_version="unknown\n")

    dev = det.detect_info()[0]

    assert dev.runtime_version_original is None


# --------------------------------------------------------------------------- #
# A faulty card is skipped, not fatal.                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "failing",
    ["cndevGetUUID", "cndevGetMemoryUsageV2", "cndevGetCardNameByDevId"],
)
def test_detect_info_skips_a_faulty_card(detector, failing):
    det, _ = detector(
        _Card(uuid="broken", failing=(failing,)),
        _Card(uuid="healthy"),
    )

    devices = det.detect_info()

    # The operator continues past a card whose required reads fail. Without that,
    # one faulty card makes the whole vendor report zero devices.
    assert [dev.uuid for dev in devices] == ["MLU-healthy"]
    # Skipping does not renumber: the index is the one the driver enumerated the
    # card at, as Device.index promises.
    assert devices[0].index == 1


def test_detect_info_fails_when_every_card_is_skipped(detector):
    # Skipping one card of several is the graceful degradation this vendor is
    # allowed. Every card failing the same read is systemic -- a driver not
    # exporting a call the loop needs -- and an empty inventory would be
    # indistinguishable from a host that has no MLU at all.
    det, _ = detector(
        _Card(uuid="broken-0", failing=("cndevGetPCIeBusId",)),
        _Card(uuid="broken-1", failing=("cndevGetPCIeBusId",)),
    )

    with pytest.raises(pycndev.CNDevError):
        det.detect_info()


def test_detect_usage_fails_when_every_card_is_skipped(detector):
    det, _ = detector(
        _Card(uuid="broken-0", failing=("cndevGetMemoryUsageV2",)),
        _Card(uuid="broken-1", failing=("cndevGetMemoryUsageV2",)),
    )

    with pytest.raises(pycndev.CNDevError):
        det.detect_usage()


def test_detect_info_tolerates_an_unreadable_optional_field(detector):
    det, _ = detector(
        _Card(
            uuid="card-0",
            failing=("cndevGetVersionInfo", "cndevGetNUMANodeIdByDevId"),
        ),
    )

    dev = det.detect_info()[0]

    # The optional reads are suppressed, so the card is still reported.
    assert dev.uuid == "MLU-card-0"
    assert dev.driver_version is None
    assert "numa" not in dev.appendix


# --------------------------------------------------------------------------- #
# detect_usage: the six fields, merged by UUID.                               #
# --------------------------------------------------------------------------- #


def test_detect_usage_merges_by_uuid(detector):
    det, _ = detector(
        _Card(uuid="card-0"),
        _Card(
            uuid="card-1",
            memory_used=2048,
            core_utilization=7,
            temperature=61,
            power_usage=200,
        ),
    )

    devices = det.detect_info()
    # Reversed on purpose: the merge joins by UUID, never by position.
    devices.reverse()

    assert det.detect_usage(devices) is devices

    by_uuid = {dev.uuid: dev for dev in devices}
    assert by_uuid["MLU-card-0"].cores_utilization == 42
    assert by_uuid["MLU-card-0"].memory_used == 1024
    assert by_uuid["MLU-card-0"].memory_utilization == _MEMORY_UTILIZATION
    assert by_uuid["MLU-card-0"].temperature == 55
    assert by_uuid["MLU-card-0"].power_used == 150
    assert by_uuid["MLU-card-1"].cores_utilization == 7
    assert by_uuid["MLU-card-1"].memory_used == 2048
    assert by_uuid["MLU-card-1"].temperature == 61
    assert by_uuid["MLU-card-1"].power_used == 200
    # The information fields survive the merge untouched.
    assert by_uuid["MLU-card-0"].index == 0
    assert by_uuid["MLU-card-0"].memory == 49152
    assert by_uuid["MLU-card-0"].name == "MLU590-M9"
    assert by_uuid["MLU-card-0"].driver_version == "5.10.22"


def test_detect_usage_detects_the_information_first(detector):
    det, _ = detector(_Card(uuid="card-0"))

    devices = det.detect_usage()

    assert [dev.uuid for dev in devices] == ["MLU-card-0"]
    assert devices[0].name == "MLU590-M9"
    assert devices[0].memory == 49152
    assert devices[0].cores_utilization == 42


def test_detect_composes_both_queries(detector):
    det, _ = detector(_Card(uuid="card-0"))

    dev = det.detect()[0]

    assert dev.name == "MLU590-M9"
    assert dev.memory == 49152
    assert dev.cores_utilization == 42
    assert dev.memory_used == 1024
    assert dev.memory_utilization == _MEMORY_UTILIZATION
    assert dev.temperature == 55
    assert dev.power_used == 150


def test_detect_usage_skips_a_faulty_card(detector):
    det, _ = detector(
        _Card(uuid="broken", failing=("cndevGetMemoryUsageV2",)),
        _Card(uuid="healthy"),
    )

    devices = det.detect_usage()

    assert [dev.uuid for dev in devices] == ["MLU-healthy"]
    assert devices[0].memory_used == 1024


# --------------------------------------------------------------------------- #
# memory_status, which both queries own.                                      #
# --------------------------------------------------------------------------- #


def test_detect_keeps_the_memory_status_through_the_merge(detector):
    det, _ = detector(_Card(uuid="card-0"))

    # merge_devices_usage overwrites memory_status along with the other five
    # usage fields, so a usage query that did not re-read the health would wipe
    # the information query's verdict back to the UNKNOWN default.
    assert det.detect()[0].memory_status == DeviceMemoryStatusEnum.HEALTHY


def test_detect_reads_no_health_state_by_default(detector):
    det, fake = detector(_Card(uuid="card-0"))

    det.detect()

    # GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK defaults to true, so a default run
    # pays for no health call at all -- a deliberate divergence from the
    # operator, which reads the card health unconditionally.
    assert "cndevGetCardHealthStateV2" not in fake.calls


def test_detect_reports_a_card_in_problem(detector, monkeypatch):
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK", False)
    det, _ = detector(
        _Card(
            uuid="card-0",
            health=pycndev.CNDEV_HEALTH_STATE_DEVICE_IN_PROBLEM,
        ),
    )

    # Both queries report the health, mirroring the operator, which flags
    # Unhealthy from DetectAccelerator and MonitorAccelerator alike.
    assert det.detect_info()[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY
    assert det.detect()[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY


def test_detect_tolerates_an_unreadable_health_state(detector, monkeypatch):
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK", False)
    det, _ = detector(
        _Card(uuid="card-0", failing=("cndevGetCardHealthStateV2",)),
    )

    assert det.detect_info()[0].memory_status == DeviceMemoryStatusEnum.HEALTHY
