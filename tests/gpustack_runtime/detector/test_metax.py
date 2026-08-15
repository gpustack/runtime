from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from gpustack_runtime import envs
from gpustack_runtime.detector import metax, pymxsml
from gpustack_runtime.detector.__types__ import (
    DeviceMemoryStatusEnum,
    ManufacturerEnum,
)
from gpustack_runtime.detector.metax import MetaXDetector


@pytest.mark.skipif(
    not MetaXDetector.is_supported(),
    reason="MetaX GPU not detected",
)
def test_detect():
    det = MetaXDetector()
    devs = det.detect()
    print(devs)


@pytest.mark.skipif(
    not MetaXDetector.is_supported(),
    reason="MetaX GPU not detected",
)
def test_get_topology():
    det = MetaXDetector()
    topo = det.get_topology()
    print(topo)


# --------------------------------------------------------------------------- #
# A fake pymxsml, so the split is provable on a host with no MetaX driver.     #
# --------------------------------------------------------------------------- #


@dataclass
class _Card:
    """
    One card as the driver would report it, in the driver's own units.
    """

    uuid: str
    name: str = "MetaX C500"
    mode: int = pymxsml.MXSML_VIRTUALIZATION_MODE_NONE
    bdf: str = "0000:01:00.0"
    driver_version: str = "2.23.0.1"
    vram_total: int = 67108864  # KiB, i.e. 65536 MiB
    vram_used: int = 1048576  # KiB, i.e. 1024 MiB
    dram_ue: int = 0
    core_usage: int = 42
    temperature: int = 5500
    power_limit: int = 350000  # mW, i.e. 350 W
    power_ways: tuple[int, ...] = (120000, 30000)  # mW, i.e. 150 W together
    node_affinity: tuple[int, ...] = (0b1,)  # NUMA node 0


@dataclass
class _DeviceInfo:
    """
    A c_mxsmlDeviceInfo_t stand-in, carrying the fields the detector reads.
    """

    uuid: str
    deviceName: str  # noqa: N815
    mode: int
    bdfId: str  # noqa: N815


@dataclass
class _MemoryInfo:
    """
    A c_mxsmlMemoryInfo_t stand-in, in KiB as the driver reports it.
    """

    vramTotal: int  # noqa: N815
    vramUse: int  # noqa: N815


@dataclass
class _EccErrorCount:
    """
    A c_mxSmlEccErrorCount_t stand-in, of which only the uncorrectable DRAM
    errors decide the memory health.
    """

    dramUE: int  # noqa: N815


@dataclass
class _BoardWayElectricInfo:
    """
    A c_mxSmlBoardWayElectricInfo_t stand-in, in mW as the driver reports it.
    """

    power: int


@dataclass
class _FakeMXSML:
    """
    A stand-in for the pymxsml binding, recording every call the detector makes.

    The error type and the enumeration constants are the real module's, so a
    fake drifting from the binding's contract fails here rather than on
    hardware. Entry points are dispatched by name, as the fake libcndev.so of
    test_pycndev.py does, which keeps the driver's camelCase out of the
    handlers' own names.
    """

    cards: list[_Card]
    calls: list[str] = field(default_factory=list)

    MXSMLError = pymxsml.MXSMLError
    MXSML_VERSION_DRIVER = pymxsml.MXSML_VERSION_DRIVER
    MXSML_USAGE_XCORE = pymxsml.MXSML_USAGE_XCORE
    MXSML_TEMPERATURE_HOTSPOT = pymxsml.MXSML_TEMPERATURE_HOTSPOT

    def __getattr__(self, name: str):
        handler = {
            "mxSmlInit": self._init,
            "mxSmlGetMacaVersion": self._get_maca_version,
            "mxSmlGetDeviceCount": self._get_device_count,
            "mxSmlGetDeviceVersion": self._get_device_version,
            "mxSmlGetDeviceInfo": self._get_device_info,
            "mxSmlGetMemoryInfo": self._get_memory_info,
            "mxSmlGetTotalEccErrors": self._get_total_ecc_errors,
            "mxSmlGetBoardPowerLimit": self._get_board_power_limit,
            "mxSmlGetNodeAffinity": self._get_node_affinity,
            "mxSmlGetDeviceIpUsage": self._get_device_ip_usage,
            "mxSmlGetTemperatureInfo": self._get_temperature_info,
            "mxSmlGetBoardPowerInfo": self._get_board_power_info,
        }.get(name)
        if handler is None:
            msg = f"module pymxsml has no attribute {name}"
            raise AttributeError(msg)

        def entry_point(*args):
            self.calls.append(name)
            return handler(*args)

        return entry_point

    def _init(self) -> None:
        pass

    def _get_maca_version(self) -> str:
        return "2.33.0.6"

    def _get_device_count(self) -> int:
        return len(self.cards)

    def _get_device_version(self, device_id: int, version_unit: int) -> str:
        assert version_unit == pymxsml.MXSML_VERSION_DRIVER
        return self.cards[device_id].driver_version

    def _get_device_info(self, device_id: int) -> _DeviceInfo:
        card = self.cards[device_id]
        return _DeviceInfo(
            uuid=card.uuid,
            deviceName=card.name,
            mode=card.mode,
            bdfId=card.bdf,
        )

    def _get_memory_info(self, device_id: int) -> _MemoryInfo:
        card = self.cards[device_id]
        return _MemoryInfo(vramTotal=card.vram_total, vramUse=card.vram_used)

    def _get_total_ecc_errors(self, device_id: int) -> _EccErrorCount:
        return _EccErrorCount(dramUE=self.cards[device_id].dram_ue)

    def _get_board_power_limit(self, device_id: int) -> int:
        return self.cards[device_id].power_limit

    def _get_node_affinity(self, device_id: int, node_set_size: int) -> list[int]:
        assert node_set_size > 0
        return list(self.cards[device_id].node_affinity)

    def _get_device_ip_usage(self, device_id: int, usage_ip: int) -> int:
        assert usage_ip == pymxsml.MXSML_USAGE_XCORE
        return self.cards[device_id].core_usage

    def _get_temperature_info(self, device_id: int, temperature_type: int) -> int:
        assert temperature_type == pymxsml.MXSML_TEMPERATURE_HOTSPOT
        return self.cards[device_id].temperature

    def _get_board_power_info(self, device_id: int) -> list[_BoardWayElectricInfo]:
        return [
            _BoardWayElectricInfo(power=power)
            for power in self.cards[device_id].power_ways
        ]


_USAGE_CALLS = (
    "mxSmlGetDeviceIpUsage",
    "mxSmlGetTemperatureInfo",
    "mxSmlGetBoardPowerInfo",
)
"""
The calls the usage query owns, i.e. the ones the information query must not
make. Deliberately not the whole metric-looking surface: mxSmlGetBoardPowerLimit
reads the power *limit* and mxSmlGetMemoryInfo the memory *total*, both of which
are inventory.
"""

_MEMORY_UTILIZATION = 1.56
"""
A default card's memory utilization: 1024 MiB used of 65536 MiB total, as
get_utilization rounds it.
"""


@pytest.fixture
def detector(monkeypatch):
    """
    Build a MetaX detector talking to a fake driver reporting the given cards.
    """

    def _install(*cards: _Card) -> tuple[MetaXDetector, _FakeMXSML]:
        fake = _FakeMXSML(cards=list(cards))
        monkeypatch.setattr(metax, "pymxsml", fake)
        det = MetaXDetector()
        # Shadowed on the instance, so the lru_cache'd static stays untouched.
        monkeypatch.setattr(det, "is_supported", lambda: True)
        return det, fake

    return _install


# --------------------------------------------------------------------------- #
# detect_info: no virtualization-mode filter, no vgpu, no usage call.         #
# --------------------------------------------------------------------------- #


def test_detect_info_reports_every_virtualization_mode(detector):
    det, _ = detector(
        _Card(uuid="GPU-bare", mode=pymxsml.MXSML_VIRTUALIZATION_MODE_NONE),
        _Card(uuid="GPU-pf", mode=pymxsml.MXSML_VIRTUALIZATION_MODE_PF),
        _Card(uuid="GPU-vf", mode=pymxsml.MXSML_VIRTUALIZATION_MODE_VF),
    )

    devices = det.detect_info()

    # The PF row is the regression guard: the detector used to `continue` on
    # MXSML_VIRTUALIZATION_MODE_PF, i.e. drop the physical function -- the whole
    # card -- so a virtualization-enabled host came up with zero devices. The VF
    # row pins the other half: no virtualization-mode filter is applied at all,
    # which is a deliberate divergence from the operator, as it still drops VF.
    assert [dev.uuid for dev in devices] == ["GPU-bare", "GPU-pf", "GPU-vf"]
    assert [dev.index for dev in devices] == [0, 1, 2]


def test_detect_info_carries_the_inventory(detector):
    det, _ = detector(_Card(uuid="GPU-0"))

    dev = det.detect_info()[0]

    assert dev.manufacturer == ManufacturerEnum.METAX
    assert dev.name == "MetaX C500"
    assert dev.uuid == "GPU-0"
    assert dev.driver_version == "2.23.0.1"
    assert dev.runtime_version == "2.33"
    assert dev.runtime_version_original == "2.33.0.6"
    assert dev.memory == 65536
    assert dev.power == 350
    assert dev.memory_status == DeviceMemoryStatusEnum.HEALTHY
    assert dev.appendix["bdf"] == "0000:01:00.0"
    assert dev.appendix["numa"] == "0"
    # The usage fields keep their defaults: the information query does not fill
    # them, and must not invent a zero that reads like a measurement.
    assert dev.cores_utilization == 0
    assert dev.memory_used == 0
    assert dev.memory_utilization == 0
    assert dev.temperature is None
    assert dev.power_used is None


def test_detect_info_issues_no_usage_call(detector):
    det, fake = detector(_Card(uuid="GPU-0"))

    det.detect_info()

    assert [call for call in fake.calls if call in _USAGE_CALLS] == []
    assert "mxSmlGetBoardPowerLimit" in fake.calls
    assert "mxSmlGetMemoryInfo" in fake.calls


def test_no_appendix_carries_vgpu(detector):
    det, _ = detector(
        _Card(uuid="GPU-pf", mode=pymxsml.MXSML_VIRTUALIZATION_MODE_PF),
        _Card(uuid="GPU-vf", mode=pymxsml.MXSML_VIRTUALIZATION_MODE_VF),
    )

    for dev in det.detect():
        assert "vgpu" not in dev.appendix


# --------------------------------------------------------------------------- #
# detect_usage: the six fields, merged by UUID.                               #
# --------------------------------------------------------------------------- #


def test_detect_usage_merges_by_uuid(detector):
    det, _ = detector(
        _Card(uuid="GPU-0"),
        _Card(
            uuid="GPU-1",
            core_usage=7,
            vram_used=2097152,
            temperature=6100,
            power_ways=(200000,),
        ),
    )

    devices = det.detect_info()
    # Reversed on purpose: the merge joins by UUID, never by position.
    devices.reverse()

    assert det.detect_usage(devices) is devices

    by_uuid = {dev.uuid: dev for dev in devices}
    assert by_uuid["GPU-0"].cores_utilization == 42
    assert by_uuid["GPU-0"].memory_used == 1024
    assert by_uuid["GPU-0"].memory_utilization == _MEMORY_UTILIZATION
    assert by_uuid["GPU-0"].temperature == 55
    assert by_uuid["GPU-0"].power_used == 150
    assert by_uuid["GPU-1"].cores_utilization == 7
    assert by_uuid["GPU-1"].memory_used == 2048
    assert by_uuid["GPU-1"].temperature == 61
    assert by_uuid["GPU-1"].power_used == 200
    # The information fields survive the merge untouched.
    assert by_uuid["GPU-0"].index == 0
    assert by_uuid["GPU-0"].memory == 65536
    assert by_uuid["GPU-0"].power == 350
    assert by_uuid["GPU-0"].name == "MetaX C500"


def test_detect_usage_detects_the_information_first(detector):
    det, _ = detector(_Card(uuid="GPU-0"))

    devices = det.detect_usage()

    assert [dev.uuid for dev in devices] == ["GPU-0"]
    assert devices[0].name == "MetaX C500"
    assert devices[0].memory == 65536
    assert devices[0].cores_utilization == 42


def test_detect_composes_both_queries(detector):
    det, _ = detector(_Card(uuid="GPU-0"))

    dev = det.detect()[0]

    assert dev.name == "MetaX C500"
    assert dev.memory == 65536
    assert dev.power == 350
    assert dev.cores_utilization == 42
    assert dev.memory_used == 1024
    assert dev.memory_utilization == _MEMORY_UTILIZATION
    assert dev.temperature == 55
    assert dev.power_used == 150


# --------------------------------------------------------------------------- #
# memory_status, which both queries own.                                      #
# --------------------------------------------------------------------------- #


def test_detect_keeps_the_memory_status_through_the_merge(detector):
    det, _ = detector(_Card(uuid="GPU-0"))

    # merge_devices_usage overwrites memory_status along with the other five
    # usage fields, so a usage query that did not re-read the health would wipe
    # the information query's verdict back to the UNKNOWN default.
    assert det.detect()[0].memory_status == DeviceMemoryStatusEnum.HEALTHY


def test_detect_reports_an_uncorrectable_ecc_error(detector, monkeypatch):
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK", False)
    det, _ = detector(_Card(uuid="GPU-0", dram_ue=3))

    # Both queries report the health, mirroring the operator, which flags
    # Unhealthy from DetectAccelerator and MonitorAccelerator alike.
    assert det.detect_info()[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY
    assert det.detect()[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY
