from __future__ import annotations

import contextlib
from dataclasses import dataclass, field

import pytest

from gpustack_runtime import envs
from gpustack_runtime.detector import mthreads, pymtml
from gpustack_runtime.detector.__types__ import (
    DeviceMemoryStatusEnum,
    ManufacturerEnum,
)
from gpustack_runtime.detector.mthreads import MThreadsDetector


@pytest.mark.skipif(
    not MThreadsDetector.is_supported(),
    reason="MThreads GPU not detected",
)
def test_detect():
    det = MThreadsDetector()
    devs = det.detect()
    print(devs)


@pytest.mark.skipif(
    not MThreadsDetector.is_supported(),
    reason="MThreads GPU not detected",
)
def test_get_topology():
    det = MThreadsDetector()
    topo = det.get_topology()
    print(topo)


# --------------------------------------------------------------------------- #
# A fake pymtml, so the split is provable on a host with no MThreads driver.   #
# --------------------------------------------------------------------------- #


@dataclass
class _Card:
    """
    One card as the driver would report it, in the driver's own units.
    """

    uuid: str
    name: str = "MTT S4000"
    virt_role: int = pymtml.MTML_VIRT_ROLE_NONE
    mpc_type: int = pymtml.MTML_MPC_TYPE_NONE
    bus: int = 0x01
    cores: int = 128
    memory_total: int = 51539607552  # byte, i.e. 49152 MiB
    memory_used: int = 1073741824  # byte, i.e. 1024 MiB
    memory_ecc_dram_ue: int = 0
    cores_utilization: int = 42
    temperature: int = 55
    power_usage: int = 150
    node_affinity: tuple[int, ...] = (0b1,)  # NUMA node 0


@dataclass
class _DeviceProperty:
    """
    A c_mtmlDeviceProperty_t stand-in.

    Its real field names, deliberately: the removed filter read a `mpcCap`
    field that the struct does not have -- it carries `mpcCapability` and
    `mpcType` -- so on a card reporting virtRole HOST_VIRTDEVICE the read raised
    AttributeError and failed the whole detect pass.
    """

    virtCapability: int  # noqa: N815
    virtRole: int  # noqa: N815
    mpcCapability: int  # noqa: N815
    mpcType: int  # noqa: N815


@dataclass
class _PciInfo:
    """
    A c_mtmlPciInfo_t stand-in, carrying the fields the detector reads.
    """

    segment: int
    bus: int
    device: int


@dataclass
class _Handle:
    """
    A device/memory/GPU handle stand-in, carrying the card it belongs to.
    """

    index: int


@dataclass
class _FakeMTML:
    """
    A stand-in for the pymtml binding, recording every call the detector makes.

    The error type and the enumeration constants the detector legitimately needs
    are the real module's, so a fake drifting from the binding's contract fails
    here rather than on hardware. The virtRole and MPC constants are left out on
    purpose: a reintroduced virtRole filter then fails loudly with AttributeError
    instead of quietly reporting fewer cards. Entry points are dispatched by
    name, which keeps the driver's camelCase out of the handlers' own names.
    """

    cards: list[_Card]
    calls: list[str] = field(default_factory=list)

    MTMLError = pymtml.MTMLError
    MTML_MEMORY_ERROR_TYPE_UNCORRECTED = pymtml.MTML_MEMORY_ERROR_TYPE_UNCORRECTED
    MTML_VOLATILE_ECC = pymtml.MTML_VOLATILE_ECC
    MTML_MEMORY_LOCATION_DRAM = pymtml.MTML_MEMORY_LOCATION_DRAM

    def __getattr__(self, name: str):
        handler = {
            "mtmlLibraryInit": self._library_init,
            "mtmlLibraryInitSystem": self._library_init_system,
            "mtmlLibraryFreeSystem": self._library_free_system,
            "mtmlLibraryCountDevice": self._library_count_device,
            "mtmlLibraryInitDeviceByIndex": self._library_init_device_by_index,
            "mtmlLibraryFreeDevice": self._library_free_device,
            "mtmlSystemGetDriverVersion": self._system_get_driver_version,
            "mtmlDeviceGetProperty": self._device_get_property,
            "mtmlDeviceGetUUID": self._device_get_uuid,
            "mtmlDeviceGetName": self._device_get_name,
            "mtmlDeviceCountGpuCores": self._device_count_gpu_cores,
            "mtmlDeviceGetPciInfo": self._device_get_pci_info,
            "mtmlDeviceGetPowerUsage": self._device_get_power_usage,
            "mtmlDeviceGetMemoryAffinityWithinNode": self._device_get_memory_affinity,
            "mtmlMemoryContext": self._memory_context,
            "mtmlMemoryGetTotal": self._memory_get_total,
            "mtmlMemoryGetUsed": self._memory_get_used,
            "mtmlMemoryGetEccErrorCounter": self._memory_get_ecc_error_counter,
            "mtmlGpuContext": self._gpu_context,
            "mtmlGpuGetUtilization": self._gpu_get_utilization,
            "mtmlGpuGetTemperature": self._gpu_get_temperature,
        }.get(name)
        if handler is None:
            msg = f"module pymtml has no attribute {name}"
            raise AttributeError(msg)

        def entry_point(*args):
            self.calls.append(name)
            return handler(*args)

        return entry_point

    def _library_init(self) -> None:
        pass

    def _library_init_system(self) -> object:
        return object()

    def _library_free_system(self, system) -> None:
        pass

    def _library_count_device(self) -> int:
        return len(self.cards)

    def _library_init_device_by_index(self, index: int) -> _Handle:
        return _Handle(index=index)

    def _library_free_device(self, device: _Handle) -> None:
        pass

    def _system_get_driver_version(self, system) -> str:
        return "2.7.0"

    def _device_get_property(self, device: _Handle) -> _DeviceProperty:
        card = self.cards[device.index]
        return _DeviceProperty(
            virtCapability=0,
            virtRole=card.virt_role,
            mpcCapability=0,
            mpcType=card.mpc_type,
        )

    def _device_get_uuid(self, device: _Handle) -> str:
        return self.cards[device.index].uuid

    def _device_get_name(self, device: _Handle) -> str:
        return self.cards[device.index].name

    def _device_count_gpu_cores(self, device: _Handle) -> int:
        return self.cards[device.index].cores

    def _device_get_pci_info(self, device: _Handle) -> _PciInfo:
        return _PciInfo(segment=0, bus=self.cards[device.index].bus, device=0)

    def _device_get_power_usage(self, device: _Handle) -> int:
        return self.cards[device.index].power_usage

    def _device_get_memory_affinity(
        self,
        device: _Handle,
        node_set_size: int,
    ) -> list[int]:
        assert node_set_size > 0
        return list(self.cards[device.index].node_affinity)

    def _memory_context(self, device: _Handle):
        return contextlib.nullcontext(_Handle(index=device.index))

    def _memory_get_total(self, memory: _Handle) -> int:
        return self.cards[memory.index].memory_total

    def _memory_get_used(self, memory: _Handle) -> int:
        return self.cards[memory.index].memory_used

    def _memory_get_ecc_error_counter(
        self,
        memory: _Handle,
        error_type: int,
        counter_type: int,
        location_type: int,
    ) -> int:
        assert error_type == pymtml.MTML_MEMORY_ERROR_TYPE_UNCORRECTED
        assert counter_type == pymtml.MTML_VOLATILE_ECC
        assert location_type == pymtml.MTML_MEMORY_LOCATION_DRAM
        return self.cards[memory.index].memory_ecc_dram_ue

    def _gpu_context(self, device: _Handle):
        return contextlib.nullcontext(_Handle(index=device.index))

    def _gpu_get_utilization(self, gpu: _Handle) -> int:
        return self.cards[gpu.index].cores_utilization

    def _gpu_get_temperature(self, gpu: _Handle) -> int:
        return self.cards[gpu.index].temperature


_USAGE_CALLS = (
    "mtmlGpuContext",
    "mtmlGpuGetUtilization",
    "mtmlGpuGetTemperature",
    "mtmlDeviceGetPowerUsage",
)
"""
The calls the usage query owns, i.e. the ones the information query must not
make. Deliberately not the whole metric-looking surface: mtmlMemoryGetTotal
reads the memory *total*, which is inventory. MTML binds no power *limit* call
at all, so nothing of the sort stays behind in the information query.
"""

_MEMORY_UTILIZATION = 2.08
"""
A default card's memory utilization: 1024 MiB used of 49152 MiB total, as
get_utilization rounds it.
"""


@pytest.fixture
def detector(monkeypatch):
    """
    Build an MThreads detector talking to a fake driver reporting the given cards.
    """

    def _install(*cards: _Card) -> tuple[MThreadsDetector, _FakeMTML]:
        fake = _FakeMTML(cards=list(cards))
        monkeypatch.setattr(mthreads, "pymtml", fake)
        det = MThreadsDetector()
        # Shadowed on the instance, so the lru_cache'd static stays untouched.
        monkeypatch.setattr(det, "is_supported", lambda: True)
        return det, fake

    return _install


# --------------------------------------------------------------------------- #
# detect_info: no virtRole filter, no vgpu, no usage call.                     #
# --------------------------------------------------------------------------- #


def test_detect_info_reports_every_virt_role(detector):
    det, _ = detector(
        _Card(uuid="GPU-bare", virt_role=pymtml.MTML_VIRT_ROLE_NONE),
        _Card(
            uuid="GPU-host-virt",
            virt_role=pymtml.MTML_VIRT_ROLE_HOST_VIRTDEVICE,
            mpc_type=pymtml.MTML_MPC_TYPE_PARENT,  # i.e. != MPC_TYPE_INSTANCE
        ),
        _Card(
            uuid="GPU-guest-virt",
            virt_role=pymtml.MTML_VIRT_ROLE_GUEST_VIRTDEVICE,
        ),
    )

    devices = det.detect_info()

    # The host-virt row is the regression guard: the detector used to read a
    # `mpcCap` field that c_mtmlDeviceProperty_t does not carry, so such a card
    # raised AttributeError and failed the whole detect pass -- the worker saw
    # zero MThreads devices, not merely a missing one. The guest-virt row pins
    # the other half: no virtRole filter is applied at all, which is a deliberate
    # divergence from the operator, as it still drops GUEST_VIRTDEVICE.
    assert [dev.uuid for dev in devices] == [
        "GPU-bare",
        "GPU-host-virt",
        "GPU-guest-virt",
    ]
    assert [dev.index for dev in devices] == [0, 1, 2]


def test_detect_info_carries_the_inventory(detector):
    det, _ = detector(_Card(uuid="GPU-0"))

    dev = det.detect_info()[0]

    assert dev.manufacturer == ManufacturerEnum.MTHREADS
    assert dev.name == "MTT S4000"
    assert dev.uuid == "GPU-0"
    assert dev.driver_version == "2.7.0"
    assert dev.cores == 128
    assert dev.memory == 49152
    assert dev.memory_status == DeviceMemoryStatusEnum.HEALTHY
    assert dev.appendix["bdf"] == "0000:01:00.0"
    assert dev.appendix["numa"] == "0"
    # MTML binds no power *limit* call, so the field stays unset rather than
    # borrowing the power usage the usage query reads.
    assert dev.power is None
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
    # The property read went away with the filter it served, and nothing else
    # consumes it.
    assert "mtmlDeviceGetProperty" not in fake.calls
    assert "mtmlMemoryGetTotal" in fake.calls


def test_no_appendix_carries_vgpu(detector):
    det, _ = detector(
        _Card(uuid="GPU-host-virt", virt_role=pymtml.MTML_VIRT_ROLE_HOST_VIRTDEVICE),
        _Card(uuid="GPU-guest-virt", virt_role=pymtml.MTML_VIRT_ROLE_GUEST_VIRTDEVICE),
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
            cores_utilization=7,
            memory_used=2147483648,  # byte, i.e. 2048 MiB
            temperature=61,
            power_usage=200,
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
    assert by_uuid["GPU-0"].memory == 49152
    assert by_uuid["GPU-0"].cores == 128
    assert by_uuid["GPU-0"].name == "MTT S4000"


def test_detect_usage_detects_the_information_first(detector):
    det, _ = detector(_Card(uuid="GPU-0"))

    devices = det.detect_usage()

    assert [dev.uuid for dev in devices] == ["GPU-0"]
    assert devices[0].name == "MTT S4000"
    assert devices[0].memory == 49152
    assert devices[0].cores_utilization == 42


def test_detect_composes_both_queries(detector):
    det, _ = detector(_Card(uuid="GPU-0"))

    dev = det.detect()[0]

    assert dev.name == "MTT S4000"
    assert dev.memory == 49152
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
    det, _ = detector(_Card(uuid="GPU-0", memory_ecc_dram_ue=3))

    # Both queries report the health, mirroring the operator, which flags
    # Unhealthy from DetectAccelerator and MonitorAccelerator alike.
    assert det.detect_info()[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY
    assert det.detect()[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY
