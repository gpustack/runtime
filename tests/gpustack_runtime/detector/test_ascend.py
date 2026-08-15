from __future__ import annotations

import ctypes
from dataclasses import dataclass, field

import pytest

from gpustack_runtime import envs
from gpustack_runtime.deployer.cdi import ascend as cdi_ascend
from gpustack_runtime.deployer.cdi.ascend import AscendGenerator
from gpustack_runtime.detector import (
    Device,
    DeviceMemoryStatusEnum,
    ManufacturerEnum,
    ascend,
    pydcmi,
)
from gpustack_runtime.detector.__utils__ import get_utilization
from gpustack_runtime.detector.ascend import AscendDetector


@pytest.mark.skipif(
    not AscendDetector.is_supported(),
    reason="Ascend GPU not detected",
)
def test_detect():
    det = AscendDetector()
    devs = det.detect()
    print(devs)


@pytest.mark.skipif(
    not AscendDetector.is_supported(),
    reason="Ascend GPU not detected",
)
def test_get_topology():
    det = AscendDetector()
    topo = det.get_topology()
    print(topo)


# --------------------------------------------------------------------------- #
# ABI of the V1 structs, pinned against dcmi_interface_api.h.                 #
#                                                                             #
# No Ascend driver exists in CI, so nothing else would catch a mislaid field: #
# the numbers below are read off the header's declarations, not off the        #
# ctypes code they check.                                                     #
# --------------------------------------------------------------------------- #


def test_pcie_info_v1_struct_matches_the_header():
    # struct dcmi_pcie_info { unsigned int deviceid, venderid, subvenderid,
    # subdeviceid, bdf_deviceid, bdf_busid, bdf_funcid; }
    assert [f[0] for f in pydcmi.c_dcmi_pcie_info._fields_] == [
        "deviceid",
        "venderid",
        "subvenderid",
        "subdeviceid",
        "bdf_deviceid",
        "bdf_busid",
        "bdf_funcid",
    ]
    assert ctypes.sizeof(pydcmi.c_dcmi_pcie_info) == 7 * 4
    for offset, name in enumerate(
        [f[0] for f in pydcmi.c_dcmi_pcie_info._fields_],
    ):
        assert getattr(pydcmi.c_dcmi_pcie_info, name).offset == offset * 4
    # The V1 struct carries no PCI domain, where the V2 one does. The asymmetry
    # is why the detector formats a domain of 0 on the fallback path.
    assert not hasattr(pydcmi.c_dcmi_pcie_info, "domain")
    assert hasattr(pydcmi.c_dcmi_pcie_info_all, "domain")


def test_memory_info_v2_struct_matches_the_header():
    # struct dcmi_memory_info { unsigned long long memory_size; unsigned int
    # freq; unsigned int utiliza; }
    assert [f[0] for f in pydcmi.c_dcmi_memory_info._fields_] == [
        "memory_size",
        "freq",
        "utiliza",
    ]
    assert pydcmi.c_dcmi_memory_info.memory_size.offset == 0
    assert pydcmi.c_dcmi_memory_info.memory_size.size == 8
    assert pydcmi.c_dcmi_memory_info.freq.offset == 8
    assert pydcmi.c_dcmi_memory_info.utiliza.offset == 12
    assert ctypes.sizeof(pydcmi.c_dcmi_memory_info) == 16
    # Unlike V3, it reports no available memory at all, which is why the used
    # memory has to be derived from the utilization percentage.
    assert not hasattr(pydcmi.c_dcmi_memory_info, "memory_available")


def test_die_id_struct_matches_the_header():
    # The V1 call fills a struct dcmi_soc_die_stru { unsigned int soc_die[5]; },
    # laid out exactly as the dcmi_die_id the V2 call fills.
    assert ctypes.sizeof(pydcmi.c_dcmi_die_id) == 5 * 4
    assert pydcmi.c_dcmi_die_id.soc_die.offset == 0


# --------------------------------------------------------------------------- #
# A fake pydcmi carrying a call log, so the type filter, the V1 fallbacks and #
# the information/usage split are provable on a host with no DCMI driver.     #
# --------------------------------------------------------------------------- #


@dataclass
class _Unit:
    """
    One device inside a dcmi card, as the driver would report it.

    The per-generation values are deliberately distinct, so a test can tell
    which call a returned value came from.
    """

    card_id: int = 0
    device_id: int = 0
    unit_type: int = pydcmi.DCMI_UNIT_TYPE_NPU
    unit_type_readable: bool = True
    logic_id: int = 0
    physical_id: int = 0
    physical_id_readable: bool = True
    aicore_cnt: int = 20
    ecc_errors: int = 0
    cores_utilization: int = 42
    temperature: int = 55
    power_deciwatts: int = 1234
    # Which generation of calls the driver exposes.
    v2_die: bool = True
    v2_pcie: bool = True
    v2_chip_info: bool = True
    hbm: bool = True
    v3_memory: bool = True

    @property
    def v2_die_id(self) -> str:
        return f"1a 2b 3c 4d {self.logic_id:x}"

    @property
    def v1_die_id(self) -> str:
        return f"9f 8e 7d 6c {self.logic_id:x}"

    @property
    def v2_chip_name(self) -> str:
        return "910B3"

    @property
    def v1_chip_name(self) -> str:
        return "910A"

    @property
    def v2_bdf(self) -> str:
        return f"0001:{0x10 + self.logic_id:02x}:00.0"

    @property
    def v1_bdf(self) -> str:
        # The V1 struct has no domain, so the detector formats it as zero.
        return f"0000:{0x20 + self.logic_id:02x}:00.0"


class _FakeStruct:
    """
    A stand-in for a ctypes struct the binding would return.
    """

    def __init__(self, **fields):
        self.__dict__.update(fields)


@dataclass
class _FakeDCMI:
    """
    A stand-in for the pydcmi binding, recording every call the detector makes.

    The error type and the enumeration constants are the real module's, so a
    fake drifting from the binding's contract fails here rather than on
    hardware. Entry points are dispatched by name, as test_metax.py's fake
    does, which keeps the driver's own naming out of the handlers' names.

    The vdev (vNPU) constants are deliberately *not* exposed: a reintroduced
    vNPU branch fails here with an AttributeError rather than passing silently.
    """

    units: list[_Unit] = field(default_factory=lambda: [_Unit()])
    calls: list[str] = field(default_factory=list)

    DCMIError = pydcmi.DCMIError
    DCMI_UNIT_TYPE_NPU = pydcmi.DCMI_UNIT_TYPE_NPU
    DCMI_UNIT_TYPE_MCU = pydcmi.DCMI_UNIT_TYPE_MCU
    DCMI_DIE_TYPE_VDIE = pydcmi.DCMI_DIE_TYPE_VDIE
    DCMI_DEVICE_TYPE_HBM = pydcmi.DCMI_DEVICE_TYPE_HBM
    DCMI_DEVICE_TYPE_DDR = pydcmi.DCMI_DEVICE_TYPE_DDR
    DCMI_INPUT_TYPE_AICORE = pydcmi.DCMI_INPUT_TYPE_AICORE
    DCMI_PORT_TYPE_ROCE_PORT = pydcmi.DCMI_PORT_TYPE_ROCE_PORT
    DCMI_ERROR_FUNCTION_NOT_FOUND = pydcmi.DCMI_ERROR_FUNCTION_NOT_FOUND
    DCMI_ERROR_NOT_SUPPORT = pydcmi.DCMI_ERROR_NOT_SUPPORT
    DCMI_ERROR_NOT_SUPPORT_IN_CONTAINER = pydcmi.DCMI_ERROR_NOT_SUPPORT_IN_CONTAINER

    # Memory, in MiB, per generation of the memory calls.
    hbm_size: int = 65536
    hbm_usage: int = 1024
    v3_size: int = 32768
    v3_available: int = 24576
    v2_size: int = 16384
    v2_utiliza: int = 25

    def __getattr__(self, name: str):
        handler = {
            "dcmi_init": self._init,
            "dcmi_get_driver_version": self._get_driver_version,
            "dcmi_get_card_list": self._get_card_list,
            "dcmi_get_device_num_in_card": self._get_device_num_in_card,
            "dcmi_get_device_type": self._get_device_type,
            "dcmi_get_device_die_v2": self._get_device_die_v2,
            "dcmi_get_device_die": self._get_device_die,
            "dcmi_get_device_chip_info_v2": self._get_device_chip_info_v2,
            "dcmi_get_device_chip_info": self._get_device_chip_info,
            "dcmi_get_device_hbm_info": self._get_device_hbm_info,
            "dcmi_get_device_memory_info_v3": self._get_device_memory_info_v3,
            "dcmi_get_device_memory_info_v2": self._get_device_memory_info_v2,
            "dcmi_get_device_ecc_info": self._get_device_ecc_info,
            "dcmi_get_device_logic_id": self._get_device_logic_id,
            "dcmi_get_device_phyid_from_logicid": self._get_phyid_from_logicid,
            "dcmi_get_device_bdf": self._get_device_bdf,
            "dcmi_get_device_pcie_info": self._get_device_pcie_info,
            "dcmi_get_device_ip": self._get_device_ip,
            "dcmi_get_affinity_cpu_info_by_device_id": self._get_affinity_cpu_info,
            "dcmi_get_device_utilization_rate": self._get_device_utilization_rate,
            "dcmi_get_device_temperature": self._get_device_temperature,
            "dcmi_get_device_power_info": self._get_device_power_info,
        }.get(name)
        if handler is None:
            msg = f"module pydcmi has no attribute {name}"
            raise AttributeError(msg)

        def entry_point(*args):
            self.calls.append(name)
            return handler(*args)

        return entry_point

    def _unit(self, card_id: int, device_id: int) -> _Unit:
        for unit in self.units:
            if unit.card_id == card_id and unit.device_id == device_id:
                return unit
        msg = f"no such device: card {card_id}, device {device_id}"
        raise AssertionError(msg)

    def _unsupported(self) -> DCMIError:
        return self.DCMIError(self.DCMI_ERROR_NOT_SUPPORT)

    def _init(self) -> None:
        pass

    def _get_driver_version(self) -> str:
        return "24.1.0"

    def _get_card_list(self) -> tuple[int, list[int]]:
        cards = sorted({unit.card_id for unit in self.units})
        return len(cards), cards

    def _get_device_num_in_card(self, card_id: int) -> int:
        return len([unit for unit in self.units if unit.card_id == card_id])

    def _get_device_type(self, card_id: int, device_id: int) -> int:
        unit = self._unit(card_id, device_id)
        if not unit.unit_type_readable:
            raise self._unsupported()
        return unit.unit_type

    def _get_device_die_v2(self, card_id: int, device_id: int, input_type: int) -> str:
        assert input_type == pydcmi.DCMI_DIE_TYPE_VDIE
        unit = self._unit(card_id, device_id)
        if not unit.v2_die:
            raise self.DCMIError(self.DCMI_ERROR_FUNCTION_NOT_FOUND)
        return unit.v2_die_id

    def _get_device_die(self, card_id: int, device_id: int) -> str:
        return self._unit(card_id, device_id).v1_die_id

    def _get_device_chip_info_v2(self, card_id: int, device_id: int) -> _FakeStruct:
        unit = self._unit(card_id, device_id)
        if not unit.v2_chip_info:
            raise self.DCMIError(self.DCMI_ERROR_FUNCTION_NOT_FOUND)
        return _FakeStruct(chip_name=unit.v2_chip_name, aicore_cnt=unit.aicore_cnt)

    def _get_device_chip_info(self, card_id: int, device_id: int) -> _FakeStruct:
        unit = self._unit(card_id, device_id)
        return _FakeStruct(chip_name=unit.v1_chip_name, aicore_cnt=unit.aicore_cnt)

    def _get_device_hbm_info(self, card_id: int, device_id: int) -> _FakeStruct:
        unit = self._unit(card_id, device_id)
        if not unit.hbm:
            raise self._unsupported()
        return _FakeStruct(memory_size=self.hbm_size, memory_usage=self.hbm_usage)

    def _get_device_memory_info_v3(self, card_id: int, device_id: int) -> _FakeStruct:
        unit = self._unit(card_id, device_id)
        if not unit.v3_memory:
            raise self.DCMIError(self.DCMI_ERROR_FUNCTION_NOT_FOUND)
        return _FakeStruct(
            memory_size=self.v3_size,
            memory_available=self.v3_available,
        )

    def _get_device_memory_info_v2(self, card_id: int, device_id: int) -> _FakeStruct:
        return _FakeStruct(memory_size=self.v2_size, utiliza=self.v2_utiliza)

    def _get_device_ecc_info(
        self,
        card_id: int,
        device_id: int,
        device_type: int,
    ) -> _FakeStruct:
        unit = self._unit(card_id, device_id)
        return _FakeStruct(
            enable_flag=1,
            single_bit_error_cnt=unit.ecc_errors,
            double_bit_error_cnt=0,
        )

    def _get_device_logic_id(self, card_id: int, device_id: int) -> int:
        return self._unit(card_id, device_id).logic_id

    def _get_phyid_from_logicid(self, logic_id: int) -> int:
        for unit in self.units:
            if unit.logic_id == logic_id:
                if not unit.physical_id_readable:
                    raise self.DCMIError(self.DCMI_ERROR_FUNCTION_NOT_FOUND)
                return unit.physical_id
        msg = f"no such logic id: {logic_id}"
        raise AssertionError(msg)

    def _get_device_bdf(self, card_id: int, device_id: int) -> str:
        unit = self._unit(card_id, device_id)
        if not unit.v2_pcie:
            # As the real wrapper does: it is built on the V2 call.
            raise self.DCMIError(self.DCMI_ERROR_FUNCTION_NOT_FOUND)
        return unit.v2_bdf

    def _get_device_pcie_info(self, card_id: int, device_id: int) -> _FakeStruct:
        unit = self._unit(card_id, device_id)
        return _FakeStruct(
            bdf_busid=0x20 + unit.logic_id,
            bdf_deviceid=0,
            bdf_funcid=0,
        )

    def _get_device_ip(self, card_id: int, device_id: int, port_type: int) -> None:
        raise self._unsupported()

    def _get_affinity_cpu_info(self, card_id: int, device_id: int) -> None:
        raise self._unsupported()

    def _get_device_utilization_rate(
        self,
        card_id: int,
        device_id: int,
        input_type: int,
    ) -> int:
        assert input_type == pydcmi.DCMI_INPUT_TYPE_AICORE
        return self._unit(card_id, device_id).cores_utilization

    def _get_device_temperature(self, card_id: int, device_id: int) -> int:
        return self._unit(card_id, device_id).temperature

    def _get_device_power_info(self, card_id: int, device_id: int) -> int:
        return self._unit(card_id, device_id).power_deciwatts


_USAGE_ONLY_CALLS = {
    "dcmi_get_device_utilization_rate",
    "dcmi_get_device_temperature",
    "dcmi_get_device_power_info",
}


@pytest.fixture(autouse=True)
def _reset_is_supported_cache():
    # is_supported()/detect_pci_devices() are lru_cache'd, so a value
    # observed by one test would otherwise leak into the next.
    AscendDetector.is_supported.cache_clear()
    AscendDetector.detect_pci_devices.cache_clear()
    yield
    AscendDetector.is_supported.cache_clear()
    AscendDetector.detect_pci_devices.cache_clear()


@pytest.fixture
def fake_pydcmi(monkeypatch):
    def _install(units: list[_Unit] | None = None, **kwargs) -> _FakeDCMI:
        fake = _FakeDCMI(units=units if units is not None else [_Unit()], **kwargs)
        monkeypatch.setattr(ascend, "pydcmi", fake)
        # No PCI sysfs tree exists on the dev machine, so bypass the PCI
        # presence check that is_supported() otherwise gates on.
        monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK", True)
        return fake

    return _install


# --------------------------------------------------------------------------- #
# detect_info: the NPU type filter.                                           #
# --------------------------------------------------------------------------- #


def test_detect_info_skips_a_non_npu_device(fake_pydcmi):
    fake = fake_pydcmi(
        [
            _Unit(card_id=0, device_id=0, logic_id=0, physical_id=0),
            _Unit(
                card_id=0,
                device_id=1,
                logic_id=1,
                physical_id=1,
                unit_type=pydcmi.DCMI_UNIT_TYPE_MCU,
            ),
        ],
    )

    devices = AscendDetector().detect_info()

    assert [dev.index for dev in devices] == [0]
    # The skipped unit is never queried any further.
    assert fake.calls.count("dcmi_get_device_die_v2") == 1


def test_detect_info_keeps_a_device_whose_type_is_unreadable(fake_pydcmi):
    # The operator skips a device only when the type call *succeeds* and
    # reports something other than an NPU.
    fake_pydcmi([_Unit(unit_type_readable=False)])

    devices = AscendDetector().detect_info()

    assert [dev.index for dev in devices] == [0]


# --------------------------------------------------------------------------- #
# detect_info: the V1 fallbacks, one test each, on value and call log.        #
# --------------------------------------------------------------------------- #


def test_detect_info_prefers_the_v2_calls(fake_pydcmi):
    fake = fake_pydcmi()
    unit = fake.units[0]

    devices = AscendDetector().detect_info()

    assert devices[0].uuid == unit.v2_die_id.upper()
    assert devices[0].name == unit.v2_chip_name
    assert devices[0].appendix["bdf"] == unit.v2_bdf
    assert devices[0].memory == fake.hbm_size
    for v1_call in (
        "dcmi_get_device_die",
        "dcmi_get_device_chip_info",
        "dcmi_get_device_pcie_info",
        "dcmi_get_device_memory_info_v3",
        "dcmi_get_device_memory_info_v2",
    ):
        assert v1_call not in fake.calls


def test_detect_info_falls_back_to_the_v1_die(fake_pydcmi):
    fake = fake_pydcmi([_Unit(v2_die=False)])

    devices = AscendDetector().detect_info()

    assert devices[0].uuid == fake.units[0].v1_die_id.upper()
    assert "dcmi_get_device_die_v2" in fake.calls
    assert "dcmi_get_device_die" in fake.calls


def test_detect_info_falls_back_to_the_v1_pcie_info(fake_pydcmi):
    fake = fake_pydcmi([_Unit(v2_pcie=False)])

    devices = AscendDetector().detect_info()

    # The V1 struct carries no domain, so it reads as 0000.
    assert devices[0].appendix["bdf"] == fake.units[0].v1_bdf
    assert "dcmi_get_device_bdf" in fake.calls
    assert "dcmi_get_device_pcie_info" in fake.calls


def test_detect_info_falls_back_to_the_v1_chip_info(fake_pydcmi):
    fake = fake_pydcmi([_Unit(v2_chip_info=False)])

    devices = AscendDetector().detect_info()

    assert devices[0].name == fake.units[0].v1_chip_name
    assert devices[0].cores == fake.units[0].aicore_cnt
    assert "dcmi_get_device_chip_info_v2" in fake.calls
    assert "dcmi_get_device_chip_info" in fake.calls


def test_detect_info_falls_back_to_the_v3_memory_when_hbm_unavailable(fake_pydcmi):
    fake = fake_pydcmi([_Unit(hbm=False)])

    devices = AscendDetector().detect_info()

    assert devices[0].memory == fake.v3_size
    assert "dcmi_get_device_memory_info_v3" in fake.calls
    assert "dcmi_get_device_memory_info_v2" not in fake.calls


def test_detect_info_falls_back_to_the_v2_memory_when_v3_unavailable(fake_pydcmi):
    fake = fake_pydcmi([_Unit(hbm=False, v3_memory=False)])

    devices = AscendDetector().detect_info()

    assert devices[0].memory == fake.v2_size
    assert "dcmi_get_device_memory_info_v2" in fake.calls


def test_detect_info_yields_a_device_on_a_v1_only_driver(fake_pydcmi):
    # The whole point of the fallbacks: a driver exposing none of the newer
    # calls still detects.
    fake = fake_pydcmi(
        [
            _Unit(
                v2_die=False,
                v2_pcie=False,
                v2_chip_info=False,
                hbm=False,
                v3_memory=False,
            ),
        ],
    )
    unit = fake.units[0]

    devices = AscendDetector().detect_info()

    assert len(devices) == 1
    assert devices[0].uuid == unit.v1_die_id.upper()
    assert devices[0].name == unit.v1_chip_name
    assert devices[0].appendix["bdf"] == unit.v1_bdf
    assert devices[0].memory == fake.v2_size


# --------------------------------------------------------------------------- #
# detect_info: no vGPU, no usage calls.                                       #
# --------------------------------------------------------------------------- #


def test_detect_info_has_no_vgpu_in_appendix(fake_pydcmi):
    fake_pydcmi()

    devices = AscendDetector().detect_info()

    assert all("vgpu" not in dev.appendix for dev in devices)


def test_detect_info_keeps_the_physical_id_appendix(fake_pydcmi):
    fake_pydcmi([_Unit(logic_id=1, physical_id=6)])

    devices = AscendDetector().detect_info()

    assert devices[0].index == 1
    assert devices[0].appendix["physical_id"] == 6


def test_detect_info_skips_a_device_without_a_readable_physical_id(fake_pydcmi):
    # /dev/davinciN is numbered by the physical id, so a device whose physical
    # id the driver will not answer cannot be addressed. Reporting it would
    # offer an NPU that no CDI spec can reference, and standing the logic id in
    # for the physical id would reference another NPU, so it is dropped -- as
    # the operator's ascend/device.go does on a failed GetPhysicalID.
    fake_pydcmi(
        [
            _Unit(card_id=0, device_id=0, logic_id=0, physical_id=0),
            _Unit(
                card_id=1,
                device_id=0,
                logic_id=1,
                physical_id=7,
                physical_id_readable=False,
            ),
        ],
    )

    devices = AscendDetector().detect_info()

    assert [dev.index for dev in devices] == [0]


def test_detect_info_issues_no_usage_calls(fake_pydcmi):
    fake = fake_pydcmi()

    AscendDetector().detect_info()

    assert _USAGE_ONLY_CALLS.isdisjoint(fake.calls)


# --------------------------------------------------------------------------- #
# detect_usage: merged by uuid, memory_status recomputed.                     #
# --------------------------------------------------------------------------- #


def test_detect_usage_merges_the_usage_fields_by_uuid(fake_pydcmi):
    fake = fake_pydcmi()
    unit = fake.units[0]

    devices = AscendDetector().detect_info()
    result = AscendDetector().detect_usage(devices)

    assert result is devices
    assert devices[0].cores_utilization == unit.cores_utilization
    assert devices[0].memory_used == fake.hbm_usage
    assert devices[0].memory_utilization == get_utilization(
        fake.hbm_usage,
        fake.hbm_size,
    )
    assert devices[0].temperature == unit.temperature
    assert devices[0].power_used == unit.power_deciwatts / 10


def test_detect_usage_recomputes_the_memory_status(fake_pydcmi, monkeypatch):
    # merge_devices_usage overwrites memory_status, so the usage pass has to
    # produce it as well or the inventory pass's verdict is lost.
    fake_pydcmi([_Unit(ecc_errors=3)])
    # The ECC read costs a driver call per device, so the health check is off
    # by default.
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK", False)

    devices = AscendDetector().detect_info()
    assert devices[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY

    AscendDetector().detect_usage(devices)

    assert devices[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY


def test_detect_usage_keeps_a_healthy_status_healthy(fake_pydcmi):
    fake_pydcmi()

    devices = AscendDetector().detect_usage(AscendDetector().detect_info())

    assert devices[0].memory_status == DeviceMemoryStatusEnum.HEALTHY


def test_detect_usage_skips_a_non_npu_device(fake_pydcmi):
    fake = fake_pydcmi(
        [
            _Unit(card_id=0, device_id=0, logic_id=0, physical_id=0),
            _Unit(
                card_id=0,
                device_id=1,
                logic_id=1,
                physical_id=1,
                unit_type=pydcmi.DCMI_UNIT_TYPE_MCU,
            ),
        ],
    )

    devices = AscendDetector().detect_info()
    fake.calls.clear()
    AscendDetector().detect_usage(devices)

    # The filter applies to the usage pass too, as it does in the operator's
    # MonitorAccelerator.
    assert fake.calls.count("dcmi_get_device_utilization_rate") == 1


def test_detect_usage_falls_back_to_the_v2_memory_when_v3_unavailable(fake_pydcmi):
    fake = fake_pydcmi([_Unit(hbm=False, v3_memory=False)])

    devices = AscendDetector().detect_usage(AscendDetector().detect_info())

    # The V2 struct reports no available memory, so the used memory comes from
    # its utilization percentage -- the operator instead subtracts an
    # always-zero available figure and so reports the card as fully used.
    assert devices[0].memory_used == fake.v2_size * fake.v2_utiliza // 100
    assert devices[0].memory_used != fake.v2_size


def test_detect_composes_info_and_usage_by_default(fake_pydcmi):
    fake = fake_pydcmi()

    devices = AscendDetector().detect()

    assert devices[0].cores_utilization == fake.units[0].cores_utilization
    assert devices[0].power_used == fake.units[0].power_deciwatts / 10
    assert "vgpu" not in devices[0].appendix


def test_detect_without_usage_issues_no_usage_calls(fake_pydcmi):
    fake = fake_pydcmi()

    devices = AscendDetector().detect(usage=False)

    assert len(devices) == 1
    assert _USAGE_ONLY_CALLS.isdisjoint(fake.calls)


# --------------------------------------------------------------------------- #
# CDI: /dev/davinci{N} only, from the appendix physical id.                   #
# --------------------------------------------------------------------------- #


def test_cdi_emits_no_vdavinci_path(monkeypatch):
    seen_paths: list[str] = []

    def _fake_device_node(path, **_kwargs):
        seen_paths.append(path)
        return {"path": path}

    monkeypatch.setattr(cdi_ascend, "device_to_cdi_device_node", _fake_device_node)
    monkeypatch.setattr(cdi_ascend, "path_to_cdi_mount", lambda **_kwargs: None)

    devices = [
        # A stale vgpu appendix must not resurrect the /dev/vdavinci path.
        Device(
            manufacturer=ManufacturerEnum.ASCEND,
            index=0,
            name="910B3",
            uuid="DIE-0",
            memory=65536,
            appendix={"card_id": 0, "device_id": 0, "physical_id": 3, "vgpu": True},
        ),
        Device(
            manufacturer=ManufacturerEnum.ASCEND,
            index=1,
            name="910B3",
            uuid="DIE-1",
            memory=65536,
            appendix={"card_id": 1, "device_id": 0},
        ),
    ]

    config = AscendGenerator().generate(devices)

    assert config is not None
    assert not any("vdavinci" in path for path in seen_paths)
    # The device carrying a physical id is addressed by it.
    assert "/dev/davinci3" in seen_paths
    # The device without one is skipped: Device.index is the logic id, so
    # standing it in for the physical id would address another NPU's node.
    assert "/dev/davinci1" not in seen_paths
