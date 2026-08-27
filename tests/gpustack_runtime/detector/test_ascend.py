from __future__ import annotations

import ctypes
from dataclasses import dataclass, field

import pytest

from gpustack_runtime import envs
from gpustack_runtime.deployer.cdi import __utils__ as cdi_utils
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

    def dcmi_api_version(self):
        # Defined on the class rather than dispatched through __getattr__, so
        # that it stays out of the call log the usage/information split is
        # asserted against. This fake serves the V1 API.
        return 1

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


# --------------------------------------------------------------------------- #
# SoC naming: the A5 generation names itself apart from its predecessors.     #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "dev_name, soc_name, variant",
    [
        # An A5 chip keeps the "Ascend" prefix that the earlier generations
        # drop, and is reported either way round.
        ("Ascend950PR", "Ascend950PR", "950"),
        ("950PR", "Ascend950PR", "950"),
        # A 950 variant the mapping does not carry yet still belongs to the
        # generation rather than falling through to nothing.
        ("Ascend950DT", "Ascend950", "950"),
        # The generations that already worked, both where the mapping answers
        # directly and where the regexes have to: the A5 prefix must claim
        # neither.
        ("910B4", "Ascend910B4", "910b"),
        ("910B9", "Ascend910B1", "910b"),
        ("310P3", "Ascend310P3", "310p"),
        ("310P9", "Ascend310P1", "310p"),
        ("910_9391", "Ascend910_9391", "a3"),
        ("910A", "Ascend910A", "910"),
    ],
)
def test_guess_soc_name_carries_the_generation(dev_name, soc_name, variant):
    assert ascend._guess_soc_name_from_dev_name(dev_name) == soc_name  # noqa: SLF001
    assert ascend.get_ascend_cann_variant(soc_name) == variant


def test_guess_soc_name_still_yields_nothing_for_an_unknown_chip():
    assert ascend._guess_soc_name_from_dev_name("NotAnAscendChip") is None  # noqa: SLF001
    assert ascend.get_ascend_cann_variant(None) is None


# --------------------------------------------------------------------------- #
# CDI: the A5 UB fabric, which the earlier generations have no part of.       #
# --------------------------------------------------------------------------- #


def test_cdi_enumerates_a_device_directory(tmp_path, monkeypatch):
    ub_dir = tmp_path / "uburma"
    ub_dir.mkdir()
    (ub_dir / "uburma0").touch()
    (ub_dir / "uburma1").touch()
    (ub_dir / "nested").mkdir()

    monkeypatch.setattr(
        cdi_utils,
        "device_to_cdi_device_node",
        lambda path, **_kwargs: {"path": path},
    )

    nodes = cdi_utils.path_to_cdi_device_nodes(path=str(ub_dir))

    assert [n["path"] for n in nodes] == [
        str(ub_dir / "uburma0"),
        str(ub_dir / "uburma1"),
    ]


def test_cdi_still_accepts_a_plain_device_node(tmp_path, monkeypatch):
    dev = tmp_path / "hisi_hdc"
    dev.touch()

    monkeypatch.setattr(
        cdi_utils,
        "device_to_cdi_device_node",
        lambda path, **_kwargs: {"path": path},
    )

    nodes = cdi_utils.path_to_cdi_device_nodes(path=str(dev))

    assert [n["path"] for n in nodes] == [str(dev)]


def test_cdi_holds_the_ub_mounts_back_from_an_earlier_generation(monkeypatch):
    patterns = _collect_ub_mount_patterns(monkeypatch, arch_family="Ascend910B4")

    # libnl and friends are ordinary system libraries: mounting them here would
    # shadow what a 910B container ships with its own image.
    assert patterns == []


def test_cdi_mounts_the_ub_libraries_for_the_a5_generation(monkeypatch):
    patterns = _collect_ub_mount_patterns(monkeypatch, arch_family="Ascend950PR")

    assert any("liburma" in p for p in patterns)
    assert any("libummu" in p for p in patterns)
    assert any("libnl" in p for p in patterns)


def test_cdi_holds_the_ub_mounts_back_without_an_arch_family(monkeypatch):
    # A device detected by an older runtime carries no arch_family at all,
    # which must not be read as the A5 generation.
    patterns = _collect_ub_mount_patterns(monkeypatch, arch_family=None)

    assert patterns == []


def test_cdi_survives_a_device_carrying_no_appendix(monkeypatch):
    # Device.appendix defaults to None, so a bare Device would crash on .get()
    # before reaching the guard meant to skip it.
    monkeypatch.setattr(
        cdi_ascend,
        "device_to_cdi_device_node",
        lambda path, **_kwargs: {"path": path},
    )
    monkeypatch.setattr(
        cdi_ascend,
        "path_to_cdi_device_nodes",
        lambda path, **_kwargs: [{"path": path}],
    )
    monkeypatch.setattr(cdi_ascend, "path_to_cdi_mount", lambda **_kwargs: None)
    monkeypatch.setattr(cdi_ascend, "glob_to_cdi_mounts", lambda **_kwargs: [])

    devices = [
        Device(
            manufacturer=ManufacturerEnum.ASCEND,
            index=0,
            name="chip",
            uuid="DIE-0",
            memory=65536,
        ),
    ]

    # No physical id, so nothing is generated -- the guard, not a crash.
    assert AscendGenerator().generate(devices) is None


def _collect_ub_mount_patterns(monkeypatch, arch_family: str | None) -> list[str]:
    """
    Run the CDI generator over one device and report the globbed mount patterns.
    """
    seen: list[str] = []

    monkeypatch.setattr(
        cdi_ascend,
        "device_to_cdi_device_node",
        lambda path, **_kwargs: {"path": path},
    )
    monkeypatch.setattr(
        cdi_ascend,
        "path_to_cdi_device_nodes",
        lambda path, **_kwargs: [{"path": path}],
    )
    monkeypatch.setattr(cdi_ascend, "path_to_cdi_mount", lambda **_kwargs: None)
    monkeypatch.setattr(
        cdi_ascend,
        "glob_to_cdi_mounts",
        lambda pattern, **_kwargs: seen.append(pattern) or [],
    )

    appendix = {"card_id": 0, "device_id": 0, "physical_id": 0}
    if arch_family is not None:
        appendix["arch_family"] = arch_family

    devices = [
        Device(
            manufacturer=ManufacturerEnum.ASCEND,
            index=0,
            name="chip",
            uuid="DIE-0",
            memory=65536,
            appendix=appendix,
        ),
    ]

    assert AscendGenerator().generate(devices) is not None

    return seen


# --------------------------------------------------------------------------- #
# is_supported: a driver serving the V2 API only, as the A5 generation does.  #
# --------------------------------------------------------------------------- #


@dataclass
class _FakeInitOnlyDCMI:
    """
    A stand-in exposing only the initialization entry points, recording which
    of them the detector reaches for.
    """

    v1_error: Exception | None = None
    v2_error: Exception | None = None
    calls: list[str] = field(default_factory=list)

    DCMIError = pydcmi.DCMIError

    def dcmi_init(self):
        self.calls.append("dcmi_init")
        if self.v1_error is not None:
            raise self.v1_error

    def dcmiv2_init(self):
        self.calls.append("dcmiv2_init")
        if self.v2_error is not None:
            raise self.v2_error

    def dcmi_library_path(self):
        return "/usr/local/dcmi/libdcmi.so"


def _install_init_only_dcmi(monkeypatch, **kwargs) -> _FakeInitOnlyDCMI:
    fake = _FakeInitOnlyDCMI(**kwargs)
    monkeypatch.setattr(ascend, "pydcmi", fake)
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK", True)
    return fake


def test_is_supported_falls_back_to_the_v2_api(monkeypatch):
    # -8255 is what a V2-only driver answers the V1 entry point with: a
    # statement about the API, not about the hardware.
    fake = _install_init_only_dcmi(
        monkeypatch,
        v1_error=pydcmi.DCMIError(pydcmi.DCMI_ERROR_NOT_SUPPORT),
    )

    assert AscendDetector.is_supported() is True
    assert fake.calls == ["dcmi_init", "dcmiv2_init"]


def test_is_supported_leaves_the_v2_api_alone_when_v1_answers(monkeypatch):
    # The generations that already worked must not pay for the fallback.
    fake = _install_init_only_dcmi(monkeypatch)

    assert AscendDetector.is_supported() is True
    assert fake.calls == ["dcmi_init"]


def test_is_supported_stays_false_when_neither_api_initializes(monkeypatch):
    fake = _install_init_only_dcmi(
        monkeypatch,
        v1_error=pydcmi.DCMIError(pydcmi.DCMI_ERROR_NOT_SUPPORT),
        v2_error=pydcmi.DCMIError(pydcmi.DCMI_ERROR_FUNCTION_NOT_FOUND),
    )

    assert AscendDetector.is_supported() is False
    assert fake.calls == ["dcmi_init", "dcmiv2_init"]


# --------------------------------------------------------------------------- #
# A fake driver serving the V2 API only, as an A5 host's does.                #
#                                                                             #
# Every V1 entry point refuses with DCMI_ERROR_NOT_SUPPORT, which is what the #
# 950PR host reported for dcmi_init and dcmi_get_driver_version alike -- so a #
# V2 path that slips back into a V1 call fails here, not on hardware.         #
# --------------------------------------------------------------------------- #


@dataclass
class _UnitV2:
    """
    One device as the V2 API reports it: flat, indexed by its logic id.
    """

    dev_id: int = 0
    unit_type: int = pydcmi.DCMI_UNIT_TYPE_NPU
    unit_type_readable: bool = True
    phy_id: int = 0
    phy_id_readable: bool = True
    chip_name: str = "Ascend950PR"
    aicore_cnt: int = 32
    hbm_size: int = 131072
    hbm_usage: int = 5225
    cores_utilization: int = 17
    temperature: int = 51
    power_deciwatts: int = 2012
    ecc_errors: int = 0
    # Which die types the driver answers for; the rest raise. A real 950PR
    # answers neither, which the die-fallback tests set explicitly.
    die_types: tuple[int, ...] = (pydcmi.DCMI_DIE_TYPE_VDIE,)

    @property
    def die_id(self) -> str:
        return f"5a 6b 7c 8d {self.dev_id:x}"

    @property
    def bdf(self) -> str:
        return f"0000:{0x01 + self.dev_id * 0x10:02x}:00.0"


@dataclass
class _FakeDCMIV2:
    """
    A stand-in for the pydcmi binding on a V2-only driver.
    """

    units: list[_UnitV2] = field(default_factory=lambda: [_UnitV2()])
    calls: list[str] = field(default_factory=list)
    dcmi_version: str | None = "25.7.rc1.6"
    # As the real binding: 1 until dcmiv2_init() succeeds. Hardcoding 2 would
    # make "caller never initialized" untestable.
    api_version: int = 1

    DCMIError = pydcmi.DCMIError
    DCMI_UNIT_TYPE_NPU = pydcmi.DCMI_UNIT_TYPE_NPU
    DCMI_UNIT_TYPE_MCU = pydcmi.DCMI_UNIT_TYPE_MCU
    DCMI_DIE_TYPE_VDIE = pydcmi.DCMI_DIE_TYPE_VDIE
    DCMI_DIE_TYPE_NDIE = pydcmi.DCMI_DIE_TYPE_NDIE
    DCMI_DEVICE_TYPE_HBM = pydcmi.DCMI_DEVICE_TYPE_HBM
    DCMI_DEVICE_TYPE_DDR = pydcmi.DCMI_DEVICE_TYPE_DDR
    DCMI_INPUT_TYPE_AICORE = pydcmi.DCMI_INPUT_TYPE_AICORE
    DCMI_ERROR_NOT_SUPPORT = pydcmi.DCMI_ERROR_NOT_SUPPORT
    DCMI_ERROR_FUNCTION_NOT_FOUND = pydcmi.DCMI_ERROR_FUNCTION_NOT_FOUND

    def dcmi_api_version(self):
        return self.api_version

    def dcmi_library_path(self):
        return "libdcmi.so"

    def _unit(self, dev_id: int) -> _UnitV2:
        for u in self.units:
            if u.dev_id == dev_id:
                return u
        raise pydcmi.DCMIError(pydcmi.DCMI_ERROR_INVALID_DEVICE_ID)

    def __getattr__(self, name: str):
        handler = {
            "dcmiv2_init": self._init,
            "dcmiv2_get_device_list": self._get_device_list,
            "dcmiv2_get_device_type": self._get_device_type,
            "dcmiv2_get_device_chip_info": self._get_chip_info,
            "dcmiv2_get_device_hbm_info": self._get_hbm_info,
            "dcmiv2_get_device_ecc_info": self._get_ecc_info,
            "dcmiv2_get_device_die_id": self._get_die_id,
            "dcmiv2_get_chip_phy_id_by_dev_id": self._get_phy_id,
            "dcmiv2_get_device_bdf": self._get_bdf,
            "dcmiv2_get_device_utilization_rate": self._get_utilization_rate,
            "dcmiv2_get_device_temperature": self._get_temperature,
            "dcmiv2_get_device_power_info": self._get_power_info,
            "dcmiv2_get_affinity_cpu_info_by_dev_id": self._get_affinity,
            "dcmiv2_get_dcmi_version": self._get_dcmi_version,
        }.get(name)
        if handler is not None:
            return handler

        if name.startswith("dcmi_"):
            # A V2 driver refuses the V1 API wholesale.
            def _refuse(*_args, **_kwargs):
                self.calls.append(name)
                raise pydcmi.DCMIError(pydcmi.DCMI_ERROR_NOT_SUPPORT)

            return _refuse

        raise AttributeError(name)

    def _init(self):
        self.calls.append("dcmiv2_init")
        self.api_version = 2

    def _get_device_list(self):
        self.calls.append("dcmiv2_get_device_list")
        return [u.dev_id for u in self.units]

    def _get_device_type(self, dev_id):
        self.calls.append("dcmiv2_get_device_type")
        u = self._unit(dev_id)
        if not u.unit_type_readable:
            raise pydcmi.DCMIError(pydcmi.DCMI_ERROR_NOT_SUPPORT)
        return u.unit_type

    def _get_chip_info(self, dev_id):
        self.calls.append("dcmiv2_get_device_chip_info")
        u = self._unit(dev_id)
        return _FakeStruct(chip_name=u.chip_name, aicore_cnt=u.aicore_cnt)

    def _get_hbm_info(self, dev_id):
        self.calls.append("dcmiv2_get_device_hbm_info")
        u = self._unit(dev_id)
        # A None size stands for a driver refusing the only memory call V2 has.
        if u.hbm_size is None:
            raise pydcmi.DCMIError(pydcmi.DCMI_ERROR_NOT_SUPPORT)
        return _FakeStruct(memory_size=u.hbm_size, memory_usage=u.hbm_usage)

    def _get_ecc_info(self, dev_id, device_type):
        self.calls.append("dcmiv2_get_device_ecc_info")
        u = self._unit(dev_id)
        return _FakeStruct(
            enable_flag=1,
            single_bit_error_cnt=u.ecc_errors,
            double_bit_error_cnt=0,
        )

    def _get_die_id(self, dev_id, input_type):
        self.calls.append("dcmiv2_get_device_die_id")
        u = self._unit(dev_id)
        if input_type not in u.die_types:
            raise pydcmi.DCMIError(pydcmi.DCMI_ERROR_NOT_SUPPORT)
        return u.die_id

    def _get_phy_id(self, dev_id):
        self.calls.append("dcmiv2_get_chip_phy_id_by_dev_id")
        u = self._unit(dev_id)
        if not u.phy_id_readable:
            raise pydcmi.DCMIError(pydcmi.DCMI_ERROR_NOT_SUPPORT)
        return u.phy_id

    def _get_bdf(self, dev_id):
        self.calls.append("dcmiv2_get_device_bdf")
        return self._unit(dev_id).bdf

    def _get_utilization_rate(self, dev_id, input_type):
        self.calls.append("dcmiv2_get_device_utilization_rate")
        assert input_type == pydcmi.DCMI_INPUT_TYPE_AICORE
        return self._unit(dev_id).cores_utilization

    def _get_temperature(self, dev_id):
        self.calls.append("dcmiv2_get_device_temperature")
        return self._unit(dev_id).temperature

    def _get_power_info(self, dev_id):
        self.calls.append("dcmiv2_get_device_power_info")
        return self._unit(dev_id).power_deciwatts

    def _get_affinity(self, dev_id):
        self.calls.append("dcmiv2_get_affinity_cpu_info_by_dev_id")
        raise pydcmi.DCMIError(pydcmi.DCMI_ERROR_NOT_SUPPORT)

    def _get_dcmi_version(self):
        self.calls.append("dcmiv2_get_dcmi_version")
        if self.dcmi_version is None:
            raise pydcmi.DCMIError(pydcmi.DCMI_ERROR_NOT_SUPPORT)
        return self.dcmi_version


@pytest.fixture
def fake_pydcmi_v2(monkeypatch):
    def _install(units: list[_UnitV2] | None = None, **kwargs) -> _FakeDCMIV2:
        fake = _FakeDCMIV2(units=units if units is not None else [_UnitV2()], **kwargs)
        monkeypatch.setattr(ascend, "pydcmi", fake)
        monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK", True)
        return fake

    return _install


def test_detect_info_v2_enumerates_devices_flat(fake_pydcmi_v2):
    fake_pydcmi_v2(
        [
            _UnitV2(dev_id=0, phy_id=3),
            _UnitV2(dev_id=1, phy_id=7),
        ],
    )

    devices = AscendDetector().detect_info()

    assert [d.index for d in devices] == [0, 1]
    # The device node is numbered by the physical id, which V2 reports through
    # a call of its own rather than deriving from the index.
    assert [d.appendix["physical_id"] for d in devices] == [3, 7]
    assert [d.name for d in devices] == ["Ascend950PR", "Ascend950PR"]
    assert [d.memory for d in devices] == [131072, 131072]
    assert devices[0].uuid == "5A 6B 7C 8D 0"
    # V2 has no card level, so nothing pretends there is one.
    assert "card_id" not in devices[0].appendix
    assert "device_id" not in devices[0].appendix
    # The whole point: an A5 chip resolves to its generation.
    assert devices[0].appendix["arch_family"] == "Ascend950PR"
    assert ascend.get_ascend_cann_variant(devices[0].appendix["arch_family"]) == "950"


def test_detect_info_v2_touches_no_v1_call(fake_pydcmi_v2):
    fake = fake_pydcmi_v2()

    AscendDetector().detect_info()

    # dcmi_init aside -- is_supported has to try it before falling back -- no
    # V1 entry point may be reached: this driver refuses every one of them.
    assert [c for c in fake.calls if c.startswith("dcmi_")] == ["dcmi_init"]


def test_detect_info_v2_skips_a_non_npu_device(fake_pydcmi_v2):
    fake_pydcmi_v2(
        [
            _UnitV2(dev_id=0),
            _UnitV2(dev_id=1, unit_type=pydcmi.DCMI_UNIT_TYPE_MCU),
        ],
    )

    devices = AscendDetector().detect_info()

    assert [d.index for d in devices] == [0]


def test_detect_info_v2_keeps_a_device_whose_type_is_unreadable(fake_pydcmi_v2):
    fake_pydcmi_v2([_UnitV2(dev_id=0, unit_type_readable=False)])

    devices = AscendDetector().detect_info()

    assert [d.index for d in devices] == [0]


def test_detect_info_v2_skips_a_device_without_a_readable_physical_id(fake_pydcmi_v2):
    fake_pydcmi_v2(
        [
            _UnitV2(dev_id=0, phy_id_readable=False),
            _UnitV2(dev_id=1, phy_id=1),
        ],
    )

    devices = AscendDetector().detect_info()

    # Standing the index in for an unreadable physical id would hand a
    # container another NPU's device node.
    assert [d.index for d in devices] == [1]


def test_detect_info_v2_reports_the_dcmi_version_as_its_own_field(fake_pydcmi_v2):
    # The DCMI library version is not the driver version, so driver_version
    # stays empty rather than carrying a different number under its name.
    fake_pydcmi_v2()

    devices = AscendDetector().detect_info()

    assert len(devices) == 1
    assert devices[0].driver_version is None
    assert devices[0].appendix["dcmi_version"] == "25.7.rc1.6"


def test_detect_info_v2_yields_devices_without_a_dcmi_version(fake_pydcmi_v2):
    # The DCMI version call may fail too, and a device is still addressable
    # without it.
    fake_pydcmi_v2(dcmi_version=None)

    devices = AscendDetector().detect_info()

    assert len(devices) == 1
    assert devices[0].driver_version is None
    assert "dcmi_version" not in devices[0].appendix


def test_detect_usage_v2_merges_the_usage_fields_by_uuid(fake_pydcmi_v2):
    fake_pydcmi_v2([_UnitV2(dev_id=0, phy_id=0)])

    devices = AscendDetector().detect(usage=True)

    assert len(devices) == 1
    dev = devices[0]
    assert dev.cores_utilization == 17
    assert dev.memory_used == 5225
    assert dev.temperature == 51
    # 0.1W as the driver reports it, W as the detector does.
    assert dev.power_used == _UnitV2().power_deciwatts / 10
    assert dev.memory_utilization == get_utilization(5225, 131072)


def test_detect_v2_reports_an_uncorrectable_ecc_error(fake_pydcmi_v2, monkeypatch):
    # The ECC read costs a driver call per device, so the health check is off
    # by default and nothing else here reaches this path.
    fake_pydcmi_v2([_UnitV2(dev_id=0, ecc_errors=3)])
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK", False)

    devices = AscendDetector().detect_info()
    assert devices[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY

    # merge_devices_usage overwrites memory_status, so the usage pass has to
    # produce it too or the verdict is lost.
    AscendDetector().detect_usage(devices)
    assert devices[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY


def test_detect_v2_reports_a_healthy_device(fake_pydcmi_v2, monkeypatch):
    fake = fake_pydcmi_v2([_UnitV2(dev_id=0)])
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK", False)

    devices = AscendDetector().detect_info()

    assert devices[0].memory_status == DeviceMemoryStatusEnum.HEALTHY
    # The verdict must come from the driver, not from a skipped health check
    # -- both produce the same enum.
    assert "dcmiv2_get_device_ecc_info" in fake.calls


def test_detect_info_v2_skips_a_device_whose_memory_is_unreadable(fake_pydcmi_v2):
    # The device is dropped, but only that one: the refusal must not take the
    # whole detection with it.
    fake_pydcmi_v2(
        [
            _UnitV2(dev_id=0, hbm_size=None),
            _UnitV2(dev_id=1, phy_id=1),
        ],
    )

    devices = AscendDetector().detect_info()

    assert [dev.index for dev in devices] == [1]


def test_detect_usage_v2_keeps_the_other_devices_when_one_memory_read_fails(
    fake_pydcmi_v2,
):
    # A device left out of a round keeps its last figures, rather than every
    # device's metrics being cleared.
    fake = fake_pydcmi_v2(
        [
            _UnitV2(dev_id=0),
            _UnitV2(dev_id=1, phy_id=1),
        ],
    )

    devices = AscendDetector().detect_info()
    AscendDetector().detect_usage(devices)
    assert devices[0].cores_utilization == _UnitV2().cores_utilization

    # The driver starts refusing the memory call, for device 0 alone.
    fake.units[0].hbm_size = None
    fake.units[1].cores_utilization = 42
    AscendDetector().detect_usage(devices)

    assert devices[0].cores_utilization == _UnitV2().cores_utilization
    assert devices[1].cores_utilization == 42


def test_get_topology_v2_reports_no_distances(fake_pydcmi_v2):
    fake = fake_pydcmi_v2([_UnitV2(dev_id=0, phy_id=0), _UnitV2(dev_id=1, phy_id=1)])

    topo = AscendDetector().get_topology()

    assert topo is not None
    assert len(topo.devices_distances) == 2
    # V2 declares no topology call, so nothing is guessed at and no V1 call is
    # made behind the scenes -- not even the initialization.
    assert [c for c in fake.calls if c.startswith("dcmi_")] == ["dcmi_init"]
    assert "dcmi_get_topo_info_by_device_id" not in fake.calls


def test_get_topology_v2_resolves_the_api_version_when_handed_devices(fake_pydcmi_v2):
    # detect_topologies() passes the devices straight in, so detect_info()
    # never runs. Left unresolved, the API version reads 1 and the V1
    # dcmi_init() raises.
    fake = fake_pydcmi_v2([_UnitV2(dev_id=0), _UnitV2(dev_id=1, phy_id=1)])
    devices = AscendDetector().detect_info()

    # Back to a process that has not probed anything yet.
    AscendDetector.is_supported.cache_clear()
    fake.api_version = 1
    fake.calls.clear()

    topo = AscendDetector().get_topology(devices)

    assert topo is not None
    assert len(topo.devices_distances) == 2
    assert "dcmi_get_topo_info_by_device_id" not in fake.calls


def test_detect_info_v2_falls_back_to_the_ndie(fake_pydcmi_v2):
    fake_pydcmi_v2([_UnitV2(dev_id=0, die_types=(pydcmi.DCMI_DIE_TYPE_NDIE,))])

    devices = AscendDetector().detect_info()

    assert [d.uuid for d in devices] == ["5A 6B 7C 8D 0"]


def test_detect_info_v2_identifies_a_dieless_device_by_its_address(fake_pydcmi_v2):
    # What the 950PR driver does: both die types answer NOT_SUPPORT. Dropping
    # the device over it would leave eight usable NPUs invisible.
    fake_pydcmi_v2([_UnitV2(dev_id=0, die_types=())])

    devices = AscendDetector().detect_info()

    assert len(devices) == 1
    assert devices[0].uuid == "0000:01:00.0"


def test_detect_usage_v2_merges_when_the_uuid_fell_back_to_the_address(fake_pydcmi_v2):
    # The usage pass merges on the uuid, so it has to arrive at the same one
    # the inventory did -- including when that was the address.
    fake_pydcmi_v2([_UnitV2(dev_id=0, die_types=())])

    devices = AscendDetector().detect(usage=True)

    assert len(devices) == 1
    assert devices[0].uuid == "0000:01:00.0"
    assert devices[0].cores_utilization == _UnitV2().cores_utilization
    assert devices[0].memory_used == _UnitV2().hbm_usage


def test_detect_reproduces_the_950pr_host(fake_pydcmi_v2):
    # The 950PR host exactly as probed: eight NPUs, no die of either type,
    # 28 AI cores and 128GiB of HBM each, on the buses npu-smi reported.
    fake_pydcmi_v2(
        [_UnitV2(dev_id=i, phy_id=i, aicore_cnt=28, die_types=()) for i in range(8)],
    )

    devices = AscendDetector().detect()

    assert len(devices) == 8
    assert {d.name for d in devices} == {"Ascend950PR"}
    assert {d.appendix["arch_family"] for d in devices} == {"Ascend950PR"}
    assert {
        ascend.get_ascend_cann_variant(d.appendix["arch_family"]) for d in devices
    } == {"950"}
    assert all(d.memory == 131072 for d in devices)
    assert all(d.cores == 28 for d in devices)
    # Every card has to be told apart, which the address fallback still manages.
    assert len({d.uuid for d in devices}) == 8
    assert [d.appendix["physical_id"] for d in devices] == list(range(8))
