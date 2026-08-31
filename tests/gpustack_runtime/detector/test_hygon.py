from __future__ import annotations

import contextlib
import ctypes
from types import SimpleNamespace

import pytest

from gpustack_runtime import envs
from gpustack_runtime.deployer.cdi import hygon as cdi_hygon
from gpustack_runtime.deployer.cdi.__types__ import ConfigDeviceNode
from gpustack_runtime.detector import (
    Device,
    DeviceMemoryStatusEnum,
    ManufacturerEnum,
    amd,
    hygon,
    pyamdgpu,
    pydmi,
    pyhsa,
)
from gpustack_runtime.detector.__utils__ import _load_pci_device_names
from gpustack_runtime.detector.hygon import HygonDetector


class _FakeDmi:
    """
    A pydmi stand-in answering out of fixture cards keyed by BDF.

    Handles are opaque c_void_p tokens the fixture hands out and decodes back,
    so a mix-up between a card, GPU instance, compute instance or MIG device
    handle reads as a wrong answer, not a passing one. Everything not faked
    here (error type, constants) comes from the real binding.
    """

    def __getattr__(self, name):
        return getattr(pydmi, name)

    def __init__(self, cards: list[dict], mig_enabled: bool = False):
        self.mig_enabled = mig_enabled
        self.cards = {card["bdf"]: card for card in cards}
        # The MIG device index space is node-global: every card's instances
        # share it, and an index belonging to another card answers NOT_FOUND.
        self.mig_by_index = {}
        for card in cards:
            for inst in card.get("mig_instances", []):
                self.mig_by_index[inst["mig_index"]] = (card, inst)

    @staticmethod
    def _card_handle(card: dict) -> ctypes.c_void_p:
        return ctypes.c_void_p(0x1000 + card["dmi_index"])

    def _card_of_handle(self, handle) -> dict:
        for card in self.cards.values():
            if self._card_handle(card).value == handle.value:
                return card
        raise pydmi.DMIError(pydmi.DMI_ERROR_NOT_FOUND)

    def dmiInit(self):
        pass

    def dmiGetSystemMigMode(self) -> tuple[int, int]:
        if self.mig_enabled:
            return pydmi.DMI_DEVICE_MIG_ENABLE, 0
        return pydmi.DMI_DEVICE_MIG_DISABLE, 0

    def dmiDeviceGetCount(self) -> int:
        return len(self.cards)

    def dmiDeviceGetHandleByPciBusId(self, bdf: str):
        card = self.cards.get(bdf)
        if card is None or card.get("dmi_unreachable"):
            raise pydmi.DMIError(pydmi.DMI_ERROR_NOT_FOUND)
        return self._card_handle(card)

    def dmiDeviceGetIndex(self, handle) -> int:
        return self._card_of_handle(handle)["dmi_index"]

    def dmiDeviceGetMaxMigDeviceCount(self, handle) -> int:
        return self._card_of_handle(handle).get("max_mig", 4)

    def dmiDeviceGetMigDeviceHandleByIndex(self, handle, index: int):
        card, _inst = self.mig_by_index.get(index) or (None, None)
        if card is None or card is not self._card_of_handle(handle):
            raise pydmi.DMIError(pydmi.DMI_ERROR_NOT_FOUND)
        return ctypes.c_void_p(0x2000 + index)

    def _mig_instance_of(self, mdev_handle) -> tuple[dict, dict]:
        card, inst = self.mig_by_index.get(mdev_handle.value - 0x2000) or (None, None)
        if inst is None:
            raise pydmi.DMIError(pydmi.DMI_ERROR_NOT_FOUND)
        return card, inst

    def dmiDeviceGetGpuInstanceId(self, mdev_handle) -> int:
        _, inst = self._mig_instance_of(mdev_handle)
        if inst.get("fail_gi_id"):
            raise pydmi.DMIError(pydmi.DMI_ERROR_UNKNOWN)
        return inst["gi_id"]

    def dmiDeviceGetComputeInstanceId(self, mdev_handle) -> int:
        _, inst = self._mig_instance_of(mdev_handle)
        return inst["ci_id"]

    def dmiDeviceGetGpuInstanceById(self, handle, gi_id: int):
        card = self._card_of_handle(handle)
        return ctypes.c_void_p(0x3000 + card["dmi_index"] * 100 + gi_id)

    def dmiGpuInstanceGetInfo(self, gi_handle):
        gi_id = gi_handle.value - 0x3000
        dmi_index, gi_id = divmod(gi_id, 100)
        card = next(c for c in self.cards.values() if c["dmi_index"] == dmi_index)
        for inst in card.get("mig_instances", []):
            if inst["gi_id"] == gi_id:
                return SimpleNamespace(
                    device=self._card_handle(card).value,
                    id=gi_id,
                    profile_id=inst["profile_id"],
                    placement=SimpleNamespace(start=inst["start"], size=inst["size"]),
                )
        raise pydmi.DMIError(pydmi.DMI_ERROR_NOT_FOUND)

    def dmiGpuInstanceGetComputeInstanceById(self, gi_handle, ci_id: int):
        return ctypes.c_void_p(0x4000 + gi_handle.value - 0x3000 + ci_id)

    def dmiComputeInstanceGetInfo(self, ci_handle):
        return SimpleNamespace(
            device=0,
            gpu_instance=0,
            id=0,
            profile_id=0,
            placement=SimpleNamespace(start=0, size=1),
        )

    def dmiDeviceGetGpuInstanceProfileInfo(self, handle, profile: int):
        card = self._card_of_handle(handle)
        prf = card.get("profiles", {}).get(profile)
        if prf is None:
            # A width the card offers no profile for is a routine gap.
            raise pydmi.DMIError(pydmi.DMI_ERROR_INVALID_ARGUMENT)
        return SimpleNamespace(**prf)

    def dmiDeviceGetMemoryInfo(self, mdev_handle):
        _, inst = self._mig_instance_of(mdev_handle)
        return SimpleNamespace(
            total=inst["memory_bytes"],
            free=inst["memory_bytes"] - inst["memory_used_bytes"],
            used=inst["memory_used_bytes"],
        )

    def dmiDeviceGetUtilizationRates(self, mdev_handle):
        _, inst = self._mig_instance_of(mdev_handle)
        if inst.get("fail_utilization"):
            raise pydmi.DMIError(pydmi.DMI_ERROR_UNKNOWN)
        return SimpleNamespace(gpu=inst["gpu_util"], memory=0)


@pytest.mark.skipif(
    not HygonDetector.is_supported(),
    reason="Hygon GPU not detected",
)
def test_detect():
    det = HygonDetector()
    devs = det.detect()
    print(devs)


@pytest.mark.skipif(
    not HygonDetector.is_supported(),
    reason="Hygon GPU not detected",
)
def test_get_topology():
    det = HygonDetector()
    topo = det.get_topology()
    print(topo)


# --------------------------------------------------------------------------- #
# Fake bindings: no Hygon driver exists in this suite, so every behaviour      #
# below is proved against stand-ins for pyrocmsmi / pyhsa, recording the calls #
# they serve -- "detect_info asks for no usage" is a statement about the calls #
# made, not about the values returned.                                        #
# --------------------------------------------------------------------------- #


class _FakeRocmSmiError(Exception):
    """
    The fake ROCm SMI's own error type.
    """


class _EccCount:
    """
    An rsmi_error_count_t stand-in.
    """

    def __init__(self, uncorrectable_err: int = 0):
        self.uncorrectable_err = uncorrectable_err


class _FakeRocmSmi:
    """
    A pyrocmsmi stand-in answering per device index out of the fixture cards.
    """

    ROCMSMIError = _FakeRocmSmiError

    def __init__(self, calls: list[str], cards: list[dict]):
        self.calls = calls
        self.cards = cards

    def rsmi_init(self, *_args):
        self.calls.append("rsmi_init")

    def rsmi_get_rocm_version(self) -> str:
        return "25.04.1"

    def rsmi_num_monitor_devices(self) -> int:
        return len(self.cards)

    def rsmi_dev_unique_id_get(self, dev_idx: int) -> str:
        return self.cards[dev_idx]["unique_id"]

    def rsmi_dev_pci_id_get(self, dev_idx: int) -> str:
        return self.cards[dev_idx]["bdf"]

    def rsmi_dev_name_get(self, dev_idx: int) -> str:
        self.calls.append("rsmi_dev_name_get")
        return self.cards[dev_idx]["name"]

    def rsmi_dev_target_graphics_version_get(self, dev_idx: int) -> str:
        return self.cards[dev_idx]["target_graphics_version"]

    def rsmi_dev_memory_total_get(self, dev_idx: int) -> int:
        self.calls.append("rsmi_dev_memory_total_get")
        return self.cards[dev_idx]["memory_total"]

    def rsmi_dev_memory_usage_get(self, dev_idx: int) -> int:
        self.calls.append("rsmi_dev_memory_usage_get")
        return self.cards[dev_idx]["memory_used"]

    def rsmi_dev_ecc_count_get(self, dev_idx: int) -> _EccCount:
        self.calls.append("rsmi_dev_ecc_count_get")
        return _EccCount(self.cards[dev_idx]["uncorrectable_err"])

    def rsmi_dev_busy_percent_get(self, dev_idx: int) -> int:
        self.calls.append("rsmi_dev_busy_percent_get")
        return self._reading(dev_idx, "busy_percent")

    def rsmi_dev_temp_metric_get(self, dev_idx: int) -> int:
        self.calls.append("rsmi_dev_temp_metric_get")
        return self._reading(dev_idx, "temperature")

    def rsmi_dev_power_cap_get(self, dev_idx: int) -> int:
        self.calls.append("rsmi_dev_power_cap_get")
        return self.cards[dev_idx]["power_cap"]

    def rsmi_dev_power_get(self, dev_idx: int) -> int:
        self.calls.append("rsmi_dev_power_get")
        return self._reading(dev_idx, "power_used")

    def _reading(self, dev_idx: int, key: str) -> int:
        # A card whose fixture carries None for a reading stands for the one
        # ROCm SMI cannot answer: the binding checks every return code and
        # raises, rather than handing back a sentinel the way AMD SMI does.
        reading = self.cards[dev_idx][key]
        if reading is None:
            msg = f"{key} is not supported on this device"
            raise _FakeRocmSmiError(msg)
        return reading

    def rsmi_topo_get_numa_node_number(self, dev_idx: int) -> int:
        return self.cards[dev_idx]["numa"]


class _FakeAmdGpu:
    """
    A libdrm stand-in exposing only what the name precedence reaches for.

    The real call answers with a pointer into libdrm's own table, and NULL for
    a device id the table does not carry, which the binding reports as "".
    """

    def __init__(self, marketing_name: str):
        self._marketing_name = marketing_name

    def __getattr__(self, name):
        # Everything not faked here comes from the real binding, so the fake
        # cannot drift from it.
        return getattr(pyamdgpu, name)

    @contextlib.contextmanager
    def amdgpu_device(self, card):
        yield SimpleNamespace(card=card)

    def amdgpu_get_marketing_name(self, _device) -> str:
        return self._marketing_name

    def amdgpu_query_gpu_info(self, _device):
        return SimpleNamespace(cu_active_number=0, family_id=0)


class _FakeHSA:
    """
    A pyhsa stand-in returning the fixture agents.
    """

    Agent = pyhsa.Agent

    def __init__(self, agents: list):
        self.agents = agents

    def get_agents(self) -> list:
        return self.agents


# An extract of the real pci.ids: the board's name lives on the subsystem line,
# which is what the operator's GetName prefers.
_PCI_IDS = """\
1d94  Chengdu Haiguang IC Design Co., Ltd.
\t6210  Kunpeng
\t\t1d94 6210  K100_AI
"""

_PCI_IDS_NAME = "K100_AI"
_HSA_NAME = "Hygon K100 AI"
_MARKETING_NAME = "Hygon DCU K100 AI"
_RSMI_NAME = "Kunpeng"

_MEMORY_TOTAL_BYTES = 68719476736  # 65536 MiB
_MEMORY_TOTAL = 65536


def _card(
    bdf: str,
    unique_id: str,
    *,
    memory_used: int = 1073741824,  # 1024 MiB
    busy_percent: int = 44,
    temperature: int = 51,
    power_used: int = 217,
    uncorrectable_err: int = 0,
) -> dict:
    """
    One fake Hygon card: the answers the fake ROCm SMI serves for its index,
    and the PCI IDs the fixture sysfs tree exposes for its BDF.
    """
    return {
        "bdf": bdf,
        "pci_ids": {
            "vendor": "0x1d94",
            "device": "0x6210",
            "subsystem_vendor": "0x1d94",
            "subsystem_device": "0x6210",
        },
        "unique_id": unique_id,
        "name": _RSMI_NAME,
        "target_graphics_version": "gfx936",
        "memory_total": _MEMORY_TOTAL_BYTES,
        "memory_used": memory_used,
        "uncorrectable_err": uncorrectable_err,
        "busy_percent": busy_percent,
        "temperature": temperature,
        "power_cap": 350,
        "power_used": power_used,
        "numa": 0,
    }


def _agent(bdf: str, compute_units: int = 104) -> pyhsa.Agent:
    return pyhsa.Agent(
        device_type=1,
        device_id="0x6210",
        bdf=bdf,
        uuid="",
        name=_HSA_NAME,
        compute_capability="gfx936",
        compute_units=compute_units,
    )


@pytest.fixture
def hygon_bindings(monkeypatch, tmp_path):
    """
    Drive the Hygon detector off the fake bindings, a fixture sysfs PCI tree
    and a fixture pci.ids database, returning the shared call log.
    """

    def _setup(
        cards: list[dict],
        agents: list | None = None,
        pci_ids: str | None = _PCI_IDS,
        dmi_mig_enabled: bool = False,
        dmi_mig_confs: dict[str, str] | None = None,
    ) -> list[str]:
        calls: list[str] = []

        monkeypatch.setattr(HygonDetector, "is_supported", staticmethod(lambda: True))
        monkeypatch.setattr(hygon, "pyrocmsmi", _FakeRocmSmi(calls, cards))
        monkeypatch.setattr(hygon, "pyhsa", _FakeHSA(list(agents or [])))
        monkeypatch.setattr(
            hygon,
            "pydmi",
            _FakeDmi(cards, mig_enabled=dmi_mig_enabled),
        )

        # The vendor's instance registry: only the confs the test names exist.
        mig_config_dir = tmp_path / "dmi_mig_config"
        if dmi_mig_confs:
            ci_dir = mig_config_dir / "ci"
            ci_dir.mkdir(parents=True)
            for name, uuid in dmi_mig_confs.items():
                (ci_dir / name).write_text(
                    "cu_count:           20\n"
                    "memory_size_MB:     16380\n"
                    f"mig_uuid:           {uuid}\n",
                    encoding="utf-8",
                )
        monkeypatch.setattr(hygon, "_DMI_MIG_CONFIG_DIR", mig_config_dir)

        pci_devices_path = tmp_path / "pci_devices"
        for card in cards:
            card_path = pci_devices_path / card["bdf"]
            card_path.mkdir(parents=True)
            for name, value in card["pci_ids"].items():
                (card_path / name).write_text(f"{value}\n")
        # The pci.ids lookup is the AMD module's -- Hygon shares it, as it
        # already shares the architecture family mapping.
        monkeypatch.setattr(amd, "_PCI_DEVICES_PATH", pci_devices_path)

        pci_ids_paths: tuple[str, ...] = ()
        if pci_ids is not None:
            pci_ids_path = tmp_path / "pci.ids"
            pci_ids_path.write_text(pci_ids, encoding="utf-8")
            pci_ids_paths = (str(pci_ids_path),)
        monkeypatch.setattr(
            "gpustack_runtime.detector.__utils__._PCI_IDS_PATHS",
            pci_ids_paths,
        )
        _load_pci_device_names.cache_clear()

        return calls

    yield _setup

    _load_pci_device_names.cache_clear()


# --------------------------------------------------------------------------- #
# detect_info: the device name's precedence, the inventory, and the usage      #
# calls it must not make.                                                     #
# --------------------------------------------------------------------------- #


def test_detect_info_prefers_the_pci_ids_name(hygon_bindings):
    # The operator resolves the board's name from pci.ids before asking the
    # driver, which only knows the chip.
    hygon_bindings(
        [_card("0000:0b:00.0", "0x9f8e7d6c5b4a3921")],
        agents=[_agent("0000:0b:00.0")],
    )

    devices = HygonDetector().detect_info()

    assert [dev.name for dev in devices] == [_PCI_IDS_NAME]


def test_detect_info_falls_back_to_the_hsa_product_name(hygon_bindings):
    hygon_bindings(
        [_card("0000:0b:00.0", "0x9f8e7d6c5b4a3921")],
        agents=[_agent("0000:0b:00.0")],
        pci_ids=None,
    )

    devices = HygonDetector().detect_info()

    assert [dev.name for dev in devices] == [_HSA_NAME]


def test_detect_info_falls_back_to_the_rocm_smi_name(hygon_bindings):
    hygon_bindings([_card("0000:0b:00.0", "0x9f8e7d6c5b4a3921")], pci_ids=None)

    devices = HygonDetector().detect_info()

    assert [dev.name for dev in devices] == [_RSMI_NAME]


def test_detect_info_asks_libdrm_for_the_marketing_name(hygon_bindings, monkeypatch):
    # The same libdrm step AMD gained: between the HSA name and the driver
    # name, the operator asks for the board's marketing name.
    hygon_bindings([_card("0000:0b:00.0", "0x9f8e7d6c5b4a3921")], pci_ids=None)
    monkeypatch.setattr(hygon, "_get_card_and_renderd_id", lambda _bdf: (1, 128))
    monkeypatch.setattr(
        hygon,
        "pyamdgpu",
        _FakeAmdGpu(marketing_name=_MARKETING_NAME),
    )

    devices = HygonDetector().detect_info()

    assert [dev.name for dev in devices] == [_MARKETING_NAME]


def test_detect_info_reports_the_inventory(hygon_bindings):
    hygon_bindings(
        [_card("0000:0b:00.0", "0x9f8e7d6c5b4a3921")],
        agents=[_agent("0000:0b:00.0")],
    )

    devices = HygonDetector().detect_info()

    dev = devices[0]
    assert dev.manufacturer is ManufacturerEnum.HYGON
    assert dev.index == 0
    assert dev.uuid == "GPU-9f8e7d6c5b4a3921"
    assert dev.runtime_version == "25.04"
    assert dev.runtime_version_original == "25.04.1"
    assert dev.compute_capability == "gfx936"
    assert dev.cores == 104
    assert dev.memory == _MEMORY_TOTAL
    assert dev.memory_status is DeviceMemoryStatusEnum.HEALTHY
    # The power limit is inventory, not usage.
    assert dev.power == 350
    assert dev.appendix["bdf"] == "0000:0b:00.0"
    assert dev.appendix["numa"] == "0"


def test_detect_reports_no_vgpu(hygon_bindings):
    hygon_bindings(
        [_card("0000:0b:00.0", "0x9f8e7d6c5b4a3921")],
        agents=[_agent("0000:0b:00.0")],
    )
    det = HygonDetector()

    # There is no virtual/PF/VF classification any more, in either query.
    assert "vgpu" not in det.detect_info()[0].appendix
    assert "vgpu" not in det.detect()[0].appendix


def test_detect_info_issues_no_usage_call(hygon_bindings):
    calls = hygon_bindings(
        [_card("0000:0b:00.0", "0x9f8e7d6c5b4a3921")],
        agents=[_agent("0000:0b:00.0")],
    )

    devices = HygonDetector().detect_info()

    assert "rsmi_dev_busy_percent_get" not in calls
    assert "rsmi_dev_temp_metric_get" not in calls
    assert "rsmi_dev_memory_usage_get" not in calls
    assert "rsmi_dev_power_get" not in calls
    # The power limit is inventory, so its own call stays.
    assert "rsmi_dev_power_cap_get" in calls

    dev = devices[0]
    assert dev.cores_utilization == 0
    assert dev.memory_used == 0
    assert dev.memory_utilization == 0
    assert dev.temperature is None
    assert dev.power_used is None


def test_detect_info_reports_unhealthy_memory(hygon_bindings, monkeypatch):
    # The ECC read is opt-in, as the health check is disabled by default.
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK", False)
    hygon_bindings(
        [_card("0000:0b:00.0", "0x9f8e7d6c5b4a3921", uncorrectable_err=3)],
        agents=[_agent("0000:0b:00.0")],
    )

    devices = HygonDetector().detect_info()

    assert devices[0].memory_status is DeviceMemoryStatusEnum.UNHEALTHY


# --------------------------------------------------------------------------- #
# detect_usage.                                                               #
# --------------------------------------------------------------------------- #


def test_detect_usage_merges_by_uuid(hygon_bindings):
    hygon_bindings(
        [
            _card("0000:0b:00.0", "0x9f8e7d6c5b4a3921"),
            _card(
                "0000:0c:00.0",
                "0x9f8e7d6c5b4a3922",
                memory_used=8589934592,  # 8192 MiB
                busy_percent=87,
                temperature=63,
                power_used=298,
            ),
        ],
        agents=[_agent("0000:0b:00.0"), _agent("0000:0c:00.0")],
    )
    det = HygonDetector()
    devices = det.detect_info()

    # Reversed, to prove the merge joins by UUID rather than by position.
    det.detect_usage(list(reversed(devices)))

    assert [dev.cores_utilization for dev in devices] == [44, 87]
    assert [dev.temperature for dev in devices] == [51, 63]
    assert [dev.memory_used for dev in devices] == [1024, 8192]
    assert [dev.power_used for dev in devices] == [217, 298]
    assert [dev.memory_utilization for dev in devices] == [1.56, 12.5]
    assert [dev.memory_status for dev in devices] == [
        DeviceMemoryStatusEnum.HEALTHY,
        DeviceMemoryStatusEnum.HEALTHY,
    ]
    # The information fields survive the merge.
    assert [dev.memory for dev in devices] == [_MEMORY_TOTAL, _MEMORY_TOTAL]
    assert [dev.power for dev in devices] == [350, 350]


def test_detect_usage_detects_the_information_first(hygon_bindings):
    hygon_bindings(
        [_card("0000:0b:00.0", "0x9f8e7d6c5b4a3921")],
        agents=[_agent("0000:0b:00.0")],
    )

    devices = HygonDetector().detect_usage()

    assert devices[0].name == _PCI_IDS_NAME
    assert devices[0].cores_utilization == 44
    assert devices[0].power_used == 217


def test_detect_composes_the_information_and_the_usage(hygon_bindings):
    hygon_bindings(
        [_card("0000:0b:00.0", "0x9f8e7d6c5b4a3921")],
        agents=[_agent("0000:0b:00.0")],
    )

    devices = HygonDetector().detect()

    assert devices[0].memory == _MEMORY_TOTAL
    assert devices[0].memory_used == 1024
    assert devices[0].power == 350
    assert devices[0].power_used == 217


def test_detect_usage_bounds_an_unreadable_power_to_its_own_card(hygon_bindings):
    # ROCm SMI raises on a reading it cannot serve, so an unwrapped read takes
    # the whole sweep down with it and leaves every card on the host with its
    # information-query values.
    hygon_bindings(
        [
            _card("0000:0b:00.0", "0x9f8e7d6c5b4a3921", power_used=None),
            _card("0000:0c:00.0", "0x9f8e7d6c5b4a3922"),
        ],
        agents=[_agent("0000:0b:00.0"), _agent("0000:0c:00.0")],
    )

    devices = HygonDetector().detect_usage()

    assert [dev.power_used for dev in devices] == [None, 217]
    # The rest of the faulty card's usage still arrives, and the healthy card
    # is untouched.
    assert [dev.cores_utilization for dev in devices] == [44, 44]
    assert [dev.temperature for dev in devices] == [51, 51]
    assert [dev.memory_used for dev in devices] == [1024, 1024]


def test_detect_usage_bounds_an_unreadable_utilization_to_its_own_reading(
    hygon_bindings,
):
    # Each reading is isolated on its own, so an unreadable utilization does
    # not carry off the temperature the next call would have served.
    hygon_bindings(
        [_card("0000:0b:00.0", "0x9f8e7d6c5b4a3921", busy_percent=None)],
        agents=[_agent("0000:0b:00.0")],
    )

    devices = HygonDetector().detect_usage()

    assert devices[0].cores_utilization == 0
    assert devices[0].temperature == 51
    assert devices[0].power_used == 217


# --------------------------------------------------------------------------- #
# The CDI generator, which numbers its device nodes from the appendix.        #
# --------------------------------------------------------------------------- #


def _fake_device_node(path: str, **_kwargs) -> ConfigDeviceNode:
    """
    Stand in for the host's device nodes: neither /dev/kfd nor /dev/dri exists
    in this suite, so the real lookup would drop every path.
    """
    return ConfigDeviceNode(path=path, type_="c")


def test_cdi_spec_numbers_the_device_nodes_from_the_appendix(monkeypatch):
    monkeypatch.setattr(cdi_hygon, "device_to_cdi_device_node", _fake_device_node)

    config = cdi_hygon.HygonGenerator().generate(
        [
            Device(
                manufacturer=ManufacturerEnum.HYGON,
                index=0,
                name=_PCI_IDS_NAME,
                uuid="GPU-9f8e7d6c5b4a3921",
                appendix={"card_id": 3, "renderd_id": 131},
            ),
        ],
    )

    # The DRM numbering comes from the appendix, never from Device.index --
    # here the card is enumerated at 0 and its nodes are card3 / renderD131.
    device_nodes = config["devices"][0]["containerEdits"]["deviceNodes"]
    assert [node["path"] for node in device_nodes] == [
        "/dev/dri/card3",
        "/dev/dri/renderD131",
    ]
    assert [dev["name"] for dev in config["devices"]] == [
        "0",
        "GPU-9f8e7d6c5b4a3921",
        "all",
    ]
    assert [node["path"] for node in config["containerEdits"]["deviceNodes"]] == [
        "/dev/kfd",
        "/dev/mkfd",
    ]


# --------------------------------------------------------------------------- #
# MIG: the node-wide mode gates per-card marking and instance enumeration.     #
# --------------------------------------------------------------------------- #

_MIG_PROFILE_1G = {
    "id": 3,
    "gi_count_max": 4,
    "cu_count": 26,
    "gpu_slice_count": 1,
    "memory_size_MB": 16380,
    "name": b"MIG 1g.16gb",
}


def _mig_card(
    bdf: str,
    unique_id: str,
    dmi_index: int,
    *,
    mig_instances: list[dict] | None = None,
    profiles: dict | None = None,
    max_mig: int = 4,
    dmi_unreachable: bool = False,
    **kwargs,
) -> dict:
    """
    One fake Hygon card plus what the fake DMI library serves for it.
    """
    card = _card(bdf, unique_id, **kwargs)
    card["dmi_index"] = dmi_index
    card["max_mig"] = max_mig
    card["profiles"] = profiles or {}
    card["mig_instances"] = mig_instances or []
    card["dmi_unreachable"] = dmi_unreachable
    return card


def _mig_instance(
    mig_index: int,
    gi_id: int,
    ci_id: int,
    *,
    profile_id: int = 3,
    start: int = 0,
    size: int = 1,
    memory_bytes: int = 17179869184,  # 16384 MiB
    memory_used_bytes: int = 4294967296,  # 4096 MiB
    gpu_util: int = 0,
    **kwargs,
) -> dict:
    return {
        "mig_index": mig_index,
        "gi_id": gi_id,
        "ci_id": ci_id,
        "profile_id": profile_id,
        "start": start,
        "size": size,
        "memory_bytes": memory_bytes,
        "memory_used_bytes": memory_used_bytes,
        "gpu_util": gpu_util,
        **kwargs,
    }


def test_detect_info_mig_mode_off_reports_physical_only(hygon_bindings):
    # The library answers and the conf registry could even hold stale entries:
    # with the node-wide mode off the detector behaves exactly as without MIG.
    hygon_bindings(
        [
            _mig_card(
                "0000:0b:00.0",
                "0x9f8e7d6c5b4a3921",
                0,
                mig_instances=[_mig_instance(0, 5, 0)],
                profiles={0: _MIG_PROFILE_1G},
            ),
        ],
        agents=[_agent("0000:0b:00.0")],
        dmi_mig_enabled=False,
        dmi_mig_confs={"dev0gi5ci0.conf": "aaaaaaaa-0000-0000-0000-000000000000"},
    )

    dev = HygonDetector().detect_info()[0]

    assert "mig" not in dev.appendix
    assert "mig_devices" not in dev.appendix


def test_detect_info_mig_library_absent_reports_physical_only(
    hygon_bindings,
    monkeypatch,
):
    class _AbsentDmi(_FakeDmi):
        def dmiInit(self):
            raise pydmi.DMIError(pydmi.DMI_ERROR_LIBRARY_NOT_FOUND)

    hygon_bindings(
        [_mig_card("0000:0b:00.0", "0x9f8e7d6c5b4a3921", 0)],
        agents=[_agent("0000:0b:00.0")],
    )
    monkeypatch.setattr(hygon, "pydmi", _AbsentDmi([], mig_enabled=True))

    dev = HygonDetector().detect_info()[0]

    assert "mig" not in dev.appendix
    assert "mig_devices" not in dev.appendix


def test_detect_info_marks_mig_cards_and_enumerates_instances(
    hygon_bindings,
    monkeypatch,
):
    # The ECC read is opt-in, as the health check is disabled by default.
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK", False)
    # Two cards, each holding instances; the node-global MIG index space is
    # interleaved (card 1's instance sits between card 0's two), so the sweep
    # proves attribution by the card's own handle rather than by index ranges.
    hygon_bindings(
        [
            _mig_card(
                "0000:0b:00.0",
                "0x9f8e7d6c5b4a3921",
                0,
                mig_instances=[
                    _mig_instance(0, 5, 0, start=0),
                    _mig_instance(2, 6, 0, start=1),
                ],
                profiles={0: _MIG_PROFILE_1G},
            ),
            _mig_card(
                "0000:0c:00.0",
                "0x9f8e7d6c5b4a3922",
                1,
                mig_instances=[_mig_instance(1, 1, 0, start=0)],
                profiles={0: _MIG_PROFILE_1G},
                uncorrectable_err=1,
            ),
        ],
        agents=[_agent("0000:0b:00.0"), _agent("0000:0c:00.0")],
        dmi_mig_enabled=True,
        dmi_mig_confs={
            "dev0gi5ci0.conf": "aaaaaaaa-0000-0000-0000-000000000000",
            "dev0gi6ci0.conf": "bbbbbbbb-0000-0000-0000-000000000000",
            "dev1gi1ci0.conf": "cccccccc-0000-0000-0000-000000000000",
        },
    )

    devices = HygonDetector().detect_info()

    assert [dev.appendix["mig"] for dev in devices] == [True, True]

    card0_migs = devices[0].appendix["mig_devices"]
    assert [m["uuid"] for m in card0_migs] == [
        "MIG-aaaaaaaa-0000-0000-0000-000000000000",
        "MIG-bbbbbbbb-0000-0000-0000-000000000000",
    ]
    assert [m["name"] for m in card0_migs] == ["1g.16gb", "1g.16gb"]
    assert [m["memory"] for m in card0_migs] == [16380, 16380]
    assert [m["cores"] for m in card0_migs] == [26, 26]
    assert [m["appendix"]["gpu_instance_id"] for m in card0_migs] == [5, 6]
    assert [m["appendix"]["compute_instance_id"] for m in card0_migs] == [0, 0]
    assert [m["appendix"]["placement"] for m in card0_migs] == [
        {"start": 0, "length": 1},
        {"start": 1, "length": 1},
    ]
    assert [m["appendix"]["bdf"] for m in card0_migs] == ["0000:0b:00.0"] * 2
    assert [m["appendix"]["numa"] for m in card0_migs] == ["0", "0"]
    assert all(m["appendix"]["mig"] and m["appendix"]["sliced"] for m in card0_migs)
    # A partition carries its card's memory-health verdict.
    assert [m["memory_status"] for m in card0_migs] == [
        DeviceMemoryStatusEnum.HEALTHY,
        DeviceMemoryStatusEnum.HEALTHY,
    ]

    card1_migs = devices[1].appendix["mig_devices"]
    assert [m["uuid"] for m in card1_migs] == [
        "MIG-cccccccc-0000-0000-0000-000000000000",
    ]
    assert [m["memory_status"] for m in card1_migs] == [
        DeviceMemoryStatusEnum.UNHEALTHY,
    ]

    # Synthetic indexes: blocks of the card's slot count above the largest
    # physical index (2), per index_mig_devices -- partitioning one card never
    # renumbers another's instances.
    assert [m["index"] for m in card0_migs] == [2, 3]
    assert [m["index"] for m in card1_migs] == [6]


def test_detect_info_mig_conf_missing_falls_back_to_a_synthetic_uuid(hygon_bindings):
    hygon_bindings(
        [
            _mig_card(
                "0000:0b:00.0",
                "0x9f8e7d6c5b4a3921",
                0,
                mig_instances=[_mig_instance(0, 5, 0)],
                profiles={0: _MIG_PROFILE_1G},
            ),
        ],
        agents=[_agent("0000:0b:00.0")],
        dmi_mig_enabled=True,
        dmi_mig_confs=None,  # no registry at all
    )

    migs = HygonDetector().detect_info()[0].appendix["mig_devices"]

    assert [m["uuid"] for m in migs] == ["MIG-0000:0b:00.0-gi5-ci0"]


def test_detect_info_mig_card_unreachable_via_dmi_degrades_to_physical(hygon_bindings):
    hygon_bindings(
        [
            _mig_card(
                "0000:0b:00.0",
                "0x9f8e7d6c5b4a3921",
                0,
                mig_instances=[_mig_instance(0, 5, 0)],
                profiles={0: _MIG_PROFILE_1G},
                dmi_unreachable=True,
            ),
            _mig_card(
                "0000:0c:00.0",
                "0x9f8e7d6c5b4a3922",
                1,
                mig_instances=[_mig_instance(1, 1, 0)],
                profiles={0: _MIG_PROFILE_1G},
            ),
        ],
        agents=[_agent("0000:0b:00.0"), _agent("0000:0c:00.0")],
        dmi_mig_enabled=True,
        dmi_mig_confs={"dev1gi1ci0.conf": "cccccccc-0000-0000-0000-000000000000"},
    )

    devices = HygonDetector().detect_info()

    assert "mig" not in devices[0].appendix
    assert devices[1].appendix["mig"] is True
    assert [m["uuid"] for m in devices[1].appendix["mig_devices"]] == [
        "MIG-cccccccc-0000-0000-0000-000000000000",
    ]


def test_detect_info_mig_one_unreadable_instance_never_aborts_the_sweep(hygon_bindings):
    hygon_bindings(
        [
            _mig_card(
                "0000:0b:00.0",
                "0x9f8e7d6c5b4a3921",
                0,
                mig_instances=[
                    _mig_instance(0, 5, 0, fail_gi_id=True),
                    _mig_instance(1, 6, 0, start=1),
                ],
                profiles={0: _MIG_PROFILE_1G},
            ),
        ],
        agents=[_agent("0000:0b:00.0")],
        dmi_mig_enabled=True,
        dmi_mig_confs={"dev0gi6ci0.conf": "bbbbbbbb-0000-0000-0000-000000000000"},
    )

    migs = HygonDetector().detect_info()[0].appendix["mig_devices"]

    assert [m["uuid"] for m in migs] == ["MIG-bbbbbbbb-0000-0000-0000-000000000000"]
    # The surviving instance keeps its per-card discovery ordinal (1) under the
    # block offset (base 1): the failed read ahead of it must not renumber it.
    assert [m["index"] for m in migs] == [2]


def test_detect_info_mig_derives_physical_cores_from_profiles(hygon_bindings):
    # With MIG enabled HSA exposes a partition's view of the card -- here one
    # slice's 26 CUs -- so the card's own count comes from its profiles
    # instead: 26 per instance times 4 instances of capacity.
    hygon_bindings(
        [
            _mig_card(
                "0000:0b:00.0",
                "0x9f8e7d6c5b4a3921",
                0,
                mig_instances=[_mig_instance(0, 5, 0)],
                profiles={0: _MIG_PROFILE_1G},
            ),
        ],
        agents=[_agent("0000:0b:00.0", compute_units=26)],
        dmi_mig_enabled=True,
        dmi_mig_confs={"dev0gi5ci0.conf": "aaaaaaaa-0000-0000-0000-000000000000"},
    )

    dev = HygonDetector().detect_info()[0]

    assert dev.cores == 104


def test_detect_info_mig_tolerates_garbled_vendor_strings(hygon_bindings, tmp_path):
    # A non-UTF-8 profile name or conf file is a vendor defect to report
    # around, never a reason to lose the whole detection.
    hygon_bindings(
        [
            _mig_card(
                "0000:0b:00.0",
                "0x9f8e7d6c5b4a3921",
                0,
                mig_instances=[_mig_instance(0, 5, 0)],
                profiles={0: {**_MIG_PROFILE_1G, "name": b"MIG \xff1g.16gb"}},
            ),
        ],
        agents=[_agent("0000:0b:00.0")],
        dmi_mig_enabled=True,
        dmi_mig_confs={"dev0gi5ci0.conf": "aaaaaaaa-0000-0000-0000-000000000000"},
    )
    conf = tmp_path / "dmi_mig_config" / "ci" / "dev0gi5ci0.conf"
    conf.write_bytes(b"\xffmig_uuid: garbled\n")

    migs = HygonDetector().detect_info()[0].appendix["mig_devices"]

    # The garbled byte decodes to a replacement char, not a raised error.
    assert [m["name"] for m in migs] == ["\N{REPLACEMENT CHARACTER}1g.16gb"]
    # The garbled conf holds no readable mig_uuid line, so the identity falls
    # back to the synthetic one.
    assert [m["uuid"] for m in migs] == ["MIG-0000:0b:00.0-gi5-ci0"]


# --------------------------------------------------------------------------- #
# MIG usage: refreshed per instance through its own handle, merged by UUID.    #
# --------------------------------------------------------------------------- #


def test_detect_usage_merges_mig_usage_by_uuid(hygon_bindings):
    hygon_bindings(
        [
            _mig_card(
                "0000:0b:00.0",
                "0x9f8e7d6c5b4a3921",
                0,
                mig_instances=[
                    _mig_instance(0, 5, 0, gpu_util=95),
                    _mig_instance(1, 6, 0, start=1),
                ],
                profiles={0: _MIG_PROFILE_1G},
            ),
        ],
        agents=[_agent("0000:0b:00.0")],
        dmi_mig_enabled=True,
        dmi_mig_confs={
            "dev0gi5ci0.conf": "aaaaaaaa-0000-0000-0000-000000000000",
            "dev0gi6ci0.conf": "bbbbbbbb-0000-0000-0000-000000000000",
        },
    )
    det = HygonDetector()
    devices = det.detect_info()

    det.detect_usage(devices)

    migs = devices[0].appendix["mig_devices"]
    assert [m["cores_utilization"] for m in migs] == [95, 0]
    assert [m["memory_used"] for m in migs] == [4096, 4096]
    assert [m["memory_utilization"] for m in migs] == [25.0, 25.0]
    # A MIG device reports neither temperature nor power, so it carries the
    # card's.
    assert [m["temperature"] for m in migs] == [51, 51]
    assert [m["power_used"] for m in migs] == [217, 217]
    # The inventory fields survive the merge.
    assert [m["memory"] for m in migs] == [16380, 16380]
    assert [m["name"] for m in migs] == ["1g.16gb", "1g.16gb"]


def test_detect_usage_mig_suppresses_a_single_unreadable_instance(hygon_bindings):
    hygon_bindings(
        [
            _mig_card(
                "0000:0b:00.0",
                "0x9f8e7d6c5b4a3921",
                0,
                mig_instances=[
                    _mig_instance(0, 5, 0, fail_utilization=True),
                    _mig_instance(1, 6, 0, start=1, gpu_util=42),
                ],
                profiles={0: _MIG_PROFILE_1G},
            ),
        ],
        agents=[_agent("0000:0b:00.0")],
        dmi_mig_enabled=True,
        dmi_mig_confs={
            "dev0gi5ci0.conf": "aaaaaaaa-0000-0000-0000-000000000000",
            "dev0gi6ci0.conf": "bbbbbbbb-0000-0000-0000-000000000000",
        },
    )
    det = HygonDetector()
    devices = det.detect_info()

    det.detect_usage(devices)

    migs = devices[0].appendix["mig_devices"]
    # The unreadable instance keeps the inventory's defaults; its sibling is
    # refreshed -- one bad read never blinds the sweep.
    assert [m["cores_utilization"] for m in migs] == [0, 42]


def test_detect_usage_mig_mode_off_makes_no_dmi_usage_call(hygon_bindings):
    hygon_bindings(
        [
            _mig_card(
                "0000:0b:00.0",
                "0x9f8e7d6c5b4a3921",
                0,
                mig_instances=[_mig_instance(0, 5, 0, gpu_util=95)],
                profiles={0: _MIG_PROFILE_1G},
            ),
        ],
        agents=[_agent("0000:0b:00.0")],
        dmi_mig_enabled=False,
        dmi_mig_confs={"dev0gi5ci0.conf": "aaaaaaaa-0000-0000-0000-000000000000"},
    )

    devices = HygonDetector().detect_usage()

    assert "mig_devices" not in devices[0].appendix
