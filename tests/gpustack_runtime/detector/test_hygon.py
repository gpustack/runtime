from __future__ import annotations

import contextlib
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
    pyhsa,
)
from gpustack_runtime.detector.__utils__ import _load_pci_device_names
from gpustack_runtime.detector.hygon import HygonDetector


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
        return self.cards[dev_idx]["busy_percent"]

    def rsmi_dev_temp_metric_get(self, dev_idx: int) -> int:
        self.calls.append("rsmi_dev_temp_metric_get")
        return self.cards[dev_idx]["temperature"]

    def rsmi_dev_power_cap_get(self, dev_idx: int) -> int:
        self.calls.append("rsmi_dev_power_cap_get")
        return self.cards[dev_idx]["power_cap"]

    def rsmi_dev_power_get(self, dev_idx: int) -> int:
        self.calls.append("rsmi_dev_power_get")
        return self.cards[dev_idx]["power_used"]

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


def _agent(bdf: str) -> pyhsa.Agent:
    return pyhsa.Agent(
        device_type=1,
        device_id="0x6210",
        bdf=bdf,
        uuid="",
        name=_HSA_NAME,
        compute_capability="gfx936",
        compute_units=104,
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
    ) -> list[str]:
        calls: list[str] = []

        monkeypatch.setattr(HygonDetector, "is_supported", staticmethod(lambda: True))
        monkeypatch.setattr(hygon, "pyrocmsmi", _FakeRocmSmi(calls, cards))
        monkeypatch.setattr(hygon, "pyhsa", _FakeHSA(list(agents or [])))

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
