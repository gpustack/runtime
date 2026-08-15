from __future__ import annotations

import contextlib
from types import SimpleNamespace

import pytest

from gpustack_runtime.deployer.cdi import amd as cdi_amd
from gpustack_runtime.deployer.cdi.__types__ import ConfigDeviceNode
from gpustack_runtime.detector import (
    Device,
    DeviceMemoryStatusEnum,
    ManufacturerEnum,
    amd,
    pyamdgpu,
    pyhsa,
)
from gpustack_runtime.detector.__utils__ import _load_pci_device_names
from gpustack_runtime.detector.amd import AMDDetector


@pytest.mark.skipif(
    not AMDDetector.is_supported(),
    reason="AMD GPU not detected",
)
def test_detect():
    det = AMDDetector()
    devs = det.detect()
    print(devs)


@pytest.mark.skipif(
    not AMDDetector.is_supported(),
    reason="AMD GPU not detected",
)
def test_get_topology():
    det = AMDDetector()
    topo = det.get_topology()
    print(topo)


# --------------------------------------------------------------------------- #
# Fake bindings: no AMD driver exists in this suite, so every behaviour below  #
# is proved against stand-ins for pyamdsmi / pyrocmsmi / pyhsa, each recording #
# the calls it serves -- "detect_info asks for no usage" is a statement about  #
# the calls made, not about the values returned.                              #
# --------------------------------------------------------------------------- #


class _FakeAmdSmiError(Exception):
    """
    The fake AMD SMI's own error type.

    The detector catches whatever the binding module it is handed exposes, so
    the fake brings its own error rather than constructing the real amdsmi
    package's, which takes a status code and is absent here anyway.
    """


class _FakeRocmSmiError(Exception):
    """
    The fake ROCm SMI's own error type.
    """


class _FakeAmdSmi:
    """
    A pyamdsmi stand-in whose processor handles are the fixture cards
    themselves, so every per-device answer is read out of the card.
    """

    AmdSmiException = _FakeAmdSmiError

    class AmdSmiGpuBlock:
        UMC = 1

    def __init__(self, calls: list[str], cards: list[dict]):
        self.calls = calls
        self.cards = cards

    def amdsmi_init(self, *_args):
        self.calls.append("amdsmi_init")

    def amdsmi_get_rocm_version(self) -> str:
        return "6.4.1-123"

    def amdsmi_get_processor_handles(self) -> list[dict]:
        return self.cards

    def amdsmi_get_gpu_asic_info(self, dev: dict) -> dict:
        self.calls.append("amdsmi_get_gpu_asic_info")
        return dev["asic_info"]

    def amdsmi_get_gpu_device_bdf(self, dev: dict) -> str:
        return dev["bdf"]

    def amdsmi_get_gpu_driver_info(self, dev: dict) -> dict:
        return dev["driver_info"]

    def amdsmi_get_gpu_vram_usage(self, dev: dict) -> dict:
        self.calls.append("amdsmi_get_gpu_vram_usage")
        return dev["vram"]

    def amdsmi_get_gpu_ecc_count(self, dev: dict, _block: int) -> dict:
        self.calls.append("amdsmi_get_gpu_ecc_count")
        return dev["ecc"]

    def amdsmi_get_power_info(self, dev: dict) -> dict:
        self.calls.append("amdsmi_get_power_info")
        return dev["power"]

    def amdsmi_get_gpu_metrics_info(self, dev: dict) -> dict:
        self.calls.append("amdsmi_get_gpu_metrics_info")
        if dev["metrics"] is None:
            msg = "GPU metrics are not supported"
            raise _FakeAmdSmiError(msg)
        return dev["metrics"]

    def amdsmi_topo_get_numa_node_number(self, dev: dict) -> int:
        return dev["numa"]

    def amdsmi_get_xgmi_info(self, dev: dict) -> dict:
        return dev["xgmi"]


class _FakeRocmSmi:
    """
    A pyrocmsmi stand-in serving the fallbacks the AMD path keeps for a driver
    whose AMD SMI answers nothing.
    """

    ROCMSMIError = _FakeRocmSmiError

    def __init__(self, calls: list[str], cards: list[dict]):
        self.calls = calls
        self.cards = cards

    def rsmi_init(self, *_args):
        self.calls.append("rsmi_init")

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
1002  Advanced Micro Devices, Inc. [AMD/ATI]
\t74a1  Aqua Vanjaram
\t\t1002 0e3b  Instinct MI300X OAM
"""

_PCI_IDS_NAME = "Instinct MI300X OAM"
_HSA_NAME = "AMD Instinct MI300X"
_ASIC_MARKET_NAME = "Aqua Vanjaram"
_MARKETING_NAME = "AMD Instinct MI300X OAM"


class _FakeAmdGpu:
    """
    A libdrm stand-in exposing only what the name precedence reaches for.

    The real call answers with a pointer into libdrm's own table, and NULL for
    a device id the table does not carry, which the binding reports as "".
    """

    def __init__(self, marketing_name: str):
        self._marketing_name = marketing_name

    def __getattr__(self, name):
        # Everything not faked here -- the error type and the family constants
        # -- comes from the real binding, so the fake cannot drift from it.
        return getattr(pyamdgpu, name)

    @contextlib.contextmanager
    def amdgpu_device(self, card):
        yield SimpleNamespace(card=card)

    def amdgpu_get_marketing_name(self, _device) -> str:
        return self._marketing_name

    def amdgpu_query_gpu_info(self, _device):
        return SimpleNamespace(cu_active_number=0, family_id=0)


_MEMORY_TOTAL = 196592


def _card(
    bdf: str,
    serial: str,
    *,
    gfx_activity: int = 37,
    hotspot: int = 58,
    vram_used: int = 1024,
    socket_power: int = 142,
    no_metrics: bool = False,
) -> dict:
    """
    One fake AMD card: the dict is the processor handle the fake AMD SMI hands
    out, and the PCI IDs the fixture sysfs tree exposes for its BDF.
    """
    return {
        "bdf": bdf,
        "pci_ids": {
            "vendor": "0x1002",
            "device": "0x74a1",
            "subsystem_vendor": "0x1002",
            "subsystem_device": "0x0e3b",
        },
        "asic_info": {
            "market_name": _ASIC_MARKET_NAME,
            "asic_serial": serial,
            "target_graphics_version": "gfx942",
        },
        "driver_info": {"driver_version": "6.12.12"},
        "vram": {"vram_total": _MEMORY_TOTAL, "vram_used": vram_used},
        "ecc": {"uncorrectable_count": 0},
        "power": {
            "power_limit": 750000000,
            "current_socket_power": socket_power,
            "average_socket_power": 138,
        },
        "metrics": None
        if no_metrics
        else {
            "average_gfx_activity": gfx_activity,
            "temperature_hotspot": hotspot,
        },
        "busy_percent": 23,
        "temperature": 47,
        "power_cap": 700,
        "power_used": 131,
        "numa": 0,
        "xgmi": {"xgmi_lanes": 16, "xgmi_hive_id": "0x2b", "xgmi_node_id": 3},
    }


def _agent(bdf: str) -> pyhsa.Agent:
    return pyhsa.Agent(
        device_type=1,
        device_id="0x74a1",
        bdf=bdf,
        uuid="",
        name=_HSA_NAME,
        compute_capability="gfx942",
        compute_units=304,
    )


@pytest.fixture
def amd_bindings(monkeypatch, tmp_path):
    """
    Drive the AMD detector off the fake bindings, a fixture sysfs PCI tree and
    a fixture pci.ids database, returning the shared call log.
    """

    def _setup(
        cards: list[dict],
        agents: list | None = None,
        pci_ids: str | None = _PCI_IDS,
    ) -> list[str]:
        calls: list[str] = []

        monkeypatch.setattr(AMDDetector, "is_supported", staticmethod(lambda: True))
        monkeypatch.setattr(amd, "pyamdsmi", _FakeAmdSmi(calls, cards))
        monkeypatch.setattr(amd, "pyrocmsmi", _FakeRocmSmi(calls, cards))
        monkeypatch.setattr(amd, "pyhsa", _FakeHSA(list(agents or [])))

        pci_devices_path = tmp_path / "pci_devices"
        for card in cards:
            card_path = pci_devices_path / card["bdf"]
            card_path.mkdir(parents=True)
            for name, value in card["pci_ids"].items():
                (card_path / name).write_text(f"{value}\n")
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


def test_detect_info_prefers_the_pci_ids_name(amd_bindings):
    # The operator resolves the board's name from pci.ids before asking the
    # driver, which only knows the chip.
    amd_bindings(
        [_card("0000:05:00.0", "0x00a1b2c3d4e5f600")],
        agents=[_agent("0000:05:00.0")],
    )

    devices = AMDDetector().detect_info()

    assert [dev.name for dev in devices] == [_PCI_IDS_NAME]


def test_detect_info_falls_back_to_the_hsa_product_name(amd_bindings):
    amd_bindings(
        [_card("0000:05:00.0", "0x00a1b2c3d4e5f600")],
        agents=[_agent("0000:05:00.0")],
        pci_ids=None,
    )

    devices = AMDDetector().detect_info()

    assert [dev.name for dev in devices] == [_HSA_NAME]


def test_detect_info_falls_back_to_the_asic_market_name(amd_bindings):
    amd_bindings([_card("0000:05:00.0", "0x00a1b2c3d4e5f600")], pci_ids=None)

    devices = AMDDetector().detect_info()

    assert [dev.name for dev in devices] == [_ASIC_MARKET_NAME]


def test_detect_info_asks_libdrm_for_the_marketing_name(amd_bindings, monkeypatch):
    # Between the HSA name and the ASIC name the operator asks libdrm for the
    # board's marketing name. With pci.ids and HSA both silent, that answer is
    # the one reported -- the ASIC market name is only the step after it.
    amd_bindings([_card("0000:05:00.0", "0x00a1b2c3d4e5f600")], pci_ids=None)
    monkeypatch.setattr(amd, "_get_card_and_renderd_id", lambda _bdf: (1, 128))
    monkeypatch.setattr(amd, "pyamdgpu", _FakeAmdGpu(marketing_name=_MARKETING_NAME))

    devices = AMDDetector().detect_info()

    assert [dev.name for dev in devices] == [_MARKETING_NAME]


def test_detect_info_falls_through_an_unnamed_board(amd_bindings, monkeypatch):
    # libdrm's table carries no entry for every device id and answers NULL
    # rather than failing, which the binding reports as an empty name.
    amd_bindings([_card("0000:05:00.0", "0x00a1b2c3d4e5f600")], pci_ids=None)
    monkeypatch.setattr(amd, "_get_card_and_renderd_id", lambda _bdf: (1, 128))
    monkeypatch.setattr(amd, "pyamdgpu", _FakeAmdGpu(marketing_name=""))

    devices = AMDDetector().detect_info()

    assert [dev.name for dev in devices] == [_ASIC_MARKET_NAME]


def test_detect_info_reports_the_inventory(amd_bindings):
    amd_bindings(
        [_card("0000:05:00.0", "0x00A1B2C3D4E5F600")],
        agents=[_agent("0000:05:00.0")],
    )

    devices = AMDDetector().detect_info()

    dev = devices[0]
    assert dev.manufacturer is ManufacturerEnum.AMD
    assert dev.index == 0
    # The ASIC serial, lowercased and stripped of its "0x".
    assert dev.uuid == "GPU-00a1b2c3d4e5f600"
    assert dev.driver_version == "6.12.12"
    assert dev.runtime_version == "6.4"
    assert dev.runtime_version_original == "6.4.1-123"
    assert dev.compute_capability == "gfx942"
    assert dev.cores == 304
    assert dev.memory == _MEMORY_TOTAL
    assert dev.memory_status is DeviceMemoryStatusEnum.HEALTHY
    # The power limit is inventory, not usage.
    assert dev.power == 750
    assert dev.appendix["bdf"] == "0000:05:00.0"
    assert dev.appendix["numa"] == "0"
    assert dev.appendix["xgmi_lanes"] == 16


def test_detect_reports_no_vgpu(amd_bindings):
    amd_bindings(
        [_card("0000:05:00.0", "0x00a1b2c3d4e5f600")],
        agents=[_agent("0000:05:00.0")],
    )
    det = AMDDetector()

    # There is no virtual/PF/VF classification any more, in either query.
    assert "vgpu" not in det.detect_info()[0].appendix
    assert "vgpu" not in det.detect()[0].appendix


def test_detect_info_issues_no_usage_call(amd_bindings):
    calls = amd_bindings(
        [_card("0000:05:00.0", "0x00a1b2c3d4e5f600")],
        agents=[_agent("0000:05:00.0")],
    )

    devices = AMDDetector().detect_info()

    assert "amdsmi_get_gpu_metrics_info" not in calls
    assert "rsmi_dev_busy_percent_get" not in calls
    assert "rsmi_dev_temp_metric_get" not in calls
    assert "rsmi_dev_power_get" not in calls
    # The power query stays, because the limit it also carries is inventory.
    assert "amdsmi_get_power_info" in calls

    dev = devices[0]
    assert dev.cores_utilization == 0
    assert dev.memory_used == 0
    assert dev.memory_utilization == 0
    assert dev.temperature is None
    assert dev.power_used is None


# --------------------------------------------------------------------------- #
# detect_usage.                                                               #
# --------------------------------------------------------------------------- #


def test_detect_usage_merges_by_uuid(amd_bindings):
    amd_bindings(
        [
            _card("0000:05:00.0", "0x00a1b2c3d4e5f600"),
            _card(
                "0000:06:00.0",
                "0x00a1b2c3d4e5f601",
                gfx_activity=91,
                hotspot=72,
                vram_used=8192,
                socket_power=311,
            ),
        ],
        agents=[_agent("0000:05:00.0"), _agent("0000:06:00.0")],
    )
    det = AMDDetector()
    devices = det.detect_info()

    # Reversed, to prove the merge joins by UUID rather than by position.
    det.detect_usage(list(reversed(devices)))

    assert [dev.cores_utilization for dev in devices] == [37, 91]
    assert [dev.temperature for dev in devices] == [58, 72]
    assert [dev.memory_used for dev in devices] == [1024, 8192]
    assert [dev.power_used for dev in devices] == [142, 311]
    assert [dev.memory_utilization for dev in devices] == [0.52, 4.17]
    assert [dev.memory_status for dev in devices] == [
        DeviceMemoryStatusEnum.HEALTHY,
        DeviceMemoryStatusEnum.HEALTHY,
    ]
    # The information fields survive the merge.
    assert [dev.memory for dev in devices] == [_MEMORY_TOTAL, _MEMORY_TOTAL]
    assert [dev.power for dev in devices] == [750, 750]


def test_detect_usage_falls_back_to_rocm_smi(amd_bindings):
    calls = amd_bindings(
        [_card("0000:05:00.0", "0x00a1b2c3d4e5f600", no_metrics=True)],
        agents=[_agent("0000:05:00.0")],
    )

    devices = AMDDetector().detect()

    assert "rsmi_dev_busy_percent_get" in calls
    assert "rsmi_dev_temp_metric_get" in calls
    assert devices[0].cores_utilization == 23
    assert devices[0].temperature == 47


def test_detect_usage_detects_the_information_first(amd_bindings):
    amd_bindings(
        [_card("0000:05:00.0", "0x00a1b2c3d4e5f600")],
        agents=[_agent("0000:05:00.0")],
    )

    devices = AMDDetector().detect_usage()

    assert devices[0].name == _PCI_IDS_NAME
    assert devices[0].cores_utilization == 37
    assert devices[0].power_used == 142


def test_detect_composes_the_information_and_the_usage(amd_bindings):
    amd_bindings(
        [_card("0000:05:00.0", "0x00a1b2c3d4e5f600")],
        agents=[_agent("0000:05:00.0")],
    )

    devices = AMDDetector().detect()

    assert devices[0].memory == _MEMORY_TOTAL
    assert devices[0].memory_used == 1024
    assert devices[0].power == 750
    assert devices[0].power_used == 142


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
    monkeypatch.setattr(cdi_amd, "device_to_cdi_device_node", _fake_device_node)

    config = cdi_amd.AMDGenerator().generate(
        [
            Device(
                manufacturer=ManufacturerEnum.AMD,
                index=0,
                name=_PCI_IDS_NAME,
                uuid="GPU-a1b2c3d4e5f600",
                appendix={"card_id": 5, "renderd_id": 133},
            ),
        ],
    )

    # The DRM numbering comes from the appendix, never from Device.index --
    # here the card is enumerated at 0 and its nodes are card5 / renderD133.
    device_nodes = config["devices"][0]["containerEdits"]["deviceNodes"]
    assert [node["path"] for node in device_nodes] == [
        "/dev/dri/card5",
        "/dev/dri/renderD133",
    ]
    assert [dev["name"] for dev in config["devices"]] == [
        "0",
        "GPU-a1b2c3d4e5f600",
        "all",
    ]
    assert [node["path"] for node in config["containerEdits"]["deviceNodes"]] == [
        "/dev/kfd",
    ]
