from __future__ import annotations

import pytest

from gpustack_runtime import envs
from gpustack_runtime.detector import (
    _DETECTORS,
    Device,
    DeviceMemoryStatusEnum,
    Devices,
    ManufacturerEnum,
)
from gpustack_runtime.detector.__types__ import (
    Detector,
    merge_devices_usage,
)
from gpustack_runtime.detector.__utils__ import (
    _load_pci_device_names,
    get_pci_device_name,
)

# --------------------------------------------------------------------------- #
# Detector ABC: detect_info / detect_usage / detect composition.               #
# --------------------------------------------------------------------------- #


class _RecordingDetector(Detector):
    """
    A detector recording which query it was asked for, as the vendors behave
    during the expand step: detect_info returns everything, detect_usage
    merges a usage payload in.
    """

    def __init__(
        self,
        devices: Devices | None = None,
        usages: Devices | None = None,
    ):
        super().__init__(ManufacturerEnum.UNKNOWN)
        self.devices = devices
        self.usages = usages
        self.calls: list[str] = []

    @staticmethod
    def is_supported() -> bool:
        return True

    def detect_info(self) -> Devices | None:
        self.calls.append("info")
        return self.devices

    def detect_usage(self, devices: Devices | None = None) -> Devices | None:
        self.calls.append("usage")
        if devices is None:
            devices = self.detect_info()
        return merge_devices_usage(devices, self.usages)


def _device(uuid: str, **kwargs) -> Device:
    device = Device(
        manufacturer=ManufacturerEnum.NVIDIA,
        index=0,
        name="NVIDIA A100-SXM4-40GB",
        uuid=uuid,
        cores=6912,
        memory=40960,
        appendix={},
    )
    for key, value in kwargs.items():
        setattr(device, key, value)
    return device


def _usage(uuid: str, cores_utilization: int = 42) -> Device:
    return Device(
        uuid=uuid,
        cores_utilization=cores_utilization,
        memory_used=1024,
        memory_utilization=2.5,
        memory_status=DeviceMemoryStatusEnum.UNHEALTHY,
        temperature=61,
        power_used=250,
    )


def test_detect_composes_information_then_usage():
    det = _RecordingDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])

    devices = det.detect()

    assert det.calls == ["info", "usage"]
    assert devices[0].cores_utilization == 42
    assert devices[0].power_used == 250


def test_detect_skips_the_usage_query_when_not_asked():
    det = _RecordingDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])

    devices = det.detect(usage=False)

    assert det.calls == ["info"]
    # The information query owns no usage field, so they keep their defaults.
    assert devices[0].cores_utilization == 0
    assert devices[0].power_used is None


def test_detect_reports_the_inventory_when_the_usage_query_fails():
    # A card exists whether or not its metrics can be read. detect_devices logs
    # a raising detector and moves on, so letting this propagate would report no
    # hardware at all on a host whose inventory query just succeeded.
    class _FailingUsageDetector(_RecordingDetector):
        def detect_usage(self, devices: Devices | None = None) -> Devices | None:
            self.calls.append("usage")
            msg = "the driver lost a device mid-pass"
            raise RuntimeError(msg)

    det = _FailingUsageDetector(devices=[_device("GPU-0")])

    devices = det.detect()

    assert det.calls == ["info", "usage"]
    assert [dev.uuid for dev in devices] == ["GPU-0"]
    assert devices[0].cores_utilization == 0


def test_detect_skips_the_usage_query_without_devices():
    for empty in (None, []):
        det = _RecordingDetector(devices=empty)

        assert det.detect() == empty
        assert det.calls == ["info"]


@pytest.mark.parametrize(
    "name, body",
    [
        # Neither query implemented.
        ("_NoQueryDetector", {}),
        # Only the information query: a vendor cannot keep a permissive usage
        # default that makes detect() look like it measured something.
        ("_InfoOnlyDetector", {"detect_info": lambda _self: None}),
        # Only the usage query.
        ("_UsageOnlyDetector", {"detect_usage": lambda _self, devices=None: devices}),
    ],
)
def test_a_detector_skipping_the_split_cannot_be_instantiated(name, body):
    detector_type = type(
        name,
        (Detector,),
        {"is_supported": staticmethod(lambda: True), **body},
    )

    # Both queries are abstract, so a future vendor fails at construction
    # rather than silently returning an unmeasured device at run time.
    with pytest.raises(TypeError):
        detector_type(ManufacturerEnum.UNKNOWN)


def test_a_detector_implementing_both_queries_can_be_instantiated():
    assert _RecordingDetector().detect() is None


# --------------------------------------------------------------------------- #
# merge_devices_usage.                                                        #
# --------------------------------------------------------------------------- #


def test_merge_devices_usage_matches_by_uuid():
    devices = [_device("GPU-0"), _device("GPU-1")]

    # Reversed *and* told apart by their payload: the operator's monitor pass
    # returns its own list and every consumer joins it by device identity,
    # never by index. Identical payloads would leave a positional join, a
    # reversed mapping and a broadcast-to-every-card indistinguishable, so the
    # values have to differ for this to pin anything.
    merge_devices_usage(
        devices,
        [_usage("GPU-1", cores_utilization=7), _usage("GPU-0", cores_utilization=42)],
    )

    assert [dev.cores_utilization for dev in devices] == [42, 7]
    assert [dev.memory_used for dev in devices] == [1024, 1024]
    assert [dev.temperature for dev in devices] == [61, 61]
    assert [dev.memory_status for dev in devices] == [
        DeviceMemoryStatusEnum.UNHEALTHY,
        DeviceMemoryStatusEnum.UNHEALTHY,
    ]


def test_merge_devices_usage_keeps_a_health_verdict_the_usage_never_read():
    # No vendor's health helper returns UNKNOWN -- they answer HEALTHY or
    # UNHEALTHY -- so an entry carrying UNKNOWN never read health, and writing it
    # would erase the information query's verdict. The CLI renders UNKNOWN as
    # ERR too, so the erasure is not even visible as one.
    devices = [_device("GPU-0", memory_status=DeviceMemoryStatusEnum.UNHEALTHY)]
    usage = _usage("GPU-0")
    usage.memory_status = DeviceMemoryStatusEnum.UNKNOWN

    merge_devices_usage(devices, [usage])

    assert devices[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY
    # Every other usage field still merges.
    assert devices[0].cores_utilization == 42
    assert devices[0].temperature == 61


def test_merge_devices_usage_downgrades_a_health_verdict_the_usage_did_read():
    devices = [_device("GPU-0", memory_status=DeviceMemoryStatusEnum.UNHEALTHY)]
    usage = _usage("GPU-0")
    usage.memory_status = DeviceMemoryStatusEnum.HEALTHY

    merge_devices_usage(devices, [usage])

    # A card that recovered must be allowed to say so.
    assert devices[0].memory_status == DeviceMemoryStatusEnum.HEALTHY


def test_merge_devices_usage_drops_an_ambiguous_uuid():
    # A driver answering the same id for every card -- rocm-smi's unique id
    # reporting 0, say -- leaves two cards indistinguishable. Writing either
    # card's metrics onto both is worse than reporting neither, so the merge
    # leaves them with what the information query read.
    devices = [_device("GPU-0"), _device("GPU-0")]

    merge_devices_usage(
        devices,
        [_usage("GPU-0", cores_utilization=7), _usage("GPU-0", cores_utilization=42)],
    )

    assert [dev.cores_utilization for dev in devices] == [0, 0]
    assert [dev.memory_used for dev in devices] == [0, 0]


def test_merge_devices_usage_keeps_the_information_fields():
    devices = [_device("GPU-0")]
    usage = _usage("GPU-0")
    usage.name = "wrong"
    usage.memory = 1
    usage.cores = 1
    usage.index = 7

    merge_devices_usage(devices, [usage])

    assert devices[0].name == "NVIDIA A100-SXM4-40GB"
    assert devices[0].memory == 40960
    assert devices[0].cores == 6912
    assert devices[0].index == 0


def test_merge_devices_usage_merges_the_mig_devices():
    mig_device = {
        "index": 2,
        "name": "1g.5gb",
        "uuid": "MIG-0-0",
        "memory": 4864,
        "cores_utilization": 0,
        "memory_used": 0,
        "memory_status": DeviceMemoryStatusEnum.HEALTHY,
        "appendix": {"sliced": True, "mig": True},
    }
    devices = [_device("GPU-0", appendix={"mig": True, "mig_devices": [mig_device]})]

    merge_devices_usage(devices, [_usage("MIG-0-0"), _usage("GPU-0")])

    assert mig_device["cores_utilization"] == 42
    assert mig_device["memory_used"] == 1024
    assert mig_device["temperature"] == 61
    assert mig_device["memory_status"] == DeviceMemoryStatusEnum.UNHEALTHY
    # The MIG entry stays an appendix entry, and its own fields stay put.
    assert mig_device["name"] == "1g.5gb"
    assert mig_device["memory"] == 4864
    assert devices[0].cores_utilization == 42


def test_merge_devices_usage_ignores_an_unknown_uuid():
    devices = [_device("GPU-0")]

    merge_devices_usage(devices, [_usage("GPU-9")])

    assert devices[0].cores_utilization == 0
    assert devices[0].power_used is None


def test_merge_devices_usage_tolerates_nothing_to_merge():
    devices = [_device("GPU-0", appendix=None)]

    assert merge_devices_usage(devices, None) is devices
    assert merge_devices_usage(devices, []) is devices
    assert merge_devices_usage(None, [_usage("GPU-0")]) is None


# --------------------------------------------------------------------------- #
# The vendor detectors, as the expand step leaves them.                       #
# --------------------------------------------------------------------------- #


def test_every_detector_implements_the_split():
    for det in _DETECTORS:
        # No vendor overrides detect: its payload is detect_info's, plus a
        # usage merge that is a no-op until the vendor migrates. That is what
        # keeps detect()'s output identical to the pre-split one.
        assert type(det).detect is Detector.detect, det.name
        assert type(det).detect_info is not Detector.detect_info, det.name


def test_every_detector_accepts_no_usage():
    for det in _DETECTORS:
        devices = det.detect(usage=False)
        assert devices is None or isinstance(devices, list), det.name


def test_no_physical_index_switch_survives():
    # Device.index is the enumeration index now, with no switch to make it the
    # driver-physical number. Asserted on the whole environment surface rather
    # than on the retired name, so a renamed reincarnation fails as well.
    assert [name for name in dir(envs) if "PHYSICAL_INDEX" in name] == []


# --------------------------------------------------------------------------- #
# get_pci_device_name: the pci.ids lookup mirroring the operator's            #
# GetPCIDeviceNames / GetName.                                                #
# --------------------------------------------------------------------------- #

# An extract of the real pci.ids, keeping its exact shape: a class section, a
# comment, vendor lines at column 0, device lines behind one tab, subsystem
# lines behind two.
_PCI_IDS = """\
#\tList of PCI ID's
#
1002  Advanced Micro Devices, Inc. [AMD/ATI]
\t744c  Navi 31 [Radeon RX 7900 XT/7900 XTX/7900M]
\t\t1002 0e0d  Radeon RX 7900 XTX
\t\t1eae 7900  RX 7900 XTX Phantom Gaming
\t74a1  Aqua Vanjaram [Instinct MI300X]
1d94  Chengdu Haiguang IC Design Co., Ltd.
\t6210  Kunpeng [K100 AI]
\t\t1d94 6210  K100_AI
10de  NVIDIA Corporation
\t2330  GH100 [H100 SXM5 80GB]
C 03  Display controller
\t00  VGA compatible controller
"""


@pytest.fixture
def pci_ids(tmp_path, monkeypatch):
    """
    Point the pci.ids lookup at a fixture database.
    """

    def _write(content: str | None = _PCI_IDS) -> None:
        paths: tuple[str, ...] = ()
        if content is not None:
            path = tmp_path / "pci.ids"
            path.write_text(content, encoding="utf-8")
            paths = (str(path),)
        monkeypatch.setattr(
            "gpustack_runtime.detector.__utils__._PCI_IDS_PATHS",
            paths,
        )
        _load_pci_device_names.cache_clear()

    yield _write

    _load_pci_device_names.cache_clear()


def test_get_pci_device_name_resolves_a_vendor_device_pair(pci_ids):
    pci_ids()

    assert get_pci_device_name("1002", "74a1") == "Aqua Vanjaram [Instinct MI300X]"
    assert get_pci_device_name("1d94", "6210") == "Kunpeng [K100 AI]"
    assert get_pci_device_name("10de", "2330") == "GH100 [H100 SXM5 80GB]"


def test_get_pci_device_name_normalizes_the_ids(pci_ids):
    pci_ids()

    # sysfs hands out "0x1002", the SMI libraries hand out an int, and pci.ids
    # is lowercase hexadecimal.
    for vendor, device in (
        ("0x1002", "0x74A1"),
        ("1002", "74A1"),
        (0x1002, 0x74A1),
    ):
        assert get_pci_device_name(vendor, device) == "Aqua Vanjaram [Instinct MI300X]"


def test_get_pci_device_name_prefers_the_subsystem_name(pci_ids):
    pci_ids()

    assert (
        get_pci_device_name("1002", "744c", "1eae", "7900")
        == "RX 7900 XTX Phantom Gaming"
    )
    # An unknown subsystem falls back to the device name, as the operator's
    # GetName does.
    assert (
        get_pci_device_name("1002", "744c", "1eae", "0000")
        == "Navi 31 [Radeon RX 7900 XT/7900 XTX/7900M]"
    )
    assert (
        get_pci_device_name("1002", "744c")
        == "Navi 31 [Radeon RX 7900 XT/7900 XTX/7900M]"
    )


def test_get_pci_device_name_returns_nothing_when_unknown(pci_ids):
    pci_ids()

    assert get_pci_device_name("1002", "ffff") == ""
    assert get_pci_device_name("ffff", "744c") == ""
    assert get_pci_device_name("", "") == ""
    # The device class table trailing the vendors closes the vendor section:
    # its entries must not be attributed to the last vendor parsed.
    assert get_pci_device_name("10de", "00") == ""


def test_get_pci_device_name_tolerates_a_missing_database(pci_ids):
    pci_ids(content=None)

    assert get_pci_device_name("1002", "74a1") == ""
