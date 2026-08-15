from __future__ import annotations

import pytest

from gpustack_runtime import envs
from gpustack_runtime.deployer.cdi import iluvatar as cdi_iluvatar
from gpustack_runtime.deployer.cdi.iluvatar import IluvatarGenerator
from gpustack_runtime.detector import Device, ManufacturerEnum, iluvatar
from gpustack_runtime.detector.__utils__ import byte_to_mebibyte
from gpustack_runtime.detector.iluvatar import IluvatarDetector


@pytest.mark.skipif(
    not IluvatarDetector.is_supported(),
    reason="Iluvatar GPU not detected",
)
def test_detect():
    det = IluvatarDetector()
    devs = det.detect()
    print(devs)


@pytest.mark.skipif(
    not IluvatarDetector.is_supported(),
    reason="Iluvatar GPU not detected",
)
def test_get_topology():
    det = IluvatarDetector()
    topo = det.get_topology()
    print(topo)


# --------------------------------------------------------------------------- #
# A fake pyixml carrying a call log, so a test can assert exactly which       #
# driver calls detect_info/detect_usage make -- several acceptance criteria   #
# are "issues no metric call", which the returned values alone cannot prove.  #
# --------------------------------------------------------------------------- #


class _FakeNVMLError(Exception):
    """
    Stand-in for pyixml.NVMLError.
    """


class _FakeHandle:
    def __init__(self, index: int):
        self.index = index


class _FakeMemoryInfo:
    def __init__(self, total: int, used: int):
        self.total = total
        self.used = used


class _FakeUtilizationRates:
    def __init__(self, gpu: int):
        self.gpu = gpu


class _FakePciInfo:
    def __init__(self, bus_id: str):
        self.busIdLegacy = bus_id


class FakePyixml:
    """
    A fake pyixml binding, standing in for the real ctypes module so the
    Iluvatar detector can be exercised on a machine with no IXML driver.
    """

    NVMLError = _FakeNVMLError
    IXML_HEALTH_OK = 0
    NVML_TEMPERATURE_GPU = 0
    NVML_AFFINITY_SCOPE_NODE = 0
    nvmlMemory_v2 = 0x02000028  # noqa: N815

    def __init__(self, *, device_count: int = 2, v2_memory: bool = True):
        self.calls: list[str] = []
        self.device_count = device_count
        self.v2_memory = v2_memory
        # Deliberately different from the v1 numbers, so a test can tell
        # which accessor a returned value came from.
        self.v2_memory_total = 32 * 1024**3
        self.v2_memory_used = 8 * 1024**3
        self.v1_memory_total = 16 * 1024**3
        self.v1_memory_used = 4 * 1024**3

    # The method names below mirror pyixml's real (camelCase) API one-for-one,
    # so a test can monkeypatch this in as a drop-in for the module.
    def nvmlInit(self):  # noqa: N802
        self.calls.append("nvmlInit")

    def nvmlSystemGetDriverVersion(self):  # noqa: N802
        self.calls.append("nvmlSystemGetDriverVersion")
        return "4.2.0"

    def nvmlSystemGetCudaDriverVersion(self):  # noqa: N802
        self.calls.append("nvmlSystemGetCudaDriverVersion")
        return 10020

    def nvmlDeviceGetCount(self):  # noqa: N802
        self.calls.append("nvmlDeviceGetCount")
        return self.device_count

    def nvmlDeviceGetHandleByIndex(self, index):  # noqa: N802
        self.calls.append("nvmlDeviceGetHandleByIndex")
        return _FakeHandle(index)

    def nvmlDeviceGetMinorNumber(self, dev):  # noqa: N802
        self.calls.append("nvmlDeviceGetMinorNumber")
        return 10 + dev.index

    def nvmlDeviceGetName(self, dev):  # noqa: N802
        self.calls.append("nvmlDeviceGetName")
        return "Iluvatar BI-V150"

    def nvmlDeviceGetUUID(self, dev):  # noqa: N802
        self.calls.append("nvmlDeviceGetUUID")
        return f"GPU-{dev.index}"

    def nvmlDeviceGetNumGpuCores(self, dev):  # noqa: N802
        self.calls.append("nvmlDeviceGetNumGpuCores")
        return 4096

    def nvmlDeviceGetMemoryInfo(self, dev, version=None):  # noqa: N802
        if version:
            self.calls.append("nvmlDeviceGetMemoryInfo_v2")
            if not self.v2_memory:
                msg = "v2 memory info not supported"
                raise self.NVMLError(msg)
            return _FakeMemoryInfo(self.v2_memory_total, self.v2_memory_used)
        self.calls.append("nvmlDeviceGetMemoryInfo")
        return _FakeMemoryInfo(self.v1_memory_total, self.v1_memory_used)

    def ixmlDeviceGetHealth(self, dev):  # noqa: N802
        self.calls.append("ixmlDeviceGetHealth")
        return self.IXML_HEALTH_OK

    def nvmlDeviceGetPowerManagementDefaultLimit(self, dev):  # noqa: N802
        self.calls.append("nvmlDeviceGetPowerManagementDefaultLimit")
        return 250_000

    def nvmlDeviceGetPowerUsage(self, dev):  # noqa: N802
        self.calls.append("nvmlDeviceGetPowerUsage")
        return 90_000

    def nvmlDeviceGetCudaComputeCapability(self, dev):  # noqa: N802
        self.calls.append("nvmlDeviceGetCudaComputeCapability")
        return (7, 0)

    def nvmlDeviceGetPciInfo(self, dev):  # noqa: N802
        self.calls.append("nvmlDeviceGetPciInfo")
        return _FakePciInfo(f"0000:0{dev.index}:00.0")

    def nvmlDeviceGetMemoryAffinity(self, dev, size, scope):  # noqa: N802
        self.calls.append("nvmlDeviceGetMemoryAffinity")
        return [0]

    def nvmlDeviceGetUtilizationRates(self, dev):  # noqa: N802
        self.calls.append("nvmlDeviceGetUtilizationRates")
        return _FakeUtilizationRates(37)

    def nvmlDeviceGetTemperature(self, dev, sensor):  # noqa: N802
        self.calls.append("nvmlDeviceGetTemperature")
        return 55


@pytest.fixture(autouse=True)
def _reset_is_supported_cache():
    # is_supported()/detect_pci_devices() are lru_cache'd, so a value
    # observed by one test would otherwise leak into the next.
    IluvatarDetector.is_supported.cache_clear()
    IluvatarDetector.detect_pci_devices.cache_clear()
    yield
    IluvatarDetector.is_supported.cache_clear()
    IluvatarDetector.detect_pci_devices.cache_clear()


@pytest.fixture
def fake_pyixml(monkeypatch):
    def _install(**kwargs) -> FakePyixml:
        fake = FakePyixml(**kwargs)
        monkeypatch.setattr(iluvatar, "pyixml", fake)
        # No PCI sysfs tree exists on the dev machine, so bypass the PCI
        # presence check that is_supported() otherwise gates on.
        monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DETECT_NO_PCI_CHECK", True)
        return fake

    return _install


# --------------------------------------------------------------------------- #
# detect_info: memory V2->V1 fallback, no vgpu, no usage calls.               #
# --------------------------------------------------------------------------- #


def test_detect_info_prefers_v2_memory_over_v1(fake_pyixml):
    fake = fake_pyixml()

    devices = IluvatarDetector().detect_info()

    assert [dev.memory for dev in devices] == [
        byte_to_mebibyte(fake.v2_memory_total),
    ] * fake.device_count
    assert "nvmlDeviceGetMemoryInfo_v2" in fake.calls
    assert "nvmlDeviceGetMemoryInfo" not in fake.calls


def test_detect_info_falls_back_to_v1_memory_when_v2_unavailable(fake_pyixml):
    fake = fake_pyixml(v2_memory=False)

    devices = IluvatarDetector().detect_info()

    assert [dev.memory for dev in devices] == [
        byte_to_mebibyte(fake.v1_memory_total),
    ] * fake.device_count
    assert fake.calls.count("nvmlDeviceGetMemoryInfo_v2") == fake.device_count
    assert fake.calls.count("nvmlDeviceGetMemoryInfo") == fake.device_count


def test_detect_info_has_no_vgpu_in_appendix(fake_pyixml):
    fake_pyixml()

    devices = IluvatarDetector().detect_info()

    assert all("vgpu" not in dev.appendix for dev in devices)


def test_detect_info_keeps_the_minor_number_appendix(fake_pyixml):
    fake_pyixml()

    devices = IluvatarDetector().detect_info()

    assert [dev.appendix["minor_number"] for dev in devices] == [10, 11]


def test_detect_info_issues_no_usage_calls(fake_pyixml):
    fake = fake_pyixml()

    IluvatarDetector().detect_info()

    usage_only_calls = {
        "nvmlDeviceGetUtilizationRates",
        "nvmlDeviceGetTemperature",
        "nvmlDeviceGetPowerUsage",
    }
    assert usage_only_calls.isdisjoint(fake.calls)


# --------------------------------------------------------------------------- #
# detect_usage: merges by uuid, same V2->V1 fallback.                        #
# --------------------------------------------------------------------------- #


def test_detect_usage_merges_the_usage_fields_by_uuid(fake_pyixml):
    fake = fake_pyixml()

    devices = IluvatarDetector().detect_info()
    fake.calls.clear()

    result = IluvatarDetector().detect_usage(devices)

    assert result is devices
    for dev in devices:
        assert dev.cores_utilization == 37
        assert dev.memory_used == byte_to_mebibyte(fake.v2_memory_used)
        assert dev.temperature == 55
        assert dev.power_used == 90


def test_detect_usage_falls_back_to_v1_memory_when_v2_unavailable(fake_pyixml):
    fake = fake_pyixml(v2_memory=False)

    devices = IluvatarDetector().detect_info()
    result = IluvatarDetector().detect_usage(devices)

    assert all(
        dev.memory_used == byte_to_mebibyte(fake.v1_memory_used) for dev in result
    )


def test_detect_composes_info_and_usage_by_default(fake_pyixml):
    fake_pyixml()

    devices = IluvatarDetector().detect()

    assert devices[0].cores_utilization == 37
    assert devices[0].power_used == 90
    assert "vgpu" not in devices[0].appendix


# --------------------------------------------------------------------------- #
# CDI: /dev/iluvatar{N} still reads the appendix minor number.               #
# --------------------------------------------------------------------------- #


def test_cdi_reads_the_appendix_minor_number(monkeypatch):
    seen_paths: list[str] = []

    def _fake_device_node(path):
        seen_paths.append(path)
        return {"path": path}

    monkeypatch.setattr(cdi_iluvatar, "device_to_cdi_device_node", _fake_device_node)

    devices = [
        Device(
            manufacturer=ManufacturerEnum.ILUVATAR,
            index=0,
            name="Iluvatar BI-V150",
            uuid="GPU-0",
            memory=32768,
            appendix={"bdf": "0000:00:00.0", "minor_number": 7},
        ),
        Device(
            manufacturer=ManufacturerEnum.ILUVATAR,
            index=1,
            name="Iluvatar BI-V150",
            uuid="GPU-1",
            memory=32768,
            appendix={"bdf": "0000:01:00.0"},
        ),
    ]

    config = IluvatarGenerator().generate(devices)

    assert config is not None
    # The device carrying a minor number is addressed by it.
    assert "/dev/iluvatar7" in seen_paths
    # The device without one falls back to Device.index.
    assert "/dev/iluvatar1" in seen_paths
