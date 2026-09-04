from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest

from gpustack_runtime.deployer.cdi import thead as cdi_thead
from gpustack_runtime.deployer.cdi.thead import THeadGenerator
from gpustack_runtime.detector import pyhgml, thead
from gpustack_runtime.detector.__types__ import (
    Device,
    DeviceMemoryStatusEnum,
    ManufacturerEnum,
)
from gpustack_runtime.detector.thead import THeadDetector


@pytest.mark.skipif(
    not THeadDetector.is_supported(),
    reason="T-Head PPU not detected",
)
def test_detect():
    det = THeadDetector()
    devs = det.detect()
    print(devs)


@pytest.mark.skipif(
    not THeadDetector.is_supported(),
    reason="T-Head PPU not detected",
)
def test_get_topology():
    det = THeadDetector()
    topo = det.get_topology()
    print(topo)


# --------------------------------------------------------------------------- #
# A fake pyhgml, so the split is provable on a host with no T-Head driver.     #
# --------------------------------------------------------------------------- #

_MIB = 1 << 20


@dataclass
class _Instance:
    """
    One GPU/compute instance of a MIG-enabled card, as the driver reports it.

    The runtime enumerates these where the operator does not: the physical card
    stays the reported device and its instances live in
    ``appendix["mig_devices"]``.
    """

    uuid: str
    gpu_instance_id: int = 0
    compute_instance_id: int = 0
    memory_total: int = 8192 * _MIB
    memory_used: int = 512 * _MIB
    ecc_errors: int = 0
    ecc_counter_error_code: int | None = None
    """
    The error code the ECC counter query raises, or None for a readable one.
    """
    sm_util: float | None = 60.0
    """
    The SM utilization GPM samples for the instance, or None when unreadable.
    """
    uuid_readable: bool = True
    """
    Whether the driver answers this instance's UUID, i.e. whether it is the one
    faulty instance of an otherwise healthy card.
    """


@dataclass
class _Card:
    """
    One card as the driver would report it, in the driver's own units.
    """

    uuid: str
    name: str = "T-Head PPU"
    bdf: str = "0000:01:00.0"
    minor_number: int | None = 3
    compute_capability: tuple[int, int] = (8, 0)
    cores: int = 128
    memory_total: int = 65536 * _MIB
    memory_used: int = 1024 * _MIB
    ecc_errors: int = 0
    ecc_counter_error_code: int | None = None
    """
    The error code the ECC counter query raises, or None for a readable one.
    """
    cores_utilization: int = 33
    temperature: int = 55
    power_limit: int = 350_000  # mW
    power_usage: int = 150_000  # mW
    instances: list[_Instance] | None = None
    """
    The card's GPU/compute instances, or None for a card with MIG disabled.
    """


class _GpmMetrics:
    """
    A GPM metrics request, as the detector fills and reads it.
    """

    def __init__(self):
        self.version = 0
        self.numMetrics = 0
        self.sample1 = None
        self.sample2 = None
        self.metrics = [SimpleNamespace(metricId=0, value=float("nan"))]


@dataclass
class _FakeHGML:
    """
    A stand-in for the pyhgml binding, recording every call the detector makes.

    The error type is the real module's and every HGML_* constant is delegated
    to it, so a fake drifting from the binding's contract fails here rather than
    on hardware. Entry points are dispatched by name, as the fake pymxsml of
    test_metax.py does, which keeps HGML's camelCase out of the handlers' own
    names -- pyhgml's surface is wide enough for that to matter.
    """

    cards: list[_Card]
    calls: list[str] = field(default_factory=list)
    gpm_supported: bool = True

    HGMLError = pyhgml.HGMLError

    c_hgmlGpmMetricsGet_t = _GpmMetrics  # noqa: N815

    instance_profile_name = "1g.8gb"
    instance_profile_memory_mb = 8192
    instance_profile_cores = 16

    def __getattr__(self, name: str):
        handler = {
            "hgmlInit": self._init,
            "hgmlSystemGetDriverVersion": self._system_get_driver_version,
            "hgmlSystemGetHggcDriverVersion": self._system_get_hggc_driver_version,
            "hgmlDeviceGetCount": self._device_get_count,
            "hgmlDeviceGetHandleByIndex": self._device_get_handle_by_index,
            "hgmlDeviceGetHggcComputeCapability": self._device_get_compute_capability,
            "hgmlDeviceGetPciInfo": self._device_get_pci_info,
            "hgmlDeviceGetMemoryAffinity": self._device_get_memory_affinity,
            "hgmlDeviceGetMinorNumber": self._device_get_minor_number,
            "hgmlDeviceGetName": self._device_get_name,
            "hgmlDeviceGetUUID": self._device_get_uuid,
            "hgmlDeviceGetNumGpuCores": self._device_get_num_gpu_cores,
            "hgmlDeviceGetMemoryInfo": self._device_get_memory_info,
            "hgmlDeviceGetMemoryErrorCounter": self._device_get_memory_error_counter,
            "hgmlDeviceGetPowerManagementDefaultLimit": self._device_get_power_limit,
            "hgmlDeviceGetUtilizationRates": self._device_get_utilization_rates,
            "hgmlDeviceGetTemperature": self._device_get_temperature,
            "hgmlDeviceGetPowerUsage": self._device_get_power_usage,
            "hgmlDeviceGetMigMode": self._device_get_mig_mode,
            "hgmlDeviceGetMaxMigDeviceCount": self._device_get_max_mig_device_count,
            "hgmlDeviceGetMigDeviceHandleByIndex": self._device_get_mig_handle,
            "hgmlDeviceGetGpuInstanceId": self._device_get_gpu_instance_id,
            "hgmlDeviceGetComputeInstanceId": self._device_get_compute_instance_id,
            "hgmlDeviceGetGpuInstanceById": self._device_get_gpu_instance_by_id,
            "hgmlGpuInstanceGetComputeInstanceById": self._gi_get_ci_by_id,
            "hgmlGpuInstanceGetInfo": self._gi_get_info,
            "hgmlComputeInstanceGetInfo": self._ci_get_info,
            "hgmlDeviceGetGpuInstanceProfileInfo": self._device_get_gi_profile_info,
            "hgmlGpuInstanceGetComputeInstanceProfileInfo": self._gi_get_ci_profile_info,
            "hgmlGpmQueryDeviceSupport": self._gpm_query_device_support,
            "hgmlGpmSampleAlloc": self._gpm_sample_alloc,
            "hgmlGpmSampleFree": self._gpm_sample_free,
            "hgmlGpmSampleGet": self._gpm_sample_get,
            "hgmlGpmMigSampleGet": self._gpm_mig_sample_get,
            "hgmlGpmMetricsGet": self._gpm_metrics_get,
        }.get(name)
        if handler is None:
            if name.startswith("HGML_"):
                # Enumeration constants are the real module's, so a fake
                # drifting from the binding's contract fails here rather than
                # on hardware.
                return getattr(pyhgml, name)
            msg = f"module pyhgml has no attribute {name}"
            raise AttributeError(msg)

        def entry_point(*args, **kwargs):
            self.calls.append(name)
            return handler(*args, **kwargs)

        return entry_point

    # System.

    def _init(self) -> None:
        pass

    def _system_get_driver_version(self) -> str:
        return "1.2.3"

    def _system_get_hggc_driver_version(self) -> int:
        return 12030  # i.e. 12.3.0

    def _device_get_count(self) -> int:
        return len(self.cards)

    def _device_get_handle_by_index(self, index: int) -> _Card:
        return self.cards[index]

    # Identity and capability.

    def _device_get_compute_capability(self, handle: _Card) -> tuple[int, int]:
        return handle.compute_capability

    def _device_get_pci_info(self, handle: _Card) -> SimpleNamespace:
        return SimpleNamespace(busIdLegacy=handle.bdf)

    def _device_get_memory_affinity(
        self,
        handle: _Card,
        node_set_size: int,
        scope: int,
    ) -> list[int]:
        msg = "no memory affinity"
        raise pyhgml.HGMLError(msg)

    def _device_get_minor_number(self, handle: _Card) -> int:
        if handle.minor_number is None:
            msg = "no minor number"
            raise pyhgml.HGMLError(msg)
        return handle.minor_number

    def _device_get_name(self, handle: _Card) -> str:
        return handle.name

    def _device_get_uuid(self, handle: _Card | _Instance) -> str:
        if not getattr(handle, "uuid_readable", True):
            raise self.HGMLError(pyhgml.HGML_ERROR_NOT_FOUND)
        return handle.uuid

    def _device_get_num_gpu_cores(self, handle: _Card) -> int:
        return handle.cores

    def _device_get_power_limit(self, handle: _Card) -> int:
        return handle.power_limit

    # Memory.

    def _device_get_memory_info(self, handle: _Card | _Instance) -> SimpleNamespace:
        return SimpleNamespace(
            total=handle.memory_total,
            used=handle.memory_used,
        )

    def _device_get_memory_error_counter(
        self,
        handle: _Card | _Instance,
        error_type: int,
        counter_type: int,
        location_type: int,
    ) -> int:
        if handle.ecc_counter_error_code is not None:
            raise pyhgml.HGMLError(handle.ecc_counter_error_code)
        return handle.ecc_errors

    # Usage.

    def _device_get_utilization_rates(self, handle: _Card) -> SimpleNamespace:
        return SimpleNamespace(gpu=handle.cores_utilization, memory=0)

    def _device_get_temperature(self, handle: _Card, sensor: int) -> int:
        return handle.temperature

    def _device_get_power_usage(self, handle: _Card) -> int:
        return handle.power_usage

    def _gpm_query_device_support(self, handle: _Card) -> SimpleNamespace:
        return SimpleNamespace(isSupportedDevice=int(self.gpm_supported))

    def _gpm_sample_alloc(self) -> object:
        return object()

    def _gpm_sample_free(self, sample: object) -> None:
        pass

    def _gpm_sample_get(self, handle: _Card, sample: object) -> None:
        self._gpm_target = handle

    def _gpm_mig_sample_get(
        self,
        handle: _Card,
        gpu_instance_id: int,
        sample: object,
    ) -> None:
        self._gpm_target = next(
            (
                inst
                for inst in handle.instances or []
                if inst.gpu_instance_id == gpu_instance_id
            ),
            None,
        )

    def _gpm_metrics_get(self, metrics_get: _GpmMetrics) -> None:
        sm_util = getattr(self._gpm_target, "sm_util", None)
        metrics_get.metrics[0].value = (
            float("nan") if sm_util is None else float(sm_util)
        )

    # GPU/compute instances.

    def _device_get_mig_mode(self, handle: _Card) -> tuple[int, int]:
        mode = (
            pyhgml.HGML_DEVICE_MIG_DISABLE
            if handle.instances is None
            else pyhgml.HGML_DEVICE_MIG_ENABLE
        )
        return mode, mode

    def _device_get_max_mig_device_count(self, handle: _Card) -> int:
        return 8

    def _device_get_mig_handle(self, handle: _Card, index: int) -> _Instance:
        instances = handle.instances or []
        if index >= len(instances):
            msg = "no instance at that slot"
            raise pyhgml.HGMLError(msg)
        return instances[index]

    def _device_get_gpu_instance_id(self, handle: _Instance) -> int:
        return handle.gpu_instance_id

    def _device_get_compute_instance_id(self, handle: _Instance) -> int:
        return handle.compute_instance_id

    def _device_get_gpu_instance_by_id(
        self,
        handle: _Card,
        gpu_instance_id: int,
    ) -> SimpleNamespace:
        return SimpleNamespace(card=handle, gpu_instance_id=gpu_instance_id)

    def _gi_get_ci_by_id(
        self,
        gpu_instance: SimpleNamespace,
        compute_instance_id: int,
    ) -> SimpleNamespace:
        return SimpleNamespace(compute_instance_id=compute_instance_id)

    def _gi_get_info(self, gpu_instance: SimpleNamespace) -> SimpleNamespace:
        return SimpleNamespace(profileId=0)

    def _ci_get_info(self, compute_instance: SimpleNamespace) -> SimpleNamespace:
        return SimpleNamespace(profileId=0)

    def _device_get_gi_profile_info(
        self,
        handle: _Card,
        profile_id: int,
    ) -> SimpleNamespace:
        # Only the 1-slice profile exists, i.e. HGML_GPU_INSTANCE_PROFILE_1_SLICE.
        if profile_id != pyhgml.HGML_GPU_INSTANCE_PROFILE_1_SLICE:
            msg = "no such GPU instance profile"
            raise pyhgml.HGMLError(msg)
        return SimpleNamespace(
            id=0,
            memorySizeMB=self.instance_profile_memory_mb,
            name=self.instance_profile_name,
        )

    def _gi_get_ci_profile_info(
        self,
        gpu_instance: SimpleNamespace,
        profile_id: int,
        engine_profile_id: int,
    ) -> SimpleNamespace:
        if (profile_id, engine_profile_id) != (
            pyhgml.HGML_COMPUTE_INSTANCE_PROFILE_1_SLICE,
            pyhgml.HGML_COMPUTE_INSTANCE_ENGINE_PROFILE_SHARED,
        ):
            msg = "no such compute instance profile"
            raise pyhgml.HGMLError(msg)
        return SimpleNamespace(id=0, multiprocessorCount=self.instance_profile_cores)


_USAGE_CALLS = (
    "hgmlGpmQueryDeviceSupport",
    "hgmlGpmSampleGet",
    "hgmlGpmMigSampleGet",
    "hgmlGpmMetricsGet",
    "hgmlDeviceGetUtilizationRates",
    "hgmlDeviceGetTemperature",
    "hgmlDeviceGetPowerUsage",
)
"""
The driver calls only the usage query is allowed to make. Deliberately not the
whole metric-looking surface: hgmlDeviceGetPowerManagementDefaultLimit reads the
power *limit* and hgmlDeviceGetMemoryInfo the memory *total*, both inventory.
"""

_CARD_MEMORY_UTILIZATION = 1.56
"""
A default card's memory utilization: 1024 MiB used of 65536 MiB total, as
get_utilization rounds it.
"""

_INSTANCE_MEMORY_UTILIZATION = 6.25
"""
A default instance's memory utilization: 512 MiB used of 8192 MiB total.
"""


@pytest.fixture
def health_check(monkeypatch):
    """
    Turn the device health check on: it is opt-in, as the queries cost a
    driver call per device, so GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK defaults
    to true. A real module attribute is set because the env lookup is cached.
    """
    monkeypatch.setattr(
        thead.envs,
        "GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK",
        False,
        raising=False,
    )


@pytest.fixture
def detector(monkeypatch):
    """
    Build a T-Head detector talking to a fake driver reporting the given cards.
    """

    def _install(*cards: _Card, **kwargs) -> tuple[THeadDetector, _FakeHGML]:
        fake = _FakeHGML(cards=list(cards), **kwargs)
        monkeypatch.setattr(thead, "pyhgml", fake)
        # is_supported() initializes the real driver, which the fake replaces.
        monkeypatch.setattr(THeadDetector, "is_supported", staticmethod(lambda: True))
        # The NUMA node comes from sysfs, so it is answered here instead of
        # letting the host decide what the test sees.
        monkeypatch.setattr(thead, "get_numa_node_by_bdf", lambda *_: "")
        # GPM samples over a 100 ms window of real time, twice per query.
        monkeypatch.setattr(thead.time, "sleep", lambda *_: None)
        return THeadDetector(), fake

    return _install


# --------------------------------------------------------------------------- #
# detect_info: inventory only, no vgpu, no usage call.                        #
# --------------------------------------------------------------------------- #


def test_detect_info_carries_the_inventory(detector):
    det, _ = detector(_Card(uuid="PPU-0"))

    dev = det.detect_info()[0]

    assert dev.manufacturer == ManufacturerEnum.THEAD
    assert dev.index == 0
    assert dev.name == "T-Head PPU"
    assert dev.uuid == "PPU-0"
    assert dev.driver_version == "1.2.3"
    assert dev.runtime_version == "12.3"
    assert dev.runtime_version_original == "12.3.0"
    assert dev.compute_capability == "8.0"
    assert dev.cores == 128
    assert dev.memory == 65536
    assert dev.power == 350
    assert dev.memory_status == DeviceMemoryStatusEnum.HEALTHY
    assert dev.appendix["bdf"] == "0000:01:00.0"
    assert dev.appendix["mig"] is False
    # The usage fields keep their defaults: the information query does not fill
    # them, and must not invent a zero that reads like a measurement.
    assert dev.cores_utilization == 0
    assert dev.memory_used == 0
    assert dev.memory_utilization == 0
    assert dev.temperature is None
    assert dev.power_used is None


def test_detect_info_issues_no_usage_call(detector):
    det, fake = detector(
        _Card(uuid="PPU-0"),
        _Card(uuid="PPU-1", instances=[_Instance(uuid="PPU-1-MIG-0")]),
    )

    det.detect_info()

    assert [call for call in fake.calls if call in _USAGE_CALLS] == []
    # The inventory reads that only look like metrics stay.
    assert "hgmlDeviceGetPowerManagementDefaultLimit" in fake.calls
    assert "hgmlDeviceGetMemoryInfo" in fake.calls


def test_no_appendix_carries_vgpu(detector):
    det, _ = detector(
        _Card(uuid="PPU-0"),
        _Card(uuid="PPU-1", instances=[_Instance(uuid="PPU-1-MIG-0")]),
    )

    for dev in det.detect():
        assert "vgpu" not in dev.appendix
        for inst in dev.appendix.get("mig_devices", []):
            assert "vgpu" not in inst["appendix"]


# --------------------------------------------------------------------------- #
# The GPU/compute instance entries, which the operator has no equivalent of.  #
# --------------------------------------------------------------------------- #


def test_detect_info_keeps_the_instance_entries(detector):
    det, _ = detector(
        _Card(uuid="PPU-0"),
        _Card(
            uuid="PPU-1",
            instances=[
                _Instance(uuid="PPU-1-MIG-0"),
                _Instance(uuid="PPU-1-MIG-1", gpu_instance_id=1),
            ],
        ),
    )

    devices = det.detect_info()

    # The physical card stays the reported device, whether or not MIG is on.
    assert [dev.uuid for dev in devices] == ["PPU-0", "PPU-1"]
    assert "mig_devices" not in devices[0].appendix

    instances = devices[1].appendix["mig_devices"]
    assert [inst["uuid"] for inst in instances] == ["PPU-1-MIG-0", "PPU-1-MIG-1"]
    # `sliced` is what the topology path keys off, so it must survive.
    assert all(inst["appendix"]["sliced"] is True for inst in instances)
    assert all(inst["appendix"]["mig"] is True for inst in instances)
    assert all(inst["appendix"]["bdf"] == "0000:01:00.0" for inst in instances)
    assert [inst["appendix"]["gpu_instance_id"] for inst in instances] == [0, 1]
    assert [inst["name"] for inst in instances] == ["1g.8gb"] * 2
    assert [inst["cores"] for inst in instances] == [16] * 2
    assert [inst["memory"] for inst in instances] == [8192] * 2
    # index_mig_devices numbers them above the cards, one block per card.
    assert [inst["index"] for inst in instances] == [2 + 1 * 8, 2 + 1 * 8 + 1]
    # The instances' usage fields keep a Device's defaults as well.
    assert [inst["cores_utilization"] for inst in instances] == [0] * 2
    assert [inst["memory_used"] for inst in instances] == [0] * 2
    assert [inst["temperature"] for inst in instances] == [None] * 2
    assert [inst["power_used"] for inst in instances] == [None] * 2


# --------------------------------------------------------------------------- #
# The minor number, recorded only when the driver answers.                    #
# --------------------------------------------------------------------------- #


def test_detect_info_records_the_minor_number(detector):
    det, _ = detector(_Card(uuid="PPU-0", minor_number=3))

    dev = det.detect_info()[0]

    assert dev.appendix["minor_number"] == 3
    # Device.index stays the enumeration index.
    assert dev.index == 0


def test_detect_info_omits_the_minor_number_when_unreadable(detector):
    det, _ = detector(_Card(uuid="PPU-0", minor_number=None))

    dev = det.detect_info()[0]

    # Absent rather than substituted by the enumeration index, as the operator
    # does: a substituted value would make a wrong ordinal look proven.
    assert "minor_number" not in dev.appendix


# --------------------------------------------------------------------------- #
# detect_usage: the six fields, merged by UUID, instances included.           #
# --------------------------------------------------------------------------- #


def test_detect_usage_merges_by_uuid(detector):
    det, _ = detector(
        _Card(uuid="PPU-0"),
        _Card(
            uuid="PPU-1",
            cores_utilization=7,
            memory_used=2048 * _MIB,
            temperature=61,
            power_usage=200_000,
        ),
    )

    devices = det.detect_info()
    # Reversed on purpose: the merge joins by UUID, never by position.
    devices.reverse()

    assert det.detect_usage(devices) is devices

    by_uuid = {dev.uuid: dev for dev in devices}
    assert by_uuid["PPU-0"].cores_utilization == 33
    assert by_uuid["PPU-0"].memory_used == 1024
    assert by_uuid["PPU-0"].memory_utilization == _CARD_MEMORY_UTILIZATION
    assert by_uuid["PPU-0"].temperature == 55
    assert by_uuid["PPU-0"].power_used == 150
    assert by_uuid["PPU-1"].cores_utilization == 7
    assert by_uuid["PPU-1"].memory_used == 2048
    assert by_uuid["PPU-1"].temperature == 61
    assert by_uuid["PPU-1"].power_used == 200
    # The information fields survive the merge untouched.
    assert by_uuid["PPU-0"].index == 0
    assert by_uuid["PPU-0"].memory == 65536
    assert by_uuid["PPU-0"].power == 350
    assert by_uuid["PPU-0"].name == "T-Head PPU"


def test_detect_usage_merges_the_instance_entries(detector):
    det, _ = detector(
        _Card(
            uuid="PPU-0",
            instances=[
                _Instance(uuid="PPU-0-MIG-0", sm_util=42.0),
                _Instance(uuid="PPU-0-MIG-1", gpu_instance_id=1, sm_util=None),
            ],
        ),
    )

    devices = det.detect()

    instances = devices[0].appendix["mig_devices"]
    assert [inst["cores_utilization"] for inst in instances] == [42, None]
    assert [inst["memory_used"] for inst in instances] == [512] * 2
    assert [inst["memory_utilization"] for inst in instances] == [
        _INSTANCE_MEMORY_UTILIZATION,
    ] * 2
    # An instance reports neither temperature nor power, so it carries the card's.
    assert [inst["temperature"] for inst in instances] == [55] * 2
    assert [inst["power_used"] for inst in instances] == [150] * 2
    # The inventory fields survive the merge untouched.
    assert [inst["name"] for inst in instances] == ["1g.8gb"] * 2
    assert [inst["memory"] for inst in instances] == [8192] * 2
    assert all(inst["appendix"]["sliced"] is True for inst in instances)


def test_detect_usage_detects_the_information_first(detector):
    det, _ = detector(_Card(uuid="PPU-0"))

    devices = det.detect_usage()

    assert [dev.uuid for dev in devices] == ["PPU-0"]
    assert devices[0].name == "T-Head PPU"
    assert devices[0].memory == 65536
    assert devices[0].cores_utilization == 33


def test_detect_composes_both_queries(detector):
    det, _ = detector(_Card(uuid="PPU-0"))

    dev = det.detect()[0]

    assert dev.name == "T-Head PPU"
    assert dev.memory == 65536
    assert dev.power == 350
    assert dev.cores_utilization == 33
    assert dev.memory_used == 1024
    assert dev.memory_utilization == _CARD_MEMORY_UTILIZATION
    assert dev.temperature == 55
    assert dev.power_used == 150


# --------------------------------------------------------------------------- #
# memory_status, which both queries own.                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.usefixtures("health_check")
def test_detect_usage_bounds_a_faulty_instance_to_itself(detector):
    # One instance refusing its UUID used to abort the whole card's MIG loop, so
    # every later instance kept the inventory's defaults -- 0 % and 0 MiB, i.e.
    # reported idle while it may be running a workload.
    before_util, after_util = 42.0, 17.0
    det, _ = detector(
        _Card(
            uuid="PPU-0",
            instances=[
                _Instance(uuid="PPU-0-MIG-0", sm_util=before_util),
                _Instance(uuid="PPU-0-MIG-1", gpu_instance_id=1, uuid_readable=False),
                _Instance(uuid="PPU-0-MIG-2", gpu_instance_id=2, sm_util=after_util),
            ],
        ),
    )

    mig_devs = det.detect()[0].appendix["mig_devices"]

    by_uuid = {m["uuid"]: m for m in mig_devs}
    assert by_uuid["PPU-0-MIG-0"]["cores_utilization"] == before_util
    # The instance past the faulty one is still enumerated, with its own reading.
    assert by_uuid["PPU-0-MIG-2"]["cores_utilization"] == after_util


@pytest.mark.usefixtures("health_check")
def test_detect_keeps_the_memory_status_through_the_merge(detector):
    det, _ = detector(
        _Card(uuid="PPU-0", instances=[_Instance(uuid="PPU-0-MIG-0")]),
    )

    # merge_devices_usage overwrites memory_status along with the other five
    # usage fields, so a usage query that did not re-read the health would wipe
    # the information query's verdict back to the UNKNOWN default. The health
    # check has to be switched on for this to pin anything: with it off both
    # queries answer HEALTHY without a driver call, and the assertion would hold
    # whether or not the usage query re-read health at all.
    dev = det.detect()[0]
    assert dev.memory_status == DeviceMemoryStatusEnum.HEALTHY
    assert (
        dev.appendix["mig_devices"][0]["memory_status"]
        == DeviceMemoryStatusEnum.HEALTHY
    )


@pytest.mark.usefixtures("health_check")
def test_detect_reports_an_uncorrectable_ecc_error(detector):
    det, _ = detector(
        _Card(
            uuid="PPU-0",
            ecc_errors=3,
            instances=[_Instance(uuid="PPU-0-MIG-0", ecc_errors=1)],
        ),
    )

    # Both queries report the health, mirroring the operator, which flags
    # Unhealthy from DetectAccelerator and MonitorAccelerator alike.
    info = det.detect_info()[0]
    assert info.memory_status == DeviceMemoryStatusEnum.UNHEALTHY
    assert (
        info.appendix["mig_devices"][0]["memory_status"]
        == DeviceMemoryStatusEnum.UNHEALTHY
    )
    dev = det.detect()[0]
    assert dev.memory_status == DeviceMemoryStatusEnum.UNHEALTHY
    assert (
        dev.appendix["mig_devices"][0]["memory_status"]
        == DeviceMemoryStatusEnum.UNHEALTHY
    )


@pytest.mark.usefixtures("health_check")
def test_detect_fails_closed_on_an_ecc_query_error(detector):
    # A card the driver refuses to answer is unhealthy, not healthy: the query
    # error used to be swallowed.
    det, _ = detector(
        _Card(
            uuid="PPU-0",
            ecc_counter_error_code=pyhgml.HGML_ERROR_UNKNOWN,
            instances=[
                _Instance(
                    uuid="PPU-0-MIG-0",
                    ecc_counter_error_code=pyhgml.HGML_ERROR_UNKNOWN,
                ),
            ],
        ),
    )

    dev = det.detect()[0]

    assert dev.memory_status == DeviceMemoryStatusEnum.UNHEALTHY
    assert (
        dev.appendix["mig_devices"][0]["memory_status"]
        == DeviceMemoryStatusEnum.UNHEALTHY
    )


@pytest.mark.usefixtures("health_check")
def test_detect_tolerates_an_unsupported_ecc_query(detector):
    # A card without the counter cannot be judged, not condemned.
    det, _ = detector(
        _Card(
            uuid="PPU-0",
            ecc_counter_error_code=pyhgml.HGML_ERROR_NOT_SUPPORTED,
        ),
    )

    dev = det.detect()[0]

    assert dev.memory_status == DeviceMemoryStatusEnum.HEALTHY


def test_detect_issues_no_health_check_call_by_default(detector):
    # The check is opt-in: at the default, detection issues no ECC call.
    det, fake = detector(_Card(uuid="PPU-0"))

    det.detect()

    assert "hgmlDeviceGetMemoryErrorCounter" not in fake.calls


# --------------------------------------------------------------------------- #
# CDI: /dev/alixpu_ppu{N} is named after the enumeration index.               #
# --------------------------------------------------------------------------- #


def _fake_node_factory(seen_paths: list[str], minors: dict[str, int]):
    """
    Stand in for device_to_cdi_device_node, giving each node a kernel minor.

    The minor is what proves an ordinal-named node reaches the card whose record
    names that minor, so a fake that cannot carry one cannot exercise the check.
    """

    def _fake_device_node(path):
        seen_paths.append(path)
        return SimpleNamespace(path=path, minor=minors.get(path, 0))

    return _fake_device_node


def test_cdi_device_node_path_is_named_after_the_enumeration_index(monkeypatch):
    seen_paths: list[str] = []

    monkeypatch.setattr(
        cdi_thead,
        "device_to_cdi_device_node",
        # The node the ordinal names carries the minor the record states, which
        # is the host this fixture reproduces.
        _fake_node_factory(seen_paths, {"/dev/alixpu_ppu0": 7}),
    )

    devices = [
        Device(
            manufacturer=ManufacturerEnum.THEAD,
            index=0,
            name="T-Head PPU",
            uuid="PPU-0",
            memory=65536,
            appendix={"bdf": "0000:01:00.0", "minor_number": 7},
        ),
        Device(
            manufacturer=ManufacturerEnum.THEAD,
            index=1,
            name="T-Head PPU",
            uuid="PPU-1",
            memory=65536,
            appendix={"bdf": "0000:02:00.0"},
        ),
    ]

    config = THeadGenerator().generate(devices)

    assert config is not None
    # Unlike /dev/nvidia{N} and /dev/iluvatar{N}, the T-Head node is named after
    # the card ordinal and not after the driver's minor number, which the
    # operator records purely to PROVE a node addresses the card it describes.
    # So the appendix number does not name the node: a card carrying one is
    # addressed by its enumeration index all the same, as is one carrying none.
    assert "/dev/alixpu_ppu0" in seen_paths
    assert "/dev/alixpu_ppu1" in seen_paths
    assert "/dev/alixpu_ppu7" not in seen_paths


def test_cdi_refuses_a_node_carrying_another_cards_minor(monkeypatch):
    # The ordinal names the node and the recorded minor proves that name reaches
    # the card the record describes. On the operator's measured host a path built
    # from the record lands on the NEIGHBOURING accelerator silently, which is
    # the failure this refuses: card 0's record says minor 7, but the node its
    # ordinal names carries 8, so that ordinal is addressing another card.
    seen_paths: list[str] = []
    monkeypatch.setattr(
        cdi_thead,
        "device_to_cdi_device_node",
        _fake_node_factory(seen_paths, {"/dev/alixpu_ppu0": 8}),
    )

    devices = [
        Device(
            manufacturer=ManufacturerEnum.THEAD,
            index=0,
            name="T-Head PPU",
            uuid="PPU-0",
            memory=65536,
            appendix={"bdf": "0000:01:00.0", "minor_number": 7},
        ),
    ]

    config = THeadGenerator().generate(devices)

    # The path was built and then rejected, so no device survives to be handed
    # over -- a missing accelerator, never someone else's.
    assert "/dev/alixpu_ppu0" in seen_paths
    assert config is None
