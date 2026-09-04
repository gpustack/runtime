from __future__ import annotations

import ctypes
import dataclasses
from types import SimpleNamespace

import pytest

from gpustack_runtime.detector import nvidia
from gpustack_runtime.detector.__types__ import DeviceMemoryStatusEnum
from gpustack_runtime.detector.__utils__ import get_utilization
from gpustack_runtime.detector.nvidia import NVIDIADetector

# --------------------------------------------------------------------------- #
# A fake pynvml, built in this module rather than shared: every vendor binding #
# exposes an unrelated API, and this one carries a call log because several    #
# criteria are "issues no metric call", which no return value can prove.      #
# --------------------------------------------------------------------------- #

_MIB = 1 << 20

_USAGE_CALLS = (
    "nvmlGpmQueryDeviceSupport",
    "nvmlGpmSampleGet",
    "nvmlGpmMigSampleGet",
    "nvmlGpmMetricsGet",
    "nvmlDeviceGetUtilizationRates",
    "nvmlDeviceGetTemperature",
    "nvmlDeviceGetPowerUsage",
)
"""
The driver calls only the usage query is allowed to make.
"""


class _NVMLError(Exception):
    """
    The fake binding's error type, standing in for pynvml.NVMLError.
    """

    def __init__(self, msg: str, value: int | None = None):
        super().__init__(msg)
        self.value = value


class _FakeFabricInfo(ctypes.Structure):
    """
    A byref()-able fabric info: the detector takes this struct's address.
    """

    _fields_ = (
        ("state", ctypes.c_uint),
        ("clusterUuid", ctypes.c_ubyte * 16),
        ("cliqueId", ctypes.c_uint),
    )


class _FakeGpmMetrics:
    """
    A GPM metrics request, as the detector fills and reads it.
    """

    def __init__(self):
        self.version = 0
        self.numMetrics = 0
        self.sample1 = None
        self.sample2 = None
        self.metrics = [SimpleNamespace(metricId=0, value=float("nan"))]


@dataclasses.dataclass
class _FakeMigDevice:
    """
    A MIG device of a MIG-enabled card.
    """

    uuid: str
    gpu_instance_id: int = 0
    compute_instance_id: int = 0
    memory: int = 4864 * _MIB
    memory_used: int = 512 * _MIB
    ecc_errors: int = 0
    sm_util: float | None = None
    mig_devices: None = None


@dataclasses.dataclass
class _FakeDevice:
    """
    A card, as the fake binding reports it. Every field is a knob a test turns.
    """

    uuid: str = "GPU-0"
    name: str = "NVIDIA L4"
    minor_number: int | None = 0
    bdf: str = "0000:6A:00.0"
    compute_capability: tuple[int, int] = (8, 9)
    cores: int = 7424
    memory: int = 23034 * _MIB
    memory_used: int = 1024 * _MIB
    memory_bus_width: int | None = 192
    ecc_mode: int = 1  # NVML_FEATURE_ENABLED
    ecc_errors: int = 0
    ecc_counter_error_code: int | None = None
    """
    The error code the ECC counter query raises, or None for a readable one.
    """
    recovery_action: int | None = 0
    reset_status: int | None = 0
    """
    The recovery field values, or None for a field the driver will not answer.
    """
    field_values_error_code: int | None = None
    """
    The error code the field-values query raises, or None for an answered one.
    """
    temperature: int = 47
    power_limit: int = 72_000  # mW
    power_used: int = 30_000  # mW
    cores_utilization: int = 12
    sm_util: float | None = None
    """
    The SM utilization GPM samples, or None for a card not supporting GPM.
    """
    mig_devices: list[_FakeMigDevice] | None = None
    """
    The card's MIG devices, or None for a card with MIG disabled.
    """


class _FakeNVML:
    """
    A pynvml stand-in exposing only what the NVIDIA detector touches, recording
    every call it receives.
    """

    NVMLError = _NVMLError
    NVML_SUCCESS = 0
    NVML_ERROR_NOT_SUPPORTED = 3
    NVML_ERROR_UNKNOWN = 999
    NVML_VALUE_TYPE_DOUBLE = 0
    NVML_VALUE_TYPE_UNSIGNED_INT = 1
    NVML_VALUE_TYPE_UNSIGNED_LONG = 2
    NVML_VALUE_TYPE_UNSIGNED_LONG_LONG = 3
    NVML_VALUE_TYPE_SIGNED_LONG_LONG = 4
    NVML_VALUE_TYPE_SIGNED_INT = 5
    NVML_VALUE_TYPE_UNSIGNED_SHORT = 6
    NVML_FI_DEV_RESET_STATUS = 226
    NVML_FI_DEV_GET_GPU_RECOVERY_ACTION = 230
    NVML_FEATURE_DISABLED = 0
    NVML_FEATURE_ENABLED = 1
    NVML_TEMPERATURE_GPU = 0
    NVML_AFFINITY_SCOPE_NODE = 1
    NVML_DEVICE_MIG_DISABLE = 0
    NVML_DEVICE_MIG_ENABLE = 1
    NVML_MEMORY_ERROR_TYPE_UNCORRECTED = 1
    NVML_VOLATILE_ECC = 0
    NVML_AGGREGATE_ECC = 1
    NVML_MEMORY_LOCATION_DRAM = 2
    NVML_MEMORY_LOCATION_SRAM = 5
    NVML_GPU_INSTANCE_PROFILE_COUNT = 3
    NVML_COMPUTE_INSTANCE_PROFILE_COUNT = 2
    NVML_COMPUTE_INSTANCE_ENGINE_PROFILE_COUNT = 1
    NVML_GPM_METRIC_SM_UTIL = 2
    NVML_GPM_METRICS_GET_VERSION = 1
    NVML_GPU_FABRIC_STATE_COMPLETED = 3

    c_nvmlGpuFabricInfoV_t = _FakeFabricInfo  # noqa: N815
    c_nvmlGpmMetricsGet_t = _FakeGpmMetrics  # noqa: N815

    mig_profile_name = "MIG 1g.5gb"
    mig_profile_memory_mb = 4864
    mig_profile_cores = 14

    def __init__(
        self,
        devices: list[_FakeDevice],
        memory_v2_binding: bool = True,
        memory_v2_driver: bool = True,
    ):
        """
        Args:
            devices:
                The cards the fake driver enumerates.
            memory_v2_binding:
                Whether the binding exposes the packed v2 struct version, as an
                older nvidia-ml-py does not.
            memory_v2_driver:
                Whether the driver answers the v2 memory call.

        """
        self.devices = list(devices)
        self.calls: list[str] = []
        self.memory_versions: list[int | None] = []
        self.memory_v2_driver = memory_v2_driver
        if memory_v2_binding:
            self.nvmlMemory_v2 = 33554472
        self._gpm_target: _FakeDevice | _FakeMigDevice | None = None

    # System.

    def nvmlInit(self):  # noqa: N802
        self.calls.append("nvmlInit")

    def nvmlSystemGetDriverVersion(self):  # noqa: N802
        self.calls.append("nvmlSystemGetDriverVersion")
        return "580.65.06"

    def nvmlSystemGetCudaDriverVersion(self):  # noqa: N802
        self.calls.append("nvmlSystemGetCudaDriverVersion")
        return 13000

    def nvmlDeviceGetCount(self):  # noqa: N802
        self.calls.append("nvmlDeviceGetCount")
        return len(self.devices)

    def nvmlDeviceGetHandleByIndex(self, index):  # noqa: N802
        self.calls.append("nvmlDeviceGetHandleByIndex")
        return self.devices[index]

    # Identity and capability.

    def nvmlDeviceGetUUID(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetUUID")
        return handle.uuid

    def nvmlDeviceGetName(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetName")
        return handle.name

    def nvmlDeviceGetCudaComputeCapability(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetCudaComputeCapability")
        return list(handle.compute_capability)

    def nvmlDeviceGetPciInfo(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetPciInfo")
        # The legacy tuple is uppercase hexadecimal, as NVML formats it.
        return SimpleNamespace(busIdLegacy=handle.bdf)

    def nvmlDeviceGetMemoryAffinity(self, handle, size, scope):  # noqa: N802
        self.calls.append("nvmlDeviceGetMemoryAffinity")
        msg = "no memory affinity"
        raise _NVMLError(msg)

    def nvmlDeviceGetNumGpuCores(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetNumGpuCores")
        return handle.cores

    def nvmlDeviceGetMinorNumber(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetMinorNumber")
        if handle.minor_number is None:
            msg = "no minor number"
            raise _NVMLError(msg)
        return handle.minor_number

    def nvmlDeviceGetPowerManagementDefaultLimit(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetPowerManagementDefaultLimit")
        return handle.power_limit

    def nvmlDeviceGetGpuFabricInfoV(self, handle, info_ref):  # noqa: N802
        self.calls.append("nvmlDeviceGetGpuFabricInfoV")
        return self.NVML_ERROR_NOT_SUPPORTED

    # Memory.

    def nvmlDeviceGetMemoryInfo(self, handle, version=None):  # noqa: N802
        self.calls.append("nvmlDeviceGetMemoryInfo")
        self.memory_versions.append(version)
        if version is not None and not self.memory_v2_driver:
            msg = "no v2 memory info"
            raise _NVMLError(msg)
        return SimpleNamespace(
            total=handle.memory,
            used=handle.memory_used,
            free=handle.memory - handle.memory_used,
        )

    def nvmlDeviceGetMemoryBusWidth(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetMemoryBusWidth")
        if handle.memory_bus_width is None:
            msg = "no memory bus width"
            raise _NVMLError(msg)
        return handle.memory_bus_width

    def nvmlDeviceGetEccMode(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetEccMode")
        return [handle.ecc_mode, handle.ecc_mode]

    def nvmlDeviceGetMemoryErrorCounter(self, handle, error_type, scope, location):  # noqa: N802
        self.calls.append("nvmlDeviceGetMemoryErrorCounter")
        error_code = getattr(handle, "ecc_counter_error_code", None)
        if error_code is not None:
            msg = "ECC counter unreadable"
            raise _NVMLError(msg, error_code)
        return handle.ecc_errors

    def nvmlDeviceGetFieldValues(self, handle, fieldIds):  # noqa: N802, N803
        self.calls.append("nvmlDeviceGetFieldValues")
        error_code = getattr(handle, "field_values_error_code", None)
        if error_code is not None:
            msg = "field values unreadable"
            raise _NVMLError(msg, error_code)
        values = {
            self.NVML_FI_DEV_GET_GPU_RECOVERY_ACTION: getattr(
                handle,
                "recovery_action",
                None,
            ),
            self.NVML_FI_DEV_RESET_STATUS: getattr(handle, "reset_status", None),
        }
        return [
            self._field_value(field_id, values.get(field_id)) for field_id in fieldIds
        ]

    @staticmethod
    def _field_value(field_id, value):
        if value is None:
            return SimpleNamespace(nvmlReturn=_FakeNVML.NVML_ERROR_NOT_SUPPORTED)
        return SimpleNamespace(
            fieldId=field_id,
            nvmlReturn=_FakeNVML.NVML_SUCCESS,
            valueType=_FakeNVML.NVML_VALUE_TYPE_UNSIGNED_INT,
            value=SimpleNamespace(uiVal=value),
        )

    # Usage.

    def nvmlDeviceGetUtilizationRates(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetUtilizationRates")
        return SimpleNamespace(gpu=handle.cores_utilization, memory=0)

    def nvmlDeviceGetTemperature(self, handle, sensor):  # noqa: N802
        self.calls.append("nvmlDeviceGetTemperature")
        return handle.temperature

    def nvmlDeviceGetPowerUsage(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetPowerUsage")
        return handle.power_used

    def nvmlGpmQueryDeviceSupport(self, handle):  # noqa: N802
        self.calls.append("nvmlGpmQueryDeviceSupport")
        return SimpleNamespace(isSupportedDevice=int(handle.sm_util is not None))

    def nvmlGpmSampleAlloc(self):  # noqa: N802
        self.calls.append("nvmlGpmSampleAlloc")
        return object()

    def nvmlGpmSampleFree(self, sample):  # noqa: N802
        self.calls.append("nvmlGpmSampleFree")

    def nvmlGpmSampleGet(self, handle, sample):  # noqa: N802
        self.calls.append("nvmlGpmSampleGet")
        self._gpm_target = handle

    def nvmlGpmMigSampleGet(self, handle, gpu_instance_id, sample):  # noqa: N802
        self.calls.append("nvmlGpmMigSampleGet")
        self._gpm_target = next(
            (
                mig
                for mig in handle.mig_devices or []
                if mig.gpu_instance_id == gpu_instance_id
            ),
            None,
        )

    def nvmlGpmMetricsGet(self, metrics):  # noqa: N802
        self.calls.append("nvmlGpmMetricsGet")
        sm_util = getattr(self._gpm_target, "sm_util", None)
        metrics.metrics[0].value = float("nan") if sm_util is None else float(sm_util)

    # MIG.

    def nvmlDeviceGetMigMode(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetMigMode")
        mode = (
            self.NVML_DEVICE_MIG_DISABLE
            if handle.mig_devices is None
            else self.NVML_DEVICE_MIG_ENABLE
        )
        return mode, mode

    def nvmlDeviceGetMaxMigDeviceCount(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetMaxMigDeviceCount")
        return 7

    def nvmlDeviceGetMigDeviceHandleByIndex(self, handle, index):  # noqa: N802
        self.calls.append("nvmlDeviceGetMigDeviceHandleByIndex")
        migs = handle.mig_devices or []
        if index >= len(migs):
            msg = "no MIG device at that slot"
            raise _NVMLError(msg)
        return migs[index]

    def nvmlDeviceGetGpuInstanceId(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetGpuInstanceId")
        return handle.gpu_instance_id

    def nvmlDeviceGetComputeInstanceId(self, handle):  # noqa: N802
        self.calls.append("nvmlDeviceGetComputeInstanceId")
        return handle.compute_instance_id

    def nvmlDeviceGetGpuInstanceById(self, handle, gpu_instance_id):  # noqa: N802
        self.calls.append("nvmlDeviceGetGpuInstanceById")
        return SimpleNamespace(card=handle, gpu_instance_id=gpu_instance_id)

    def nvmlGpuInstanceGetComputeInstanceById(self, gpu_instance, compute_instance_id):  # noqa: N802
        self.calls.append("nvmlGpuInstanceGetComputeInstanceById")
        return SimpleNamespace(compute_instance_id=compute_instance_id)

    def nvmlGpuInstanceGetInfo(self, gpu_instance):  # noqa: N802
        self.calls.append("nvmlGpuInstanceGetInfo")
        return SimpleNamespace(profileId=0)

    def nvmlComputeInstanceGetInfo(self, compute_instance):  # noqa: N802
        self.calls.append("nvmlComputeInstanceGetInfo")
        return SimpleNamespace(profileId=0)

    def nvmlDeviceGetGpuInstanceProfileInfo(self, handle, profile_id):  # noqa: N802
        self.calls.append("nvmlDeviceGetGpuInstanceProfileInfo")
        if profile_id != 0:
            msg = "no such GPU instance profile"
            raise _NVMLError(msg)
        return SimpleNamespace(
            id=0,
            memorySizeMB=self.mig_profile_memory_mb,
            sliceCount=1,
            name=self.mig_profile_name,
        )

    def nvmlGpuInstanceGetComputeInstanceProfileInfo(  # noqa: N802
        self,
        gpu_instance,
        profile_id,
        engine_profile_id,
    ):
        self.calls.append("nvmlGpuInstanceGetComputeInstanceProfileInfo")
        if (profile_id, engine_profile_id) != (0, 0):
            msg = "no such compute instance profile"
            raise _NVMLError(msg)
        return SimpleNamespace(id=0, multiprocessorCount=self.mig_profile_cores)


@pytest.fixture
def health_check(monkeypatch):
    """
    Turn the device health check on: it is opt-in, as the queries cost a
    driver call per device, so GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK defaults
    to true. A real module attribute is set because the env lookup is cached.
    """
    monkeypatch.setattr(
        nvidia.envs,
        "GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK",
        False,
        raising=False,
    )


@pytest.fixture
def fake_nvml(monkeypatch):
    """
    Install a fake binding in place of pynvml, and return the installer.
    """

    def _install(devices: list[_FakeDevice], **kwargs) -> _FakeNVML:
        fake = _FakeNVML(devices, **kwargs)
        monkeypatch.setattr(nvidia, "pynvml", fake)
        # is_supported() initializes the real driver, which the fake replaces.
        monkeypatch.setattr(NVIDIADetector, "is_supported", staticmethod(lambda: True))
        # The NUMA node comes from sysfs, so it is answered here instead of
        # letting the host decide what the test sees.
        monkeypatch.setattr(nvidia, "get_numa_node_by_bdf", lambda *_: "")
        # GPM samples over a 100 ms window of real time, twice per query.
        monkeypatch.setattr(nvidia.time, "sleep", lambda *_: None)
        return fake

    return _install


# --------------------------------------------------------------------------- #
# The information query.                                                      #
# --------------------------------------------------------------------------- #


def test_detect_info_reports_the_card(fake_nvml):
    fake = fake_nvml([_FakeDevice()])

    devices = NVIDIADetector().detect_info()

    assert len(devices) == 1
    device = devices[0]
    assert device.index == 0
    assert device.name == "NVIDIA L4"
    assert device.uuid == "GPU-0"
    assert device.driver_version == "580.65.06"
    assert device.runtime_version == "13.0"
    assert device.runtime_version_original == "13.0.0"
    assert device.compute_capability == "8.9"
    assert device.cores == 7424
    assert device.power == 72
    assert device.memory_status == DeviceMemoryStatusEnum.HEALTHY
    assert device.appendix == {
        "arch_family": "Ada-Lovelace",
        "mig": False,
        "bdf": "0000:6a:00.0",
        "minor_number": 0,
    }
    # The usage fields stay at their defaults: this query does not read them.
    assert device.cores_utilization == 0
    assert device.memory_used == 0
    assert device.memory_utilization == 0
    assert device.temperature is None
    assert device.power_used is None
    assert [call for call in fake.calls if call in _USAGE_CALLS] == []


def test_detect_info_issues_no_usage_call(fake_nvml):
    # A card supporting GPM and hosting MIG devices, i.e. every usage call the
    # detector knows is reachable.
    fake = fake_nvml(
        [
            _FakeDevice(
                uuid="GPU-0",
                name="NVIDIA H100 80GB HBM3",
                memory_bus_width=5120,
                sm_util=61.0,
                mig_devices=[_FakeMigDevice(uuid="MIG-0-0", sm_util=33.0)],
            ),
        ],
    )

    NVIDIADetector().detect_info()

    assert [call for call in fake.calls if call in _USAGE_CALLS] == []
    # ... while the inventory calls did happen.
    assert "nvmlDeviceGetMemoryInfo" in fake.calls
    assert "nvmlDeviceGetPowerManagementDefaultLimit" in fake.calls
    assert "nvmlDeviceGetMigDeviceHandleByIndex" in fake.calls


def test_detect_info_records_no_vgpu(fake_nvml):
    # Whole-card reporting: no virtual/PF/VF classification anywhere.
    fake_nvml([_FakeDevice(mig_devices=[_FakeMigDevice(uuid="MIG-0-0")])])

    devices = NVIDIADetector().detect_info()

    assert "vgpu" not in devices[0].appendix
    assert "vgpu" not in devices[0].appendix["mig_devices"][0]["appendix"]
    assert not hasattr(nvidia, "_is_vgpu")


def test_detect_info_omits_an_unreadable_minor_number(fake_nvml):
    fake_nvml([_FakeDevice(minor_number=None)])

    devices = NVIDIADetector().detect_info()

    assert "minor_number" not in devices[0].appendix


@pytest.mark.usefixtures("health_check")
def test_detect_info_reports_the_memory_health(fake_nvml):
    fake_nvml([_FakeDevice(ecc_errors=1)])

    devices = NVIDIADetector().detect_info()

    assert devices[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY


# --------------------------------------------------------------------------- #
# The health check fails closed: a card the driver cannot answer is unhealthy. #
# --------------------------------------------------------------------------- #


@pytest.mark.usefixtures("health_check")
def test_detect_info_fails_closed_on_an_ecc_query_error(fake_nvml):
    # A wedged GSP answers the ECC query with an error after its RPC timeout
    # (Xid 119); swallowing that error reported the card healthy.
    fake_nvml([_FakeDevice(ecc_counter_error_code=_FakeNVML.NVML_ERROR_UNKNOWN)])

    devices = NVIDIADetector().detect_info()

    assert devices[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY


@pytest.mark.usefixtures("health_check")
def test_detect_info_tolerates_an_unsupported_ecc_query(fake_nvml):
    # A card without the counter cannot be judged, not condemned.
    fake_nvml([_FakeDevice(ecc_counter_error_code=_FakeNVML.NVML_ERROR_NOT_SUPPORTED)])

    devices = NVIDIADetector().detect_info()

    assert devices[0].memory_status == DeviceMemoryStatusEnum.HEALTHY


@pytest.mark.usefixtures("health_check")
@pytest.mark.parametrize(
    "knobs",
    [
        {"recovery_action": 1},  # Xid 154: GPU Reset Required.
        {"reset_status": 1},  # A pending/past reset.
    ],
)
def test_detect_info_reports_a_card_awaiting_reset(fake_nvml, knobs):
    # The recovery state survives a GSP failure that leaves the ECC counters
    # readable at zero, so the check probes it directly.
    fake_nvml([_FakeDevice(**knobs)])

    devices = NVIDIADetector().detect_info()

    assert devices[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY


@pytest.mark.usefixtures("health_check")
def test_detect_info_falls_back_to_ecc_when_the_recovery_fields_are_unreadable(
    fake_nvml,
):
    # An old driver or card answers the fields with an error apiece: cannot
    # judge, so the ECC verdict stands.
    fake_nvml([_FakeDevice(recovery_action=None, reset_status=None)])

    devices = NVIDIADetector().detect_info()

    assert devices[0].memory_status == DeviceMemoryStatusEnum.HEALTHY


@pytest.mark.usefixtures("health_check")
def test_detect_info_fails_closed_on_a_field_values_error(fake_nvml):
    fake_nvml([_FakeDevice(field_values_error_code=_FakeNVML.NVML_ERROR_UNKNOWN)])

    devices = NVIDIADetector().detect_info()

    assert devices[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY


def test_detect_issues_no_health_check_call_by_default(fake_nvml):
    # The check is opt-in: at the default, detection issues no ECC or
    # field-values call.
    fake = fake_nvml([_FakeDevice()])

    NVIDIADetector().detect()

    assert "nvmlDeviceGetMemoryErrorCounter" not in fake.calls
    assert "nvmlDeviceGetFieldValues" not in fake.calls


# --------------------------------------------------------------------------- #
# Memory is what the card can allocate, not its ECC-restored capacity.        #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "bus_width, ecc_mode",
    [
        (192, 1),  # L4: GDDR with ECC on -- where the operator would restore.
        (384, 1),  # L40S: the widest GDDR bus.
        (5120, 1),  # H100: HBM keeps ECC out of the user-visible memory anyway.
        (384, 0),  # GDDR, ECC off: nothing was carved out.
        (None, 1),  # An unreadable bus width.
    ],
)
def test_detect_info_reports_the_allocatable_memory(fake_nvml, bus_width, ecc_mode):
    # A deliberate divergence from the operator, which adds back the ~1/16 that
    # ECC parity carves out of a GDDR part. That capacity is not reachable, and
    # the operator's restored figure is a display value taking no part in
    # allocation -- but `memory` here does, and `memory - memory_used` has to
    # mean free space, so restoring it would over-commit the card.
    total = 23034
    fake_nvml(
        [
            _FakeDevice(
                memory=total * _MIB,
                memory_bus_width=bus_width,
                ecc_mode=ecc_mode,
            ),
        ],
    )

    devices = NVIDIADetector().detect_info()

    assert devices[0].memory == total


def test_detect_falls_back_to_the_host_memory(fake_nvml, monkeypatch):
    # A deliberate divergence from the operator, which skips such a device.
    fake_nvml([_FakeDevice(memory=0, memory_used=0)])
    monkeypatch.setattr(nvidia, "get_memory", lambda: (65536, 4096))

    detector = NVIDIADetector()
    devices = detector.detect_info()

    assert devices[0].memory == 65536
    assert devices[0].memory_used == 0

    detector.detect_usage(devices)

    assert devices[0].memory_used == 4096
    assert devices[0].memory_utilization == get_utilization(4096, 65536)


# --------------------------------------------------------------------------- #
# The v2 memory structure.                                                    #
# --------------------------------------------------------------------------- #


def test_detect_info_prefers_the_v2_memory_structure(fake_nvml):
    fake = fake_nvml([_FakeDevice()])

    NVIDIADetector().detect_info()

    # The packed struct version the driver validates, not a plain ordinal.
    assert fake.memory_versions == [fake.nvmlMemory_v2]


def test_detect_info_falls_back_to_the_v1_memory_structure(fake_nvml):
    fake = fake_nvml([_FakeDevice()], memory_v2_driver=False)

    devices = NVIDIADetector().detect_info()

    assert fake.memory_versions == [fake.nvmlMemory_v2, None]
    assert devices[0].memory == 23034


def test_detect_info_skips_the_v2_call_the_binding_lacks(fake_nvml):
    fake = fake_nvml([_FakeDevice()], memory_v2_binding=False)

    devices = NVIDIADetector().detect_info()

    assert fake.memory_versions == [None]
    assert devices[0].memory == 23034


# --------------------------------------------------------------------------- #
# The usage query.                                                            #
# --------------------------------------------------------------------------- #


def test_detect_fills_the_usage_fields(fake_nvml):
    fake_nvml([_FakeDevice()])

    devices = NVIDIADetector().detect()

    device = devices[0]
    assert device.cores_utilization == 12
    assert device.memory_used == 1024
    # The three memory fields of one card agree: the utilization is measured
    # against the same total that `memory` reports.
    assert device.memory == 23034
    assert device.memory_utilization == get_utilization(1024, 23034)
    assert device.memory_status == DeviceMemoryStatusEnum.HEALTHY
    assert device.temperature == 47
    assert device.power_used == 30


def test_detect_without_usage_leaves_the_usage_fields_alone(fake_nvml):
    fake = fake_nvml([_FakeDevice()])

    devices = NVIDIADetector().detect(usage=False)

    assert devices[0].cores_utilization == 0
    assert devices[0].temperature is None
    assert [call for call in fake.calls if call in _USAGE_CALLS] == []


def test_detect_usage_merges_into_the_given_devices(fake_nvml):
    fake_nvml([_FakeDevice(uuid="GPU-0"), _FakeDevice(uuid="GPU-1")])
    detector = NVIDIADetector()
    devices = detector.detect_info()

    merged = detector.detect_usage(devices)

    assert merged is devices
    assert [device.temperature for device in devices] == [47, 47]
    # The information fields survive the merge.
    assert [device.uuid for device in devices] == ["GPU-0", "GPU-1"]
    assert [device.name for device in devices] == ["NVIDIA L4"] * 2
    assert [device.memory for device in devices] == [23034] * 2


def test_detect_usage_detects_the_information_first(fake_nvml):
    fake_nvml([_FakeDevice()])

    devices = NVIDIADetector().detect_usage()

    assert [device.uuid for device in devices] == ["GPU-0"]
    assert devices[0].cores_utilization == 12


def test_detect_usage_prefers_gpm(fake_nvml):
    fake = fake_nvml([_FakeDevice(sm_util=61.4)])

    devices = NVIDIADetector().detect()

    assert devices[0].cores_utilization == 61
    assert "nvmlDeviceGetUtilizationRates" not in fake.calls


@pytest.mark.usefixtures("health_check")
def test_detect_usage_reports_the_memory_health(fake_nvml):
    fake_nvml([_FakeDevice(ecc_errors=1)])

    devices = NVIDIADetector().detect()

    assert devices[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY


# --------------------------------------------------------------------------- #
# The MIG devices, which stay in the card's appendix.                         #
# --------------------------------------------------------------------------- #


def _mig_card(**kwargs) -> _FakeDevice:
    return _FakeDevice(
        uuid="GPU-0",
        name="NVIDIA A100-SXM4-40GB",
        compute_capability=(8, 0),
        memory=40960 * _MIB,
        memory_bus_width=5120,
        # GPM is queried on the card, whether the sample is the card's or an
        # instance's.
        sm_util=55.0,
        mig_devices=[
            _FakeMigDevice(uuid="MIG-0-0", gpu_instance_id=1, sm_util=33.0),
            _FakeMigDevice(uuid="MIG-0-1", gpu_instance_id=2, sm_util=44.0),
        ],
        **kwargs,
    )


def test_detect_info_keeps_the_mig_devices_in_the_appendix(fake_nvml):
    fake_nvml([_mig_card()])

    devices = NVIDIADetector().detect_info()

    assert devices[0].appendix["mig"] is True
    mig_devices = devices[0].appendix["mig_devices"]
    assert [mig["uuid"] for mig in mig_devices] == ["MIG-0-0", "MIG-0-1"]
    assert [mig["name"] for mig in mig_devices] == ["1g.5gb"] * 2
    # Numbered by index_mig_devices: a block above the cards' own indexes.
    assert [mig["index"] for mig in mig_devices] == [1, 2]
    assert [mig["memory"] for mig in mig_devices] == [4864] * 2
    assert [mig["cores"] for mig in mig_devices] == [14] * 2
    assert mig_devices[0]["appendix"]["sliced"] is True
    assert mig_devices[0]["appendix"]["mig"] is True
    assert mig_devices[0]["appendix"]["gpu_instance_id"] == 1
    # The usage fields of an instance stay at their defaults too.
    assert mig_devices[0]["cores_utilization"] == 0
    assert mig_devices[0]["memory_used"] == 0
    assert mig_devices[0]["memory_utilization"] == 0
    assert mig_devices[0]["temperature"] is None
    assert mig_devices[0]["power_used"] is None


def test_detect_fills_the_mig_devices_usage(fake_nvml):
    fake_nvml([_mig_card()])

    devices = NVIDIADetector().detect()

    mig_devices = devices[0].appendix["mig_devices"]
    assert [mig["cores_utilization"] for mig in mig_devices] == [33, 44]
    assert [mig["memory_used"] for mig in mig_devices] == [512] * 2
    assert [mig["memory_utilization"] for mig in mig_devices] == [
        get_utilization(512, 4864),
    ] * 2
    assert [mig["memory_status"] for mig in mig_devices] == [
        DeviceMemoryStatusEnum.HEALTHY,
    ] * 2
    # An instance reports neither temperature nor power, so it carries the
    # card's, as the information query's entries do.
    assert [mig["temperature"] for mig in mig_devices] == [47] * 2
    assert [mig["power_used"] for mig in mig_devices] == [30] * 2
    # The instances' own information fields are untouched by the merge.
    assert [mig["index"] for mig in mig_devices] == [1, 2]
    assert [mig["memory"] for mig in mig_devices] == [4864] * 2


def test_detect_reports_a_mig_enabled_card_without_instances(fake_nvml):
    # MIG on, nothing partitioned yet: the card is still the reported device.
    fake_nvml([_FakeDevice(mig_devices=[])])

    devices = NVIDIADetector().detect()

    assert len(devices) == 1
    assert devices[0].appendix["mig"] is True
    assert devices[0].appendix["mig_devices"] == []


# --------------------------------------------------------------------------- #
# The hardware paths, exercised only where a driver exists.                   #
# --------------------------------------------------------------------------- #


@pytest.mark.skipif(
    not NVIDIADetector.is_supported(),
    reason="NVIDIA GPU not detected",
)
def test_detect():
    det = NVIDIADetector()
    devs = det.detect()
    print(devs)


@pytest.mark.skipif(
    not NVIDIADetector.is_supported(),
    reason="NVIDIA GPU not detected",
)
def test_get_topology():
    det = NVIDIADetector()
    topo = det.get_topology()
    print(topo)
