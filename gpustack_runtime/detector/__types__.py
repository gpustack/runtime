from __future__ import annotations as __future_annotations__

import copy
import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from typing import Any

from dataclasses_json import dataclass_json

from .. import envs
from ..logging import debug_log_exception, debug_log_warning

logger = logging.getLogger(__name__)

_monotonic = time.monotonic
"""
The clock the identity cache's time to live is measured on. A module level
indirection, so tests can inject a clock of their own.
"""


class ManufacturerEnum(str, Enum):
    """
    Enum for Manufacturers.
    """

    AMD = "amd"
    """
    Advanced Micro Devices, Inc.
    """
    ASCEND = "ascend"
    """
    Huawei Technologies Co., Ltd.
    """
    CAMBRICON = "cambricon"
    """
    Cambricon Technologies Corporation Limited
    """
    HYGON = "hygon"
    """
    Chengdu Higon Integrated Circuit Design Co., Ltd.
    """
    ILUVATAR = "iluvatar"
    """
    Shanghai Iluvatar CoreX Semiconductor Co., Ltd.
    """
    METAX = "metax"
    """
    MetaX Integrated Circuits (Shanghai) Co., Ltd.
    """
    MTHREADS = "mthreads"
    """
    Moore Threads Technology Co.,Ltd
    """
    NVIDIA = "nvidia"
    """
    NVIDIA Corporation
    """
    THEAD = "thead"
    """
    T-Head Semiconductor Co., Ltd.
    """
    UNKNOWN = "unknown"
    """
    Unknown Manufacturer
    """

    def __str__(self):
        return self.value


_MANUFACTURER_BACKEND_MAPPING: dict[ManufacturerEnum, str] = {
    ManufacturerEnum.AMD: "rocm",
    ManufacturerEnum.ASCEND: "cann",
    ManufacturerEnum.CAMBRICON: "neuware",
    ManufacturerEnum.HYGON: "dtk",
    ManufacturerEnum.ILUVATAR: "corex",
    ManufacturerEnum.METAX: "maca",
    ManufacturerEnum.MTHREADS: "musa",
    ManufacturerEnum.NVIDIA: "cuda",
    ManufacturerEnum.THEAD: "hggc",
}
"""
Mapping of manufacturer to runtime backend,
which should map to the gpustack-runner's backend names.
"""


@lru_cache
def manufacturer_to_backend(manufacturer: ManufacturerEnum) -> str:
    """
    Convert manufacturer to runtime backend,
    e.g., NVIDIA -> cuda, AMD -> rocm.

    This is used to determine the appropriate runtime backend
    based on the device manufacturer.

    Args:
        manufacturer: The manufacturer of the device.

    Returns:
        The corresponding runtime backend.
        Return "unknown" if the manufacturer is unknown.

    """
    backend = _MANUFACTURER_BACKEND_MAPPING.get(manufacturer)
    if backend:
        return backend
    return ManufacturerEnum.UNKNOWN.value


@lru_cache
def backend_to_manufacturer(backend: str) -> ManufacturerEnum:
    """
    Convert runtime backend to manufacturer,
    e.g., cuda -> NVIDIA, rocm -> AMD.

    This is used to determine the device manufacturer
    based on the runtime backend.

    Args:
        backend: The runtime backend.

    Returns:
        The corresponding manufacturer.
        Return ManufacturerEnum.Unknown if the backend is unknown.

    """
    for manufacturer, mapped_backend in _MANUFACTURER_BACKEND_MAPPING.items():
        if mapped_backend == backend:
            return manufacturer
    return ManufacturerEnum.UNKNOWN


class DeviceMemoryStatusEnum(str, Enum):
    """
    Enum for Device Memory Status.
    """

    HEALTHY = "healthy"
    """
    Device is healthy.
    """
    UNHEALTHY = "unhealthy"
    """
    Device is unhealthy.
    """
    UNKNOWN = "unknown"
    """
    Device status is unknown.
    """

    def __str__(self):
        return self.value


@dataclass_json
@dataclass
class Device:
    """
    Device information.
    """

    manufacturer: ManufacturerEnum = ManufacturerEnum.UNKNOWN
    """
    Manufacturer of the device.
    """
    index: int = 0
    """
    Index of the device, as the detector enumerates it.
    Driver-physical numbering, which a device node path or a vendor tool needs,
    lives in `appendix` instead, e.g. `minor_number` or `card_id`/`physical_id`.
    """
    name: str = ""
    """
    Name of the device.
    """
    uuid: str = ""
    """
    UUID of the device.
    """
    driver_version: str | None = None
    """
    Driver version of the device.
    """
    runtime_version: str | None = None
    """
    Runtime version in major[.minor] of the device.
    """
    runtime_version_original: str | None = None
    """
    Original runtime version string of the device.
    """
    compute_capability: str | None = None
    """
    Compute capability of the device.
    """
    cores: int | None = None
    """
    Total cores of the device.
    """
    cores_utilization: int | float = 0
    """
    Core utilization of the device in percentage.
    """
    memory: int = 0
    """
    Total memory of the device in MiB.
    """
    memory_used: int = 0
    """
    Used memory of the device in MiB.
    """
    memory_utilization: float = 0
    """
    Memory utilization of the device in percentage.
    """
    memory_status: DeviceMemoryStatusEnum = DeviceMemoryStatusEnum.UNKNOWN
    """
    Status of the device.
    """
    temperature: int | float | None = None
    """
    Temperature of the device in Celsius.
    """
    power: int | float | None = None
    """
    Power consumption of the device in Watts.
    """
    power_used: int | float | None = None
    """
    Used power of the device in Watts.
    """
    appendix: dict[str, Any] = None
    """
    Appendix information of the device.
    """


Devices = list[Device]
"""
A list of Device objects.
"""


@dataclass_json
@dataclass
class Topology:
    """
    Topology information between devices.
    """

    manufacturer: ManufacturerEnum
    """
    Manufacturer of the devices that this topology applies to.
    """
    devices_distances: list[list[int]]
    """
    A 2D list representing the distances between devices.
    The value at row i and column j represents the distance
    between device i and device j.
    """
    devices_cpu_affinities: list[str]
    """
    A list representing the CPU affinity associated with each device.
    The value at index i represents the CPU set for device i.
    """
    devices_numa_affinities: list[str]
    """
    A list representing the NUMA affinity associated with each device.
    The value at index i represents the Memory set for device i.
    """
    appendices: list[dict[str, Any]]
    """
    Appendices information of devices.
    Each entry corresponds to a device and contains additional metadata.
    """

    def __init__(
        self,
        manufacturer: ManufacturerEnum,
        devices_count: int,
    ):
        """
        Initialize the Topology object.

        Args:
            manufacturer:
                Manufacturer of the devices.
            devices_count:
                Count of devices in the topology.

        """
        self.manufacturer = manufacturer
        self.devices_distances = [[0] * devices_count for _ in range(devices_count)]
        self.devices_cpu_affinities = [""] * devices_count
        self.devices_numa_affinities = [""] * devices_count
        self.appendices = [{}] * devices_count

    def stringify(self) -> list[list[str]]:
        """
        Stringify the devices distances and return the maximum width.

        Returns:
            A 2D list representing the devices distances with string values.

        """
        devices_count = len(self.devices_distances)
        devices_info: list[list[str]] = [[]] * devices_count
        for i in range(devices_count):
            devices_info[i] = [
                stringify_devices_distance(d) for d in self.devices_distances[i]
            ]
            devices_info[i] += [
                self.devices_cpu_affinities[i]
                if self.devices_cpu_affinities[i]
                else "N/A",
            ]
            devices_info[i] += [
                self.devices_numa_affinities[i]
                if self.devices_numa_affinities[i]
                else "N/A",
            ]
        return devices_info

    def get_affinities(
        self,
        device_indexes: list[int] | int,
        deduplicate: bool = True,
    ) -> tuple[list[str], list[str]]:
        """
        Get the CPU and NUMA affinities for the given device indexes.

        Args:
            device_indexes:
                A list of device indexes or a single device index.
                If an empty list is provided, return all affinities.
            deduplicate:
                Whether to deduplicate the affinities.
                If True, the returned lists will contain unique affinities only.

        Returns:
            A tuple containing:
            - A list contains the CPU affinities for the given device indexes.
            - A list contains the NUMA affinities for the given device indexes.

        """
        if isinstance(device_indexes, int):
            device_indexes = [device_indexes]

        cpu_affinities: list[str] = []
        numa_affinities: list[str] = []
        if not device_indexes:
            cpu_affinities.extend(self.devices_cpu_affinities)
            numa_affinities.extend(self.devices_numa_affinities)
        else:
            for index in sorted(set(device_indexes)):
                cpu_affinities.append(self.devices_cpu_affinities[index])
                numa_affinities.append(self.devices_numa_affinities[index])

        if deduplicate:
            cpu_affinities = list(set(cpu_affinities))
            numa_affinities = list(set(numa_affinities))
        return cpu_affinities, numa_affinities


class TopologyDistanceEnum(int, Enum):
    """
    Enum for Topology Distance Levels.
    """

    SELF = 0
    """
    Self connection.
    """
    LINK = 5
    """
    Connection traversing with High-Speed Link (e.g., AMD XGMI, Ascend HCCS, NVIDIA NVLink).
    """
    PIX = 10
    """
    Connection traversing at most a single PCIe bridge.
    """
    PXB = 20
    """
    Connection traversing multiple PCIe bridges (without traversing the PCIe Host Bridge).
    """
    PHB = 30
    """
    Connection traversing PCIe as well as a PCIe Host Bridge (typically the CPU).
    """
    NODE = 40
    """
    Connection traversing PCIe and the interconnect between NUMA nodes.
    """
    SYS = 50
    """
    Connection traversing PCIe as well as the SMP interconnect between NUMA nodes (e.g., QPI/UPI).
    """
    UNK = 100
    """
    Unknown connection.
    """


def stringify_devices_distance(distance: int) -> str:
    """
    Stringify the devices distance to a human-readable format.

    Args:
        distance:
            The distance between two devices.

    Returns:
        A string representing the distance.

    """
    match distance:
        case TopologyDistanceEnum.SELF:
            return "X"
        case TopologyDistanceEnum.LINK:
            return "LINK"
        case TopologyDistanceEnum.PIX:
            return "PIX"
        case TopologyDistanceEnum.PXB:
            return "PXB"
        case TopologyDistanceEnum.PHB:
            return "PHB"
        case TopologyDistanceEnum.NODE:
            return "NODE"
        case TopologyDistanceEnum.SYS:
            return "SYS"
        case _:
            return "N/A"


def reduce_devices_distances(
    devices_distances: list[list[int]],
) -> dict[int, list[int]]:
    """
    Reduce the devices distances and return a brief relationship mapping.

    The key of relationship mapping is the device index,
    and the value is device indexes sorted from near to far.

    For example, given 4 devices with the following devices distances:
    `[[0, 10, 20, 30], [10, 0, 15, 25], [20, 15, 0, 5], [30, 25, 5, 0]]`,
    the resulting relationship will be:
    `{0: [1, 2, 3], 1: [0, 2, 3], 2: [3, 1, 0], 3: [2, 1, 0]}`.

    Args:
        devices_distances:
            A 2D list representing the distances between devices.

    Returns:
        A dictionary representing the relationship.

    """
    result: dict[int, list[int]] = {}

    devices_count = len(devices_distances)
    for index in range(devices_count):
        device_indexes = list(range(devices_count))
        distances = zip(device_indexes, devices_distances[index], strict=False)
        sorted_distances = sorted(distances, key=lambda x: x[1])
        sorted_indexes = [device_index for device_index, _ in sorted_distances]
        result[index] = sorted_indexes[1:]

    return result


def index_mig_devices(
    devices: Devices,
    mig_devices: dict[int, list[dict]],
    slots: int,
) -> None:
    """
    Number the given cards' MIG devices in place.

    A MIG device carries no driver-side inventory index, so its index is
    synthetic: every card owns a block of `slots` indexes, and the blocks
    start above the largest index the physical cards report. A detector's
    reported index is not necessarily zero-based and contiguous — Ascend
    reports the DCMI logic id — hence the offset is measured from the reported
    indexes instead of the card count. Sizing a block by the slots a card can host, rather
    than by the MIG devices it currently has, keeps a card's numbering
    independent of its neighbours: partitioning one card never renumbers
    another's MIG devices.

    Args:
        devices:
            The detected physical cards, already indexed.
        mig_devices:
            The MIG devices to number, keyed by the enumeration index of the
            card hosting them. Each entry's `index` is the driver slot it was
            found at, which the block offset is added to.
        slots:
            The number of MIG devices a card can host, i.e. the block size.

    """
    base = max((dev.index for dev in devices), default=-1) + 1
    for dev_idx, migs in mig_devices.items():
        for mig in migs:
            mig["index"] += base + dev_idx * slots


_DEVICE_USAGE_FIELDS = (
    "cores_utilization",
    "memory_used",
    "memory_utilization",
    "memory_status",
    "temperature",
    "power_used",
)
"""
The fields the usage query owns, i.e. everything a detector must re-read to
refresh a device it already knows. `memory_status` belongs to both queries,
mirroring the operator, which reports health from `DetectAccelerator` and
`MonitorAccelerator` alike.
"""


def merge_devices_usage(
    devices: Devices | None,
    usages: Devices | None,
) -> Devices | None:
    """
    Merge the given usage into the given devices in place, matched by UUID.

    The usage query returns devices of its own, keyed by UUID, which this
    joins into the devices to refresh: the operator does the same, its
    `MonitorAccelerator` returning a separate metrics list that every consumer
    joins by device identity and never by index, as an index is not stable
    across a re-detection.

    MIG devices are refreshed as well: they live in the card's
    `appendix["mig_devices"]` and carry their own UUID, so a usage entry
    matching one is merged into that entry.

    Args:
        devices:
            The devices to refresh.
        usages:
            The devices carrying the usage to merge.

    Returns:
        The given devices, refreshed.

    """
    if not devices or not usages:
        return devices

    # The join is only as good as the identities behind it, and a driver
    # answering the same id for every card is how that breaks: taking either
    # entry would write one card's utilization, memory and temperature onto
    # another. An ambiguous id is therefore dropped rather than guessed at,
    # leaving those cards with what the information query read.
    usages_map: dict[str, Device] = {}
    ambiguous_uuids: set[str] = set()
    for usage in usages:
        if not usage.uuid:
            continue
        if usage.uuid in usages_map:
            ambiguous_uuids.add(usage.uuid)
            continue
        usages_map[usage.uuid] = usage
    for uuid in ambiguous_uuids:
        debug_log_warning(
            logger,
            "Skipping usage of uuid %s, reported by more than one device",
            uuid,
        )
        del usages_map[uuid]

    for dev in devices:
        if usage := usages_map.get(dev.uuid):
            for field in _usage_fields_to_merge(usage):
                setattr(dev, field, getattr(usage, field))

        # Keyed by the instance's own UUID, so what lands in a MIG entry is the
        # usage query's reading for that instance -- the entry is written, never
        # read from.
        for mig_dev in (dev.appendix or {}).get("mig_devices") or []:
            if mig_usage := usages_map.get(mig_dev.get("uuid")):
                for field in _usage_fields_to_merge(mig_usage):
                    mig_dev[field] = getattr(mig_usage, field)

    return devices


def _usage_fields_to_merge(usage: Device) -> tuple[str, ...]:
    """
    Select the fields of the given usage entry that are worth writing over a device.

    Args:
        usage:
            The device carrying the usage to merge.

    Returns:
        The names of the fields to copy.

    """
    if usage.memory_status != DeviceMemoryStatusEnum.UNKNOWN:
        return _DEVICE_USAGE_FIELDS

    # No vendor's health helper returns UNKNOWN: they answer HEALTHY or
    # UNHEALTHY, and HEALTHY when the check is switched off. So an entry carrying
    # UNKNOWN never read health, and writing it would erase the verdict the
    # information query found -- which the CLI renders as ERR, exactly as it
    # renders UNHEALTHY.
    return tuple(field for field in _DEVICE_USAGE_FIELDS if field != "memory_status")


def _clear_health_verdict(devices: Devices) -> None:
    """
    Drop the health verdict of the given devices, in place.

    A verdict read when the devices were detected says nothing about them
    now, and the refresh that could have confirmed it failed. So the devices
    are served as inventory, with no verdict, rather than with one that went
    stale.

    Args:
        devices:
            The devices to drop the verdict of.

    """
    for dev in devices:
        dev.memory_status = DeviceMemoryStatusEnum.UNKNOWN
        # A MIG device is an entry of the card's appendix, and carries its own
        # verdict, which the CLI reports as well.
        for mig_dev in (dev.appendix or {}).get("mig_devices") or []:
            mig_dev["memory_status"] = DeviceMemoryStatusEnum.UNKNOWN


@dataclass
class _IdentityCacheEntry:
    """
    A cached device identity and the moment it expires.
    """

    devices: Devices
    """
    The detected identity, handed out as a copy.
    """

    expires_at: float
    """
    The clock reading after which the identity is stale.
    """


class _RefreshFlight:
    """
    One identity refresh of a manufacturer.

    A flight carries its own completion, so a caller that joined it takes
    that flight's outcome, whatever a later flight does.
    """

    def __init__(self, stale: Devices | None = None):
        self.done = threading.Event()
        self.devices: Devices | None = None
        self.error: BaseException | None = None
        self.stale = stale
        """
        The last good identity when the flight started: the fallback a failed
        flight owes its callers.
        """

    def complete(self, devices: Devices | None, error: BaseException | None) -> None:
        """
        Complete the flight with the devices it detected or the error it
        raised.

        Args:
            devices:
                The devices the flight detected, empty when it detected none.
            error:
                The exception the flight raised, None when it succeeded.

        """
        self.devices = devices
        self.error = error
        self.done.set()

    def outcome(self) -> Devices | None:
        """
        Return the devices the flight detected, or the last good identity
        when the flight failed.

        The last good identity is served without its health verdict: the
        verdict was read when the identity was detected, and the refresh that
        could have confirmed it failed.

        Returns:
            The devices to serve, as a copy.

        Raises:
            The exception the flight raised, when no identity is cached to
            fall back on.

        """
        if self.error is not None:
            if self.stale is None:
                raise self.error
            # The last good identity is better than nothing, and the next call
            # retries the detection.
            devices = copy.deepcopy(self.stale)
            _clear_health_verdict(devices)
            return devices
        return copy.deepcopy(self.devices)


class _VendorIdentityState:
    """
    The identity cache state of one manufacturer.

    The condition guards the entry and the live flight. A refresh runs
    outside the lock, so a wedged vendor delays only the callers waiting for
    that vendor.
    """

    def __init__(self):
        self.condition = threading.Condition()
        self.entry: _IdentityCacheEntry | None = None
        self.flight: _RefreshFlight | None = None
        """
        The refresh running now, if any. A caller that finds one joins it
        rather than starting a second detection of the same vendor.
        """


class _IdentityCache:
    """
    The process wide cache of detected device identities.

    One state per manufacturer, so a slow refresh of one vendor delays only
    the callers waiting for that vendor.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._states: dict[ManufacturerEnum, _VendorIdentityState] = {}

    def state(self, manufacturer: ManufacturerEnum) -> _VendorIdentityState:
        """
        Return the cache state of the given manufacturer.

        Args:
            manufacturer:
                The manufacturer to take the state of.

        Returns:
            The state of the manufacturer.

        """
        with self._lock:
            state = self._states.get(manufacturer)
            if state is None:
                state = _VendorIdentityState()
                self._states[manufacturer] = state
            return state

    def reset(self) -> None:
        """
        Drop every cached identity.
        """
        with self._lock:
            self._states.clear()

    def identity(self, detector: Detector, ttl: float) -> Devices | None:
        """
        Return the identity of the given detector, from the cache when it is
        still fresh.

        The identity is handed out as a copy: the usage merge writes into the
        devices it is given, so a shared object would let one caller
        contaminate the cache.

        A fresh identity is served with the health verdict it was detected
        with. A stale one is served without any verdict: the refresh that
        could have confirmed it failed, so the last good identity stands for
        the inventory only.

        Args:
            detector:
                The detector to take the identity of.
            ttl:
                The time to live of a cached identity, in seconds.
                Zero disables the cache.

        Returns:
            The devices carrying the identity, or None if there is none.

        Raises:
            The exception the detection raised, when the cache holds no
            identity to fall back on. An interruption propagates to the caller
            that asked for the detection, and the waiters of the same refresh
            get its outcome.

        """
        if ttl <= 0:
            # The cache is disabled: the identity is detected on every call.
            return detector.detect_info()

        state = self.state(detector.manufacturer)
        with state.condition:
            entry = state.entry
            if entry is not None and _monotonic() < entry.expires_at:
                return copy.deepcopy(entry.devices)
            flight = state.flight
            if flight is not None:
                # A refresh is running: join it, so the callers of one vendor
                # share one detection. The flight carries its own completion,
                # so a later flight cannot postpone this caller's outcome.
                state.condition.wait_for(flight.done.is_set)
                return flight.outcome()
            # No refresh is running: start one. The last good identity is the
            # fallback the flight owes its callers when it fails.
            flight = _RefreshFlight(
                stale=entry.devices if entry is not None else None,
            )
            state.flight = flight

        # The detection runs outside the lock: a wedged vendor must not hold
        # the cache for the other vendors.
        devices: Devices | None = None
        error: BaseException | None = None
        try:
            devices = detector.detect_info()
        except Exception as e:
            debug_log_exception(
                logger,
                "Failed to detect %s devices identity, keeping the last known one",
                detector.manufacturer,
            )
            error = e
        except BaseException as e:
            # An interruption settles the flight as well: a flight left
            # pending blocks every later caller of the vendor forever. The
            # interruption keeps propagating to the thread that asked for the
            # detection, while the waiters get the flight's outcome.
            debug_log_exception(
                logger,
                "Interrupted while detecting %s devices identity",
                detector.manufacturer,
            )
            error = e
            raise
        finally:
            with state.condition:
                if error is None and devices:
                    # Only a non empty identity is cached: one enumeration glitch
                    # must not pin an empty inventory for the time to live.
                    state.entry = _IdentityCacheEntry(
                        devices=devices,
                        expires_at=_monotonic() + ttl,
                    )
                flight.complete(devices, error)
                state.flight = None
                state.condition.notify_all()
        return flight.outcome()


_IDENTITY_CACHE = _IdentityCache()
"""
The process wide cache of detected device identities.
"""


def _identity_cache_ttl() -> float:
    """
    Return the time to live of a cached device identity, in seconds.

    Zero disables the cache. The environment variable is read once per
    process, so it must be set before the first detection.

    Returns:
        The time to live, in seconds.

    """
    return float(envs.GPUSTACK_RUNTIME_DETECT_INFO_CACHE_TTL)


def _reset_identity_cache() -> None:
    """
    Drop every cached device identity.
    """
    _IDENTITY_CACHE.reset()


class Detector(ABC):
    """
    Base class for all detectors.
    """

    manufacturer: ManufacturerEnum = ManufacturerEnum.UNKNOWN
    """
    Manufacturer of the detector.
    """

    @staticmethod
    @abstractmethod
    def is_supported() -> bool:
        """
        Check if the detector is supported on the current environment.

        Returns:
            True if supported, False otherwise.

        """
        raise NotImplementedError

    def __init__(self, manufacturer: ManufacturerEnum):
        self.manufacturer = manufacturer

    @property
    def backend(self) -> str:
        """
        The backend name of the detector, e.g., 'cuda', 'rocm'.
        """
        return manufacturer_to_backend(self.manufacturer)

    @property
    def name(self) -> str:
        """
        The name of the detector, e.g., 'nvidia', 'amd'.
        """
        return str(self.manufacturer)

    @abstractmethod
    def detect_info(self) -> Devices | None:
        """
        Detect devices' inventory, without usage metrics.

        Returns:
            A list of detected Device objects, or None if not supported.

        """
        raise NotImplementedError

    @abstractmethod
    def detect_usage(self, devices: Devices | None = None) -> Devices | None:
        """
        Fetch the usage of the given devices, merged into them in place.

        Args:
            devices:
                The devices to refresh, matched by UUID, MIG entries in
                ``appendix["mig_devices"]`` included.
                If None, detects the devices' information first.

        Returns:
            The devices carrying usage, or None if not supported.

        """
        raise NotImplementedError

    def detect(self, usage: bool = True) -> Devices | None:
        """
        Detect devices and return a list of Device objects.

        The identity of the devices is cached process wide, per manufacturer,
        so a repeated collection pays only the usage query. The usage is read
        on every call, and so is the health it carries.

        Args:
            usage:
                Whether to fetch the devices' usage as well.

        Returns:
            A list of detected Device objects, or None if detection fails.

        """
        devices = _IDENTITY_CACHE.identity(self, _identity_cache_ttl())
        if usage and devices:
            try:
                self.detect_usage(devices)
            except Exception:
                # The inventory is already in hand, and a card exists whether or
                # not its metrics could be read. Letting this propagate costs
                # every device of the vendor -- detect_devices logs and moves on
                # -- so a host with eight healthy cards reports none because one
                # metric query failed. The usage fields keep what the
                # information query read.
                debug_log_exception(
                    logger,
                    "Failed to fetch %s devices usage, reporting information only",
                    self.manufacturer,
                )
        return devices

    def get_topology(self, devices: Devices | None = None) -> Topology | None:
        """
        Get the Topology object between the given devices.

        Args:
            devices:
                A list of Device objects.

        Returns:
            A Topology object, or None if not supported.

        """
        return None
