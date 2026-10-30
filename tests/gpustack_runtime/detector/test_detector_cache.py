from __future__ import annotations

import logging
import threading
import time

import pytest

from gpustack_runtime import envs
from gpustack_runtime.detector import (
    Device,
    DeviceMemoryStatusEnum,
    Devices,
    ManufacturerEnum,
    detect_devices,
)
from gpustack_runtime.detector.__types__ import (
    _IDENTITY_CACHE,
    Detector,
    merge_devices_usage,
)


class _FakeDetector(Detector):
    """
    A detector counting its queries, as the vendors behave during the expand
    step: detect_info returns everything, detect_usage merges a usage payload
    in.
    """

    def __init__(
        self,
        manufacturer: ManufacturerEnum = ManufacturerEnum.NVIDIA,
        devices: Devices | None = None,
        usages: Devices | None = None,
    ):
        super().__init__(manufacturer)
        self.devices = devices
        self.usages = usages
        self.info_calls = 0
        self.usage_calls = 0
        self.info_threads: list[int] = []
        self.info_error: BaseException | None = None
        self.on_info = None
        self.on_usage = None

    @staticmethod
    def is_supported() -> bool:
        return True

    def detect_info(self) -> Devices | None:
        self.info_calls += 1
        self.info_threads.append(threading.get_ident())
        if self.on_info is not None:
            self.on_info()
        if self.info_error is not None:
            raise self.info_error
        return self.devices

    def detect_usage(self, devices: Devices | None = None) -> Devices | None:
        self.usage_calls += 1
        if devices is None:
            devices = self.detect_info()
        # The hook runs before the merge, so a test can hold a caller inside
        # its usage query while another one merges.
        if self.on_usage is not None:
            self.on_usage()
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


def _usage(
    uuid: str,
    cores_utilization: int = 42,
    memory_status: DeviceMemoryStatusEnum = DeviceMemoryStatusEnum.UNHEALTHY,
) -> Device:
    return Device(
        uuid=uuid,
        cores_utilization=cores_utilization,
        memory_used=1024,
        memory_utilization=2.5,
        memory_status=memory_status,
        temperature=61,
        power_used=250,
    )


def _mig_device() -> dict:
    return {
        "index": 2,
        "name": "1g.5gb",
        "uuid": "MIG-0-0",
        "memory": 4864,
        "cores_utilization": 0,
        "memory_used": 0,
        "memory_status": DeviceMemoryStatusEnum.HEALTHY,
        "appendix": {"sliced": True, "mig": True},
    }


class _ObservedCondition(threading.Condition):
    """
    A condition reporting when a caller joins a refresh flight.

    A caller reaching the wait path of a flight has found it, and that flight
    is the one it will get, whatever a later flight does. The event makes the
    arrival observable, so a test can hold a failed flight open until its
    follower is waiting on it.
    """

    def __init__(self):
        super().__init__()
        self.joined = threading.Event()

    def wait_for(self, *args, **kwargs):
        self.joined.set()
        return super().wait_for(*args, **kwargs)


@pytest.fixture
def ttl(monkeypatch):
    """
    Set the time to live of the cached identity, and reset the memoized
    environment variable it is read from.
    """

    def _set(value: int) -> None:
        monkeypatch.setenv("GPUSTACK_RUNTIME_DETECT_INFO_CACHE_TTL", str(value))
        envs.__getattr__.cache_clear()

    yield _set
    envs.__getattr__.cache_clear()


@pytest.fixture
def clock(monkeypatch):
    """
    The clock the time to live is measured on, under the test's control.
    """
    now = [1000.0]
    monkeypatch.setattr(
        "gpustack_runtime.detector.__types__._monotonic",
        lambda: now[0],
    )

    def _advance(seconds: float) -> None:
        now[0] += seconds

    return _advance


def test_a_second_call_within_the_time_to_live_runs_no_identity_query():
    det = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])

    det.detect()
    det.detect()

    assert det.info_calls == 1


def test_the_usage_query_runs_on_every_call():
    det = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])

    det.detect()
    det.detect()
    det.detect()

    assert det.usage_calls == 3
    assert det.info_calls == 1


def test_a_zero_time_to_live_disables_the_cache(ttl):
    ttl(0)
    det = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])

    det.detect()
    det.detect()

    assert det.info_calls == 2
    assert det.usage_calls == 2


def test_mutating_a_handed_out_device_leaves_the_cache_alone():
    det = _FakeDetector(
        devices=[_device("GPU-0", appendix={"mig_devices": [_mig_device()]})],
        usages=[_usage("GPU-0"), _usage("MIG-0-0")],
    )

    first = det.detect()
    first[0].appendix["mig_devices"][0]["memory"] = 1
    first[0].appendix["tampered"] = True

    second = det.detect()

    # The mutation did not reach the cache: the second call was a cache hit.
    assert det.info_calls == 1
    assert "tampered" not in second[0].appendix
    assert second[0].appendix["mig_devices"][0]["memory"] == 4864


def test_a_concurrent_merge_leaves_the_cache_alone():
    det = _FakeDetector(
        devices=[_device("GPU-0", appendix={"mig_devices": [_mig_device()]})],
        usages=[_usage("GPU-0"), _usage("MIG-0-0")],
    )
    det.detect()

    # A caller holds a copy of the identity.
    held = det.detect()

    merging = threading.Event()
    release = threading.Event()

    def on_usage():
        # The caller is inside its usage query, about to merge.
        merging.set()
        release.wait(timeout=10)

    det.on_usage = on_usage

    def worker():
        det.detect()

    thread = threading.Thread(target=worker)
    thread.start()

    # The mutation happens while the next call merges into its own copy: a
    # shared copy would carry the mutation into the cache.
    assert merging.wait(timeout=10)
    held[0].appendix["mig_devices"][0]["marker"] = "mutated"
    release.set()
    thread.join(timeout=10)

    final = det.detect()

    assert det.info_calls == 1
    assert "marker" not in final[0].appendix["mig_devices"][0]
    assert final[0].appendix["mig_devices"][0]["memory_used"] == 1024


def test_an_empty_identity_is_not_cached():
    det = _FakeDetector(devices=[], usages=[])

    assert det.detect() == []

    det.devices = [_device("GPU-0")]
    devices = det.detect()

    assert [dev.uuid for dev in devices] == ["GPU-0"]
    assert det.info_calls == 2


def test_an_absent_identity_is_not_cached():
    det = _FakeDetector(devices=None, usages=None)

    assert det.detect() is None

    det.devices = [_device("GPU-0")]
    devices = det.detect()

    assert [dev.uuid for dev in devices] == ["GPU-0"]
    assert det.info_calls == 2


def test_a_failed_refresh_serves_the_last_good_identity(caplog, clock):
    det = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])
    det.detect()

    clock(61)
    det.info_error = RuntimeError("the driver went away")

    with caplog.at_level(logging.DEBUG, logger="gpustack_runtime.detector.__types__"):
        devices = det.detect()

    assert [dev.uuid for dev in devices] == ["GPU-0"]
    assert det.info_calls == 2
    assert "Failed to detect" in caplog.text


def test_a_successful_refresh_replaces_the_stale_identity(clock):
    det = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])
    det.detect()

    clock(61)
    det.info_error = RuntimeError("the driver went away")
    det.detect()

    det.info_error = None
    det.devices = [_device("GPU-1")]
    devices = det.detect()

    assert [dev.uuid for dev in devices] == ["GPU-1"]
    assert det.info_calls == 3


def test_a_failed_refresh_beyond_the_time_to_live_serves_no_health_verdict(clock):
    det = _FakeDetector(
        devices=[_device("GPU-0", memory_status=DeviceMemoryStatusEnum.UNHEALTHY)],
        usages=[_usage("GPU-0", memory_status=DeviceMemoryStatusEnum.UNKNOWN)],
    )

    # The information query reads the health, so the cached identity carries
    # a verdict.
    fresh = det.detect()

    assert fresh[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY

    # The identity is stale now, and the refresh fails. The usage query runs
    # as well, and it carries no verdict to supply.
    clock(61)
    det.info_error = RuntimeError("the driver went away")
    stale = det.detect()

    # The failed refresh serves the inventory, but not the verdict read when
    # the identity was detected: it went stale with it.
    assert [dev.uuid for dev in stale] == ["GPU-0"]
    assert stale[0].memory_status == DeviceMemoryStatusEnum.UNKNOWN
    assert det.usage_calls == 2

    # The next successful detection reads the health again, and serves it.
    det.info_error = None
    det.devices = [_device("GPU-0", memory_status=DeviceMemoryStatusEnum.UNHEALTHY)]
    recovered = det.detect()

    assert recovered[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY
    assert det.info_calls == 3
    assert det.usage_calls == 3


def test_an_interrupted_refresh_leaves_no_flight_behind():
    det = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])
    det.info_error = KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        det.detect()

    # The interruption settled the flight: a flight left pending would block
    # every later caller of the vendor.
    state = _IDENTITY_CACHE.state(ManufacturerEnum.NVIDIA)
    assert state.flight is None

    # The next call detects again, and returns normally.
    det.info_error = None
    devices = det.detect()

    assert [dev.uuid for dev in devices] == ["GPU-0"]
    assert det.info_calls == 2


def test_the_merge_of_a_fresh_usage_over_a_cached_identity_matches_a_detection():
    baseline = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])

    # The shape a detection without a cache produces: the information query
    # first, then the usage merged into it.
    expected = baseline.detect_usage(baseline.detect_info())

    det = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])
    det.detect()
    devices = det.detect()

    # The second call was a cache hit, and it merged the same way.
    assert det.info_calls == 1
    assert devices == expected
    assert devices[0].cores_utilization == 42
    assert devices[0].power_used == 250
    assert devices[0].memory_status == DeviceMemoryStatusEnum.UNHEALTHY


def test_the_time_to_live_is_measured_on_the_injected_clock(clock):
    det = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])

    det.detect()
    det.detect()

    assert det.info_calls == 1

    clock(61)
    det.detect()

    assert det.info_calls == 2


def test_the_time_to_live_is_measured_on_a_monotonic_clock(monkeypatch):
    det = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])

    det.detect()

    # The wall clock jumping forward does not expire the identity: the time to
    # live is measured on a monotonic clock.
    monkeypatch.setattr(time, "time", lambda: 1000000.0)
    det.detect()

    assert det.info_calls == 1


def test_a_slow_refresh_is_not_duplicated_by_concurrent_callers():
    det = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])

    started = threading.Event()
    release = threading.Event()

    def slow_info():
        started.set()
        release.wait(timeout=10)

    det.on_info = slow_info

    detected = []

    def worker():
        detected.append(det.detect())

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for thread in threads:
        thread.start()

    assert started.wait(timeout=10)
    release.set()

    for thread in threads:
        thread.join(timeout=10)

    # The followers entered the wait path of the running refresh instead of
    # detecting on their own: only one thread detected.
    assert det.info_calls == 1
    assert len(set(det.info_threads)) == 1
    assert len(detected) == 3
    assert all([dev.uuid for dev in devices] == ["GPU-0"] for devices in detected)


def test_two_instances_of_the_same_manufacturer_share_one_identity():
    first = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])
    second = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])

    first.detect()
    devices = second.detect()

    # The identity is cached per manufacturer, so the second instance serves
    # what the first one detected.
    assert first.info_calls == 1
    assert second.info_calls == 0
    assert [dev.uuid for dev in devices] == ["GPU-0"]


def test_a_barging_retry_cannot_postpone_a_waiters_stale_fallback(clock):
    det = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])

    # The condition reports when a caller joins a flight, so the test knows
    # the follower is waiting on the flight it is meant to join.
    state = _IDENTITY_CACHE.state(ManufacturerEnum.NVIDIA)
    condition = _ObservedCondition()
    state.condition = condition

    det.detect()
    clock(61)
    # The cached identity is stale now, so the next detection refreshes it.

    in_flight = threading.Event()
    release_flight = threading.Event()
    barged = threading.Event()
    release_barge = threading.Event()
    follower_done = threading.Event()

    flights = []

    def info():
        flights.append(1)
        if len(flights) == 1:
            # The first flight waits for a follower to join it.
            in_flight.set()
            release_flight.wait(timeout=10)
            return
        # The barging retry stays running.
        barged.set()
        release_barge.wait(timeout=10)

    det.on_info = info
    det.info_error = RuntimeError("the driver went away")

    def owner():
        # The first detection fails and is owed the last good identity; the
        # retry is the barging flight, and it stays running.
        det.detect()
        det.detect()

    follower_result = []

    def follower():
        follower_result.append(det.detect())
        follower_done.set()

    owner_thread = threading.Thread(target=owner)
    owner_thread.start()
    assert in_flight.wait(timeout=10)

    follower_thread = threading.Thread(target=follower)
    follower_thread.start()

    # The follower reached the wait path of the running flight, so it will
    # get that flight's outcome. The flight is released only now, so the
    # follower cannot miss it.
    assert condition.joined.wait(timeout=10)
    release_flight.set()

    try:
        assert barged.wait(timeout=10)
        # The follower took the outcome of the flight it joined, rather than
        # waiting for the barging retry to finish first.
        assert follower_done.wait(timeout=5)
        assert [dev.uuid for dev in follower_result[0]] == ["GPU-0"]
        # The follower joined the first flight instead of detecting on its own:
        # the priming detection, the failed flight and the barging retry.
        assert det.info_calls == 3
    finally:
        release_barge.set()
        owner_thread.join(timeout=10)
        follower_thread.join(timeout=10)


def test_detect_devices_caches_the_identity_of_the_fake_detectors(monkeypatch):
    det = _FakeDetector(devices=[_device("GPU-0")], usages=[_usage("GPU-0")])
    monkeypatch.setattr("gpustack_runtime.detector._DETECTORS", [det])

    first = detect_devices(fast=False)
    second = detect_devices(fast=False)

    assert det.info_calls == 1
    assert det.usage_calls == 2
    assert [dev.uuid for dev in first] == ["GPU-0"]
    assert [dev.uuid for dev in second] == ["GPU-0"]
