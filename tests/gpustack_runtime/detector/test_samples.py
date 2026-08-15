from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any

import pytest

from gpustack_runtime.detector import Device
from gpustack_runtime.detector.__types__ import (
    DeviceMemoryStatusEnum,
    ManufacturerEnum,
)

_SAMPLES_PATH = Path(__file__).parent / "samples"

_DEVICE_FIELDS = {field.name for field in dataclasses.fields(Device)}

_RETIRED_APPENDIX_KEYS = (
    # Whole-card reporting: no virtual/PF/VF classification.
    "vgpu",
    # MIG instances are appendix entries of their card, not devices of their
    # own, so they no longer carry a device index per instance handle.
    "gpu_instance_index",
    "compute_instance_index",
)
"""
Appendix keys no detector emits anymore, which a sample must not resurrect.
"""


def _samples() -> list[Path]:
    """
    The detect output samples. `topology_output_*.json` describes a Topology,
    not a Device, and is out of this guard's scope.
    """
    return sorted(_SAMPLES_PATH.glob("detect_output_*.json"))


def _load(sample: Path) -> list[dict[str, Any]]:
    devices = json.loads(sample.read_text())
    assert devices, f"{sample.name} carries no device"
    return devices


def _mig_devices(device: dict[str, Any]) -> list[dict[str, Any]]:
    return (device.get("appendix") or {}).get("mig_devices") or []


def _assert_deserializes(sample: Path, entry: dict[str, Any]) -> None:
    unknown = sorted(set(entry) - _DEVICE_FIELDS)
    assert not unknown, f"{sample.name} carries fields Device cannot hold: {unknown}"

    device = Device.from_dict(entry)
    assert isinstance(device.manufacturer, ManufacturerEnum)
    assert isinstance(device.memory_status, DeviceMemoryStatusEnum)


@pytest.mark.parametrize("sample", _samples(), ids=lambda sample: sample.name)
def test_sample_deserializes_into_device(sample: Path):
    for entry in _load(sample):
        _assert_deserializes(sample, entry)
        for mig_device in _mig_devices(entry):
            _assert_deserializes(sample, mig_device)


@pytest.mark.parametrize("sample", _samples(), ids=lambda sample: sample.name)
def test_sample_carries_no_retired_appendix_key(sample: Path):
    for entry in _load(sample):
        appendices = [entry.get("appendix") or {}]
        appendices += [
            mig_device.get("appendix") or {} for mig_device in _mig_devices(entry)
        ]
        for appendix in appendices:
            retired = sorted(set(appendix) & set(_RETIRED_APPENDIX_KEYS))
            assert not retired, f"{sample.name} carries retired keys: {retired}"


@pytest.mark.parametrize("sample", _samples(), ids=lambda sample: sample.name)
def test_sample_keeps_mig_instances_inside_the_card(sample: Path):
    devices = _load(sample)
    indexes = [entry["index"] for entry in devices]

    for entry in devices:
        appendix = entry.get("appendix") or {}
        assert not appendix.get("sliced"), (
            f"{sample.name} reports a MIG instance as a device of its own"
        )
        if appendix.get("mig"):
            assert "mig_devices" in appendix, (
                f"{sample.name} enables MIG without reporting its instances"
            )
        for mig_device in _mig_devices(entry):
            assert mig_device["appendix"]["sliced"]
            # index_mig_devices numbers the instances above every card.
            assert mig_device["index"] > max(indexes)
