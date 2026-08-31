from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from gpustack_runtime.cmds import detector as cmds_detector
from gpustack_runtime.detector import (
    Device,
    DeviceMemoryStatusEnum,
    ManufacturerEnum,
)


# --------------------------------------------------------------------------- #
# Helpers.                                                                    #
# --------------------------------------------------------------------------- #
def _parse(*argv: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    cmds_detector.DetectDevicesSubCommand.register(subparsers)
    return parser.parse_args(["detect", *argv])


def _device() -> Device:
    return Device(
        manufacturer=ManufacturerEnum.NVIDIA,
        index=0,
        name="NVIDIA A100-SXM4-40GB",
        uuid="GPU-0",
        driver_version="580.65.06",
        runtime_version="13.0",
        compute_capability="8.0",
        cores=6912,
        memory=40960,
        memory_status=DeviceMemoryStatusEnum.HEALTHY,
        appendix={},
    )


# --------------------------------------------------------------------------- #
# `detect --no-usage` threading.                                              #
# --------------------------------------------------------------------------- #
def test_detect_asks_for_usage_by_default(monkeypatch):
    calls: list[dict] = []

    def fake_detect_devices(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(cmds_detector, "detect_devices", fake_detect_devices)

    cmds_detector.DetectDevicesSubCommand(_parse("--format", "json")).run()

    assert calls == [{"fast": False, "usage": True}]


def test_detect_no_usage_skips_the_usage_query(monkeypatch):
    calls: list[dict] = []

    def fake_detect_devices(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(cmds_detector, "detect_devices", fake_detect_devices)

    cmds_detector.DetectDevicesSubCommand(
        _parse("--no-usage", "--format", "json"),
    ).run()

    # The flag is the only thing standing between the CLI and a metric call:
    # every detector's usage query is skipped when `usage` is false.
    assert calls == [{"fast": False, "usage": False}]


# --------------------------------------------------------------------------- #
# Table rendering: `N/A` for what was never measured.                         #
# --------------------------------------------------------------------------- #
def test_table_renders_measured_usage_as_numbers():
    dev = _device()
    dev.cores_utilization = 42
    dev.memory_used = 1024
    dev.temperature = 61

    table = cmds_detector.format_devices_table([dev])

    assert "1024MiB / 40960MiB" in table
    assert "42%" in table
    assert "61C" in table


def test_table_renders_unmeasured_usage_as_not_available():
    table = cmds_detector.format_devices_table([_device()], usage=False)

    # A zero here is indistinguishable from a real idle reading, which is
    # exactly the ambiguity `--no-usage` exists to remove.
    assert "N/A / 40960MiB" in table
    assert "0%" not in table
    # The used-memory slot, i.e. everything left of the "/", carries no number.
    assert not re.search(r"\d+MiB / ", table)
    assert not re.search(r"\d+C", table)
    # Information fields keep their values, and health is reported by both
    # queries, so the status column stays meaningful.
    assert "NVIDIA A100-SXM4-40GB" in table
    assert "8.0" in table
    assert "OK" in table


# --------------------------------------------------------------------------- #
# JSON rendering: an unmeasured field is absent, not zero.                     #
# --------------------------------------------------------------------------- #
def test_json_renders_measured_usage_as_numbers():
    dev = _device()
    dev.cores_utilization = 42
    dev.memory_used = 1024
    dev.temperature = 61

    payload = json.loads(cmds_detector.format_devices_json([dev]))

    assert payload[0]["cores_utilization"] == 42
    assert payload[0]["memory_used"] == 1024
    assert payload[0]["temperature"] == 61


def test_json_omits_unmeasured_usage():
    dev = _device()
    dev.appendix = {"mig_devices": [{"uuid": "MIG-0", "memory_used": 0}]}

    payload = json.loads(cmds_detector.format_devices_json([dev], usage=False))

    # A serialized 0 is indistinguishable from a real idle reading, which is
    # what `--no-usage` exists to remove -- and a machine-readable consumer is
    # the more likely of the two to act on it.
    for key in ("cores_utilization", "memory_used", "memory_utilization"):
        assert key not in payload[0]
    for key in ("temperature", "power_used"):
        assert key not in payload[0]
    # MIG instances carry the same fields, so they are dropped there too.
    assert "memory_used" not in payload[0]["appendix"]["mig_devices"][0]
    # Information fields keep their values, and health is reported by both
    # queries, so the status stays meaningful.
    assert payload[0]["memory"] == 40960
    assert payload[0]["memory_status"] == DeviceMemoryStatusEnum.HEALTHY


# --------------------------------------------------------------------------- #
# Tree-wide guard: no detector can emit a `vgpu` appendix key.                 #
# --------------------------------------------------------------------------- #
def test_no_detector_emits_a_vgpu_appendix_key():
    package = Path(cmds_detector.__file__).parent.parent
    offenders = [
        str(path.relative_to(package))
        for path in sorted(package.rglob("*.py"))
        if re.search(r"""['"]vgpu['"]""", path.read_text(encoding="utf-8"))
    ]

    # The vGPU/virtual-card classification is gone for every vendor, so no
    # shipped module may name the appendix key again. The vendor bindings'
    # own `*_VGPU_*` constants are upstream SDK symbols, not appendix keys,
    # and do not match.
    assert offenders == []


# --------------------------------------------------------------------------- #
# Hygon MIG inventory serialization.                                          #
# --------------------------------------------------------------------------- #
def test_json_keeps_a_hygon_mig_devices_entry():
    dev = Device(
        manufacturer=ManufacturerEnum.HYGON,
        index=0,
        name="K100_AI",
        uuid="GPU-9f8e7d6c5b4a3921",
        appendix={
            "mig": True,
            "bdf": "0000:0b:00.0",
            "numa": "0",
            "mig_devices": [
                {
                    "index": 2,
                    "name": "1g.16gb",
                    "uuid": "MIG-aaaaaaaa-0000-0000-0000-000000000000",
                    "cores": 26,
                    "cores_utilization": 0,
                    "memory": 16380,
                    "memory_used": 0,
                    "memory_utilization": 0,
                    "temperature": None,
                    "power": 350,
                    "power_used": None,
                    "appendix": {
                        "mig": True,
                        "sliced": True,
                        "bdf": "0000:0b:00.0",
                        "numa": "0",
                        "gpu_instance_id": 5,
                        "compute_instance_id": 0,
                        "placement": {"start": 0, "length": 1},
                    },
                },
            ],
        },
    )

    payload = json.loads(cmds_detector.format_devices_json([dev]))

    assert payload[0]["appendix"]["mig"] is True
    mig = payload[0]["appendix"]["mig_devices"][0]
    assert mig["uuid"] == "MIG-aaaaaaaa-0000-0000-0000-000000000000"
    assert mig["index"] == 2
    assert mig["name"] == "1g.16gb"
    assert mig["cores"] == 26
    assert mig["memory"] == 16380
    assert mig["appendix"]["gpu_instance_id"] == 5
    assert mig["appendix"]["compute_instance_id"] == 0
    assert mig["appendix"]["placement"] == {"start": 0, "length": 1}
