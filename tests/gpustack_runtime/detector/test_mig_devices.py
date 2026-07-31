from __future__ import annotations

from gpustack_runtime.detector import (
    Device,
    ManufacturerEnum,
    expand_mig_devices,
)
from gpustack_runtime.detector.__types__ import index_mig_devices


def _card(index: int, uuid: str, mig_devices: list[dict] | None = None) -> Device:
    appendix: dict = {"mig": mig_devices is not None}
    if mig_devices is not None:
        appendix["mig_devices"] = mig_devices
    return Device(
        manufacturer=ManufacturerEnum.NVIDIA,
        index=index,
        name="NVIDIA A100-SXM4-40GB",
        uuid=uuid,
        memory=40960,
        appendix=appendix,
    )


def _mig(slot: int, uuid: str) -> dict:
    # As detection reports it: "index" is the driver slot, numbered later.
    return {
        "index": slot,
        "name": "1g.5gb",
        "uuid": uuid,
        "memory": 4864,
        "appendix": {"vgpu": True, "sliced": True, "mig": True},
    }


def test_index_mig_devices_numbers_above_the_cards():
    cards = [_card(0, "GPU-0"), _card(1, "GPU-1")]
    mig_devices = {0: [_mig(0, "MIG-0-0"), _mig(1, "MIG-0-1")]}

    index_mig_devices(cards, mig_devices, 8)

    assert [m["index"] for m in mig_devices[0]] == [2, 3]


def test_index_mig_devices_keeps_the_blocks_apart():
    # Every card partitioned to the maximum: with a block per card, no two MIG
    # devices can land on the same index however many cards there are.
    cards = [_card(i, f"GPU-{i}") for i in range(2)]
    mig_devices = {
        dev_idx: [_mig(slot, f"MIG-{dev_idx}-{slot}") for slot in range(7)]
        for dev_idx in range(2)
    }

    index_mig_devices(cards, mig_devices, 8)

    indexes = [card.index for card in cards] + [
        mig["index"] for migs in mig_devices.values() for mig in migs
    ]
    assert len(indexes) == len(set(indexes))


def test_index_mig_devices_clears_physical_indexes():
    # Physical indexes are minor numbers when physical index priority is on,
    # so they are not bound by the card count: numbering from the count would
    # collide with the cards themselves.
    cards = [_card(2, "GPU-2"), _card(3, "GPU-3")]
    mig_devices = {
        0: [_mig(0, "MIG-2-0"), _mig(1, "MIG-2-1")],
        1: [_mig(0, "MIG-3-0")],
    }

    index_mig_devices(cards, mig_devices, 8)

    assert [m["index"] for m in mig_devices[0]] == [4, 5]
    assert [m["index"] for m in mig_devices[1]] == [12]


def test_index_mig_devices_isolates_a_cards_numbering():
    # Destroying an instance on one card must not renumber another's.
    cards = [_card(0, "GPU-0"), _card(1, "GPU-1")]
    full = {
        0: [_mig(slot, f"MIG-0-{slot}") for slot in range(3)],
        1: [_mig(0, "MIG-1-0")],
    }
    partitioned = {0: [_mig(0, "MIG-0-0")], 1: [_mig(0, "MIG-1-0")]}

    index_mig_devices(cards, full, 8)
    index_mig_devices(cards, partitioned, 8)

    assert full[1][0]["index"] == partitioned[1][0]["index"]


def test_expand_mig_devices_substitutes_the_card():
    cards = [
        _card(0, "GPU-0", [_mig(0, "MIG-0-0"), _mig(1, "MIG-0-1")]),
        _card(1, "GPU-1"),
    ]
    index_mig_devices(cards, {0: cards[0].appendix["mig_devices"]}, 8)

    expanded = expand_mig_devices(cards)

    assert [dev.uuid for dev in expanded] == ["MIG-0-0", "MIG-0-1", "GPU-1"]
    assert [dev.index for dev in expanded] == [2, 3, 1]
    assert all(dev.manufacturer == ManufacturerEnum.NVIDIA for dev in expanded)
    assert expanded[0].memory == 4864


def test_expand_mig_devices_keeps_an_unpartitioned_card():
    # MIG enabled, no GPU instance yet: nothing to address, keep the card.
    cards = [_card(0, "GPU-0", [])]

    assert expand_mig_devices(cards) == cards


def test_expand_mig_devices_tolerates_no_devices():
    assert expand_mig_devices(None) == []
