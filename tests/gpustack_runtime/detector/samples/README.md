# GPUStack Runtime Detector Samples

This directory contains output samples fetched by the GPUStack Runtime Detector.

`test_samples.py` guards the shape of every `detect_output_*.json` against the `Device` contract.

## Captured from real hardware

These were captured with `gpustack-runtime detect --format json` / `topology --format json` on the
host named, so their values are measurements rather than hand-alignments:

| Sample | Host |
| --- | --- |
| `*_amd_rx7800xt.json` | 2 × AMD Radeon RX 7800 XT, ROCm 7.2.0, driver 6.16.13 |
| `*_ascend_910b2.json` | 8 × Ascend 910B2 |
| `*_nvidia_rtx4090_48g.json` | 2 × RTX 4090 48 GB, driver 595.84 |
| `*_nvidia_rtx5090d.json` | 1 × RTX 5090 D, driver 595.84 |
| `*_thead_ppu.json` | 16 × PPU-ZW810E |

Two facts worth reading off them, because both decide a device-node path:

- **Ascend carries `appendix["physical_id"]` on every device.** `/dev/davinciN` is numbered by it, and
  a device whose physical id cannot be read is dropped rather than addressed by the logic id. On this
  host the two happen to agree (0–7).
- **T-Head's `appendix["minor_number"]` is not its enumeration index.** They run one apart on this host
  (index 3 → minor 4, index 15 → minor 16) because `/dev/alixpu` holds minor 0 of the same
  character-device major, but that offset is an observation about this host and driver, not a rule —
  neither number is ever computed from the other. `/dev/alixpu_ppu<N>` is named by the *card ordinal*;
  the minor is the card's identity, and the CDI generator compares it against the node's own minor to
  prove the ordinal reached the right card.

## Not captured from real hardware

The remaining samples are hand-aligned and carry the caveats below.

In `detect_output_nvidia_h100_mig.json`, the two MIG-enabled cards (index 0 and 3) are rebuilt by
hand from the instances that the retired standalone-MIG shape reported in their place, so they carry
only the fields those instances copied off the card: `name`, `uuid`, `cores`, `memory`, the usage
fields and the fabric keys are absent because the old shape did not record them, not because the
driver had no answer. Card 3's instances are still named after their compute-instance profile
(`1c.3g.40gb`), where the detector now reports the GPU-instance profile. Refresh from real hardware
before relying on either — no MIG-enabled host was available.

`detect_output_hygon_k100ai.json` is likewise unrefreshed. The Hygon *code path* was exercised on the
AMD host by pointing its PCI vendor gate at `0x1002`, which proves the path runs and its field names
resolve, but the values it produced are an AMD card's and were deliberately not written here.
