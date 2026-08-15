# Spec: Align runtime detector with the operator's device manager, and surface workload exit status

Status: Built
Type: Feature

## Summary

`gpustack_runtime.detector` and `gpustack-operator/pkg/devicemanager/detector` describe the same
hardware through two independently-grown code paths, and the runtime side has drifted: it misses
capability queries the operator performs, it classifies cards as virtual/vGPU, it fetches
utilization metrics it cannot avoid paying for, and it indexes devices by driver minor number —
a scheme that conflicts with `CUDA_DEVICE_ORDER=PCI_BUS_ID`
([gpustack/gpustack#6041](https://github.com/gpustack/gpustack/issues/6041)). Separately, a
`Workload` never reports why a container died, so a Kubernetes deployment whose image cannot be
pulled stays silent in the UI ([gpustack/gpustack#5869](https://github.com/gpustack/gpustack/issues/5869)).

This change realigns the detect path with the operator vendor by vendor, drops the vGPU notion in
favour of whole-card reporting (MIG instances stay in the card's `appendix`), splits detection into
an information query and a usage query so callers can skip metrics, retires the physical-index
environment switch and pins `CUDA_DEVICE_ORDER=PCI_BUS_ID` on the NVIDIA workloads that can see
every card of the host, and exposes structured per-container exit status — including Kubernetes
image-pull failures backed by Pod Events.

## Motivation

### Goals

- **Audited detect-path parity.** For every one of the nine supported manufacturers, the runtime's
  detect path performs the same driver queries, version fallbacks and eligibility filters as the
  operator's, or records a deliberate, documented divergence. Target users: GPUStack worker
  operators who expect `gpustack-runtime detect` and the operator's `Devices` object to describe the
  same card identically.
- **Whole-card inventory.** Detection reports the card the driver enumerates, with no
  virtual/vGPU/PF/VF classification. MIG instances of a MIG-enabled card keep their current
  representation inside `Device.appendix["mig_devices"]`.
- **Metrics are opt-out.** A caller that only needs inventory (name, uuid, total memory, cores,
  versions, topology keys) can obtain it without paying for utilization, temperature or power
  queries — which on NVIDIA alone cost a 100 ms GPM sampling window per card.
- **Two composable queries.** Detection is split into an information query and a usage query;
  the default keeps today's behaviour by running both and merging the result.
- **Predictable device ordering.** `Device.index` is the detector's enumeration index. The
  `GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY` switch is removed; driver-physical numbering
  that device-node paths need moves into the appendix. A container that can see every card of the
  host — because it is privileged, or because it requested all devices — gets its manufacturer's
  ordering pinned by default (`CUDA_DEVICE_ORDER=PCI_BUS_ID` on NVIDIA), so the runtime, the vendor
  tooling and the workload all number the cards the same way.
- **Diagnosable workloads.** `WorkloadStatus` carries per-container exit information on Docker,
  Podman and Kubernetes; a Kubernetes workload whose image cannot be pulled reports `Failed` with
  the ErrImagePull/ImagePullBackOff reason and the corresponding Pod Event message.

Success is testable: `make test` green, with new tests covering the split API, the removal of the
vGPU classification, the appendix physical indexes, the env propagation, and the exit-status /
image-pull parsing (fixture-driven, no hardware required).

### Non-Goals

- Aligning `get_topology` / topology data between the two code bases. Explicitly out of scope.
- Changing how the operator partitions MIG or how it reports devices; the operator is the reference,
  not the subject.
- Introducing a metrics/monitor loop in the runtime equivalent to the operator's
  `Detector.Start`. The runtime exposes the queries; scheduling stays with the caller.
- Supporting Ascend vNPU (`/dev/vdavinci`) detection. It is removed with the vGPU logic.
- Changing `Device`'s existing field names or the JSON shape of already-reported fields, beyond the
  additions and removals listed below.

## Proposal

The runtime's detector becomes a faithful, worker-side twin of the operator's device manager for
everything on the detect path, with a query split that lets callers choose how much they pay for.
The deployer stops relying on driver minor numbers for ordering, and starts reporting why a
container is not running.

### User Stories

#### Story 1
As a GPUStack worker operator, I want `gpustack-runtime detect` to report the same card name, memory
size and health as the operator's device manager on the same node, so that a discrepancy between
the two is a real hardware event and not a code-path difference.

#### Story 2
As a GPUStack worker operator running MetaX cards on a virtualization-enabled host, I want the
detector to report my physical cards, so that the worker does not come up with zero devices.

#### Story 3
As a caller of the runtime library that only needs an inventory (for example to build CDI specs), I
want to ask for device information without utilization metrics, so that a detect pass does not spend
a GPM sampling window per card.

#### Story 4
As a caller that renders a live device dashboard, I want to refresh only the usage fields of devices
I already know, so that I can poll cheaply.

#### Story 5
As a GPUStack user deploying a model onto all of a node's NVIDIA cards, I want CUDA inside the
inference container to number the GPUs the way `nvidia-smi` and the runtime's detection do, so that
"GPU 2" means the same card everywhere and I do not have to configure anything to get it.

#### Story 6
As a GPUStack user deploying a model on a Kubernetes cluster with an unpullable backend image, I
want the deployment to report the pull failure and its reason, so that I understand why the model
never becomes ready instead of watching it hang.

#### Story 7
As a GPUStack user whose inference container crashed, I want the workload status to tell me the exit
code and the termination reason (`OOMKilled`, `Error`, signal), so that I can act without shelling
into the node.

### Core Features & Acceptance Criteria

#### F1 — Detect-path parity audit and fixes

Compared method-by-method against the operator (`DetectAccelerator` only; `MonitorAccelerator` is
covered by F3). Known gaps, each to be closed or documented as a deliberate divergence:

| Vendor | Gap in the runtime today | Expected |
| --- | --- | --- |
| NVIDIA | No GDDR ECC capacity restore. Operator reads `GetMemoryBusWidth` + `GetEccMode` and restores `memory * 16 / 15` when the bus is `< 1024`-bit and ECC is on. | **Withdrawn during PR review, and the divergence kept deliberately.** The restore was implemented in `T5`, then removed: the operator's corrected figure is a *display* value that takes no part in allocation, while `memory` here does, and `memory - memory_used` has to mean free space. Adding back capacity that is not reachable would over-commit every GDDR card with ECC on. `memory` is therefore what the driver reports, i.e. what the card can allocate. |
| NVIDIA | `nvmlDeviceGetMemoryInfo` called without a version preference; operator prefers V2 with a V1 fallback. | Prefer the v2 memory structure, fall back to v1, matching the operator's `GetMemoryInfoV`. The argument is the **packed struct-version constant** `pynvml.nvmlMemory_v2`, not the literal `2` — `pynvml` assigns it straight into the struct's `version` field, which the driver validates, so a literal is rejected. Probe for the constant rather than raising the dependency floor. |
| NVIDIA | `nvmlDeviceGetPciInfo` called without a version preference. | **No change, recorded as a documented divergence.** `pynvml` exposes no v2 accessor at all: `nvmlDeviceGetPciInfo` is an alias of `nvmlDeviceGetPciInfo_v3`, and `nvmlPciInfo_v2_t` is declared but unused. v3 is a superset of the operator's preferred v2, and the operator does not read the struct's string anyway — its `GetBusId()` formats the BDF from domain/bus/device, so the struct version is invisible to what either side consumes. Reaching v2 would mean hand-rolling the struct inside the detector, against this repo's "`pynvml` is a thin wrapper over the upstream package" constraint. |
| NVIDIA | Falls back to *host* memory (`get_memory()`) when the device total reads 0; the operator skips such a device. | Keep, and record it as a deliberate divergence with the reason in a code comment (WSL/iGPU tolerance). |
| Ascend | No device-type filter; the operator skips a device whose `GetType() != NPU_TYPE`. | Skip non-NPU devices, matching the operator's exact semantics: skip **only** when the type call succeeds and the type differs; a device whose type is unreadable is kept. |
| Ascend | Two divergences found while building `T6`, both deliberately left unfixed and carried to the **Deferred** table below. **(a)** The operator's `MonitorAccelerator` reads utilization through a v2→v1 fallback — `dcmi_get_device_utilization_rate_v2` fills a `MultiUtilizationInfo` in one call, else four separate `dcmi_get_device_utilization_rate` calls. `pydcmi` binds no `_v2` utilization at all and the runtime reads only the AICORE rate, so it is permanently on the operator's V1 path. **(b)** `_get_device_memory_status` tries HBM then DDR ECC unconditionally, where the operator picks the ECC device type from whichever memory query succeeded. | Both recorded, neither implemented: **(a)** is outside the four fallbacks `F1` names for Ascend and needs a `pydcmi` addition of its own; **(b)** is pre-existing behaviour and out of `T6`'s scope. |
| Ascend | `*_v2`/`*_v3` DCMI calls with no V1 fallback (vdie, PCIe, chip info, memory); the operator falls back. | Add the V1 fallbacks so older drivers still detect. **Three of the four need entry points `pydcmi` does not bind yet** — `dcmi_get_device_die`, `dcmi_get_device_pcie_info` and `dcmi_get_device_memory_info_v2`, plus the `c_dcmi_pcie_info` / `c_dcmi_memory_info` structs — so `T6` owns `pydcmi/__init__.py` to add them raw, keeping the fallback *policy* in `ascend.py` where the fake can prove it. On the memory fallback, neither struct carries `memory_available`: report `used = memory_size * utiliza / 100` from the utilization percent rather than mirroring the operator, whose `Memory_available == 0` makes `memory_size - available` report **every card as 100 % used** on that path. Verified against `npu-smi` at `C2`. |
| MetaX | **`continue`s on `MXSML_VIRTUALIZATION_MODE_PF`** — i.e. it drops the physical function, the whole card — where the operator drops `VF`. | No virtualization-mode filter at all (see F2); the physical cards are reported. |
| MThreads | Skips `MTML_VIRT_ROLE_HOST_VIRTDEVICE` when `mpcCap != MPC_TYPE_INSTANCE`; the operator skips `GUEST_VIRTDEVICE`. **Worse than a wrong filter: `mpcCap` is not a field of `c_mtmlDeviceProperty_t`** — the real names are `virtCapability, virtRole, mpcCapability, mpcType, rsvd`, and `_PrintableStructure` defines no `__getattr__`, so the read raises `AttributeError`, which `except Exception: raise` propagates and **the whole MThreads detect pass fails**. A non-virtualized card short-circuits on the `and`, which is why it has never been hit. Also note the constant compared against, `MTML_MPC_TYPE_INSTANCE`, belongs to `mpcType`, not to a capability field. | No virtRole filter at all (see F2), which removes the crash and the drop together. **Separately, and deliberately not fixed by `T9`:** `detect_info` frees the device handle in its `finally` (`mthreads.py:135`) and then uses it to open the memory context (`:142`) — a use-after-free on the MTML handle that predates this work. `T9` leaves it untouched because fixing it is a behaviour change outside its ask, and its new `detect_usage` does not reproduce it (it holds the handle open across both contexts and frees it once). **Needs its own task**, which can also carry `mtmlDeviceGetPowerUsage`'s unconverted raw value. |
| Iluvatar | Memory read without the V2→V1 fallback the operator performs. | Add the fallback. |
| Hygon / AMD | Device name resolved from HSA/ASIC only; the operator prefers a `pci.ids` lookup (`GetPCIDeviceNames`) first, then HSA `ProductName`, then the amdgpu marketing name. | The operator's full precedence, **including the libdrm step**: `pci.ids` → HSA `ProductName` → `amdgpu_get_marketing_name` → ASIC market name (AMD) / `rsmi_dev_name_get` (Hygon). The libdrm call was originally left unbound and recorded as a divergence; PR review asked for it, so `pyamdgpu` now binds it and both detectors reach for it. Verified against libdrm on the AMD host, which answers `AMD Radeon RX 7800 XT` for both cards. |
| Cambricon | Not a binding at all: a `cnmon info -e -m -u -j` shell-out parsing `cnmon_info.json`, with a `TODO(thxCode)` where the sample output should be. No driver version, no Neuware version, no cores, no power, no BDF, no NUMA, no health/ECC, no PCIe bus id. | New hand-written `pycndev` ctypes binding (following `pydcmi`/`pymxsml`) and a rewritten `cambricon.py` calling `GetDeviceCount`, `GetDeviceHandleByIndex`, `GetUUID`, `GetPCIeInfoV`, `GetCardName`, `GetMemoryInfoV`, `GetCardHealthStateV`, `GetVersionInfo`, `GetUtilizationInfo`, `GetTemperatureInfo`, `GetPowerInfo`, plus the Neuware version from `/usr/local/neuware/version.txt`. `cndev.h` in the operator's `binding/cndev` is the header of record. Four decisions taken while building `T3`, all recorded rather than assumed: **cores and the power limit stay unreported** — `pycndev` binds no core-count entry point and the operator reads neither for this vendor, so closing those two needs a binding addition of its own; **`driver_version` carries `major.minor.build`** where the operator formats only `major.minor`, since it is free precision from the same query and every other vendor here reports the driver's full string; **health comes from `cndevGetCardHealthStateV2().health` alone, with no ECC read**, because that is literally what the operator does (`memoryUnhealthy = healthInfo.Health == 0`); and **memory needs no unit conversion** — the header says MB but the operator, `cnmon` and this repo's Ascend detector all treat the value as MiB, so converting would under-report ~4.8 % against the operator on the same host, which is the very discrepancy Story 1 exists to remove. `C2` confirms the memory figure against `cnmon`. |
| Cambricon | Per-device failure policy diverges from every sibling **on purpose**: `T3` follows the operator and `continue`s past a card whose required reads fail, where the other eight detectors let the error propagate. | A single faulty card therefore costs one device on Cambricon and **all** devices on the other eight. Graceful degradation is very likely the right behaviour everywhere and the other eight should move toward it, but that is a repo-wide behaviour change deserving its own task — not something to slip in per vendor. |
| THead | Enumerates MIG-style GPU/compute instances, which the operator does not. | Keep — this is the appendix mechanism F2 preserves. |
| **All vendors** | `GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK` **defaults to true**, so a default run never reads an ECC counter and every `memory_status` comes back `healthy`. The operator reads the uncorrected-ECC counter unconditionally. | Keep the default (the check costs a driver call per card per pass), but record it as a deliberate divergence: on the same host the runtime can report `healthy` where the operator reports `Unhealthy`, which is a code-path difference of exactly the kind `Story 1` exists to eliminate. `C2` must therefore compare health with the flag switched **off**, or it compares nothing. Found while building `T5`. |

Acceptance criteria:
- ~~A written parity table lives in the repo (module docstring or `docs/`) listing, per vendor, the
  driver calls the detect path makes and any deliberate divergence from the operator, with its
  reason.~~ **Withdrawn after the build, by request:** `T11` wrote `docs/detector-parity.md` and it
  was published, then dropped as not belonging in the shipped documentation. Nothing it recorded is
  lost — the deferred items it carried are listed under **Deferred** below, and every divergence it
  explained is stated in a comment at the code that makes it.
- Every gap above is either fixed or annotated as deliberate; no gap is left silently open.
- Sample/fixture-driven tests assert that NVIDIA memory is reported as the driver reports it, the Ascend non-NPU skip, and
  the MetaX card no longer being dropped.

#### F2 — Whole-card reporting, no vGPU classification

- `Device.appendix` no longer carries a `vgpu` key, for any vendor.
- All virtual-card detection is removed: NVIDIA's `_is_vgpu` PCI-capability sniff, the Ascend vNPU
  (`dcmi` vdev query) branch, the SR-IOV physical-function comparisons in AMD / Hygon / Iluvatar /
  THead, MetaX's virtualization-mode check, MThreads' `virtRole` check.
- MIG instances keep the existing mechanism unchanged: the physical card is the reported device, its
  instances live in `appendix["mig_devices"]`, numbered by `index_mig_devices`, and
  `expand_mig_devices` still substitutes them for callers that address instances.
- `deployer/cdi/ascend.py` always uses `/dev/davinci{index}`; the `/dev/vdavinci` branch is removed.
- No detector filters devices by virtualization mode or role; whatever the driver enumerates is
  reported.

#### F3 — Split information and usage queries

- `Detector` grows two methods and keeps `detect` as the composing entry point:
  - `detect_info() -> Devices | None` — abstract; identity, capability and inventory fields only.
  - `detect_usage(devices: Devices | None = None) -> Devices | None` — utilization, used memory,
    memory utilization, temperature, used power, plus a refreshed `memory_status`. With `devices`
    given it merges into them (matched by `uuid`, MIG entries included); with `None` it runs
    `detect_info()` first.
  - `detect(usage: bool = True) -> Devices | None` — `detect_info()`, then `detect_usage()` merged in
    when `usage` is true. Default behaviour is byte-for-byte what `detect()` returns today.
- Field split: **information** = `manufacturer, index, name, uuid, driver_version, runtime_version,
  runtime_version_original, compute_capability, cores, memory, power, appendix`; **usage** =
  `cores_utilization, memory_used, memory_utilization, temperature, power_used`, and
  `memory_status` is produced by both (mirroring the operator, which reports `Unhealthy` from both
  `DetectAccelerator` and `MonitorAccelerator`).
- Module level: `detect_devices(fast=True, manufacturer=None, usage=True)`; the new keyword threads
  through to every detector.
- `gpustack-runtime detect` grows `--no-usage`; the table format renders `N/A` for the omitted
  columns instead of `0`.
- With `usage=False`, no utilization/temperature/power driver call is made — asserted per vendor by
  monkeypatched-binding tests that fail if such a call happens.

#### F4 — Retire the physical-index switch, propagate `CUDA_DEVICE_ORDER`

- `GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY` is removed from `envs.py` and from all three
  call sites (NVIDIA, Ascend, Iluvatar). `Device.index` is the detector's enumeration index.
- Driver-physical numbering that a device-node path or a vendor tool needs is exposed in the
  appendix instead — mirroring the operator, which keeps a sequential `Index` next to
  `PhysicalIndexes`. AMD's `card_id`/`renderd_id` and Ascend's `card_id`/`device_id` already do this;
  NVIDIA, Iluvatar and THead gain their minor number the same way — but **only NVIDIA's and
  Iluvatar's name a device node.** THead's `/dev/alixpu_ppu{N}` is named after the card ordinal, i.e.
  the enumeration index; the operator's own comment at its `GetMinorNumber` call site says so, and it
  keeps the number purely to *prove* a node addresses the card it describes, by comparing it against
  the node's character-device minor. So THead records `minor_number` in the appendix and its CDI path
  keeps reading `Device.index`.
- Every consumer of `dev.index` that means "device node number" (`deployer/cdi/*`) is reviewed
  against the appendix values, so no `/dev/...` path changes meaning. Two of them actually read the
  retired numbering, because the switch defaults to **on** and therefore describes today's real
  behaviour: `cdi/ascend.py`'s `/dev/davinci{...}` (physical id from the DCMI logic id) and
  `cdi/iluvatar.py`'s `/dev/iluvatar{...}` (NVML minor number). Both move to the appendix key in the
  same change that retires the switch, so no host ever sees a window where a workload is handed
  another card's device node. `cdi/amd.py`, `cdi/hygon.py` and `cdi/metax.py` already build their
  paths from `appendix["card_id"]` / `["renderd_id"]`, `cdi/thead.py` was never on the switch, and
  NVIDIA has no CDI generator in this repository.
- A container that ends up seeing **every** accelerator of the host gets its manufacturer's device
  ordering pinned by default, on Docker, Podman and Kubernetes alike. "Every accelerator" is exactly
  the condition the deployers already resolve per device request: the container is `privileged`, or
  its device request is `all`. On NVIDIA that means `CUDA_DEVICE_ORDER=PCI_BUS_ID`; no other
  manufacturer documents an ordering switch, so no other variable is injected.
  - It is a **default**, not an override: a container that declares `CUDA_DEVICE_ORDER` itself keeps
    its own value.
  - A container that was given a specific subset is left alone — it does not see the cards it was
    not given, and the deployer already scopes its visible-devices env. The criterion is the subset,
    not who allocated it: a device plugin handing out *every* card of the node (an `all` request
    under the KDP injection policy) still leaves the container seeing everything, so the ordering is
    pinned there too.
  - The worker's own environment is irrelevant to this decision. The ordering is a property of what
    the deployer hands the container, so the deployer decides it.
  - The operator pins the same variable, by the same never-overwrite rule, at a **complementary**
    point: its NVIDIA device plugin sets it on *sliced* containers, because HAMi-core fills its
    `CUDA_DEVICE_MEMORY_LIMIT_<i>` table in NVML enumeration order but reads a limit back by CUDA
    ordinal, and the two coincide only under `PCI_BUS_ID`. So on an operator-managed cluster a sliced
    workload gets it from the plugin and a privileged / all-devices workload gets it from here. Same
    name, same value, both set-if-absent — the two injections cannot disagree.
- `Device.index`'s docstring loses the physical-index paragraph.
- **A skipped device does not renumber its survivors.** The operator compacts its `Index`, so its
  second card becomes `Index 0` when the first is skipped. The runtime keeps the driver's enumeration
  index, because compacting would make the index *unstable across passes*: a transiently faulty card
  would shift every later card's index down and back again on recovery, and `DevicesMaterial`'s
  `runtime_values` / `backend_values` / `numa_affinities` are all keyed by `str(dev.index)`. A
  non-contiguous index is a static fact a caller can hold; a shifting one silently repoints a cached
  index at a different card. Decided while building `T3`, the only detector that skips a device.

#### F5 — Workload exit status and Kubernetes image-pull failures

- `WorkloadStatus` gains a per-container exit list — one entry per container that has terminated or
  is blocked from starting — carrying: container name, operation token, exit code, reason, message,
  started/finished timestamps, and restart count. Absent containers are simply not listed.
- Docker and Podman populate it from each container's `State` (`ExitCode`, `Error`, `OOMKilled`,
  `StartedAt`, `FinishedAt`, `RestartCount`), including init containers.
- Kubernetes populates it from `container_statuses` / `init_container_statuses`, reading
  `last_state.terminated` and `state.terminated` for exit codes and `state.waiting` for
  reason/message.
- A Kubernetes workload whose image cannot be pulled (`ErrImageNeverPull`, `ErrImagePull`,
  `ImagePullBackOff`, `InvalidImageName`, `RegistryUnavailable`) reports `state = Failed` — not
  `Pending` — with the reason and message in `state_message`, and the matching Pod `Event` message
  appended. `ErrImageNeverPull` was added by the end-of-build review: it is the one genuinely
  unrecoverable reason of the five (`imagePullPolicy: Never` with the image absent), and it was
  missing while recoverable reasons were present.
- The appended Event is selected by `involvedObject.uid` as well as name, chosen by timestamp rather
  than list order — the API guarantees none — and prefers the kubelet's `Failed` Event, so a stale
  `FailedScheduling` cannot stand in as the diagnosis.
- Pod Events are read only when the Pod is in such a blocked state, and an Events call denied by
  RBAC degrades gracefully (state and reason still reported, Event detail omitted, one debug log).
- `deploy/manifests/kubernetes.yaml` grants `get`/`list` on core `events`.
- Fixture-driven tests cover: Docker exited-nonzero, Docker OOMKilled, K8s terminated exit code,
  K8s `ImagePullBackOff` → `Failed` with reason, and an Events call raising `403`.

### Notes / Constraints / Caveats

- Python ≥ the project's floor; `uv` for dependency management; `ruff` + `pre-commit` for lint.
- Vendor bindings are hand-written `ctypes` modules under `gpustack_runtime/detector/py*` (see
  `pydcmi`, 1.2k lines; `pymxsml`, 1.6k lines) except `pynvml`/`pymtml`, which are thin wrappers over
  upstream PyPI packages. `pycndev` follows the hand-written pattern.
- The operator is the reference implementation and is not modified by this work.
- All new fields are `dataclass_json` dataclass fields, defaulted so existing serialized payloads
  keep deserializing.
- Hardware-dependent tests stay `pytest.mark.skipif(not <Detector>.is_supported())`, as today; new
  behaviour is covered by fixtures and monkeypatched bindings so CI without GPUs still exercises it.

### Boundaries

- **Always:** keep MIG instances in `appendix["mig_devices"]` with the current numbering; keep
  `detect()`'s default output identical to today's; keep the topology **data** untouched — a
  `get_topology` body may be repointed at `detect_info()` so topology stops paying for metrics it
  never reads, but the `Topology` it returns must not change; keep `Device`'s existing field names.
- **Ask first:** before changing what a `/dev/...` device-node path resolves to for any vendor;
  before adding an RBAC verb beyond core `events`; before touching the operator repository.
- **Never:** align `get_topology`'s data or semantics with the operator's, or change what it returns;
  add a monitor/scheduling loop to the runtime; override a
  `CUDA_DEVICE_ORDER` the container already declares; inject an ordering variable into a container
  that was given a device subset.

### Risks and Mitigations

- Ascend `/dev/davinci{N}` numbering may follow the driver's physical id, not the logic id → keep
  the physical id in the appendix and have the CDI generator read it from there; confirm against a
  real 910B/910C node before merging.
- Retiring the physical-index switch shifts the *keys* other consumers derive from `Device.index`:
  `DevicesMaterial.runtime_values` / `backend_values` / `numa_affinities` are keyed by
  `str(dev.index)` (`deployer/__types__.py:1414-1427`), and every CDI generator emits its devices as
  `ConfigDevice(name=str(dev.index))`. On NVIDIA and Iluvatar those keys go from the minor number to
  the enumeration index, and on Ascend from the physical id to the logic id → harmless within one
  process, because a single detect pass feeds both sides, and it is the direction issue #6041 asks
  for; but a caller that persisted indexes across the upgrade sees them move, so it belongs in the
  release note next to the ordering change.
- Removing the Ascend vNPU branch drops detection on hosts that pre-split NPUs → documented as a
  Non-Goal and a release note; partitioning is the operator's device manager's job.
- Removing MetaX's `PF` skip and MThreads' `virtRole` skip changes reported device counts on
  virtualization-enabled hosts → sample-driven tests pin the expected inventory for both.
- A new `pycndev` binding cannot be validated without Cambricon hardware → derive it from the
  operator's `cndev.h`, unit-test struct layouts and parsing against captured samples, and keep the
  hardware path behind `is_supported()`.
- The `usage=False` path could silently keep issuing metric calls → tests assert on the binding
  functions actually invoked, not just on the returned values.
- Reading Pod Events adds API traffic per status poll → only for blocked Pods, only field-selected
  to the Pod, and skipped entirely once the workload runs.
- Pinning `CUDA_DEVICE_ORDER=PCI_BUS_ID` changes the CUDA ordinals inside a container that previously
  ran under the `FASTEST_FIRST` default, so a workload addressing cards by CUDA ordinal sees a
  different card for the same number on a heterogeneous host → that is the defect being fixed (the
  ordinals now agree with `nvidia-smi` and with the runtime's own detection), but it is a visible
  behaviour change on such hosts, so it belongs in the release note; a container that needs the old
  behaviour declares `CUDA_DEVICE_ORDER=FASTEST_FIRST` itself and keeps it.
- `T1` mechanically renames `detect` → `detect_info` in all nine vendor files, so it conflicts with any
  in-flight branch touching a detector → land it first and alone, before the per-vendor tasks start.
- `samples/` is documentation-only — no test reads it, and it has already drifted from the code
  (`detect_output_nvidia_h100_mig.json` still uses the retired standalone-MIG shape with
  `gpu_instance_index` / `compute_instance_index`) → `T12` realigns every sample and adds a
  schema-guard test, so a future drift fails the suite instead of rotting silently.
- `pytest.ini` lists an `integration_tests` testpath that does not exist → create the directory (with
  the integration scenarios below) or drop the entry, before adding integration tests.
- No coverage measurement exists today (`addopts` carries no `--cov`), so the Test Plan's per-package
  numbers are targets, not deltas → introduce `pytest-cov` as an optional prerequisite, or accept
  hand-review of the new tests' reach.
- Real-hardware validation cannot run during the build (macOS dev machine, no vendor drivers) → every
  task verifies through fixtures/monkeypatched bindings, and `C2` gates the merge on the user-provided
  environments.

## Design Details

### Commands

**Environment.** Implementation and the whole automated suite run **locally** (macOS dev machine, `uv`).
No vendor driver exists there, so every detector's hardware path is skipped by `is_supported()` and all
new behaviour must be provable through fixtures and monkeypatched bindings — that constraint shapes the
Test Plan. Real-hardware validation is checkpoint `C2`, on the environments below.

**`C2` environments** (supplied 2026-08-15). The checkout lives at `~/github.com/gpustack/runtime` on
each target and its git tree must be brought in line with the branch under test.

| Vendor | Access |
| --- | --- |
| THead (ppu) | `root@192.168.100.151`, jumping through `frank@192.168.50.17` |
| AMD | `frank@192.168.50.17` |
| NVIDIA | `frank@192.168.50.13` and `frank@192.168.50.16` |
| Ascend | `root@120.241.57.28` |

Two ways to get the code running there, per target:

- **From source**, where the target has Python: `make install`, then run the CLI from the checkout's
  `.venv`.
- **From a container image**, built on `frank@192.168.50.17`:
  `PACKAGE_NAMESPACE=thxcode PACKAGE_TAG=dev-<hash> make package`, pushing `thxcode/runtime:dev-<hash>`
  to Docker Hub, then running that image on the target.

```bash
# On each target, per pass
gpustack-runtime detect --format json
gpustack-runtime detect --no-usage --format json
gpustack-runtime topology --format json
# Health only means anything with the ECC read switched on -- see F1's all-vendors row
GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK=false gpustack-runtime detect --format json
```

**One question only the AMD/Hygon host can answer.** Both build their UUID as
`f"GPU-{rsmi_dev_unique_id_get(idx)[2:]}"`, and that wrapper returns `hex(value)`, so a driver
answering `0` yields `"GPU-0"` for *every* card — a truthy but non-unique identity, which is the key
the usage merge joins on. The merge now drops an ambiguous id rather than broadcasting one card's
metrics onto the rest, so the harm is contained either way, but whether the ids are actually distinct
is a measurement, not a guess: compare the `uuid` values `detect --format json` reports on the AMD
host. If they collide, the identity needs a distinct fallback (the BDF is read on the same pass) and
that becomes its own task.

**These four cover four of the nine vendors**, plus Hygon by proxy (below). Iluvatar, MetaX, MThreads
and Cambricon have no environment, so their detect paths stay proven only by fixtures — and in
particular the MetaX temperature Open Question and Cambricon's "the driver says MB but means MiB"
reading **cannot be settled by `C2` as scoped**. Both must keep their markers until hardware appears.

**Hygon's logic is exercised on the AMD host, by proxy.** A Hygon DCU *is* a ROCm device, and
`hygon.py` runs on the same binding stack AMD does — `pyrocmsmi` + `pyhsa`, with `pyamdgpu` for the
cores fallback. The only vendor-specific gate is one PCI vendor id: `hygon.py`'s
`get_pci_devices(vendor="0x1d94")` against `amd.py`'s `"0x1002"`. Temporarily pointing Hygon's at
`0x1002` on the AMD host makes `HygonDetector.is_supported()` pass and runs every line of its
`detect_info` / `detect_usage` against a real driver.

- **What that proves:** the Hygon path executes without raising on real hardware; the `pyrocmsmi` /
  `pyhsa` field names it reads exist; its fallback chains resolve; the usage merge and the
  `memory_status` recomputation work; and its CDI generator emits paths.
- **What it does not prove:** any Hygon-specific *value*. The names, ECC counters, memory and core
  counts will be the AMD card's, so nothing about a K100AI's own reporting is settled by it.
- The edit is a throwaway on the target, recorded in the evidence and reverted; it must never be
  committed. **The trick does not generalise** — Iluvatar, MetaX, MThreads and Cambricon each load
  their own vendor library, which is absent on these hosts, so there is nothing for a vendor-id swap
  to reach.

```bash
make prepare              # REQUIRED FIRST: writes the gitignored gpustack_runtime/_version_appendix.py.
                          # Without it any `uv run` fails at the editable build with
                          # "Forced include not found: .../gpustack_runtime/_version_appendix.py".
make deps                 # uv sync --all-packages && uv lock && uv tree
make lint                 # uv run pre-commit run --all-files --show-diff-on-failure
                          # (ruff-check --fix --unsafe-fixes, ruff-format, codespell — it rewrites files)
make test                 # uv run pytest   (pytest.ini: pythonpath=., addopts=--no-header -vvv)
make docs                 # mkdocs build

# Task-scoped verification
uv run pytest tests/gpustack_runtime/detector -q
uv run pytest tests/gpustack_runtime/deployer -q
uv run pytest tests/gpustack_runtime/detector/test_samples.py -q

# Manual smoke on the dev machine (no devices -> empty list, must not traceback)
uv run gpustack-runtime detect --format json
uv run gpustack-runtime detect --no-usage --format json
```

### Project Structure

```
gpustack_runtime/
├── detector/
│   ├── __init__.py          # detect_devices/detect_backend/expand_mig_devices facade
│   ├── __types__.py         # Device, Devices, Topology, Detector ABC, index_mig_devices
│   ├── __utils__.py         # PCI/NUMA/version/unit helpers
│   ├── <vendor>.py          # amd, ascend, cambricon, hygon, iluvatar, metax, mthreads, nvidia, thead
│   └── py<sdk>/             # bindings. Hand-written ctypes: pyamdgpu, pyamdsmi, pydcmi, pyhgml,
│                            #   pyhsa, pyixml, pymxsml, pyrocmsmi (+ NEW pycndev).
│                            #   Thin wrappers over upstream PyPI: pynvml, pymtml.
├── deployer/
│   ├── __types__.py         # Container/Workload{Plan,Status}, Deployer ABC, DevicesMaterial
│   ├── docker.py            # DockerWorkloadStatus.parse_state
│   ├── podman.py
│   ├── kuberentes.py        # KubernetesWorkloadStatus.parse_state, Pod/Event reads
│   └── cdi/<vendor>.py      # CDI spec generation, consumes Device.index/appendix
├── cmds/detector.py         # `detect` / `topology` sub-commands
└── envs.py                  # GPUSTACK_RUNTIME_* env surface
deploy/manifests/kubernetes.yaml   # RBAC for the deployer
tests/gpustack_runtime/
├── detector/
│   ├── fixtures/__init__.py  # load() helper only — no data files yet
│   ├── samples/              # detect_output_*.json / topology_output_*.json captured from real
│   │                         #   hardware. Documentation-only today: NO test reads them, so they
│   │                         #   have drifted (detect_output_nvidia_h100_mig.json still uses the
│   │                         #   retired standalone-MIG shape). T12 realigns them and adds a guard.
│   └── test_<vendor>.py      # hardware-gated smoke tests (skipif not is_supported())
└── deployer/
    ├── fixtures/
    └── test_workload_status.py, test_privileged.py, test_runtime_class.py, test_utils.py
```

### Code Style

The query split, as the ABC composes it (`detector/__types__.py`):

```python
class Detector(ABC):
    @abstractmethod
    def detect_info(self) -> Devices | None:
        """
        Detect devices' inventory, without usage metrics.
        """
        raise NotImplementedError

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
        return devices

    def detect(self, usage: bool = True) -> Devices | None:
        devices = self.detect_info()
        if usage and devices:
            self.detect_usage(devices)
        return devices
```

A vendor's information query, showing the operator-mirroring comment convention:

```python
class NVIDIADetector(Detector):
    def detect_info(self) -> Devices | None:
        """
        Detect NVIDIA GPUs' inventory using pynvml, without usage metrics.

        Returns:
            A list of detected NVIDIA GPU devices,
            or None if not supported.

        Raises:
            If there is an error during detection.

        """
        if not self.is_supported():
            return None

        ret: Devices = []
        try:
            pynvml.nvmlInit()
            ...
            dev_mem = 0
            with contextlib.suppress(pynvml.NVMLError):
                dev_mem_info = pynvml.nvmlDeviceGetMemoryInfo(
                    dev,
                    version=pynvml.nvmlMemory_v2,  # packed struct version, not a literal 2
                )
                # What the driver reports, i.e. what the card can allocate. The
                # operator restores the ~1/16 that ECC parity carves out of a GDDR
                # part, but that figure is display-only there, while this one takes
                # part in allocation -- see F1's first row.
                dev_mem = byte_to_mebibyte(dev_mem_info.total)
        except pynvml.NVMLError:
            debug_log_exception(logger, "Failed to fetch devices")
            raise
        return ret
```

Conventions: `from __future__ import annotations as __future_annotations__` first; `@dataclass_json
@dataclass` models with per-field docstrings; `contextlib.suppress(<Vendor>Error)` around optional
driver calls; `debug_log_exception` / `debug_log_warning` for diagnostics; Google-style docstrings
with `Args:` / `Returns:` / `Raises:`; comments explain *why*, referencing the operator when the
behaviour is a deliberate mirror.

### Implementation Plan

Two independent tracks — the detector track (T*) and the deployer track (D*) share no file and start in
parallel — joined by two checkpoints. The detector track is sequenced **expand → migrate → contract**,
because splitting `detect` is one mechanical change whose blast radius covers all nine vendor files: no
vertical slice can land green while half the vendors still override `detect()` and the other half
implement `detect_info()`.

#### Detector track

- [x] **T1 · Prefactor: split the Detector query surface (expand), retire the physical-index switch**
      Blocked by: None
      Owns: `gpustack_runtime/detector/__types__.py`, `gpustack_runtime/detector/__init__.py`,
      `gpustack_runtime/detector/__utils__.py`, `gpustack_runtime/detector/*.py`,
      `gpustack_runtime/envs.py`, `gpustack_runtime/deployer/cdi/ascend.py`,
      `gpustack_runtime/deployer/cdi/iluvatar.py`,
      `tests/gpustack_runtime/detector/test_detector_types.py`,
      `tests/gpustack_runtime/detector/test_mig_devices.py`
      Gate: review
      Scope: add `detect_info()` / `detect_usage(devices=None)` / `detect(usage=True)` to the `Detector`
      ABC as shown in Code Style; rename every vendor's `detect()` to `detect_info()` with no body
      change, so `detect()` output is unchanged and `usage=False` is an accepted no-op until each
      vendor migrates; thread `usage=` through `detect_devices()` and call it with `usage=False` from
      `get_devices_topologies()`, and point the nine vendors' own `get_topology()` at `detect_info()`
      so topology stops paying for metrics once T3–T10 land (identical body during expand, so no
      behaviour change); add a module-level `merge_devices_usage(devices, usages)` to `__types__.py`
      joining by `uuid` across cards **and** `appendix["mig_devices"]` entries, mirroring the
      operator's `MonitorAccelerator`, which returns a separate UUID-keyed metrics list that consumers
      join by identity, never by index — T3–T10 call it from their `detect_usage`; delete
      `GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY` from `envs.py` (both the `TYPE_CHECKING` stub
      and the `variables` registry entry that actually resolves it) and its three call sites (NVIDIA,
      Ascend, Iluvatar), moving the driver-physical number into the appendix (`minor_number` for
      NVIDIA/Iluvatar — THead's belongs to T10, whose index the switch never touched; Ascend adds
      `physical_id` beside its existing `card_id`/`device_id`); **keep the two `/dev/...` paths that
      read the retired numbering byte-for-byte** — `cdi/ascend.py`'s `/dev/davinci{...}` and
      `cdi/iluvatar.py`'s `/dev/iluvatar{...}` become one-line reads of the appendix key, since the
      switch defaults to **on** and those paths are physical numbering today (T6/T7 then only re-pick
      which key); add a `get_pci_device_name()` pci.ids lookup helper to `__utils__.py` for T4; drop
      the physical-index paragraph from `Device.index`'s docstring, wording it as the index the
      detector enumerates the device at, without claiming contiguity (Ascend reports the DCMI logic
      id, and MIG numbering punches holes).
      Acceptance: `detect()`'s payload is unchanged **by construction** — no vendor overrides
      `detect()`, so it is `detect_info()`'s payload plus a no-op merge; asserted structurally (every
      vendor implements `detect_info`, none overrides `detect`, the composition preserves the info
      fields) rather than against nine golden fixtures, which would need the `fixtures/bindings.py`
      fakes the Test Plan lists as a prerequisite and T3–T10 build. "Unchanged" means every
      pre-existing field and appendix key keeps its value, the appendix *gains* the physical-number
      key, and an index moves only where F4 intends it to. Plus: `detect(usage=False)` is accepted by
      every detector; the env var appears nowhere in the tree; `Device.index` is the enumeration index
      and the physical number is reachable from the appendix; `/dev/davinci{N}` and `/dev/iluvatar{N}`
      resolve to the same nodes as before the change; `get_pci_device_name()` resolves a known
      vendor:device pair, prefers a subsystem name over the device name, and returns `""` on a miss.
      Verify: `make prepare && uv run pytest tests/gpustack_runtime/detector -q`

- [x] **T2 · PoC: `pycndev` ctypes binding**
      Blocked by: None
      Owns: `gpustack_runtime/detector/pycndev/**`, `tests/gpustack_runtime/detector/test_pycndev.py`,
      `ruff.toml`
      Gate: review
      Scope: hand-written ctypes binding following `pydcmi` / `pymxsml`, derived from the operator's
      `binding/cndev/cndev.h` (the header of record): library loader, init/release, `GetDeviceCount`,
      handle-by-index, `GetUUID`, `GetPCIeInfoV` (V2), `GetCardName`, `GetMemoryInfoV` (V2),
      `GetCardHealthStateV` (V2), `GetVersionInfo`, `GetUtilizationInfo`, `GetTemperatureInfo`,
      `GetPowerInfo`.
      Acceptance: the module imports cleanly with no Cambricon library present and makes no call at
      import time; every struct's `ctypes.sizeof` and field offsets match `cndev.h`; a missing library
      surfaces as the binding's own error type, not a bare `OSError`.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_pycndev.py -q`

- [x] **T3 · Cambricon detector on `pycndev`**
      Blocked by: T1, T2
      Owns: `gpustack_runtime/detector/cambricon.py`,
      `tests/gpustack_runtime/detector/test_cambricon.py`
      Gate: review
      Scope: replace the `cnmon info -e -m -u -j` shell-out (and its `TODO(thxCode)` placeholder) with
      `pycndev`. `detect_info`: uuid, card name, total memory, driver version, Neuware version from
      `/usr/local/neuware/version.txt` (`\d+\.\d+\.\d+`, as the operator reads it), PCIe bus id, NUMA
      node, health from `GetCardHealthStateV`. `detect_usage`: core utilization, used memory, memory
      utilization, temperature, power. Drop `appendix["vgpu"]`.
      Acceptance: with a monkeypatched `pycndev`, `detect_info()` returns a Device carrying name, uuid,
      memory, driver_version, runtime_version, bdf, numa and a real `memory_status`, and issues no
      utilization/temperature/power call; `detect()` adds the usage fields; `cnmon` is invoked nowhere
      in the tree.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_cambricon.py -q`

- [x] **T4 · AMD + Hygon: pci.ids name precedence, de-vGPU, usage split**
      Blocked by: T1
      Owns: `gpustack_runtime/detector/amd.py`, `gpustack_runtime/detector/hygon.py`,
      `gpustack_runtime/deployer/cdi/amd.py`, `gpustack_runtime/deployer/cdi/hygon.py`,
      `tests/gpustack_runtime/detector/test_amd.py`, `tests/gpustack_runtime/detector/test_hygon.py`
      Scope: adopt the operator's name precedence — pci.ids (`get_pci_device_name` from T1) → HSA
      product name → amdgpu marketing name → ASIC market name; delete `appendix["vgpu"]` and the
      `get_physical_function_by_bdf` comparison; move `amdsmi_get_gpu_metrics_info`, used power,
      `rsmi_dev_busy_percent_get`, `rsmi_dev_temp_metric_get` and used memory into `detect_usage`,
      keeping the power *limit* in `detect_info`; confirm the CDI generators read
      `appendix["card_id"]` / `["renderd_id"]`, not `Device.index`.
      Acceptance: pci.ids hit wins over the HSA name; with pci.ids absent the previous name is
      returned; no appendix carries `vgpu`; `detect_info()` makes no utilization/temperature/used-power
      call; the CDI spec for both vendors is byte-identical to the pre-change output for the same
      fixture.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_amd.py tests/gpustack_runtime/detector/test_hygon.py -q`

- [x] **T5 · NVIDIA: memory version preference, de-vGPU, usage split**
      Blocked by: T1
      Owns: `gpustack_runtime/detector/nvidia.py`, `tests/gpustack_runtime/detector/test_nvidia.py`,
      `tests/gpustack_runtime/detector/test_mig_devices.py`
      Gate: review
      Scope: prefer the v2 memory structure with a v1 fallback; delete `_is_vgpu` and
      `appendix["vgpu"]`; keep the MIG appendix mechanism, moving the MIG entries' usage fields
      (`cores_utilization`, `memory_used`, `memory_utilization`, `temperature`, `power_used`) into
      `detect_usage`; keep the host-memory fallback for a zero total and record in a comment that it
      is a deliberate divergence from the operator (WSL / iGPU tolerance); record
      `appendix["minor_number"]`.
      **The GDDR ECC capacity restore this task originally carried was reverted during PR review**
      — see F1's first row — so `_restore_ecc_reserved_memory()` and its bus-width/ECC-mode
      acceptance cases exist nowhere in the tree; `memory` is what the driver reports.
      Acceptance: monkeypatched NVML — the v2 memory structure is preferred and a binding without it
      falls back to v1; a GDDR card with ECC on reports the driver's total unrestored; `detect_info()`
      issues no GPM/utilization/temperature/power-usage call; `vgpu` absent from every appendix; a
      MIG-enabled card still carries `appendix["mig_devices"]` with its instances numbered by
      `index_mig_devices`.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_nvidia.py tests/gpustack_runtime/detector/test_mig_devices.py -q`

- [x] **T6 · Ascend: NPU type filter, V1 fallbacks, drop the vNPU branch, usage split**
      Blocked by: T1
      Owns: `gpustack_runtime/detector/ascend.py`, `gpustack_runtime/detector/pydcmi/__init__.py`,
      `gpustack_runtime/deployer/cdi/ascend.py`,
      `tests/gpustack_runtime/detector/test_ascend.py`
      Gate: review
      Scope: skip a device whose DCMI type is not NPU, as the operator does; add V1 fallbacks for vdie,
      PCIe, chip-info and memory so older drivers still detect; delete the vdev (vNPU) branch and
      `appendix["vgpu"]`; make the CDI generator always emit `/dev/davinci{...}` from the appendix
      value that matches the driver's device-node numbering — T1 already wired it to `physical_id` to
      preserve today's paths, so this is a one-line re-pick if `C2` shows the logic id is the right
      key (see Open Questions); move utilization, temperature and power into `detect_usage`.
      Acceptance: a non-NPU device in the card is not reported; a driver exposing only the V1 calls
      still yields a Device; no `/dev/vdavinci` path is produced anywhere; `detect_info()` makes no
      utilization/temperature/power call.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_ascend.py -q`

- [x] **T7 · Iluvatar: memory V2→V1 fallback, de-vGPU, usage split**
      Blocked by: T1
      Owns: `gpustack_runtime/detector/iluvatar.py`, `gpustack_runtime/deployer/cdi/iluvatar.py`,
      `tests/gpustack_runtime/detector/test_iluvatar.py`
      Scope: add the memory V2→V1 fallback the operator performs; delete `appendix["vgpu"]` and the
      `get_physical_function_by_bdf` comparison; move utilization, temperature and used power into
      `detect_usage`; confirm the CDI `/dev/iluvatar{N}` path still reads the appendix minor number
      T1 wired it to.
      Acceptance: v2 unavailable → v1 result returned; no appendix carries `vgpu`; `detect_info()`
      makes no utilization/temperature/used-power call; the CDI spec is unchanged for the same fixture.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_iluvatar.py -q`

- [x] **T8 · MetaX: remove the inverted PF skip, de-vGPU, usage split**
      Blocked by: T1
      Owns: `gpustack_runtime/detector/metax.py`, `gpustack_runtime/deployer/cdi/metax.py`,
      `tests/gpustack_runtime/detector/test_metax.py`
      Gate: review
      Scope: delete the `continue` on `MXSML_VIRTUALIZATION_MODE_PF` — it currently drops the physical
      function, i.e. the whole card, where the operator drops `VF` — and apply no virtualization-mode
      filter at all; delete `appendix["vgpu"]`; move core utilization, used memory, temperature and
      board power into `detect_usage`.
      Acceptance: a fixture reporting `mode == PF` yields that card as a Device (it is dropped today);
      a fixture reporting `mode == VF` also yields a Device; `detect_info()` makes no
      utilization/temperature/power call.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_metax.py -q`

- [x] **T9 · MThreads: remove the virtRole skip, de-vGPU, usage split**
      Blocked by: T1
      Owns: `gpustack_runtime/detector/mthreads.py`, `tests/gpustack_runtime/detector/test_mthreads.py`
      Gate: review
      Scope: delete the `MTML_VIRT_ROLE_HOST_VIRTDEVICE` / `mpcCap` skip and `appendix["vgpu"]`, and
      apply no virtRole filter; move GPU utilization, temperature, used memory and used power into
      `detect_usage`.
      Acceptance: a fixture reporting a host virt device with `mpcCap != MPC_TYPE_INSTANCE` is now
      reported (it is dropped today); `detect_info()` makes no utilization/temperature/power call.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_mthreads.py -q`

- [x] **T10 · THead: de-vGPU, usage split (MIG appendix preserved)**
      Blocked by: T1
      Owns: `gpustack_runtime/detector/thead.py`, `gpustack_runtime/deployer/cdi/thead.py`,
      `tests/gpustack_runtime/detector/test_thead.py`
      Scope: delete `appendix["vgpu"]` (both the card's and the instance entries') and the
      `get_physical_function_by_bdf` comparison; keep the GPU/compute-instance enumeration in the
      appendix — the operator has no THead equivalent and this is the mechanism F2 preserves; move
      utilization, temperature and used power into `detect_usage`, instance entries included; record
      `appendix["minor_number"]` where the binding exposes it. Creates `test_thead.py`, which does not
      exist today.
      Acceptance: no appendix carries `vgpu`; instance entries survive with `sliced` intact (the
      topology path keys off it); `detect_info()` makes no GPM/utilization/temperature/used-power call.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_thead.py -q`

- [x] **T11 · Contract: make the split mandatory, `detect --no-usage`, ~~parity table~~**
      Blocked by: T3, T4, T5, T6, T7, T8, T9, T10
      Owns: `gpustack_runtime/detector/__types__.py`, `gpustack_runtime/detector/__utils__.py`,
      `gpustack_runtime/cmds/detector.py`, ~~`docs/**`, `mkdocs.yml`~~,
      `tests/gpustack_runtime/detector/test_detector_cli.py`,
      `tests/gpustack_runtime/detector/test_detector_types.py`
      Gate: review
      Scope: mark **both** `detect_info` and `detect_usage` `@abstractmethod`, so a future vendor
      cannot silently skip the split — `raise NotImplementedError` would only fail when called, which
      is exactly the silence being removed, and a vendor with genuinely no usage query is better off
      declaring a two-line stub than inheriting a no-op that makes `detect()` look like it measured
      something. All nine already implement both, so the cost is zero. Also migrate the two
      expand-state fixtures in `test_detector_types.py` (`_UnimplementedDetector`, `_InfoOnlyDetector`)
      that exist precisely to pin the un-contracted state; delete `get_physical_function_by_bdf` if no caller
      remains; add `detect --no-usage`, rendering `N/A` instead of `0` in the table for the omitted
      columns; ~~write the per-vendor parity table (driver calls made, and every deliberate divergence
      with its reason) into `docs/`~~.
      **The parity document was withdrawn after the build, by request** — `docs/detector-parity.md`,
      its `mkdocs.yml` nav entry and the test asserting every vendor appeared in it were all removed,
      so `docs/` and `mkdocs.yml` carry nothing from this work. Nothing it recorded is lost: the
      deferred items it carried are the **Deferred** table below, and every divergence it explained
      is stated in a comment at the code that makes it.
      Acceptance: a `Detector` subclass without `detect_info` fails to instantiate; `detect --no-usage`
      prints `N/A` for utilization/temperature and issues no metric call; ~~the parity table names all
      nine vendors and every divergence recorded in F1~~ (withdrawn with the document);
      **no detector can emit a `vgpu` key** —
      each vendor task could only assert its own output, so this guard lands here, scoped to the
      shipped package (`gpustack_runtime/**/*.py`). Deliberately **not** a literal tree-wide text
      grep: `test_ascend.py` plants a stale `vgpu` in an appendix on purpose, to prove it cannot
      resurrect `/dev/vdavinci`, and the captured `samples/` are covered by `T12`'s own schema guard.
      Verify: `uv run pytest tests/gpustack_runtime/detector -q && make docs`

- [x] **T12 · Align `samples/detect_output_*.json` with the new Device contract**
      Blocked by: T3, T4, T5, T6, T7, T8, T9, T10
      Owns: `tests/gpustack_runtime/detector/samples/**`,
      `tests/gpustack_runtime/detector/test_samples.py`
      Scope: realign every `detect_output_*.json` field-by-field to the post-change shape — no `vgpu`;
      MIG instances inside the card's `appendix["mig_devices"]` (`detect_output_nvidia_h100_mig.json`
      still uses the retired standalone-MIG shape with `gpu_instance_index` /
      `compute_instance_index`); the physical-index appendix keys present; NVIDIA totals left as the
      driver reports them. Add the schema-guard test that loads every sample and deserializes it,
      so the samples stop drifting silently. `topology_output_*.json` is untouched (topology is a
      Non-Goal).
      Acceptance: every `detect_output_*.json` deserializes into `Device`; none carries `vgpu`, or
      `gpu_instance_index` / `compute_instance_index` at the top level; MIG samples carry
      `appendix["mig_devices"]`; the guard test fails when a sample grows a key `Device` cannot hold.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_samples.py -q`

#### Deployer track

- [x] **D1 · Pin the device ordering for containers that see every card**
      Blocked by: None
      Owns: `gpustack_runtime/deployer/__types__.py`, `gpustack_runtime/deployer/docker.py`,
      `gpustack_runtime/deployer/podman.py`, `gpustack_runtime/deployer/kuberentes.py`,
      `tests/gpustack_runtime/deployer/test_visible_devices_ordering.py`
      Gate: review
      Scope: add `Deployer.map_visible_devices_ordering(runtime_envs) -> dict[str, str]` beside the
      existing `map_backend_visible_devices` / `map_visible_devices_affinities` — it resolves each
      runtime visible-devices env name to its manufacturer through `get_manufacturer()` and returns
      `{"CUDA_DEVICE_ORDER": "PCI_BUS_ID"}` when NVIDIA is among them, `{}` otherwise. Call it from all
      three deployers' device-request loops (`docker.py`, `podman.py`, `kuberentes.py`), right beside
      the existing `if r_v != "all" and privileged:` backend-visible-devices block, under the
      complementary condition `r_v == "all" or privileged` — the deployers' own resolution of "this
      container sees every card". Never overwrite a value the container already declares (the
      Kubernetes site appends to a `V1EnvVar` list, so it needs a name check).
      Acceptance: NVIDIA + `all` → the container carries `CUDA_DEVICE_ORDER=PCI_BUS_ID` on all three
      deployers; NVIDIA + privileged with a specific device request → injected; NVIDIA + a specific
      device request without privilege → not injected; a non-NVIDIA manufacturer → nothing injected;
      a container declaring `CUDA_DEVICE_ORDER=FASTEST_FIRST` keeps its own value; the auto-mapping
      resource key on a mixed-manufacturer node injects once, not once per manufacturer.
      Verify: `uv run pytest tests/gpustack_runtime/deployer/test_visible_devices_ordering.py -q`

- [x] **D2 · `WorkloadStatus` exit-status model**
      Blocked by: D1
      Owns: `gpustack_runtime/deployer/__types__.py`,
      `tests/gpustack_runtime/deployer/test_workload_status.py`
      Gate: review
      Scope: add a `WorkloadStatusExit` `dataclass_json` dataclass (name, token, exit_code, reason,
      message, started_at, finished_at, restart_count) and a defaulted `WorkloadStatus` field holding a
      list of them. Serial after D1 purely to keep a single writer on `__types__.py`.
      Acceptance: a status with no exits round-trips; a payload serialized before this change still
      deserializes; the new field is excluded from no existing consumer's expectations.
      Verify: `uv run pytest tests/gpustack_runtime/deployer/test_workload_status.py -q`

- [x] **D3 · Docker: populate exit status**
      Blocked by: D2
      Owns: `gpustack_runtime/deployer/docker.py`,
      `tests/gpustack_runtime/deployer/test_docker_status.py`
      Scope: fill the exit list from each container's `State` (`ExitCode`, `Error`, `OOMKilled`,
      `StartedAt`, `FinishedAt`, `RestartCount`) for init and run containers, and carry the reason into
      `state_message`. `parse_state`'s existing verdicts do not change.
      Acceptance: exited with 137 and `OOMKilled` → the existing state verdict is preserved and an exit
      entry carries 137 plus the `OOMKilled` reason; a running container contributes no entry; a
      container whose `State` lacks the keys degrades to an entry with the code alone.
      Verify: `uv run pytest tests/gpustack_runtime/deployer/test_docker_status.py -q`

- [x] **D4 · Podman: populate exit status**
      Blocked by: D2
      Owns: `gpustack_runtime/deployer/podman.py`,
      `tests/gpustack_runtime/deployer/test_podman_status.py`
      Scope: the D3 change applied to the Podman deployer's own status class. Runs concurrently with D3
      and D5 — disjoint files.
      Acceptance: same as D3, against Podman's container attrs.
      Verify: `uv run pytest tests/gpustack_runtime/deployer/test_podman_status.py -q`

- [x] **D5 · Kubernetes: exit status, image-pull failure, Pod Events, RBAC**
      Blocked by: D2
      Owns: `gpustack_runtime/deployer/kuberentes.py`, `deploy/manifests/kubernetes.yaml`,
      `tests/gpustack_runtime/deployer/test_kubernetes_status.py`
      Gate: review
      Scope: fill the exit list from `container_statuses` / `init_container_statuses`, reading
      `state.terminated` and `last_state.terminated` for codes and `state.waiting` for reason and
      message; make `parse_state` return `Failed` — not `Pending` — for `ErrImagePull`,
      `ImagePullBackOff`, `InvalidImageName` and `RegistryUnavailable`, with reason and message in
      `state_message`; read Pod Events, field-selected to the Pod, only in that blocked state and
      append the Event message; degrade a `403` to a debug log; grant `get`/`list` on core `events` in
      the manifest.
      Acceptance: Pending + `ImagePullBackOff` → `Failed` carrying the reason and the registry message;
      `terminated(1, "Error")` → an exit entry with 1; Events raising `ApiException(403)` → state and
      reason still reported and nothing raised; a Running Pod triggers no Events call.
      Verify: `uv run pytest tests/gpustack_runtime/deployer/test_kubernetes_status.py -q`

#### End-of-build review

Three axes ran over the whole branch diff: the **spec** axis (`spec-reviewer`), the **standards** axis
(`agent-skills:code-reviewer`), and two external cross-checks (codex and kimi). The spec axis returned
Missing 0 / Unasked 0 / Wrong 1 — a self-contradiction of this document's own Boundaries on
`get_topology`, fixed at the source. The other three produced 20 findings between them, of which 14
were real and are fixed above the C2 line, each with a regression test that fails without the fix:

- **The Ascend `/dev/davinciN` fallback**, flagged High by codex and Critical by the standards axis
  while kimi read it as preserved behaviour. All three were partly right: the pre-change default path
  *did* build the node from the logic id, because the physical id was only used when
  `GPUSTACK_RUNTIME_DETECT_PHYSICAL_INDEX_PRIORITY` was set. So it is a pre-existing defect this work
  preserved and can now close, not a regression it introduced — and it is exactly what this document's
  "**Ask first:** before changing what a `/dev/...` device-node path resolves to" exists to catch.
- Two identity defects in the new usage join: an ambiguous UUID broadcasting one card's metrics onto
  every card, and `cndevGetUUID` manufacturing a bare `"MLU-"` that is such an id.
- Two availability defects: a usage-query failure discarding an inventory already in hand, and every
  Cambricon card being skipped reading as "no hardware".
- Two reporting defects a consumer would have acted on: `--format json` ignoring `--no-usage`, and a
  container that merely exited non-zero carrying no reason and so no `state_message`.
- **Two tests that could not fail**, which matter more than the findings they hid: the usage-join test
  used identical payloads for every card, so a positional join, a reversed mapping and a
  broadcast-to-all all satisfied it; and the memory-status merge test ran with health checks off, where
  both queries answer `healthy` without a driver call.

#### Deferred — each needs its own task

These were found during the build or its review, judged real, and deliberately **not** fixed here,
because each is a behaviour change wider than this work's scope. They were recorded in a parity
document that has since been dropped, so the list lives here:

| Vendor | Deferred item |
| --- | --- |
| All | Eight bindings check `_libInitialized` **outside** the load lock, so two concurrent first callers can both reach the driver's init and the loser's error is cached as permanent state. `pycndev` is fixed; `pyamdsmi`, `pydcmi`, `pyhgml`, `pyixml`, `pymtml`, `pymxsml`, `pynvml` and `pyrocmsmi` are not — they keep upstream `pynvml`'s shape on purpose so they can be re-synced. One mechanical sweep, provable per binding with the same two-caller test. |
| Ascend | Utilization is read through the operator's **V1** path only, and only the AICORE rate; `pydcmi` binds no `_v2` utilization entry point at all. Needs a binding addition. |
| Ascend | `_get_device_memory_status` tries HBM ECC then DDR ECC unconditionally, where the operator picks the ECC device type from whichever memory query succeeded. Pre-existing. |
| Cambricon | The only vendor that degrades **per card**; the other eight lose the whole pass on one faulty card. Graceful degradation is very likely right everywhere, but that is a repo-wide change. |
| Cambricon | A card skipped by the **usage** pass keeps the information query's zeroes rather than reading as unmeasured. Its `memory_status` still carries the health that query read, so a failing card is not fully masked — but telling "not measured" from 0 needs `cores_utilization`, `memory_used` and `memory_utilization` to become optional, which is a cross-vendor change to `Device`, its consumers and every sample. |
| Cambricon | `cores` and the power **limit** are not reported: `pycndev` binds no core-count entry point and the operator reads neither. |
| MetaX | **The temperature divisor is unresolved** — see Open Questions. Needs hardware. |
| MThreads | `detect_info` frees the MTML handle in its `finally` and then uses it to open the memory context: a use-after-free. Pre-existing, and `detect_usage` does not reproduce it. |
| MThreads | `mtmlDeviceGetPowerUsage` is reported without unit conversion. Same follow-up as above. |

- [x] **C1 · Checkpoint: whole suite, lint, CLI smoke**
      Blocked by: T11, T12, D3, D4, D5
      Owns: None
      Gate: review
      Acceptance: `make lint` leaves the tree clean, `make test` is green, and both
      `gpustack-runtime detect --format json` and `--no-usage --format json` run on the dev machine
      returning an empty list without a traceback.
      Verify: `make prepare && make lint && make test`

- [x] **C2 · Checkpoint: hardware validation on the user-provided environments**
      Blocked by: C1
      Owns: `tests/gpustack_runtime/detector/samples/**`
      Gate: review
      Scope: on each environment the user provides, run `detect` with and without `--no-usage`, compare
      the reported card name, memory, cores and health against the operator's `Devices` object on the
      same node — **health only with `GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK=false`**, since the
      default skips the ECC read entirely and would report `healthy` unconditionally (see F1's
      all-vendors row), deploy one workload with an unpullable image to confirm the reported failure, and
      refresh any sample whose real output differs from T12's hand-alignment. Also settles the Ascend
      `/dev/davinci{N}` Open Question.
      **Three value-level refreshes `T12` deliberately refused to invent, so they are `C2`'s to
      capture:** the keys absent from the realigned samples (`minor_number` on NVIDIA and THead,
      `physical_id` on Ascend, and the card-level fields of the two rebuilt MIG cards in
      `detect_output_nvidia_h100_mig.json`) — their absence is honest, not a discrepancy; the two
      `1c.3g.40gb` MIG instances, still carrying the *compute-instance* profile name from the capture
      while today's detector reports the *GPU-instance* one; and the AMD / Hygon `name` values, which
      now prefer the host's local pci.ids board name and so cannot be derived from a sample at all.
      Acceptance: per environment, name/memory/health match the operator's report for every card;
      `--no-usage` omits the metric fields; the unpullable-image workload reports `Failed` with the
      pull reason; samples committed from real output.

      **Result.** All five environments ran `detect`, `detect --no-usage`, `topology` and a
      `GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK=false` pass, every one exiting 0 with an empty stderr:
      2 × AMD RX 7800 XT, 1 × RTX 5090 D, 2 × RTX 4090 48 GB, 8 × Ascend 910B2, 16 × T-Head
      PPU-ZW810E. `--no-usage` omitted exactly the five usage keys and kept `memory`, `memory_status`
      and `power`. Health read the same verdict with the ECC check on as off on every card, so no
      card on these hosts is carrying an uncorrected error.

      Four things it measured that no fixture could:

      1. **T-Head's minor number is not its enumeration index**, on any of the 16 cards. They ran one
         apart here (index 3 → minor 4, index 15 → minor 16), and the operator's allocator records the
         same relation on its own measured host — but **that offset is an observation about a host and
         a driver, not a rule**, and nothing on either side may compute one number from the other. The
         operator says so at `allocator/thead/mig.go:640-645`, and its own note explains the offset on
         that host: `/dev/alixpu` holds minor 0 of the same character-device major, so the per-card
         nodes start one along. What the measurement establishes is only the load-bearing part — the
         two numbers differ — which is why `/dev/alixpu_ppu<N>` is named by the **card ordinal** and
         the minor is read from `hgmlDeviceGetMinorNumber` as the card's *identity*. Using the
         identity as the name lands on the neighbouring card, and on the last card of a 16-card host
         names a `ppu16` that does not exist at all.
      2. **Ascend reports `physical_id` for all 8 NPUs**, so the invariant the Ascend fix relies on
         holds on a real driver. Logic and physical id agree here (0–7), which is why the fix is also
         provably non-regressive on this host.
      3. **AMD's per-card UUIDs are distinct** (`GPU-5c88007d760374f3` / `GPU-d99e7fe92c7bdf75`), so
         `rsmi_dev_unique_id_get` answering a constant is not happening here. The merge's
         ambiguous-id guard stays as defence in depth; no identity change is needed.
      4. **Hygon by proxy executes clean** on the AMD host. Its appendix lacks `card_id` /
         `renderd_id` there, which is *not* a defect: it reads `/sys/module/hycu` and
         `/sys/module/hydcu`, and the host runs `amdgpu`. `driver_version` is null for the same
         reason. Exactly the "cannot prove a Hygon-specific value" limit recorded above. The vendor-id
         edit was reverted and never committed.

      `F5` end to end, on the k3s cluster of the RTX 5090 host and on Docker 29.6.1 of the AMD host:
      an unpullable image reports `state = Failed` with
      `state_message = "ImagePullBackOff: Back-off pulling image ...; Error: ErrImagePull"` — the
      waiting reason *plus* the appended Pod Event — and an `exits` entry whose `started_at` and
      `finished_at` are empty because it never ran. A container exiting 7 reports `exit_code = 7`,
      `reason = "Error"` and `state_message = "Error"`, which is the end-of-build fix: before it,
      Docker left `State.Error` empty for a container that ran and exited non-zero, so the commonest
      crash of all carried no reason and set no message. Its timestamps came back with six-digit
      microseconds, i.e. the nanosecond truncation holds against a real daemon.

      **What C2 could not settle**, as scoped: Iluvatar, MetaX, MThreads and Cambricon have no
      environment, so the MetaX temperature divisor and Cambricon's MB-means-MiB reading keep their
      markers; and no MIG-enabled host existed, so `detect_output_nvidia_h100_mig.json`'s two rebuilt
      cards and their compute-instance profile names stay unrefreshed. Both are recorded in the
      samples README rather than guessed at.
      Verify: `uv run gpustack-runtime detect --format json` on the provided host (access method
      supplied by the user at that point)

### Test Plan

[x] I/we understand the owners of the involved components may require updates to existing tests to make
this code solid enough prior to committing the changes necessary to implement this enhancement.

#### Prerequisite testing updates

- `tests/gpustack_runtime/detector/fixtures/` holds only a `load()` helper and no data files. Every
  detector test today is `skipif not is_supported()` and therefore never runs in CI, so each vendor
  task must bring a fake binding — fake device handles plus a monkeypatch of its binding module — to
  exercise `detect_info` / `detect_usage` on a machine with no driver.
- Each vendor task builds that fake **inside its own test module**, not in a shared
  `fixtures/bindings.py`. The fakes have nothing to share in practice (`pynvml`, `pymxsml`, `pydcmi`
  and the rest expose unrelated APIs), while one shared file would serialize seven vendor tasks whose
  paths are otherwise disjoint — the coordination cost outweighs the deduplication.
- Every fake carries a **call log**, because several acceptance criteria are "issues **no** metric
  call" and cannot be asserted from return values. A dozen lines per vendor.
- `pytest.ini` lists an `integration_tests` testpath that does not exist — create the directory (see
  Integration tests) or drop the entry.
- Fixture `pci.ids` extract for `get_pci_device_name()` (T1) covering one AMD and one Hygon
  vendor:device pair.
- Optional: add `pytest-cov` so the per-package numbers below become measurable rather than reviewed.

#### Unit tests

Coverage is not measured today (`pytest.ini`'s `addopts` carries no `--cov`), so these are entry targets
for the non-hardware code paths, not deltas from a baseline:

- `gpustack_runtime/detector`: `2026-08-14` - not measured; target `>=70%`
- `gpustack_runtime/detector/pycndev`: `2026-08-14` - not measured (new); target `>=60%` (struct
  layouts and error mapping; the library-loaded path stays hardware-gated)
- `gpustack_runtime/deployer` (status parsing and plan defaulting only): `2026-08-14` - not measured;
  target `>=60%`

Per-unit coverage added by task: the ABC composition and usage merge (T1); `cndev` struct layouts and
error mapping (T2); each vendor's `detect_info` / `detect_usage` split, its parity fix and the absence
of `vgpu` (T3–T10); abstractness of `detect_info` and the `--no-usage` rendering (T11); the sample
schema guard (T12); the ordering injection across all/privileged/subset/non-NVIDIA/already-declared
and all three deployers (D1); exit-status
round-tripping and backward-compatible deserialization (D2); Docker, Podman and Kubernetes exit-status
parsing plus the image-pull verdict and the Events `403` degradation (D3–D5).

#### Integration tests

Needing a real Docker daemon and a real Kubernetes cluster, so they live under `integration_tests/` and
are not part of `make test`'s default run. Concrete test names are added after the implementation PR
merges:

- Docker: a workload whose run container exits non-zero reports the exit code and reason.
- Docker: a workload killed by the OOM killer reports `OOMKilled`.
- Kubernetes: a workload with an unresolvable image (`does-not-exist.invalid/x:y`) reports `Failed`
  with the pull reason and the Event message.
- Kubernetes: the same workload with the `events` RBAC rule removed still reports `Failed` with the
  reason, and logs rather than raises.
- Kubernetes: a healthy running workload triggers no Events request (asserted on the API call log).

#### e2e tests

None automated in this repository. `gpustack_runtime` is a library consumed by the GPUStack worker and
the operator, and end-to-end coverage belongs to those repositories' suites; duplicating it here would
require provisioning nine vendors' hardware in CI. The end-to-end guarantee for this change is
checkpoint `C2`: a manual pass on the environments the user provides, comparing the runtime's report
against the operator's `Devices` object on the same node and deploying one unpullable-image workload.

## Alternatives

- **Keep a single `detect()` with a boolean flag, no split.** Rejected: a caller that only wants a
  metrics refresh would still re-run the whole inventory query, and the operator's proven
  detect/monitor separation would have no counterpart.
- **Mirror the operator's names (`detect_accelerator` / `monitor_accelerator`).** Rejected: the
  runtime's public surface is `detect_devices` / `Detector.detect`; renaming churns every caller for
  cosmetic symmetry.
- **Copy `CUDA_DEVICE_ORDER` from the worker process's environment.** Rejected: it only works if
  something upstream remembers to set it on the worker, which is the same configuration gap that
  produced issue #6041 in the first place. Whether a container sees every card is something the
  deployer knows and the worker does not, so the deployer decides. **Confirmed independently by the
  operator**, which reached the same conclusion on its own side and declined to set the variable on
  the worker Deployment — "that process runs no CUDA and propagates nothing to workloads, so the
  ordering has to be stated where the injection is consumed" (`fix(devicemanager): pin CUDA's device
  order where a slice's limits are read by ordinal`). Copying from the worker would therefore have
  been copying a variable nobody sets.
- **Inject `CUDA_DEVICE_ORDER=PCI_BUS_ID` into every NVIDIA workload unconditionally.** Rejected: a
  container given a specific device subset already has a scoped visible-devices env, and pinning the
  ordering there would silently change the ordinals a caller addressing that subset relies on.
- **Introduce a generic `GPUSTACK_RUNTIME_DEPLOY_INHERIT_ENVS` passthrough list.** Rejected for now:
  a new configuration surface for a single known variable. Revisit when a second one appears.
- **Report image-pull failures from `container_statuses` only, without Pod Events.** Rejected: the
  waiting reason alone ("ImagePullBackOff") omits the registry error the user needs; Events carry it.
- **Keep the `vgpu` flag but stop acting on it.** Rejected: a field nothing consumes is drift waiting
  to happen, and the Ascend CDI path proves consumers appear.

## Open Questions

- ~~Does `/dev/davinci{N}` follow the DCMI logic id or the physical id on the driver versions we
  support?~~ **Answered by the end-of-build review: the physical id, and nothing stands in for it.**
  The operator's `ascend/device.go` skips a device whose `GetPhysicalID` fails rather than guessing,
  and so does the detector now; the CDI generator refuses to build a path without
  `appendix["physical_id"]`. `C2` no longer decides this — it confirms it, by checking that the
  refreshed Ascend samples carry a `physical_id` and that the node paths address the right NPUs.
- **MetaX temperature is scaled three different ways and at most one can be right.** The runtime
  divides `mxSmlGetTemperatureInfo(_, TEMPERATURE_HOTSPOT)` by 100 with the comment `mC to C` — but
  millidegrees to degrees is a division by 1000, and the very next line converts board power with
  `// 1000  # mW to W`, so the two are inconsistent with each other. The operator applies **no**
  conversion at all (`temperature = uint32(tempInfo)`). So the runtime reports the driver value ÷100
  and the operator reports it raw, which is exactly the Story 1 class of discrepancy this work exists
  to remove. Found while building `T8`; deliberately left untouched, because `F1`'s MetaX row scopes
  that task to the PF skip and guessing at a unit without hardware would just move the error.
  **Settled by `C2`**: read one card's hotspot temperature on a real MetaX host and compare all three
  readings against `mx-smi`.
- Should the usage query also be exposed at module level (e.g. `monitor_devices(devices)`) for
  callers running their own polling loop, or is `Detector.detect_usage` enough? The plan ships only
  `detect_devices(..., usage=...)`; add the module-level entry point when a caller needs it.
- Does the pinned `nvidia-ml-py>=13.580.65` expose `nvmlDeviceGetMemoryInfo(handle, version=2)` and
  a v2 PCI-info accessor under those names, or is a version probe needed? **Answered while building
  `T5`** — if the names differ, `T5` adds the probe rather than pinning a newer floor.
- ~~Is there an upstream Python `cndev` binding worth depending on instead of hand-writing
  `pycndev`?~~ **Answered by `T2`: no.** The name does not exist on PyPI (checked against the full
  simple index), Cambricon publishes no Python device-management binding at all — their own
  `libcndev.so` consumers (`mlu-exporter`, `cambricon-k8s-device-plugin`) are Go, and `mlu-exporter`
  makes the operator supply `cndev.h` themselves because the header ships with the driver. The only
  third-party candidate, `mlu-api`, vendors its own `libcndev.so` (so it cannot track the host
  driver's ABI), is cp310/x86_64-only and exposes device count plus core utilization and nothing
  else. CNDev is a driver-versioned C ABI, i.e. the `pydcmi`/`pymxsml` situation, not the
  `pynvml`/`pymtml` one — hand-written it is.
