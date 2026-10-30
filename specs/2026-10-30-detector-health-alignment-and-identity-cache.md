# Spec: Re-align detector health checks with the operator, and cache device identity between collections

Status: Shipped
Type: Feature

## Summary

The operator tightened how it derives device health: on Ascend it reads the DCMI health
register first and treats only major-or-worse (`health >= 2`) as unhealthy, falling back to
double-bit ECC errors only when the health query fails; on Cambricon it falls back from the
health-state V2 call to V1 on any V2 failure
([gpustack-operator@34090bd](https://github.com/gpustack/gpustack-operator/commit/34090bd740f1da32b423033c1e53380295b7bfc1)).
The runtime still marks an Ascend card unhealthy on single-bit ECC errors and never consults
the health register, so the worker and the operator can disagree about the same card, and a
card the operator considers healthy is drained from scheduling. Four more vendors share a
second drift: the operator leaves a card healthy when its health query errors, while the
runtime fails closed and marks it unhealthy.

Separately, GPUStack's worker collector calls `detect_devices(fast=False)` on every status
sync (30 s by default) and on every metrics scrape, and each call re-runs the full identity
detection — chip info, BDF, die, RoCE addresses, CPU affinity — although device identity
rarely changes. The operator's own device manager separates `DetectAccelerator` from
`MonitorAccelerator` for exactly this reason.

This change re-aligns the health derivation vendor by vendor with the operator, and adds a
process-wide identity cache inside the runtime: the first detection pays for identity, later
collections within the TTL refresh usage and health only. GPUStack needs no code change.
It also bumps `gpustack-runner` to `>= 0.1.32.post2`.

## Motivation

### Goals

- **Health verdict parity.** For every supported manufacturer, the runtime derives
  `Device.memory_status` from the same readable values and fallback chain as the operator
  derives its unhealthy flag, or records a deliberate divergence in this spec. Target users:
  GPUStack operators who compare the worker's device health with the operator's view on the
  same node.
- **No over-checking.** A failed query never produces an unhealthy verdict; only a readable
  counter, register or field can. On Ascend this also means a card with corrected single-bit
  ECC errors stays healthy, and the authoritative DCMI health register decides instead of a
  two-call ECC loop.
- **Detect once, monitor continuously.** Within the TTL, repeated collections reuse the
  cached identity and pay only the usage queries (utilization, memory, temperature, power,
  health). The public `detect_devices()` API is unchanged, so GPUStack's worker collector and
  metrics exporter benefit without modification.
- **Track gpustack-runner.** The dependency floor moves to `>= 0.1.32.post2`.

Success is testable: `make test` green, with new fixture-driven tests (no hardware required)
covering the Ascend health verdicts, the Cambricon V1 fallback, each re-aligned vendor's
error default, and the cache's TTL, copy, concurrency and failure semantics.

### Non-Goals

- Introducing a monitor loop or daemon in the runtime; scheduling stays with the caller
  (carried over from `specs/2026-08-14-detector-alignment-and-workload-exit-status.md`).
- Changing `GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK`'s default. It stays `1` (health queries
  off unless the deployer opts in): on NVIDIA a health query against a wedged GPU can block
  for the driver's RPC timeout (up to 45 s with GSP firmware), and this spec does not change
  that vendor's cost profile.
- Changing GPUStack or the operator. Both are reference and beneficiary, not subject.
- Cross-process or on-disk caching. The cache lives inside one process, like the existing
  `is_supported()` cache.
- Topology alignment between the two code bases (out of scope in the predecessor spec).

## Proposal

The runtime's detectors derive health the way the operator does, vendor by vendor: a verdict
comes only from a readable value, and the fallback chains match. The default collection path
stops re-paying for identity it already knows.

### User Stories

#### Story 1
As a GPUStack operator running Ascend nodes, I want the worker to report the same card health
as the operator's device manager, so that a card with corrected single-bit ECC errors stays
schedulable and a genuinely failing card is drained in both views.

#### Story 2
As a GPUStack operator with an older Ascend driver that refuses the V2 DCMI API, I want the
health verdict to fall back through V1 health and ECC the way the operator does, so that
driver age does not blank the verdict.

#### Story 3
As a GPUStack user, I want worker status collection and metrics scrapes to stop re-enumerating
device identity every 30 seconds, so that the worker puts less pressure on the vendor driver
and reports faster.

#### Story 4
As a runtime maintainer, I want the caching to be one bounded, tunable mechanism with a
documented staleness window, so that a hotplug or an operator-driven MIG repartition shows up
within a known time and I can disable the cache with one environment variable.

### Core Features & Acceptance Criteria

**Feature A — Ascend health alignment.**

- `pydcmi` gains `dcmiv2_get_device_health()`; the V1 `dcmi_get_device_health()` already
  exists. Status values: 0 = Normal, 1 = Minor, 2 = Major, 3 = Critical, 0xFFFFFFFF =
  device not found.
- `_get_device_memory_status()` (V1 path) and `_get_device_memory_status_v2()` derive the
  verdict as the operator's `evaluateDeviceUnhealthy` does: query the device's health first;
  a successful query is unhealthy if and only if `health >= 2`; when the health query fails,
  fall back to the ECC counters of the memory type the device uses (HBM when the HBM info
  query answers with memory — `memory_size > 0`, the same test `_get_device_memory_info`
  uses — otherwise DDR; one query, not today's loop), unhealthy if and only if ECC is
  enabled and `double_bit_error_cnt > 0`. A single-bit-only ECC count is healthy. Every
  query unreadable is healthy.
- Acceptance: fixture-driven unit tests faking `pydcmi` cover, on both API versions: health
  0 and 1 → HEALTHY; 2, 3, 0xFFFFFFFF → UNHEALTHY; health query error + single-bit ECC →
  HEALTHY; + double-bit → UNHEALTHY; health and ECC both unreadable → HEALTHY. The ECC
  fallback rows are V1-only: on V2, a device whose HBM query fails is skipped outright (as
  the operator's V2 path does), so the V2 fallback covers HBM alone.

**Feature B — Cambricon V1 fallback.**

- `_get_memory_status()` falls back to `cndevGetCardHealthState` (V1) when the V2 call fails
  for any reason, not only when the V2 symbol is missing; the verdict stays
  `health == 0 → UNHEALTHY`, and two unreadable calls stay HEALTHY. This matches the
  operator's `evaluateCardUnhealthy`.
- Acceptance: unit tests faking `pycndev` cover V2 success, V2 error → V1 success, and both
  failing.

**Feature C — Fail-open error default on nvidia, amd, iluvatar and thead.**

An audit against the operator's current detectors found hygon, metax and mthreads already
aligned (same counter, same comparison, both fail open). The other four fail closed today:

- **nvidia** — the operator reads the volatile uncorrected DRAM ECC counter and treats a
  failed read as healthy. The runtime additionally marks a card unhealthy on a genuine
  `NVMLError`. Aligned: a failed counter read yields no verdict. The runtime's extra probes
  stay, as a deliberate, recorded divergence (see Notes): the GSP recovery-action /
  reset-status field values (a GSP failure leaves the ECC counters readable at zero, so the
  operator's counter alone misses it), and the aggregate-ECC check on MIG entries.
- **amd** — the operator reads the UMC uncorrectable ECC count and treats a failed read as
  healthy. Aligned: `AmdSmiException` yields no verdict; the ROCm-SMI fallback stays, and —
  a change from today's code — a terminal error inside that fallback now also yields
  HEALTHY instead of UNHEALTHY.
- **iluvatar** — the operator reads `GetHealth()` and treats a failed query as healthy; the
  runtime fails closed unless the error is `NOT_SUPPORTED`. Aligned: any read error yields
  HEALTHY.
- **thead** — as iluvatar, with the M-device aggregate-ECC probe kept as a recorded
  divergence.
- The `GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK` docstring in `envs.py` stops describing
  NVIDIA's "a query the driver errors on reports the device unhealthy", which is no longer
  true.
- Acceptance: for each of the four vendors, fixture-driven unit tests pin: counter readable
  and above threshold → UNHEALTHY; readable and at zero → HEALTHY; read error → HEALTHY
  (changed); probe paths where kept still produce UNHEALTHY from a readable value.

**Feature D — Identity cache.**

- The cache lives in the `Detector.detect()` flow, so `detect_devices()` and any direct
  per-detector caller share one mechanism; `detect_info()` and `detect_usage()` stay
  uncached primitives.
- Each supported detector's `detect_info()` result is cached process-wide, keyed by
  manufacturer. The TTL defaults to 15 s — the operator's device manager monitor
  period — measured on a monotonic clock, and is set by
  `GPUSTACK_RUNTIME_DETECT_INFO_CACHE_TTL` (seconds; `0` disables, and a value
  that is not a number disables too). The environment
  variable is read once per process, so it must be set before the first detection.
- Every call still runs `detect_usage()` fresh, merged onto a **deep copy** of the cached
  identity: `Device.appendix` nests mutable structures (`mig_devices`) and the merge
  writes into them, so a shallow copy would let concurrent callers contaminate the cache.
- Only a non-empty identity is cached: a `detect_info()` that succeeds with an empty
  result is not cached, so one transient enumeration glitch cannot pin an empty inventory
  for the TTL.
- Refreshes run outside the cache lock, single-flight per detector: a slow or wedged
  refresh delays only the callers waiting on that vendor, not unrelated vendors or pure
  cache hits.
- A refresh failure keeps serving the last good identity and logs at debug level; the next
  call retries. The served copy carries **no health verdict** — `memory_status`
  is UNKNOWN, including the MIG entries in `appendix["mig_devices"]` — because the
  refresh that could have confirmed it failed: the identity stands for inventory only.
  An interrupted detection (any `BaseException`) settles its flight the same way, so
  later callers retry instead of blocking on it forever.
- Acceptance: fixture-driven unit tests with a fake detector cover: second call within the
  TTL issues no identity queries; usage queries run on every call; TTL = 0 runs both;
  mutating a handed-out device's nested `appendix` leaves the cache and the next caller
  unaffected, including under a concurrent merge; an empty identity is not cached and the
  next call re-detects; a refresh failure serves the stale identity and the following
  success replaces it without a restart; the merge of fresh usage over cached identity
  matches today's output shape; the TTL clock is monotonic and injectable in tests.

**Feature E — gpustack-runner bump.**

- `pyproject.toml` moves the floor to `gpustack-runner>=0.1.32.post2`; `uv.lock` is
  regenerated (`uv lock --upgrade-package gpustack-runner`; a dry run resolves
  `0.1.32.post1 → 0.1.32.post2` cleanly).
- Acceptance: `uv sync` succeeds and `make test` passes against the new floor.

### Notes / Constraints / Caveats

- **Deliberate divergences kept.** The runtime reads signals the operator does not, and
  keeps them under this spec because they derive a verdict from readable device state, not
  from a failed query: NVIDIA's GSP recovery-action / reset-status fields (a GSP failure,
  Xid 119/154, wedges the management channel and leaves the ECC counters readable at zero,
  so the operator's counter alone misses it), NVIDIA's aggregate-ECC check on MIG appendix
  entries, thead's aggregate-ECC check on M-devices, and AMD's ROCm-SMI fallback when the
  primary SDK errors. Everything else about the four vendors' verdicts matches the operator.
  Convergence path: porting the GSP recovery fields into the operator's NVML binding would
  remove the largest divergence; that is an operator-side change, out of scope here.
  Follow-up: once the runtime change ships, file a todo issue on gpustack-operator and
  gather every insight this work produced, for the operator team to follow up and confirm:
  the "ECC = 0 is not health evidence" chain (a GSP failure wedges the management channel —
  Xid 119/154, RPC timeouts, stale reads — and volatile ECC counters stop accumulating and
  zero out on reset, so a readable zero is absence of evidence, not health); the
  fail-open/fail-closed trade-off and why the runtime now lets a failed query produce no
  verdict; the probes worth porting (GSP recovery-action/reset-status fields, per-MIG and
  M-device aggregate ECC — the operator publishes no MIG-level health today); the Cambricon
  V1-fallback-on-any-failure pattern; the cheaper Ascend health-first check; and the
  blocking-cost caveat (a wedged GPU can hold a query up to the driver's RPC timeout) with
  `GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK` as the escape hatch.
- The cache is per-process and deliberate: the `gpustack-runtime` CLI invokes one-shot
  processes and never sees staleness. In-process consumers inherit the staleness window:
  GPUStack's worker collector and metrics exporter, and — a second consumer easy to
  miss — the deploy path, where `Deployer._prepare()` builds workload materials from
  `detect_devices(fast=False)` and `expand_mig_devices()`. Within the process the views
  stay mutually consistent; across the TTL they can lag the host by up to 15 s. GPUStack
  itself needs no code change; it inherits the behavior.
- The staleness window covers the whole identity, including NVIDIA's `mig_devices` appendix.
  A just-repartitioned MIG instance can take up to the TTL to appear in the worker's
  inventory. 15 s sits inside one worker status sync (30 s apart by default) and matches
  the operator's monitor period, so a change lands within the next report.
- Health stays part of the usage refresh: `merge_devices_usage()` overwrites
  `memory_status` on every collection, so caching identity does not freeze health.
- On Ascend the aligned check is cheaper than today's: one health call per device instead of
  up to two ECC calls.
- `GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK` still short-circuits every health verdict to
  HEALTHY when the deployer leaves it at its default. The default stays: alignment fixes
  the verdict, not the blocking tail (a wedged GPU can hold a query up to 45 s on a driver
  RPC timeout, on a path that runs every 30 s sync and every scrape), and GPUStack's worker
  does not carry `memory_status` today, so a default flip would tax every deployment for a
  verdict nothing in-tree reads. Re-enabling is a separate, later decision, best staged:
  bound the cost first (query timeout or side thread), or start with the cheap single-call
  vendors (Ascend, Cambricon) while NVIDIA stays off, and flip when a default consumer
  exists.

### Boundaries

- **Always:** refresh usage and health on every collection; keep `detect_devices()`'s
  signature and output shape unchanged; keep per-vendor fallback chains identical to the
  operator's, or document the divergence in this spec.
- **Ask first:** flipping `GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK`'s default (stage it:
  bound the query cost or start per-vendor); changing the default TTL; adding cross-process
  or on-disk caching; removing one of the kept probes; filing the gpustack-operator
  follow-up issue (an external, visible action — post it after the runtime change merges,
  with its text confirmed first).
- **Never:** cache usage, utilization or health readings; share a cached object with a
  caller without copying; let one vendor's cache or health failure fail another vendor's
  collection; produce an unhealthy verdict from a failed query; modify the gpustack or
  gpustack-operator repositories under this spec.

### Risks and Mitigations

- Stale MIG inventory after an operator-driven repartition → bounded by the TTL, tunable
  through the environment variable, documented in the env docstring.
- Deploy materials built from a stale identity — `Deployer._prepare()` reads the same
  cache, so a MIG instance destroyed inside the window can be targeted and fail visibly at
  deploy. The views stay consistent within the process, a retry after the window recovers,
  and the window is the same knob → accepted and documented.
- A wedged driver makes the health query hang → pre-existing exposure, unchanged by this
  spec; refreshes run single-flight so one wedged vendor does not serialize the others, and
  `GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK` remains the escape hatch.
- Concurrent scrape and sync mutate shared state → the cache hands out deep copies and
  merges usage onto them, never onto cached objects.
- The cache masks a device removal for up to the TTL; a persistently failing refresh
  keeps serving the last good inventory with its health verdict blanked, and the debug
  log is the removal signal.
- An identity-time health verdict can look current — when a usage refresh errors, the
  returned devices carry the `memory_status` read at identity detection, up to TTL old
  (today the same figure is at most one detection old). Bounded by the TTL and by the
  health check's off-by-default switch.
- Vendor alignment drifts again → each vendor's verdict is pinned by fixture-driven tests
  that quote the operator's comparison, so a future drift is a visible test change.

## Design Details

### Commands

Environment: local — this macOS checkout, no vendor hardware. Every detector test runs on
fakes; the baseline suite is 715 passed, 20 skipped in about two seconds.

```
make lint                                    # pre-commit on all files
make test                                    # uv run pytest (tests/ + integration_tests/)
uv lock --upgrade-package gpustack-runner    # regenerate uv.lock for Feature E
uv sync                                      # apply the new lock
make docs                                    # docstrings render into the API reference
```

### Project Structure

- `gpustack_runtime/detector/__init__.py` — the detector registry; unchanged, the
  cache serves through `Detector.detect()`.
- `gpustack_runtime/detector/__types__.py` — cache state and helpers beside the `Detector`
  base class.
- `gpustack_runtime/detector/ascend.py` — health-first verdict, V1 and V2 paths.
- `gpustack_runtime/detector/pydcmi/__init__.py` — `dcmiv2_get_device_health()`.
- `gpustack_runtime/detector/cambricon.py` — V2 → V1 fallback on any failure.
- `gpustack_runtime/detector/nvidia.py`, `amd.py`, `iluvatar.py`, `thead.py` — fail-open
  error defaults; kept probes documented.
- `gpustack_runtime/envs.py` — `GPUSTACK_RUNTIME_DETECT_INFO_CACHE_TTL`; corrected
  `GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK` docstring.
- `pyproject.toml`, `uv.lock` — Feature E.
- `tests/gpustack_runtime/detector/` — per-feature tests beside the existing per-vendor
  tests; the cache gains its own `test_detector_cache.py`.

### Code Style

The verdict helper keeps the operator's comparison visible in one place:

```python
def _evaluate_device_unhealthy(
    health: int | None,
    read_ecc: Callable[[], EccInfo],
) -> DeviceMemoryStatusEnum:
    # DCMI health: 0 = Normal, 1 = Minor, >= 2 = Major/Critical/NotFound.
    # Mirrors the operator's evaluateDeviceUnhealthy: ECC double-bit errors
    # are a fallback, never the primary verdict.
    if health is not None:
        return _unhealthy(health >= 2)
    ...
```

Conventions: `from __future__ import annotations as __future_annotations__` at the top;
Google-style docstrings on public APIs; detectors degrade and log at debug level instead of
raising; env variables are declared in `envs.py` with docstrings and a lazy getter.

### Implementation Plan

Environment: local (see Commands). No PoC task: the design was cross-checked at spec stage,
every risk has a pinned acceptance row, and the baseline suite is green.

- [x] **T1 · Identity cache core**
      Blocked by: None
      Owns: `gpustack_runtime/envs.py`, `gpustack_runtime/detector/__types__.py`,
      `gpustack_runtime/detector/__init__.py`,
      `tests/gpustack_runtime/detector/test_detector_cache.py`,
      `tests/gpustack_runtime/detector/conftest.py` (autouse cache-reset fixture;
      widened during the review round — the process-wide cache leaks between every
      detector test file, so the reset lives in one fixture instead of per-file
      edits, and the cache stays manufacturer-wide as specified)
      Gate: review
      Acceptance: `GPUSTACK_RUNTIME_DETECT_INFO_CACHE_TTL` declared in `envs.py`
      (default 15 s; `0` disables; read once per process, tests reset the `lru_cache` on
      `__getattr__`). `Detector.detect()` caches per supported manufacturer, serves a deep
      copy, runs `detect_usage()` fresh on every call, caches non-empty identities only,
      refreshes single-flight per detector outside the cache lock, serves the last good
      identity on refresh failure with a debug log, and keeps `detect_info()` /
      `detect_usage()` uncached. Every Feature D acceptance row has a test.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_detector_cache.py tests/gpustack_runtime/detector/test_detector_types.py -q`

- [x] **T2 · Ascend health-first verdict**
      Blocked by: None
      Owns: `gpustack_runtime/detector/pydcmi/__init__.py`,
      `gpustack_runtime/detector/ascend.py`,
      `tests/gpustack_runtime/detector/test_ascend.py`,
      `tests/gpustack_runtime/detector/test_pydmi.py`
      Acceptance: `dcmiv2_get_device_health()` added beside the V1 call (same
      `_dcmiGetFunctionPointer` pattern); both `_get_device_memory_status()` and
      `_get_device_memory_status_v2()` implement health-first — unhealthy iff
      `health >= 2` — with a single-query ECC double-bit-only fallback (V1: HBM else DDR;
      V2: HBM only); single-bit-only stays HEALTHY; every query unreadable stays HEALTHY.
      Feature A's acceptance matrix pinned on both API versions.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_ascend.py tests/gpustack_runtime/detector/test_pydmi.py -q`

- [x] **T3 · Cambricon V1 fallback**
      Blocked by: None
      Owns: `gpustack_runtime/detector/cambricon.py`,
      `tests/gpustack_runtime/detector/test_cambricon.py`
      Acceptance: `_get_memory_status()` falls back to `pycndev.cndevGetCardHealthState` on
      any `CNDevError` from the V2 call; the verdict stays `health == 0 → UNHEALTHY`; two
      unreadable calls stay HEALTHY. Feature B's matrix pinned.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_cambricon.py -q`

- [x] **T4 · Fail-open error defaults on four vendors**
      Blocked by: T1
      Owns: `gpustack_runtime/detector/nvidia.py`, `gpustack_runtime/detector/amd.py`,
      `gpustack_runtime/detector/iluvatar.py`, `gpustack_runtime/detector/thead.py`,
      `gpustack_runtime/envs.py`, `tests/gpustack_runtime/detector/test_nvidia.py`,
      `tests/gpustack_runtime/detector/test_amd.py`,
      `tests/gpustack_runtime/detector/test_iluvatar.py`,
      `tests/gpustack_runtime/detector/test_thead.py`
      Gate: review
      Acceptance: a failed health read yields HEALTHY on nvidia, amd, iluvatar and thead —
      the existing fail-closed pins (e.g. `test_detect_fails_closed_on_an_ecc_query_error`)
      are rewritten to the fail-open contract; `NOT_SUPPORTED` stays HEALTHY; the kept
      probes still produce UNHEALTHY only from readable values (nvidia
      recovery-action/reset-status fields, nvidia MIG aggregate ECC, thead M-device
      aggregate ECC, amd ROCm-SMI fallback whose terminal errors now also yield HEALTHY);
      the `GPUSTACK_RUNTIME_DETECT_NO_HEALTH_CHECK` docstring stops claiming that errors
      mark devices unhealthy.
      Verify: `uv run pytest tests/gpustack_runtime/detector/test_nvidia.py tests/gpustack_runtime/detector/test_amd.py tests/gpustack_runtime/detector/test_iluvatar.py tests/gpustack_runtime/detector/test_thead.py -q`

- [x] **T5 · gpustack-runner bump**
      Blocked by: None
      Owns: `pyproject.toml`, `uv.lock`
      Acceptance: the dependency floor is `gpustack-runner>=0.1.32.post2`, `uv.lock`
      resolved to `0.1.32.post2`, and the environment syncs.
      Verify: `uv lock --upgrade-package gpustack-runner && uv sync && uv run python -c "import importlib.metadata as md; print(md.version('gpustack-runner'))"`

- [x] **T6 · Integration checkpoint**
      Blocked by: T1, T2, T3, T4, T5
      Owns: none (verification only; any fix goes back to the owning task)
      Acceptance: full suite green with every feature landed; the pre-existing deployer
      tests stay green with the cache enabled (the documented second consumer);
      docstrings render.
      Verify: `make lint && make test && make docs`

### Test Plan
[ ] I/we understand the owners of the involved components may require updates to existing tests to make this
code solid enough prior to committing the changes necessary to implement this enhancement.

#### Prerequisite testing updates
- The four vendors' fail-closed contracts are pinned by existing tests (for example
  `test_thead.py::test_detect_fails_closed_on_an_ecc_query_error` and
  `test_detect_tolerates_an_unsupported_ecc_query`, with counterparts in the nvidia, amd
  and iluvatar test files). T4 rewrites these pins to the fail-open contract; the
  assertion sites live in `tests/gpustack_runtime/detector/test_{nvidia,amd,iluvatar,thead}.py`.
- Env-dependent tests need `envs.__getattr__.cache_clear()` isolation, because the
  module's lazy attribute is memoized; T1 introduces the reset pattern in the new cache
  test file.

#### Unit tests
- `gpustack_runtime.detector` (cache): 2026-10-30 - new `test_detector_cache.py` covering
  every Feature D acceptance row (TTL hit, miss and disable; deep-copy isolation under a
  concurrent merge; empty-not-cached; serve-stale-then-recover with the stale verdict
  blanked to UNKNOWN; single-flight refresh; a barging retry cannot postpone a failed
  flight's outcome; an interrupted detection strands no flight) plus a combined-failure
  regression past one TTL. Coverage percentage is not tracked in this repo (no
  pytest-cov); the gate is the full suite staying green.
- `gpustack_runtime.detector.ascend` + `gpustack_runtime.detector.pydcmi`: 2026-10-30 -
  Feature A's verdict matrix on both API versions, plus an ABI pin for
  `dcmiv2_get_device_health` in the style of the existing struct-pinning tests.
- `gpustack_runtime.detector.cambricon`: 2026-10-30 - Feature B's matrix.
- `gpustack_runtime.detector.nvidia` / `.amd` / `.iluvatar` / `.thead`: 2026-10-30 -
  fail-open flips and kept-probe positives per Feature C.

#### Integration tests
- `test_detector_cache.py` drives `detect_devices(fast=False)` over fake detectors — the
  exact call shape GPUStack's worker collector and deployer use.
- The existing deployer suite stays green with the cache enabled: it pins the documented
  second consumer (`Deployer._prepare()`).
- `integration_tests/` is untouched; its hardware-gated skips remain.

#### e2e tests
- None. CI carries no vendor hardware or vendor libraries, so every verdict is
  fixture-driven by construction. Real-hardware validation of the Ascend and Cambricon
  paths happens on a staged node before release (manual), and its evidence feeds the
  gpustack-operator follow-up issue.

## Alternatives

- **Split collection and monitoring in GPUStack** — cache identity in the worker collector
  and call `detector.detect_usage()` directly. Rejected: it is the GPUStack change this spec
  exists to avoid; the runtime already exposes `detect_usage()` for callers who want an
  explicit split.
- **Strict operator parity on the four vendors** — also drop the recovery fields, the
  MIG/M-device ECC checks and the ROCm fallback. Rejected by decision: those probes read
  real device state, and dropping them would re-open the GSP-failure hole (a GSP-failed
  GPU's ECC counters read zero while its queries error), turning "zero" into false evidence
  of health.
- **Fine-grained static caching inside each detector** (topology, RoCE, affinity as
  per-field caches). Rejected: nine files of per-vendor surgery with high drift risk, against
  one coarse mechanism with one knob.
- **A runtime monitor loop** (the operator's `Detector.Start` shape). Rejected: carried-over
  non-goal; the caller owns scheduling.
- **Cache usage readings too.** Rejected: monitoring must stay live; identity is the only
  invariant worth caching.
- **Do nothing.** Rejected: every 30 s sync and every scrape pays the full identity
  enumeration; on Ascend each pass also mislabels single-bit-ECC cards as unhealthy, and on
  four vendors a failed query mislabels a card as unhealthy.

## Open Questions

- None blocking. Feature C's per-vendor outcomes are recorded above; if implementation
  meets a vendor whose operator logic cannot be mirrored cleanly, the divergence lands in
  this spec's Notes before the change merges.
