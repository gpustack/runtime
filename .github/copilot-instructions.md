# Copilot Code Review — GPUStack Runtime

GPUStack Runtime is a Python library for detecting accelerator (GPU/NPU) resources and managing
GPU workloads on Docker, Kubernetes and Podman. [AGENTS.md](../AGENTS.md) owns the coding
conventions and the architecture map; what follows is the part a reviewer can see and evaluate.
Keep feedback specific and actionable; cite the file and line.

## Reading sources for review

Review behavior against the PR head and its dependency versions (`pyproject.toml`, `uv.lock`).

- Docstrings are the usage contract and render into `site/` via mkdocstrings. Specs under
  `specs/` preserve design rationale; confirm a spec's status before treating a proposal as
  shipped behavior.
- For a vendor behavior claim, read the matching `py*` binding under
  `gpustack_runtime/detector/` and the detector's tests; captured vendor outputs live in
  `tests/gpustack_runtime/detector/samples/`.
- For an error or upstream compatibility claim, locate the stable error text, then inspect the
  relevant dependency's issues and pinned version. Confirm that the reviewed version contains the
  cited fix.
- Cite the relevant passage or code location and explain the failing scenario. Distinguish source
  statements from inference, and preserve qualifications when summarizing evidence.

## Out of scope — do not review

- `gpustack_runtime/_version.py`, `gpustack_runtime/_version_appendix.py` — generated.
- `gpustack_runtime/deployer/__patches__.py` — vendored patch of the Podman client.
- `gpustack_runtime/detector/py*/**` — ctypes bindings to vendor libraries; review the detector
  logic that consumes them, not the binding internals.
- `site/`, `dist/` — build output.

## Hard invariants — flag as required changes

- Never hand-edit `gpustack_runtime/_version.py` or `_version_appendix.py`; they come from
  hatch-vcs and `make prepare`.
- `uv.lock` changes come from `uv lock` (`make deps`), never from hand edits.
- A PR adding a detector must register it in `gpustack_runtime/detector/__init__.py` and carry
  sample-based tests; a PR adding a deployer must register it in
  `gpustack_runtime/deployer/__init__.py`.
- Commit messages follow Conventional Commits (`type: subject`); the commitizen hook enforces it.

## Python conventions

- Favor clear code over cleverness; flag needless complexity or speculative abstraction.
- Every package module starts with `from __future__ import annotations as __future_annotations__`.
- Public APIs are type-annotated and re-exported from the package `__init__.py` (`__all__`).
- Errors are handled explicitly: deployers raise `OperationError` / `UnsupportedError` from
  `deployer.__types__`; flag runtime-client exceptions leaking through the public API. Detectors
  degrade to an empty result on a host without that vendor; flag a detector that raises instead.
- Docstrings are Google style; they are the rendered API reference, so flag missing or stale
  docstrings on changed public APIs.
- Keep concurrency simple and minimal; flag shared mutable state that risks races.
- Comments stay plain and short; flag emoji, decorative symbols and revision narratives.

## Testing conventions

- Tests must not require real hardware or a real container runtime. Detector tests fake the `py*`
  binding in the test module; deployer tests fake the runtime client. Flag a test that probes the
  host.
- Sample data comes from `tests/gpustack_runtime/detector/samples/`; flag large inline copies of
  vendor output when a sample file would do.
- Each test verifies one behavior; assert observable state, not implementation details.
- Tests must be deterministic; flag time-, ordering- or randomness-dependent assertions.
