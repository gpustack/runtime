# The ranktable cases below read the module's own private constant, which is
# the point: the warning and the mount list must agree on one path.
# ruff: noqa: SLF001

import inspect

import pytest

from gpustack_runtime.deployer.cdi import ascend as cdi_ascend
from gpustack_runtime.deployer.cdi.__types__ import (
    Config,
    ConfigDeviceNode,
    ConfigMount,
)
from gpustack_runtime.detector import Device, ManufacturerEnum


def _ascend_device(index: int, soc_name: str) -> Device:
    return Device(
        manufacturer=ManufacturerEnum.ASCEND,
        index=index,
        name=soc_name,
        appendix={"arch_family": soc_name, "physical_id": index},
    )


# ---------------------------------------------------------------------------
# The A5 mount profile.
# ---------------------------------------------------------------------------


@pytest.fixture
def _synthetic_host(monkeypatch):
    """
    Make the generator's host probes answer for every path it asks about, so
    the mount list reflects the code's intent rather than the test machine.
    """
    monkeypatch.setattr(
        cdi_ascend,
        "device_to_cdi_device_node",
        lambda path, container_path=None: ConfigDeviceNode(
            path=container_path or path,
            host_path=path,
        ),
    )
    monkeypatch.setattr(
        cdi_ascend,
        "path_to_cdi_device_nodes",
        lambda path: [ConfigDeviceNode(path=path)],
    )
    monkeypatch.setattr(
        cdi_ascend,
        "path_to_cdi_mount",
        lambda path: ConfigMount(host_path=path, options=["ro"]),
    )
    monkeypatch.setattr(
        cdi_ascend,
        "glob_to_cdi_mounts",
        lambda pattern: [ConfigMount(host_path=pattern, options=["ro"])],
    )


def _mount_targets(cfg: Config) -> set[str]:
    return {m.container_path for m in cfg.container_edits.mounts}


@pytest.mark.usefixtures("_synthetic_host")
def test_a5_omits_the_host_ranktable():
    """
    A5 loads libhccl_v2, which rejects a 1.0 ranktable with EI0014 rather than
    ignoring it, and nothing here writes a 2.0 one -- so GPUStack must not
    mount it. See the comment at the mount list for the measurement.
    """
    cfg = cdi_ascend.AscendGenerator().generate(
        devices=[_ascend_device(0, "Ascend950PR")],
    )

    assert cfg is not None
    targets = _mount_targets(cfg)
    assert cdi_ascend._HCCL_RANKTABLE_PATH not in targets
    # The rest of the vendor's named list is untouched.
    assert "/usr/local/Ascend/driver/topo" in targets
    assert "/usr/local/Ascend/driver/lib64" in targets
    assert "/usr/local/Ascend/driver/include" in targets
    assert "/usr/local/dcmi" in targets
    assert "/usr/local/bin/npu-smi" in targets


@pytest.mark.usefixtures("_synthetic_host")
def test_non_a5_keeps_the_host_ranktable():
    """An older generation's own ranktable is correct for it."""
    cfg = cdi_ascend.AscendGenerator().generate(
        devices=[_ascend_device(0, "Ascend910B4")],
    )

    assert cdi_ascend._HCCL_RANKTABLE_PATH in _mount_targets(cfg)


@pytest.mark.usefixtures("_synthetic_host")
def test_only_a5_gets_the_ub_user_space_mounts():
    """
    Unchanged behaviour, asserted here because the A5 branch was restructured
    around it: libnl and friends are ordinary system libraries, and mounting
    them for an older generation would shadow what its image ships with.
    """
    a5 = cdi_ascend.AscendGenerator().generate(
        devices=[_ascend_device(0, "Ascend950PR")],
    )
    a2 = cdi_ascend.AscendGenerator().generate(
        devices=[_ascend_device(0, "Ascend910B4")],
    )

    assert "/usr/lib64/liburma*" in _mount_targets(a5)
    assert "/usr/lib64/liburma*" not in _mount_targets(a2)


# ---------------------------------------------------------------------------
# The warning -- the only thing that reaches the default Env path.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name, version, soc_name, expected",
    [
        ("a5 with a stale A2 table", "1.0", "Ascend950PR", True),
        ("a5 with an A3 table", "1.2", "Ascend950PR", True),
        ("a5 with the right table", "2.0", "Ascend950PR", False),
        # An absent file is the healthy host: a node that never ran
        # mindcluster-tools has nothing to be wrong.
        ("a5 with no table at all", None, "Ascend950PR", False),
        # Only the A5 row of the generation mapping has a measured failure.
        ("a2 with its own table", "1.0", "Ascend910B4", False),
    ],
)
def test_warn_incompatible_ranktable(name, version, soc_name, expected, monkeypatch):
    monkeypatch.setattr(cdi_ascend, "read_ranktable_version", lambda: version)
    monkeypatch.setattr(
        cdi_ascend,
        "detect_devices",
        # Scoped to Ascend: the warning must not pay for a whole-host probe.
        lambda manufacturer: (
            [_ascend_device(0, soc_name)]
            if manufacturer == ManufacturerEnum.ASCEND
            else []
        ),
    )
    cdi_ascend.warn_incompatible_ranktable.cache_clear()

    msg = cdi_ascend.warn_incompatible_ranktable()

    assert (msg is not None) == expected, f"case {name}"
    if expected:
        assert "EI0014" in msg, "the warning must name the error the operator sees"


def test_warn_incompatible_ranktable_does_not_detect_without_the_file(monkeypatch):
    """A host without the file must not pay for device detection."""
    monkeypatch.setattr(cdi_ascend, "read_ranktable_version", lambda: None)

    calls = []

    def _detect(manufacturer):
        calls.append(manufacturer)
        return []

    monkeypatch.setattr(cdi_ascend, "detect_devices", _detect)
    cdi_ascend.warn_incompatible_ranktable.cache_clear()

    assert cdi_ascend.warn_incompatible_ranktable() is None
    assert calls == [], "detection must not run when there is no ranktable"


def test_warn_incompatible_ranktable_warns_once(monkeypatch):
    """It runs per deploy, so it has to stay quiet after the first time."""
    monkeypatch.setattr(cdi_ascend, "read_ranktable_version", lambda: "1.0")
    monkeypatch.setattr(
        cdi_ascend,
        "detect_devices",
        lambda manufacturer: [_ascend_device(0, "Ascend950PR")],  # noqa: ARG005
    )
    cdi_ascend.warn_incompatible_ranktable.cache_clear()

    warnings = []
    monkeypatch.setattr(cdi_ascend.logger, "warning", warnings.append)

    for _ in range(3):
        cdi_ascend.warn_incompatible_ranktable()

    assert len(warnings) == 1, f"warned {len(warnings)} times, expected once"


@pytest.mark.parametrize(
    "name, content, expected",
    [
        ("well formed", '{"version": "1.0"}', "1.0"),
        ("no version key", '{"server_count": "1"}', None),
        ("not an object", "[]", None),
        ("not json", "{", None),
        ("version not a string", '{"version": 1.0}', None),
    ],
)
def test_read_ranktable_version(name, content, expected, tmp_path):
    path = tmp_path / "hccl_rootinfo.json"
    path.write_text(content, encoding="utf-8")

    assert cdi_ascend.read_ranktable_version(str(path)) == expected, f"case {name}"


def test_read_ranktable_version_missing_file(tmp_path):
    assert cdi_ascend.read_ranktable_version(str(tmp_path / "absent.json")) is None


def test_the_warning_reads_the_path_the_mount_list_decides_on():
    """
    Both resolve the ranktable as this process sees it, so they cannot
    disagree about whether it exists. A tmp_path test cannot catch a divergence
    here -- it has to assert on the path the code itself reaches for.
    """
    default = (
        inspect.signature(cdi_ascend.read_ranktable_version).parameters["path"].default
    )

    assert default == cdi_ascend._HCCL_RANKTABLE_PATH
    assert cdi_ascend._HCCL_RANKTABLE_PATH == "/etc/hccl_rootinfo.json"
