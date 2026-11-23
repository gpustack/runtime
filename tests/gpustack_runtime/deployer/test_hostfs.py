import errno
import os
import stat
from pathlib import Path

import pytest

from gpustack_runtime import envs
from gpustack_runtime.deployer.__hostfs__ import (
    is_host_root_mount_path,
    normalize_host_path,
    open_host_root,
)


def _make_host_root(tmp_path: Path) -> Path:
    root = tmp_path / "host"
    for marker in ("etc", "proc", "usr"):
        (root / marker).mkdir(parents=True)
    return root


def test_open_host_root_validates_markers(tmp_path):
    root = _make_host_root(tmp_path)
    with open_host_root(str(root)) as hfs:
        assert stat.S_ISDIR(hfs.stat("/etc").st_mode)

    # Missing marker fails.
    incomplete = tmp_path / "incomplete"
    (incomplete / "etc").mkdir(parents=True)
    (incomplete / "proc").mkdir(parents=True)
    with pytest.raises(FileNotFoundError):
        open_host_root(str(incomplete))

    # Non-directory marker fails.
    bad_marker = tmp_path / "bad_marker"
    (bad_marker / "etc").mkdir(parents=True)
    (bad_marker / "proc").mkdir(parents=True)
    (bad_marker / "usr").write_text("not a dir", encoding="utf-8")
    with pytest.raises(NotADirectoryError):
        open_host_root(str(bad_marker))

    # Empty path fails.
    with pytest.raises(ValueError, match="must not be empty"):
        open_host_root("")


def test_normalize_host_path():
    assert normalize_host_path("/") == []
    assert normalize_host_path("///") == []
    assert normalize_host_path("/etc//hccl_rootinfo.json/") == [
        "etc",
        "hccl_rootinfo.json",
    ]
    assert normalize_host_path("/./lib/./route.conf") == ["lib", "route.conf"]

    with pytest.raises(ValueError, match="must not be empty"):
        normalize_host_path("")
    with pytest.raises(ValueError, match="NUL"):
        normalize_host_path("/etc/a\x00b")
    with pytest.raises(ValueError, match=r"\.\."):
        normalize_host_path("/etc/../etc/hccl_rootinfo.json")


def test_hostfs_resolves_intermediate_symlinks_inside_root(tmp_path):
    root = _make_host_root(tmp_path)
    (root / "usr" / "lib").mkdir(parents=True)
    (root / "usr" / "lib" / "route.conf").write_text("route\n", encoding="utf-8")

    # Absolute symlink /lib -> /usr/lib resolved strictly inside root.
    (root / "lib").symlink_to("/usr/lib")
    # Relative symlink lib-rel -> usr/lib resolved strictly inside root.
    (root / "lib-rel").symlink_to("usr/lib")

    with open_host_root(str(root)) as hfs:
        assert stat.S_ISREG(hfs.lstat("/lib/route.conf").st_mode)
        assert stat.S_ISREG(hfs.lstat("/lib-rel/route.conf").st_mode)
        assert hfs.read_regular_file("/lib/route.conf", 1024) == b"route\n"
        assert hfs.read_regular_file("/lib-rel/route.conf", 1024) == b"route\n"


def test_hostfs_rejects_leaf_symlink_directory_fifo_and_oversized(tmp_path):
    root = _make_host_root(tmp_path)
    real_file = root / "etc" / "real.json"
    real_file.write_text('{"version": "2.0"}', encoding="utf-8")
    (root / "etc" / "link.json").symlink_to("real.json")
    (root / "etc" / "subdir").mkdir()
    os.mkfifo(root / "etc" / "pipe")

    exact = b"x" * 16
    (root / "etc" / "exact.bin").write_bytes(exact)
    (root / "etc" / "too_big.bin").write_bytes(b"x" * 17)

    with open_host_root(str(root)) as hfs:
        assert stat.S_ISLNK(hfs.lstat("/etc/link.json").st_mode)
        assert not stat.S_ISREG(hfs.lstat("/etc/link.json").st_mode)

        with pytest.raises(OSError, match="not a regular file"):
            hfs.read_regular_file("/etc/link.json", 1024)
        with pytest.raises(OSError, match="not a regular file"):
            hfs.read_regular_file("/etc/subdir", 1024)
        with pytest.raises(OSError, match="not a regular file"):
            hfs.read_regular_file("/etc/pipe", 1024)

        assert hfs.read_regular_file("/etc/exact.bin", 16) == exact
        with pytest.raises(ValueError, match="exceeds limit"):
            hfs.read_regular_file("/etc/too_big.bin", 16)


def test_hostfs_blocks_symlink_escape_and_loops(tmp_path):
    outside = tmp_path / "secret.txt"
    outside.write_text("secret", encoding="utf-8")

    root = _make_host_root(tmp_path)
    (root / "escape").symlink_to("../")
    (root / "loop_a").symlink_to("loop_b")
    (root / "loop_b").symlink_to("loop_a")

    with open_host_root(str(root)) as hfs:
        with pytest.raises(PermissionError, match="escapes host root"):
            hfs.lstat("/escape/secret.txt")

        with pytest.raises(OSError, match="symlink hops") as exc_info:
            hfs.lstat("/loop_a/file")
        assert exc_info.value.errno == errno.ELOOP


def test_is_host_root_mount_path(monkeypatch):
    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT", "/host")
    assert is_host_root_mount_path("/host")
    assert is_host_root_mount_path("/host/")
    assert is_host_root_mount_path("//host//")
    assert not is_host_root_mount_path("/host/data")
    assert not is_host_root_mount_path("/etc")
    assert not is_host_root_mount_path("")
    assert not is_host_root_mount_path(None)

    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT", "/custom-host/")
    assert is_host_root_mount_path("/custom-host")
    assert not is_host_root_mount_path("/host")

    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT", "")
    assert not is_host_root_mount_path("/host")

    monkeypatch.setattr(envs, "GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT", "/")
    assert not is_host_root_mount_path("/")
