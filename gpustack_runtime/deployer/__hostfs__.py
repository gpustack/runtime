from __future__ import annotations as __future_annotations__

import errno
import os
import posixpath
import stat
from typing import Self

from .. import envs

DEFAULT_HOST_ROOT = "/host"
"""
Default container mount path where the host root filesystem is mounted read-only.
"""

_MAX_SYMLINK_HOPS = 40
"""
Maximum number of symbolic link expansions allowed per host-path resolution.
"""

_HOST_ROOT_MARKERS = ("etc", "proc", "usr")
"""
Top-level directories required inside a valid Linux host root filesystem.
"""

_READ_CHUNK_SIZE = 65536


def normalize_mount_path(path: str | None) -> str:
    """
    Normalize a POSIX container mount target path for equality comparisons.

    Args:
        path:
            The mount path to normalize.

    Returns:
        The normalized POSIX path, or an empty string if path is empty.

    """
    if not path:
        return ""
    normalized = posixpath.normpath(path)
    if normalized.startswith("//"):
        normalized = "/" + normalized.lstrip("/")
    return normalized


def is_host_root_mount_path(
    path: str | None,
    host_root: str | None = None,
) -> bool:
    """
    Report whether a container mount destination matches the configured host root.

    Args:
        path:
            The container mount destination to check.
        host_root:
            Optional host-root mount path override. Defaults to
            ``envs.GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT``.

    Returns:
        True if the destination matches the configured host root.

    """
    if host_root is None:
        host_root = envs.GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT
    normalized_host_root = normalize_mount_path(host_root)
    if not normalized_host_root or normalized_host_root == "/":
        return False
    return normalize_mount_path(path) == normalized_host_root


def normalize_host_path(host_path: str) -> list[str]:
    """
    Validate and split a native host path without folding ``..`` segments.

    Args:
        host_path:
            The native host path (e.g., ``/etc/hccl_rootinfo.json``).

    Returns:
        A list of clean path segments, or an empty list for ``/``.

    Raises:
        ValueError:
            If host_path is empty, contains a NUL byte, or contains a ``..`` segment.

    """
    if not host_path:
        msg = "Host path must not be empty"
        raise ValueError(msg)
    if "\x00" in host_path:
        msg = f"Host path {host_path!r} contains NUL byte"
        raise ValueError(msg)

    clean_parts: list[str] = []
    for part in host_path.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            msg = f"Host path {host_path!r} must not contain '..' segment"
            raise ValueError(msg)
        clean_parts.append(part)
    return clean_parts


class ReadOnlyHostFS:
    """
    Strictly read-only inspector for a host root filesystem mounted at a prefix.

    Callers pass native host paths (such as ``/etc/hccl_rootinfo.json`` or
    ``/lib/route.conf``) without prepending the mount prefix. Every lookup is
    confined inside the opened root directory descriptor, resolving intermediate
    relative and absolute symbolic links within that root boundary.
    """

    def __init__(self, root_fd: int, root_path: str):
        self._root_fd: int | None = root_fd
        self._root_path = root_path

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def close(self) -> None:
        """
        Release the underlying host root directory descriptor.
        """
        if self._root_fd is not None:
            fd = self._root_fd
            self._root_fd = None
            os.close(fd)

    def _require_root_fd(self) -> int:
        if self._root_fd is None:
            msg = f"Host root {self._root_path!r} is closed"
            raise ValueError(msg)
        return self._root_fd

    def _resolve(
        self,
        host_path: str,
        *,
        follow_leaf: bool,
    ) -> tuple[int, str | None, bool]:
        """
        Walk host_path component-by-component inside the host root.

        Returns:
            A tuple ``(parent_fd, leaf_name, close_parent)``. When
            ``close_parent`` is True, the caller must close ``parent_fd``.
            ``leaf_name`` is None when host_path resolves to the root itself.

        """
        root_fd = self._require_root_fd()
        segments = normalize_host_path(host_path)
        if not segments:
            return root_fd, None, False

        stack: list[int] = [root_fd]
        queue = list(segments)
        hops = 0

        try:
            while queue:
                seg = queue.pop(0)
                if seg in ("", "."):
                    continue
                if seg == "..":
                    if len(stack) == 1:
                        msg = f"Symlink target in {host_path!r} escapes host root"
                        raise PermissionError(msg)
                    os.close(stack.pop())
                    continue

                is_leaf = len(queue) == 0
                if is_leaf and not follow_leaf:
                    close_parent = len(stack) > 1
                    parent_fd = stack.pop() if close_parent else root_fd
                    return parent_fd, seg, close_parent

                cur_fd = stack[-1]
                st = os.lstat(seg, dir_fd=cur_fd)

                if stat.S_ISLNK(st.st_mode):
                    hops += 1
                    if hops > _MAX_SYMLINK_HOPS:
                        raise OSError(
                            errno.ELOOP,
                            f"Exceeded {_MAX_SYMLINK_HOPS} symlink hops while resolving {host_path!r}",
                        )
                    target = os.readlink(seg, dir_fd=cur_fd)
                    if not target or "\x00" in target:
                        msg = f"Invalid symlink target {target!r} while resolving {host_path!r}"
                        raise ValueError(msg)
                    if target.startswith("/"):
                        while len(stack) > 1:
                            os.close(stack.pop())
                    target_parts = [p for p in target.split("/") if p not in ("", ".")]
                    queue = target_parts + queue
                    continue

                if is_leaf:
                    close_parent = len(stack) > 1
                    parent_fd = stack.pop() if close_parent else root_fd
                    return parent_fd, seg, close_parent

                if not stat.S_ISDIR(st.st_mode):
                    raise NotADirectoryError(
                        errno.ENOTDIR,
                        "Not a directory",
                        host_path,
                    )

                next_fd = os.open(
                    seg,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=cur_fd,
                )
                stack.append(next_fd)

            close_parent = len(stack) > 1
            parent_fd = stack.pop() if close_parent else root_fd
            return parent_fd, None, close_parent
        finally:
            for fd in reversed(stack[1:]):
                os.close(fd)

    def stat(self, host_path: str) -> os.stat_result:
        """
        Stat host_path inside the host root, following symlinks inside the root.
        """
        parent_fd, leaf_name, close_parent = self._resolve(host_path, follow_leaf=True)
        try:
            if leaf_name is None:
                return os.fstat(parent_fd)
            return os.lstat(leaf_name, dir_fd=parent_fd)
        finally:
            if close_parent:
                os.close(parent_fd)

    def lstat(self, host_path: str) -> os.stat_result:
        """
        Lstat host_path inside the host root without following the leaf component.

        Intermediate directory symlinks (such as merged-usr ``/lib -> /usr/lib``)
        are resolved inside the host root.
        """
        parent_fd, leaf_name, close_parent = self._resolve(host_path, follow_leaf=False)
        try:
            if leaf_name is None:
                return os.fstat(parent_fd)
            return os.lstat(leaf_name, dir_fd=parent_fd)
        finally:
            if close_parent:
                os.close(parent_fd)

    def read_regular_file(self, host_path: str, max_bytes: int) -> bytes:
        """
        Read up to max_bytes from a regular non-symlink file at host_path.

        Intermediate directory symlinks are resolved inside the host root, while
        a symbolic link, directory, FIFO, device, or file exceeding max_bytes at
        the leaf is rejected.

        Args:
            host_path:
                The native host file path.
            max_bytes:
                Maximum allowed file size in bytes.

        Returns:
            The raw bytes of the file.

        Raises:
            OSError:
                If the path cannot be inspected, is not a regular file, changes
                identity while opening, or cannot be read.
            ValueError:
                If max_bytes is non-positive, the path is invalid, or the file
                exceeds max_bytes.

        """
        if max_bytes <= 0:
            msg = f"max_bytes must be positive, got {max_bytes}"
            raise ValueError(msg)

        parent_fd, leaf_name, close_parent = self._resolve(host_path, follow_leaf=False)
        try:
            if leaf_name is None:
                msg = f"{host_path!r} is not a regular file"
                raise IsADirectoryError(errno.EISDIR, msg, host_path)

            lstat_info = os.lstat(leaf_name, dir_fd=parent_fd)
            if not stat.S_ISREG(lstat_info.st_mode):
                msg = f"{host_path!r} is not a regular file"
                raise OSError(errno.EINVAL, msg, host_path)
            if lstat_info.st_size > max_bytes:
                msg = (
                    f"{host_path!r} size {lstat_info.st_size} exceeds limit {max_bytes}"
                )
                raise ValueError(msg)

            open_flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_NONBLOCK", 0)
            file_fd = os.open(leaf_name, open_flags, dir_fd=parent_fd)
            try:
                fstat_info = os.fstat(file_fd)
                if not stat.S_ISREG(fstat_info.st_mode):
                    msg = f"Opened {host_path!r} is not a regular file"
                    raise OSError(errno.EINVAL, msg, host_path)
                if not os.path.samestat(lstat_info, fstat_info):
                    msg = f"File identity changed while opening {host_path!r}"
                    raise OSError(errno.EIO, msg, host_path)
                if fstat_info.st_size > max_bytes:
                    msg = f"{host_path!r} size {fstat_info.st_size} exceeds limit {max_bytes}"
                    raise ValueError(msg)

                chunks: list[bytes] = []
                total = 0
                limit = max_bytes + 1
                while total < limit:
                    chunk = os.read(file_fd, min(_READ_CHUNK_SIZE, limit - total))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    total += len(chunk)

                if total > max_bytes:
                    msg = f"{host_path!r} grew beyond limit {max_bytes} during read"
                    raise ValueError(msg)

                return b"".join(chunks)
            finally:
                os.close(file_fd)
        finally:
            if close_parent:
                os.close(parent_fd)


def _validate_host_root_markers(hfs: ReadOnlyHostFS, root_path: str) -> None:
    for marker in _HOST_ROOT_MARKERS:
        st = hfs.stat(f"/{marker}")
        if not stat.S_ISDIR(st.st_mode):
            msg = f"Host root {root_path!r} marker {marker!r} is not a directory"
            raise NotADirectoryError(errno.ENOTDIR, msg, marker)


def open_host_root(root_path: str | None = None) -> ReadOnlyHostFS:
    """
    Open a host root filesystem and verify that ``etc``, ``proc``, and ``usr``
    exist inside it as directories.

    Args:
        root_path:
            The container path where the host root is mounted. Defaults to
            ``envs.GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT``.

    Returns:
        An open ReadOnlyHostFS instance.

    Raises:
        ValueError:
            If root_path is empty or a required marker is not a directory.
        OSError:
            If root_path or a required marker cannot be opened or stated.

    """
    if root_path is None:
        root_path = envs.GPUSTACK_RUNTIME_DEPLOY_HOST_ROOT
    if not root_path:
        msg = "Host root path must not be empty"
        raise ValueError(msg)

    root_fd = os.open(root_path, os.O_RDONLY | os.O_DIRECTORY)
    hfs = ReadOnlyHostFS(root_fd, root_path)
    try:
        _validate_host_root_markers(hfs, root_path)
    except Exception:
        hfs.close()
        raise

    return hfs
