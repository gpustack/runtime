from __future__ import annotations as __future_annotations__

import os
import stat
from dataclasses import dataclass
from pathlib import Path

from dataclasses_json import dataclass_json

from .__types__ import ConfigDeviceNode, ConfigMount


@dataclass_json
@dataclass
class LinuxDevice:
    """
    Linux device information.

    """

    path: str
    """
    Path to the device file.
    """
    type: str
    """
    Device type: 'b' for block, 'c' for character, 'p' for pipe.
    """
    major: int
    """
    Major device number.
    """
    minor: int
    """
    Minor device number.
    """
    file_mode: int | None = None
    """
    File mode (permissions) of the device.
    """
    uid: int | None = None
    """
    User ID of the device owner.
    """
    gid: int | None = None
    """
    Group ID of the device owner.
    """


def linux_device_from_path(path: Path | str | None) -> LinuxDevice | None:
    """
    Get the Linux device information for a given path.

    Args:
        path:
            The path to the device file.

    Returns:
        The LinuxDevice object, or None if the path does not exist or is not a device

    """
    if not path:
        return None
    if isinstance(path, str):
        path = Path(path)
    if not path.exists():
        return None

    path_stat = path.lstat()
    if not path_stat:
        return None

    dev_mode = stat.S_IFMT(path_stat.st_mode)
    match dev_mode:
        case stat.S_IFBLK:
            dev_type = "b"
        case stat.S_IFCHR:
            dev_type = "c"
        case stat.S_IFIFO:
            dev_type = "p"
        case _:
            return None

    dev_number = path_stat.st_rdev
    dev_major = os.major(dev_number)
    dev_minor = os.minor(dev_number)

    dev_file_mode = stat.S_IMODE(path_stat.st_mode)

    dev_uid = path_stat.st_uid
    dev_gid = path_stat.st_gid

    return LinuxDevice(
        path=str(path),
        type=dev_type,
        major=dev_major,
        minor=dev_minor,
        file_mode=dev_file_mode,
        uid=dev_uid,
        gid=dev_gid,
    )


def device_to_cdi_device_node(
    path: str,
    container_path: str | None = None,
    permission: str = "rw",
    no_user: bool = False,
) -> ConfigDeviceNode | None:
    """
    Convert a device path to a ConfigDeviceNode.

    Args:
        path:
            Path to the device on the host.
        container_path:
            Path to the device inside the container.
        permission:
            Permissions for the device.
        no_user:
            Whether to omit user and group information.

    Returns:
        The ConfigDeviceNode object.
        None if the device does not exist.

    """
    dev = linux_device_from_path(path)
    if not dev:
        return None

    return ConfigDeviceNode(
        path=dev.path,
        host_path=container_path,
        type_=dev.type,
        major=dev.major,
        minor=dev.minor,
        file_mode=dev.file_mode,
        permissions=permission,
        uid=None if no_user else dev.uid,
        gid=None if no_user else dev.gid,
    )


def path_to_cdi_mount(
    path: str,
    container_path: str | None = None,
    options: list[str] | None = None,
    ignore_notfound: bool = False,
) -> ConfigMount | None:
    """
    Convert a file/directory path to a ConfigMount.

    Args:
        path:
            Path to the file or directory on the host.
        container_path:
            Path to the file or directory inside the container.
        options:
            Mount options.
        ignore_notfound:
            Whether to ignore if the path does not exist.

    Returns:
        The ConfigMount object.
        None if the path does not exist.

    """
    if not Path(path).exists() and not ignore_notfound:
        return None

    if container_path is None:
        container_path = path

    if options is None:
        options = ["ro", "nosuid", "nodev", "rbind", "rprivate"]

    return ConfigMount(
        host_path=path,
        container_path=container_path,
        options=options,
    )
