from __future__ import annotations as __future_annotations__

from abc import ABC, abstractmethod
from functools import lru_cache
from typing import TYPE_CHECKING, Literal

from ... import envs
from ...detector import (
    ManufacturerEnum,
    manufacturer_to_backend,
)
from ..__utils__ import load_yaml_or_json, safe_json, safe_yaml

if TYPE_CHECKING:
    from pathlib import Path

    from ...detector import Devices

_DEFAULT_CDI_VERSION = "0.5.0"


class ConfigDeviceNode(dict):
    """
    CDI device node configuration.

    """

    def __init__(
        self,
        path: str,
        host_path: str | None = None,
        type_: str = "c",
        major: int | None = None,
        minor: int | None = None,
        file_mode: int | None = None,
        permissions: str | None = None,
        uid: int | None = None,
        gid: int | None = None,
    ):
        """
        Initialize a CDI container edit configuration.

        Args:
            path:
                The path inside the container.
            host_path:
                The path on the host system. Optional.
                None means same as path.
            type_:
                The type of the device.
            major:
                The major number of the device. Optional.
            minor:
                The minor number of the device. Optional.
            file_mode:
                The file mode for the device. Optional.
            permissions:
                The permissions for the device. Optional.
                CGroup permissions of the device, candidates are one or more of
                'r' (read), 'w' (write), 'm' (mknod).
                'r': allows container to read from the device.
                'w': allows container to write to the device.
                'm': allows container to create device files that do not yet exist.
            uid:
                The user ID for the device. Optional.
            gid:
                The group ID for the device. Optional.

        """
        if not path:
            msg = "path cannot be empty"
            raise ValueError(msg)

        super().__init__()

        self["path"] = path
        if host_path is not None:
            self["hostPath"] = host_path
        if type_ is not None:
            self["type"] = type_
        if major is not None and minor is not None:
            self["major"] = major
            self["minor"] = minor
        if file_mode is not None:
            self["fileMode"] = file_mode
        if permissions is not None:
            self["permissions"] = permissions
        if uid is not None:
            self["uid"] = uid
        if gid is not None:
            self["gid"] = gid

    @property
    def path(self) -> str:
        """
        Return the path inside the container.

        Returns:
            The path inside the container.

        """
        return self["path"]

    @property
    def host_path(self) -> str | None:
        """
        Return the host path if present.

        Returns:
            The host path if present, else None.

        """
        return self.get("hostPath", None)

    @property
    def permissions(self) -> str | None:
        """
        Return the permissions if present.

        Returns:
            The permissions if present, else None.

        """
        return self.get("permissions", None)

    @property
    def type_(self) -> str:
        """
        Return the type of the device.

        Returns:
            The type of the device.

        """
        return self["type"]

    @property
    def major(self) -> int | None:
        """
        Return the major number if present.

        Returns:
            The major number if present, else None.

        """
        return self.get("major", None)

    @property
    def minor(self) -> int | None:
        """
        Return the minor number if present.

        Returns:
            The minor number if present, else None.

        """
        return self.get("minor", None)

    @property
    def uid(self) -> int | None:
        """
        Return the user ID if present.

        Returns:
            The user ID if present, else None.

        """
        return self.get("uid", None)

    @property
    def gid(self) -> int | None:
        """
        Return the group ID if present.

        Returns:
            The group ID if present, else None.

        """
        return self.get("gid", None)


class ConfigMount(dict):
    """
    CDI mount configuration.

    """

    def __init__(
        self,
        host_path: str,
        container_path: str | None = None,
        type_: str | None = None,
        options: list[str] | None = None,
    ):
        """
        Initialize a CDI mount configuration.

        Args:
            host_path:
                The path on the host system.
            container_path:
                The path inside the container. Optional.
                None means same as host_path.
            type_:
                The mount type. Optional.
            options:
                The mount options. Optional.

        """
        if not host_path:
            msg = "host_path cannot be empty"
            raise ValueError(msg)

        super().__init__()

        self["hostPath"] = host_path
        self["containerPath"] = host_path or container_path
        if type_ is not None:
            self["type"] = type_
        if options is not None:
            self["options"] = options

    @property
    def host_path(self) -> str:
        """
        Return the path on the host system.
        """
        return self["hostPath"]

    @property
    def container_path(self) -> str:
        """
        Return the path inside the container.
        """
        return self["containerPath"]

    @property
    def options(self) -> list[str] | None:
        """
        Return the mount options if present.
        """
        return self.get("options", None)

    @property
    def type_(self) -> str | None:
        """
        Return the mount type if present.
        """
        return self.get("type", None)


class ConfigHook(dict):
    """
    CDI hook configuration.

    """

    def __init__(
        self,
        hook_name: str,
        path: str,
        args: list[str] | None = None,
        env: list[str] | None = None,
        timeout: int | None = None,
    ):
        """
        Initialize a CDI hook configuration.

        Args:
            hook_name:
                The name of the hook.
            path:
                The path to the hook executable.
            args:
                The arguments for the hook. Optional.
            env:
                The environment variables for the hook. Optional.
            timeout:
                The timeout for the hook in seconds. Optional.

        """
        if not hook_name:
            msg = "hook_name cannot be empty"
            raise ValueError(msg)
        if not path:
            msg = "path cannot be empty"
            raise ValueError(msg)

        super().__init__()

        self["hookName"] = hook_name
        self["path"] = path
        if args is not None:
            self["args"] = args
        if env is not None:
            self["env"] = env
        if timeout is not None:
            self["timeout"] = timeout

    @property
    def hook_name(self) -> str:
        """
        Return the name of the hook.
        """
        return self["hookName"]

    @property
    def path(self) -> str:
        """
        Return the path to the hook executable.
        """
        return self["path"]

    @property
    def args(self) -> list[str] | None:
        """
        Return the arguments for the hook if present.
        """
        return self.get("args", None)

    @property
    def env(self) -> list[str] | None:
        """
        Return the environment variables for the hook if present.
        """
        return self.get("env", None)

    @property
    def timeout(self) -> int | None:
        """
        Return the timeout for the hook in seconds if present.
        """
        return self.get("timeout", None)


class ConfigContainerEdits(dict):
    """
    CDI container edits configuration.

    """

    def __init__(
        self,
        env: list[str] | None = None,
        device_nodes: list[ConfigDeviceNode] | None = None,
        mounts: list[ConfigMount] | None = None,
        hooks: list[ConfigHook] | None = None,
    ):
        """
        Initialize a CDI container edits configuration.

        Args:
            env:
                The environment variables to set. Optional.
            device_nodes:
                The device nodes to add. Optional.
            mounts:
                The mounts to add. Optional.
            hooks:
                The hooks to add. Optional.

        """
        if not (device_nodes or mounts or hooks):
            msg = "At least one of device_nodes, mounts, or hooks must be provided"
            raise ValueError(msg)

        super().__init__()

        if env is not None:
            self["env"] = env
        if device_nodes is not None:
            self["deviceNodes"] = device_nodes
        if mounts is not None:
            self["mounts"] = mounts
        if hooks is not None:
            self["hooks"] = hooks

    @property
    def env(self) -> list[str] | None:
        """
        Return the environment variables if present.

        Returns:
            The environment variables if present, else None.

        """
        return self.get("env", None)

    @property
    def device_nodes(self) -> list[ConfigDeviceNode] | None:
        """
        Return the device nodes if present.

        Returns:
            The device nodes if present, else None.

        """
        return self.get("deviceNodes", None)

    @property
    def mounts(self) -> list[ConfigMount] | None:
        """
        Return the mounts if present.

        Returns:
            The mounts if present, else None.

        """
        return self.get("mounts", None)

    @property
    def hooks(self) -> list[ConfigHook] | None:
        """
        Return the hooks if present.

        Returns:
            The hooks if present, else None.

        """
        return self.get("hooks", None)


class ConfigDevice(dict):
    """
    CDI device configuration.

    """

    def __init__(
        self,
        name: str,
        container_edits: ConfigContainerEdits,
        annotations: dict[str, str] | None = None,
    ):
        """
        Initialize a CDI device configuration.

        Args:
            name:
                The name of the device.
            container_edits:
                The container edits for the device.
            annotations:
                Optional annotations for the device.

        """
        super().__init__()

        self["name"] = name
        self["containerEdits"] = container_edits
        if annotations is not None:
            self["annotations"] = annotations

    @property
    def name(self) -> str:
        """
        Return the name of the device.

        Returns:
            The name of the device.

        """
        return self["name"]

    @property
    def container_edits(self) -> ConfigContainerEdits:
        """
        Return the container edits of the device.

        Returns:
            The container edits.

        """
        return self["containerEdits"]

    @property
    def annotations(self) -> dict[str, str] | None:
        """
        Return the annotations of the device.

        Returns:
            The annotations if present, else None.

        """
        return self.get("annotations", None)


class Config(dict):
    """
    CDI configuration.
    """

    @classmethod
    def from_file(cls, path: str | Path, strict: bool = False) -> Config:
        """
        Load a CDI configuration from a file.

        Args:
            path:
                The path to the CDI configuration file.
            strict:
                Whether to enable strict mode.

        Returns:
            The loaded CDI configuration.

        """
        data = load_yaml_or_json(path)
        if isinstance(data, list):
            data_size = len(data)
            if data_size == 0:
                msg = f"Parsed CDI config is empty, check the content of {path}"
                raise RuntimeError(msg)
            if data_size > 1 and strict:
                msg = f"Parsed CDI config has multiple objects, check the content of {path}"
                raise RuntimeError(msg)
            data = data[0]

        return cls(
            kind=data["kind"],
            devices=data["devices"],
            cdi_version=data.get("cdiVersion", _DEFAULT_CDI_VERSION),
            annotations=data.get("annotations", None),
        )

    def __init__(
        self,
        kind: str,
        devices: list[ConfigDevice],
        container_edits: list[ConfigContainerEdits] | None = None,
        cdi_version: str = _DEFAULT_CDI_VERSION,
        annotations: dict[str, str] | None = None,
    ):
        """
        Initialize a CDI configuration.

        Args:
            kind:
                The kind of the CDI configuration.
            devices:
                The list of devices in the CDI configuration.
            container_edits:
                The list of container edits in the CDI configuration.
                Applies to all devices. Optional.
            cdi_version:
                The CDI version. Default is "0.5.0".
            annotations:
                Optional annotations for the CDI configuration.

        """
        super().__init__()

        self["cdiVersion"] = cdi_version
        self["kind"] = kind
        self["devices"] = devices
        if container_edits is not None:
            self["containerEdits"] = container_edits
        if annotations is not None:
            self["annotations"] = annotations

    @property
    def devices(self) -> list[ConfigDevice]:
        """
        Return the list of devices in the CDI configuration.

        Returns:
            The list of devices.

        """
        return self["devices"]

    @property
    def kind(self) -> str:
        """
        Return the kind of the CDI configuration.

        Returns:
            The kind of the CDI configuration.

        """
        return self["kind"]

    @property
    def container_edits(self) -> list[ConfigContainerEdits] | None:
        """
        Return the list of container edits in the CDI configuration.

        Returns:
            The list of container edits if present, else None.

        """
        return self.get("containerEdits", None)

    @property
    def cdi_version(self) -> str:
        """
        Return the CDI version of the configuration.

        Returns:
            The CDI version.

        """
        return self["cdiVersion"]

    @property
    def annotations(self) -> dict[str, str] | None:
        """
        Return the annotations of the CDI configuration.

        Returns:
            The annotations if present, else None.

        """
        return self.get("annotations", None)

    def stringify(self, _format: Literal["yaml", "json"] = "yaml") -> str:
        """
        Stringify the CDI configuration to the specified format.

        Args:
            _format:
                The format of the CDI configuration.
                Either "yaml" or "json". Default is "yaml".

        Returns:
            The string representation of the CDI configuration in the specified format.

        """
        if _format == "yaml":
            return safe_yaml(self, indent=2, sort_keys=False)
        return safe_json(self, indent=2, sort_keys=False)


@lru_cache
def manufacturer_to_cdi_kind(manufacturer: ManufacturerEnum) -> str | None:
    """
    Map a manufacturer to its corresponding CDI config kind,
    based on `GPUSTACK_RUNTIME_DETECT_BACKEND_MAP_RESOURCE_KEY`
    and `GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_CDI` envs.

    It is in form of `{Vendor}/{Class}`.

    Args:
        manufacturer:
            The manufacturer enum.

    Returns:
        The corresponding CDI config kind as a string.
        None if not found.

    """
    backend = manufacturer_to_backend(manufacturer)
    resource_key = envs.GPUSTACK_RUNTIME_DETECT_BACKEND_MAP_RESOURCE_KEY.get(backend)
    if not resource_key:
        return None
    kind = envs.GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_CDI.get(resource_key)
    return kind


@lru_cache
def manufacturer_to_runtime_env(manufacturer: ManufacturerEnum) -> str | None:
    """
    Map a manufacturer to its corresponding runtime environment variable prefix,
    based on `GPUSTACK_RUNTIME_DETECT_BACKEND_MAP_RESOURCE_KEY`
    and `GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_RUNTIME_VISIBLE_DEVICES` envs.

    Args:
        manufacturer:
            The manufacturer enum.

    Returns:
        The corresponding runtime environment variable prefix as a string.
        None if not found.

    """
    backend = manufacturer_to_backend(manufacturer)
    resource_key = envs.GPUSTACK_RUNTIME_DETECT_BACKEND_MAP_RESOURCE_KEY.get(backend)
    if not resource_key:
        return None
    env = envs.GPUSTACK_RUNTIME_DEPLOY_RESOURCE_KEY_MAP_RUNTIME_VISIBLE_DEVICES.get(
        resource_key,
    )
    return env


class Generator(ABC):
    """
    Base class for all CDI generators.
    """

    manufacturer: ManufacturerEnum = ManufacturerEnum.UNKNOWN
    """
    Manufacturer of the detector.
    """

    def __init__(self, manufacturer: ManufacturerEnum):
        self.manufacturer = manufacturer

    @property
    def name(self) -> str:
        """
        Return the name of the generator.

        Returns:
            The name of the generator.

        """
        return str(self.manufacturer)

    @abstractmethod
    def generate(
        self,
        devices: Devices | None = None,
        include_all_devices: bool = True,
    ) -> Config | None:
        """
        Generate the CDI specification.

        Args:
            devices:
                The devices to generate the CDI specification for.
                If None, all available devices are considered.
            include_all_devices:
                Whether to include a device entry that represents all AMD devices.

        Returns:
            The Config object, or None if not supported.

        """
        raise NotImplementedError
