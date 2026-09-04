from __future__ import annotations as __future_annotations__

import logging

from ...detector import (
    Devices,
    ManufacturerEnum,
    detect_devices,
    filter_devices_by_manufacturer,
)
from ...detector.ascend import get_ascend_cann_variant
from .__types__ import (
    Config,
    ConfigContainerEdits,
    ConfigDevice,
    Generator,
    manufacturer_to_cdi_kind,
    manufacturer_to_runtime_env,
)
from .__utils__ import (
    device_to_cdi_device_node,
    glob_to_cdi_mounts,
    path_to_cdi_device_nodes,
    path_to_cdi_mount,
)

logger = logging.getLogger(__name__)

_A5_CANN_VARIANT = "950"
"""
The CANN variant of the A5 generation, whose UB fabric replaces what the
earlier generations reach through the shared memory device.
"""

_A5_UB_MOUNT_PATTERNS = [
    "/usr/lib64/libummu*",
    "/usr/lib64/liburma*",
    "/usr/lib64/urma",
    "/usr/lib64/libnl*",
    "/usr/bin/urma_admin",
    "/usr/bin/urma_perftest",
    "/usr/bin/urma_ping",
]
"""
The UB user-space libraries an A5 container needs, mirroring the operator's
mount profile for the Ascend950 generation, see
https://gitcode.com/Ascend/mind-cluster/blob/master/component/ascend-common/cdi/mount/profile.go.

These are mounted for the A5 generation only. Some of them -- libnl above all
-- are ordinary system libraries present on any host, so mounting them
unconditionally would shadow what an earlier generation's container ships with
its own image.

The /usr/lib64 prefix is the operator's, and holds on the openEuler and CentOS
hosts an A5 ships on; a Debian-derived host uses /usr/lib/<triplet>, where none
of these match -- hence the log line when a pattern finds nothing.
"""


class AscendGenerator(Generator):
    """
    CDI generator for Ascend devices.
    """

    def __init__(self):
        super().__init__(ManufacturerEnum.ASCEND)

    def generate(
        self,
        devices: Devices | None = None,
        include_all_devices: bool = True,
    ) -> Config | None:
        """
        Generate the CDI configuration for Ascend devices.

        Args:
            devices:
                The detected devices.
                If None, all available devices are considered.
            include_all_devices:
                Whether to include a device entry that represents all Ascend devices.

        Returns:
            The Config object, or None if not supported.

        """
        if devices is None:
            devices = detect_devices(manufacturer=self.manufacturer)
        else:
            devices = filter_devices_by_manufacturer(
                devices,
                manufacturer=self.manufacturer,
            )

        if not devices:
            return None

        kind = manufacturer_to_cdi_kind(self.manufacturer)
        if not kind:
            return None

        common_device_nodes = []
        for p in [
            "/dev/davinci_manager_docker",
            "/dev/davinci_manager",
        ]:
            cdn = device_to_cdi_device_node(
                path=p,
                container_path="/dev/davinci_manager",
            )
            if cdn:
                common_device_nodes.append(cdn)
                break
        for p in [
            "/dev/dvpp_cmdlist",
            # UB exposes a directory of device nodes rather than a single one,
            # so every entry below it has to be injected.
            "/dev/uburma",
            "/dev/ummu",
            "/dev/devmm_svm",
            "/dev/hisi_hdc",
        ]:
            common_device_nodes.extend(path_to_cdi_device_nodes(path=p))
        if not common_device_nodes:
            return None

        # Device.appendix defaults to None and callers pass their own devices
        # in, so every read goes through `or {}`.
        is_a5 = any(
            get_ascend_cann_variant((dev.appendix or {}).get("arch_family"))
            == _A5_CANN_VARIANT
            for dev in devices
            if dev
        )

        if is_a5:
            # A5's UB fabric management lives under the driver tree (ube_mgmt,
            # device), siblings of lib64 that the per-subdir profile omits, so
            # HCCL cannot init UB -- mount the whole driver. hccl_rootinfo.json
            # is dropped: nothing here generates it, and a stale host copy makes
            # rootInfo detection fail.
            mount_paths = [
                "/usr/local/Ascend/driver",
                "/usr/local/dcmi",
                "/usr/local/bin/npu-smi",
                "/var/queue_schedule",
            ]
        else:
            mount_paths = [
                "/etc/hccl_rootinfo.json",
                "/usr/local/Ascend/driver/topo",
                "/usr/local/Ascend/driver/lib64",
                "/usr/local/Ascend/driver/include",
                "/usr/local/dcmi",
                "/usr/local/bin/npu-smi",
                "/var/queue_schedule",
            ]

        common_mounts = []
        for p in mount_paths:
            cm = path_to_cdi_mount(
                path=p,
            )
            if cm:
                common_mounts.append(cm)

        if is_a5:
            for pattern in _A5_UB_MOUNT_PATTERNS:
                ub_mounts = glob_to_cdi_mounts(pattern=pattern)
                if not ub_mounts:
                    # Otherwise this surfaces later as a container that cannot
                    # reach the UB fabric.
                    logger.debug(
                        "No UB library matched %s, an A5 container will lack it",
                        pattern,
                    )
                common_mounts.extend(ub_mounts)

        cdi_devices: list[ConfigDevice] = []

        all_device_nodes = []

        for dev in devices:
            if not dev:
                continue

            container_device_nodes = []

            # The device node is numbered by the driver's physical id, which
            # Device.index no longer carries: it is the detector's enumeration
            # index, i.e. the DCMI logic id here. The two are different
            # numbers, so a device without a physical id is skipped instead of
            # addressed by the index, which would resolve to another NPU's
            # node. The detector already drops such a device; this guards the
            # devices a caller passes in.
            cdn_number = (dev.appendix or {}).get("physical_id")
            if cdn_number is None:
                continue
            cdn_path = f"/dev/davinci{cdn_number}"
            cdn = device_to_cdi_device_node(
                path=cdn_path,
            )
            if not cdn:
                continue
            all_device_nodes.append(cdn)
            container_device_nodes.append(cdn)

            # Add specific container edits for each device.
            cdi_devices.append(
                ConfigDevice(
                    name=str(dev.index),
                    container_edits=ConfigContainerEdits(
                        device_nodes=container_device_nodes,
                    ),
                ),
            )

        if not cdi_devices:
            return None

        # Add common container edits for all devices.
        if include_all_devices:
            cdi_devices.append(
                ConfigDevice(
                    name="all",
                    container_edits=ConfigContainerEdits(
                        device_nodes=all_device_nodes,
                    ),
                ),
            )

        runtime_env = manufacturer_to_runtime_env(self.manufacturer)

        return Config(
            kind=kind,
            devices=cdi_devices,
            container_edits=ConfigContainerEdits(
                env=[
                    f"{runtime_env}=void",
                ],
                device_nodes=common_device_nodes,
                mounts=common_mounts,
            ),
        )
