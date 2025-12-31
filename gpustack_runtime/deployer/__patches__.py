from __future__ import annotations

import contextlib
import copy
import re
from collections.abc import MutableMapping
from typing import Any

from podman.domain.containers_create import NAMED_VOLUME_PATTERN, CreateMixin
from podman.domain.pods import Pod
from podman.domain.secrets import Secret


# NB(thxCode): Modify from podman-py 5.6.0 source code to fit our need,
# see https://github.com/containers/podman-py/blob/435e7a904b92560b8c0d883ed8994f57eea27171/podman/domain/containers_create.py#L432-L844.
def patch_render_payload(kwargs: MutableMapping[str, Any]) -> dict[str, Any]:
    """Map create/run kwargs into body parameters."""
    args = copy.copy(kwargs)

    if "links" in args:
        if len(args["links"]) > 0:
            msg = "'links' are not supported by Podman service."
            raise ValueError(msg)
        del args["links"]

    # Ignore these keywords
    for key in (
        "cpu_count",
        "cpu_percent",
        "nano_cpus",
        "platform",  # used by caller
        "remove",  # used by caller
        "stderr",  # used by caller
        "stdout",  # used by caller
        "stream",  # used by caller
        "detach",  # used by caller
        "volume_driver",
    ):
        with contextlib.suppress(KeyError):
            del args[key]

    # Handle environment variables
    environment = args.pop("environment", None)
    if environment is not None:
        if isinstance(environment, list):
            try:
                environment = CreateMixin._convert_env_list_to_dict(environment)
            except ValueError as e:
                msg = (
                    "Failed to convert environment variables list to dictionary. "
                    f"Error: {e!s}"
                )
                raise ValueError(
                    msg,
                ) from e
        elif not isinstance(environment, dict):
            msg = (
                "Environment variables must be provided as either a dictionary "
                "or a list of strings in the format ['KEY=value']"
            )
            raise TypeError(
                msg,
            )

    # These keywords are not supported for various reasons.
    unsupported_keys = set(
        args.keys(),
    ).intersection(
        (
            "blkio_weight",
            "blkio_weight_device",  # FIXME In addition to device Major/Minor include path
            "device_cgroup_rules",  # FIXME Where to map for Podman API?
            "device_read_bps",  # FIXME In addition to device Major/Minor include path
            "device_read_iops",  # FIXME In addition to device Major/Minor include path
            "device_requests",  # FIXME In addition to device Major/Minor include path
            "device_write_bps",  # FIXME In addition to device Major/Minor include path
            "device_write_iops",  # FIXME In addition to device Major/Minor include path
            "domainname",
            "network_disabled",  # FIXME Where to map for Podman API?
            "storage_opt",  # FIXME Where to map for Podman API?
            "tmpfs",  # FIXME Where to map for Podman API?
        ),
    )
    if len(unsupported_keys) > 0:
        msg = (
            f"""Keyword(s) '{" ,".join(unsupported_keys)}' are"""
            f""" currently not supported by Podman API."""
        )
        raise TypeError(
            msg,
        )

    def pop(k):
        return args.pop(k, None)

    def normalize_nsmode(
        mode: str | MutableMapping[str, str],
    ) -> dict[str, str]:
        if isinstance(mode, dict):
            return mode
        return {"nsmode": mode}

    def to_bytes(size: int | str | None) -> int | None:
        """
        Converts str or int to bytes.
        Input can be in the following forms :
        0) None - e.g. None -> returns None
        1) int - e.g. 100 == 100 bytes
        2) str - e.g. '100' == 100 bytes
        3) str with suffix - available suffixes:
           b | B - bytes
           k | K = kilobytes
           m | M = megabytes
           g | G = gigabytes
           e.g. '100m' == 104857600 bytes.
        """
        size_type = type(size)
        if size is None:
            return size
        if size_type is int:
            return size
        if size_type is str:
            try:
                return int(size)
            except ValueError as bad_size:
                mapping = {"b": 0, "k": 1, "m": 2, "g": 3}
                mapping_regex = "".join(mapping.keys())
                search = re.search(rf"^(\d+)([{mapping_regex}])$", size.lower())
                if search:
                    return int(search.group(1)) * (1024 ** mapping[search.group(2)])
                msg = f"Passed string size {size} should be in format\\d+[bBkKmMgG] (e.g. '100m')"
                raise TypeError(
                    msg,
                ) from bad_size
        else:
            msg = (
                f"Passed size {size} should be a type of unicode, str "
                f"or int (found : {size_type})"
            )
            raise TypeError(
                msg,
            )

    # Transform keywords into parameters
    params = {
        "annotations": pop("annotations"),  # TODO document, podman only
        "apparmor_profile": pop("apparmor_profile"),  # TODO document, podman only
        "cap_add": pop("cap_add"),
        "cap_drop": pop("cap_drop"),
        "cgroup_parent": pop("cgroup_parent"),
        "cgroups_mode": pop("cgroups_mode"),  # TODO document, podman only
        "cni_networks": [pop("network")],
        "command": args.pop("command", args.pop("cmd", None)),
        "conmon_pid_file": pop("conmon_pid_file"),  # TODO document, podman only
        "containerCreateCommand": pop(
            "containerCreateCommand",
        ),  # TODO document, podman only
        "devices": [],
        "dns_option": pop("dns_opt"),
        "dns_search": pop("dns_search"),
        "dns_server": pop("dns"),
        "entrypoint": pop("entrypoint"),
        "env": environment,
        "env_host": pop("env_host"),  # TODO document, podman only
        "expose": {},
        "groups": pop("group_add"),
        "healthconfig": pop("healthcheck"),
        "health_check_on_failure_action": pop("health_check_on_failure_action"),
        "hostadd": [],
        "hostname": pop("hostname"),
        "httpproxy": pop("use_config_proxy"),
        "idmappings": pop("idmappings"),  # TODO document, podman only
        "image": pop("image"),
        "image_volume_mode": pop("image_volume_mode"),  # TODO document, podman only
        "image_volumes": pop("image_volumes"),  # TODO document, podman only
        "init": pop("init"),
        "init_path": pop("init_path"),
        "isolation": pop("isolation"),
        "labels": pop("labels"),
        "log_configuration": {},
        "lxc_config": pop("lxc_config"),
        "mask": pop("masked_paths"),
        "mounts": [],
        "name": pop("name"),
        "namespace": pop("namespace"),  # TODO What is this for?
        "network_options": pop("network_options"),  # TODO document, podman only
        "networks": pop("networks"),
        "no_new_privileges": pop("no_new_privileges"),  # TODO document, podman only
        "oci_runtime": pop("runtime"),
        "oom_score_adj": pop("oom_score_adj"),
        "overlay_volumes": pop("overlay_volumes"),  # TODO document, podman only
        "portmappings": [],
        "privileged": pop("privileged"),
        "procfs_opts": pop("procfs_opts"),  # TODO document, podman only
        "publish_image_ports": pop("publish_all_ports"),
        "r_limits": [],
        "raw_image_name": pop("raw_image_name"),  # TODO document, podman only
        "read_only_filesystem": pop("read_only"),
        "read_write_tmpfs": pop("read_write_tmpfs"),
        "remove": args.pop("remove", args.pop("auto_remove", None)),
        "resource_limits": {},
        "rootfs": pop("rootfs"),
        "rootfs_propagation": pop("rootfs_propagation"),
        "sdnotifyMode": pop("sdnotifyMode"),  # TODO document, podman only
        "seccomp_policy": pop("seccomp_policy"),  # TODO document, podman only
        "seccomp_profile_path": pop(
            "seccomp_profile_path",
        ),  # TODO document, podman only
        "secrets": [],  # TODO document, podman only
        "selinux_opts": pop("security_opt"),
        "shm_size": to_bytes(pop("shm_size")),
        "static_mac": pop("mac_address"),
        "stdin": pop("stdin_open"),
        "stop_signal": pop("stop_signal"),
        "stop_timeout": pop("stop_timeout"),  # TODO document, podman only
        "sysctl": pop("sysctls"),
        "systemd": pop("systemd"),  # TODO document, podman only
        "terminal": pop("tty"),
        "timezone": pop("timezone"),
        "umask": pop("umask"),  # TODO document, podman only
        "unified": pop("unified"),  # TODO document, podman only
        "unmask": pop("unmasked_paths"),  # TODO document, podman only
        "use_image_hosts": pop("use_image_hosts"),  # TODO document, podman only
        "use_image_resolve_conf": pop(
            "use_image_resolve_conf",
        ),  # TODO document, podman only
        "user": pop("user"),
        "version": pop("version"),
        "volumes": [],
        "volumes_from": pop("volumes_from"),
        "work_dir": pop("workdir") or pop("working_dir"),
    }

    for device in args.pop("devices", []):
        params["devices"].append({"path": device})

    for item in args.pop("exposed_ports", []):
        port, protocol = item.split("/")
        params["expose"][int(port)] = protocol

    for hostname, ip in args.pop("extra_hosts", {}).items():
        params["hostadd"].append(f"{hostname}:{ip}")

    if "log_config" in args:
        params["log_configuration"]["driver"] = args["log_config"].get("Type")

        if "Config" in args["log_config"]:
            params["log_configuration"]["path"] = args["log_config"]["Config"].get(
                "path",
            )
            params["log_configuration"]["size"] = args["log_config"]["Config"].get(
                "size",
            )
            params["log_configuration"]["options"] = args["log_config"]["Config"].get(
                "options",
            )
        args.pop("log_config")

    for item in args.pop("mounts", []):
        normalized_item = {key.lower(): value for key, value in item.items()}
        mount_point = {
            "destination": normalized_item.get("target"),
            "options": [],
            "source": normalized_item.get("source"),
            "type": normalized_item.get("type"),
        }

        # some names are different for podman-py vs REST API due to compatibility with docker
        # some (e.g. chown) despite listed in podman-run documentation fails with error
        names_dict = {"read_only": "ro", "chown": "U"}

        options = []
        simple_options = ["propagation", "relabel"]
        bool_options = ["read_only", "U", "chown"]
        regular_options = ["consistency", "mode", "size"]

        for k, v in item.items():
            _k = k.lower()
            option_name = names_dict.get(_k, _k)
            if _k in bool_options and v is True:
                options.append(option_name)
            elif _k in regular_options:
                options.append(f"{option_name}={v}")
            elif _k in simple_options:
                options.append(v)

        mount_point["options"] = options

        params["mounts"].append(mount_point)

    if "pod" in args:
        pod = args.pop("pod")
        if isinstance(pod, Pod):
            pod = pod.id
        params["pod"] = pod  # TODO document, podman only

    def parse_host_port(_container_port, _protocol, _host):
        result = []
        port_map = {"container_port": int(_container_port), "protocol": _protocol}
        if _host is None:
            result.append(port_map)
        elif isinstance(_host, int) or (isinstance(_host, str) and _host.isdigit()):
            port_map["host_port"] = int(_host)
            result.append(port_map)
        elif isinstance(_host, tuple):
            port_map["host_ip"] = _host[0]
            port_map["host_port"] = int(_host[1])
            result.append(port_map)
        elif isinstance(_host, list):
            for host_list in _host:
                host_list_result = parse_host_port(
                    _container_port,
                    _protocol,
                    host_list,
                )
                result.extend(host_list_result)
        elif isinstance(_host, dict):
            _host_port = _host.get("port")
            if _host_port is not None:
                if isinstance(_host_port, int) or (
                    isinstance(_host_port, str) and _host_port.isdigit()
                ):
                    port_map["host_port"] = int(_host_port)
                elif isinstance(_host_port, tuple):
                    port_map["host_ip"] = _host_port[0]
                    port_map["host_port"] = int(_host_port[1])
            if _host.get("range"):
                port_map["range"] = _host.get("range")
            if _host.get("ip"):
                port_map["host_ip"] = _host.get("ip")
            result.append(port_map)
        return result

    for container, host in args.pop("ports", {}).items():
        # avoid redefinition of the loop variable, then ensure it's a string
        str_container = container
        if isinstance(str_container, int):
            str_container = str(str_container)

        if "/" in str_container:
            container_port, protocol = str_container.split("/")
        else:
            container_port, protocol = str_container, "tcp"

        port_map_list = parse_host_port(container_port, protocol, host)
        params["portmappings"].extend(port_map_list)

    if "restart_policy" in args:
        params["restart_policy"] = args["restart_policy"].get("Name")
        params["restart_tries"] = args["restart_policy"].get("MaximumRetryCount")
        args.pop("restart_policy")

    params["resource_limits"]["pids"] = {"limit": args.pop("pids_limit", None)}

    params["resource_limits"]["cpu"] = {
        "cpus": args.pop("cpuset_cpus", None),
        "mems": args.pop("cpuset_mems", None),
        "period": args.pop("cpu_period", None),
        "quota": args.pop("cpu_quota", None),
        "realtimePeriod": args.pop("cpu_rt_period", None),
        "realtimeRuntime": args.pop("cpu_rt_runtime", None),
        "shares": args.pop("cpu_shares", None),
    }

    params["resource_limits"]["memory"] = {
        "disableOOMKiller": args.pop("oom_kill_disable", None),
        "kernel": to_bytes(args.pop("kernel_memory", None)),
        "kernelTCP": args.pop("kernel_memory_tcp", None),
        "limit": to_bytes(args.pop("mem_limit", None)),
        "reservation": to_bytes(args.pop("mem_reservation", None)),
        "swap": to_bytes(args.pop("memswap_limit", None)),
        "swappiness": args.pop("mem_swappiness", None),
        "useHierarchy": args.pop("mem_use_hierarchy", None),
    }

    for item in args.pop("ulimits", []):
        params["r_limits"].append(
            {
                "type": item["Name"],
                "hard": item["Hard"],
                "soft": item["Soft"],
            },
        )

    for item in args.pop("volumes", {}).items():
        key, value = item
        extended_mode = value.get("extended_mode", [])
        if not isinstance(extended_mode, list):
            msg = "'extended_mode' value should be a list"
            raise ValueError(msg)

        options = extended_mode
        mode = value.get("mode")
        if mode is not None:
            if not isinstance(mode, str):
                msg = "'mode' value should be a str"
                raise ValueError(msg)
            options.append(mode)

        # The Podman API only supports named volumes through the ``volume`` parameter. Directory
        # mounting needs to happen through the ``mounts`` parameter. Luckily the translation
        # isn't too complicated so we can just do it for the user if we suspect that the key
        # isn't a named volume.
        if NAMED_VOLUME_PATTERN.match(key):
            volume = {"Name": key, "Dest": value["bind"], "Options": options}
            params["volumes"].append(volume)
        else:
            mount_point = {
                "destination": value["bind"],
                "options": options,
                "source": key,
                "type": "bind",
            }
            params["mounts"].append(mount_point)

    for item in args.pop("secrets", []):
        if isinstance(item, Secret):
            params["secrets"].append({"source": item.id})
        elif isinstance(item, str):
            params["secrets"].append({"source": item})
        elif isinstance(item, dict):
            secret = {}
            secret_opts = ["source", "target", "uid", "gid", "mode"]
            for k, v in item.items():
                if k in secret_opts:
                    secret.update({k: v})
            params["secrets"].append(secret)

    if "secret_env" in args:
        params["secret_env"] = args.pop("secret_env", {})

    if "cgroupns" in args:
        params["cgroupns"] = normalize_nsmode(args.pop("cgroupns"))

    if "ipc_mode" in args:
        params["ipcns"] = normalize_nsmode(args.pop("ipc_mode"))

    if "network_mode" in args:
        params["netns"] = normalize_nsmode(args.pop("network_mode"))

    if "pid_mode" in args:
        params["pidns"] = normalize_nsmode(args.pop("pid_mode"))

    if "userns_mode" in args:
        params["userns"] = normalize_nsmode(args.pop("userns_mode"))

    if "uts_mode" in args:
        params["utsns"] = normalize_nsmode(args.pop("uts_mode"))

    if len(args) > 0:
        raise TypeError(
            "Unknown keyword argument(s): " + " ,".join(f"'{k}'" for k in args),
        )

    return params
