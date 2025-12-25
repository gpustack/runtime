from __future__ import annotations

import contextlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


@dataclass
class PCIDevice:
    vendor: str
    """
    Vendor ID of the PCI device.
    """
    path: str
    """
    Path to the PCI device in sysfs.
    """
    root: str
    """
    Root of the PCI device.
    """
    switches: list[str]
    """
    Switches of the PCI device.
    """
    address: str
    """
    Address of the PCI device.
    """
    class_: bytes
    """
    Class of the PCI device.
    """
    config: bytes
    """
    Device ID of the PCI device.
    """


def get_pci_devices(
    address: list[str] | str | None = None,
    vendor: list[str] | str | None = None,
) -> list[PCIDevice]:
    """
    Get PCI devices.

    Args:
        address: List of PCI addresses or a single address to filter by.
        vendor: List of vendor IDs or a single vendor ID to filter by.

    Returns:
        List of PCIDevice objects.

    """
    pci_devices = []
    sysfs_pci_path = Path("/sys/bus/pci/devices")
    if not sysfs_pci_path.exists():
        return pci_devices

    if address and isinstance(address, str):
        address = [address]
    if vendor and isinstance(vendor, str):
        vendor = [vendor]

    for dev_path in sysfs_pci_path.iterdir():
        dev_address = dev_path.name.lower()
        if address and dev_address not in address:
            continue

        dev_vendor_file = dev_path / "vendor"
        if not dev_vendor_file.exists():
            continue
        with contextlib.suppress(OSError), dev_vendor_file.open("r") as vf:
            dev_vendor = vf.read().strip()
            if vendor and dev_vendor not in vendor:
                continue
        if not dev_vendor:
            continue

        dev_class_file = dev_path / "class"
        dev_config_file = dev_path / "config"
        if not dev_class_file.exists() or not dev_config_file.exists():
            continue

        dev_class, dev_config = None, None
        with contextlib.suppress(OSError):
            with dev_class_file.open("rb") as f:
                dev_class = f.read().strip()
            with dev_config_file.open("rb") as f:
                dev_config = f.read().strip()
        if dev_class is None or dev_config is None:
            continue

        dev_path_resolved = dev_path.resolve()
        dev_switches = []
        dev_root = dev_path_resolved.parent
        while (dev_root != sysfs_pci_path) and (dev_root.name.count(":") == 2):
            dev_switches.append(dev_root.name)
            dev_root = dev_root.parent

        pci_devices.append(
            PCIDevice(
                vendor=dev_vendor,
                path=str(dev_path_resolved),
                root=dev_root.name,
                switches=dev_switches,
                address=dev_address,
                class_=dev_class,
                config=dev_config,
            ),
        )

    return pci_devices


def compare_pci_devices(
    dev_a: PCIDevice | None,
    dev_b: PCIDevice | None,
) -> int:
    """
    Compare two PCI devices.

    Args:
        dev_a:
            The first PCI device.
        dev_b:
            The second PCI device.

    Returns:
        -1 if devices have different roots,
        0 if devices have the same root but different switches,
        1 if devices have the same root and same switches.

    """
    if dev_a and dev_b:
        is_same_root = dev_a.root == dev_b.root
        is_same_switch = is_same_root and len(dev_a.switches) == len(dev_b.switches)
        if is_same_switch:
            for sw_a, sw_b in zip(dev_a.switches, dev_b.switches, strict=False):
                if sw_a != sw_b:
                    is_same_switch = False
                    break
        if is_same_switch:
            return 1
        if is_same_root:
            return 0
    return -1


@dataclass
class DeviceFile:
    path: str
    """
    Path to the device file.
    """
    number: int | None = None
    """
    Number of the device file.
    """


def get_device_files(pattern: str, directory: Path | str = "/dev") -> list[DeviceFile]:
    r"""
    Get device files with the given pattern.

    Args:
        pattern:
            Pattern of the device files to search for.
            Pattern must include a regex group for the number,
            e.g nvidia(?P<number>\d+).
        directory:
            Directory to search for device files,
            e.g /dev.

    Returns:
        List of DeviceFile objects.

    """
    if "(?P<number>" not in pattern:
        msg = "Pattern must include a regex group for the number, e.g nvidia(?P<number>\\d+)."
        raise ValueError(msg)

    if isinstance(directory, str):
        directory = Path(directory)

    device_files = []
    if not directory.exists():
        return device_files

    regex = re.compile(f"^{directory!s}/{pattern}$")
    for file_path in directory.iterdir():
        matched = regex.match(str(file_path))
        if not matched:
            continue
        file_number = matched.group("number")
        try:
            file_number = int(file_number)
        except ValueError:
            file_number = None
        device_files.append(
            DeviceFile(
                path=str(file_path),
                number=file_number,
            ),
        )

    # Sort by number in ascending order, None values at the end
    return sorted(
        device_files,
        key=lambda df: (df.number is None, df.number),
    )


def support_command(command: str) -> bool:
    """
    Determine whether a command is available.

    Args:
        command:
            The name of the command to check.

    Returns:
        True if the command is available, False otherwise.

    """
    return shutil.which(command) is not None


def execute_shell_command(command: str, cwd: str | None = None) -> str | None:
    """
    Execute a shell command and return its output.

    Args:
        command:
            The command to run.
        cwd:
            The working directory to run the command in, or None to use a temporary directory.

    Returns:
        The output of the command.

    Raises:
        If the command fails or returns a non-zero exit code.

    """
    if cwd is None:
        cwd = tempfile.gettempdir()

    command = command.strip()
    if not command:
        msg = "Command is empty"
        raise ValueError(msg)

    try:
        result = subprocess.run(  # noqa: S602
            command,
            capture_output=True,
            check=False,
            shell=True,
            text=True,
            cwd=cwd,
            encoding="utf-8",
        )
    except Exception as e:
        msg = f"Failed to run command '{command}'"
        raise RuntimeError(msg) from e
    else:
        if result.returncode != 0:
            msg = f"Unexpected result: {result}"
            raise RuntimeError(msg)

        return result.stdout


def execute_command(command: list[str], cwd: str | None = None) -> str | None:
    """
    Execute a command and return its output.

    Args:
        command:
            The command to run.
        cwd:
            The working directory to run the command in, or None to use a temporary directory.

    Returns:
        The output of the command.

    Raises:
        If the command fails or returns a non-zero exit code.

    """
    if cwd is None:
        cwd = tempfile.gettempdir()

    if not command:
        msg = "Command list is empty"
        raise ValueError(msg)

    try:
        result = subprocess.run(  # noqa: S603
            command,
            capture_output=True,
            check=False,
            text=True,
            cwd=cwd,
            encoding="utf-8",
        )
    except Exception as e:
        msg = f"Failed to run command '{command}'"
        raise RuntimeError(msg) from e
    else:
        if result.returncode != 0:
            msg = f"Unexpected result: {result}"
            raise RuntimeError(msg)

        return result.stdout


def safe_int(value: Any, default: int = 0) -> int:
    """
    Safely convert a value to int.

    Args:
        value:
            The value to convert.
        default:
            The default value to return if conversion fails.

    Returns:
        The converted int value, or 0 if conversion fails.

    """
    if value is None:
        return default
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    """
    Safely convert a value to float.

    Args:
        value:
            The value to convert.
        default:
            The default value to return if conversion fails.

    Returns:
        The converted float value, or 0.0 if conversion fails.

    """
    if value is None:
        return default
    if isinstance(value, float):
        return value
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_bool(value: Any, default: bool = False) -> bool:
    """
    Safely convert a value to bool.

    Args:
        value:
            The value to convert.
        default:
            The default value to return if conversion fails.

    Returns:
        The converted bool value, or False if conversion fails.

    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        value_lower = value.strip().lower()
        if value_lower in ("true", "1", "yes", "on"):
            return True
        if value_lower in ("false", "0", "no", "off"):
            return False
    try:
        return bool(value)
    except (ValueError, TypeError):
        return default


def safe_str(value: Any, default: str = "") -> str:
    """
    Safely convert a value to str.

    Args:
        value:
            The value to convert.
        default:
            The default value to return if conversion fails.

    Returns:
        The converted str value, or an empty string if conversion fails.

    """
    if value is None:
        return default
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8", errors="ignore")
        except (ValueError, TypeError):
            return default
    try:
        return str(value)
    except (ValueError, TypeError):
        return default


def kibibyte_to_mebibyte(value: int) -> int:
    """
    Convert KiB to MiB.

    Args:
        value:
            The value in kilobytes.

    Returns:
        The value in MiB, or 0 if the input is None or negative.

    """
    if value is None or value < 0:
        return 0

    try:
        return value >> 10
    except (ValueError, TypeError, OverflowError):
        return 0


def byte_to_mebibyte(value: int) -> int:
    """
    Convert bytes to MiB.

    Args:
        value:
            The value in bytes.

    Returns:
        The value in MiB, or 0 if the input is None or negative.

    """
    if value is None or value < 0:
        return 0

    try:
        return value >> 20
    except (ValueError, TypeError, OverflowError):
        return 0


def stringify_uuid(data: bytes) -> str:
    """
    Convert a UUID in bytes to a string representation.

    Args:
        data:
            The UUID in bytes.

    Returns:
        The string representation of the UUID.

    """
    return str(uuid.UUID(bytes=data))


def get_brief_version(version: str | None) -> str | None:
    """
    Get a brief version string,
    e.g., "11.2.152" -> "11.2".

    Args:
        version:
            The full version string.

    Returns:
        The brief version string, or None if the input is None or empty.

    """
    if not version:
        return None

    splits = version.split(".", 3)
    if len(splits) >= 2:
        return ".".join(splits[:2])
    if len(splits) == 1:
        return splits[0]
    return None


def get_utilization(used: int | None, total: int | None) -> float:
    """
    Calculate utilization percentage.

    Args:
        used:
            The used value.
        total:
            The total value.

    Returns:
        The utilization percentage, rounded to two decimal places.

    """
    if used is None or total is None or used < 0 or total <= 0:
        return 0.0
    try:
        result = (used / total) * 100
    except (OverflowError, ZeroDivisionError):
        return 0.0
    return round(result, 2)


def get_memory() -> tuple[int, int]:
    """
    Get total and used memory in MiB on Linux systems.
    Refer to https://docs.nvidia.com/dgx/dgx-spark/known-issues.html.

    Returns:
        A tuple containing total and used memory in MiB.
        If unable to read /proc/meminfo, returns (0, 0).

    """
    try:
        with Path("/proc/meminfo").open() as f:
            mem_total_kb = -1
            mem_available_kb = -1
            swap_total_kb = -1
            swap_free_kb = -1
            huge_tlb_total_pages = -1
            huge_tlb_free_pages = -1
            huge_tlb_page_size = -1

            for line in f:
                line = line.strip()  # noqa: PLW2901

                if line.startswith("MemTotal:"):
                    with contextlib.suppress(ValueError, IndexError):
                        mem_total_kb = int(line.split()[1])
                elif line.startswith("MemAvailable:"):
                    with contextlib.suppress(ValueError, IndexError):
                        mem_available_kb = int(line.split()[1])
                elif line.startswith("SwapTotal:"):
                    with contextlib.suppress(ValueError, IndexError):
                        swap_total_kb = int(line.split()[1])
                elif line.startswith("SwapFree:"):
                    with contextlib.suppress(ValueError, IndexError):
                        swap_free_kb = int(line.split()[1])
                elif line.startswith("HugePages_Total:"):
                    with contextlib.suppress(ValueError, IndexError):
                        huge_tlb_total_pages = int(line.split()[1])
                elif line.startswith("HugePages_Free:"):
                    with contextlib.suppress(ValueError, IndexError):
                        huge_tlb_free_pages = int(line.split()[1])
                elif line.startswith("Hugepagesize:"):
                    with contextlib.suppress(ValueError, IndexError):
                        huge_tlb_page_size = int(line.split()[1])

                if (
                    mem_total_kb != -1
                    and mem_available_kb != -1
                    and swap_total_kb != -1
                    and swap_free_kb != -1
                    and huge_tlb_total_pages != -1
                    and huge_tlb_free_pages != -1
                    and huge_tlb_page_size != -1
                ):
                    break

            if huge_tlb_total_pages not in (0, -1):
                mem_available_kb = huge_tlb_free_pages * huge_tlb_page_size
                swap_free_kb = 0

            mem_total_kb = mem_total_kb + swap_total_kb
            mem_available_kb = mem_available_kb + swap_free_kb
            mem_used_kb = mem_total_kb - mem_available_kb

            return (
                kibibyte_to_mebibyte(mem_total_kb),
                kibibyte_to_mebibyte(mem_used_kb),
            )

    except OSError:
        return 0, 0


@lru_cache(maxsize=1)
def get_bits_size() -> int:
    """
    Get the system bit size.

    Returns:
        The system bit size (32 or 64).

    """
    if sys.maxsize > 2**32:
        return 64
    return 32


@lru_cache(maxsize=1)
def get_cpu_size():
    """
    Get the number of CPUs.

    Returns:
        The number of CPUs available.

    """
    n_proc = os.cpu_count()
    if n_proc:
        return n_proc

    return os.sysconf("SC_NPROCESSORS_CONF")


@lru_cache(maxsize=1)
def get_cpuset_size() -> int:
    """
    Get the CPU set size.

    Returns:
        The number of the available CPU set size.

    """
    n_cpus = get_cpu_size()
    n_bits = get_bits_size()
    return (n_cpus + n_bits) // n_bits


@lru_cache(maxsize=1)
def get_numa_node_size():
    """
    Get the number of NUMA nodes.

    Returns:
        The number of NUMA nodes available.

    """
    max_node = 0

    with contextlib.suppress(Exception):
        with Path("/sys/devices/system/node/possible").open("r") as f:
            s = f.read().strip()
        for part in s.split(","):
            if "-" in part:
                _, hi = part.split("-")
                max_node = max(max_node, int(hi))
            else:
                max_node = max(max_node, int(part))

    return max_node + 1


@lru_cache(maxsize=1)
def get_numa_nodeset_size() -> int:
    """
    Get the NUMA node set size.

    Returns:
        The number of the available NUMA node set size.

    """
    n_nodes = get_numa_node_size()
    n_bits = get_bits_size()

    return (n_nodes + n_bits) // n_bits


@lru_cache(maxsize=1)
def get_cpu_numa_node_mapping() -> list[int | None]:
    """
    Map CPU cores to NUMA nodes.
    The index corresponds to the CPU core number,
    and the value is the NUMA node the core belongs to.

    Returns:
        A list mapping CPU cores to NUMA nodes.
        If failed to detect the NUMA node of a CPU core, its corresponding value is `None`.

    """
    n_cpus = get_cpu_size()
    n_nodes = get_numa_node_size()

    mapping: list[int | None] = [None] * n_cpus
    for i in range(n_nodes):
        with contextlib.suppress(Exception):
            with Path(f"/sys/devices/system/node/node{i}/cpulist").open("r") as f:
                s = f.read().strip()
            for part in s.split(","):
                if "-" in part:
                    lo, hi = part.split("-")
                    for cpu in range(int(lo), int(hi) + 1):
                        mapping[cpu] = i
                else:
                    cpu = int(part)
                    mapping[cpu] = i

    return mapping


@lru_cache(maxsize=1)
def get_numa_node_cpu_mapping() -> dict[int, list[int]]:
    """
    Map NUMA nodes to CPU cores.
    The key corresponds to the NUMA node number,
    and the value is the list of CPU cores that belong to that NUMA node.

    Returns:
        A dictionary mapping NUMA nodes to lists of CPU cores.

    """
    cpu_numa_mapping = get_cpu_numa_node_mapping()

    numa_cpu_mapping: dict[int, list[int]] = {}
    for cpu_idx, numa_node in enumerate(cpu_numa_mapping):
        if numa_node is not None:
            if numa_node not in numa_cpu_mapping:
                numa_cpu_mapping[numa_node] = []
            numa_cpu_mapping[numa_node].append(cpu_idx)
    return numa_cpu_mapping


@lru_cache(maxsize=128)
def get_numa_node_by_bdf(bdf: str) -> str:
    """
    Get the NUMA node for a given PCI device BDF (Bus:Device.Function) address.

    Args:
        bdf:
            The PCI device BDF address (e.g., "0000:00:1f.0").

    Returns:
        The NUMA node index if found, otherwise blank string.

    """
    if bdf:
        with contextlib.suppress(Exception):
            with Path(f"/sys/bus/pci/devices/{bdf}/numa_node").open("r") as f:
                s = f.read().strip()
            numa_node = int(s)
            if numa_node >= 0:
                return str(numa_node)

    return ""


@lru_cache
def map_cpu_affinity_to_numa_node(cpu_affinity: int | str | None) -> str:
    """
    Map CPU affinity to NUMA nodes.

    Args:
        cpu_affinity:
            The CPU affinity as an integer bitmask or a string (e.g., "0-3,5,7-9").

    Returns:
        A comma-separated string of NUMA node indices,
        or blank string if no NUMA nodes are found.

    """
    if cpu_affinity is None:
        return ""

    if isinstance(cpu_affinity, int):
        cpu_indices = bitmask_to_list(cpu_affinity)
    else:
        if not cpu_affinity:
            return ""
        cpu_indices: list[int] = []
        for part in cpu_affinity.split(","):
            if "-" in part:
                lo, hi = part.split("-")
                lo_idx = safe_int(lo, -1)
                hi_idx = safe_int(hi, -1)
                if lo_idx == -1 or hi_idx == -1 or lo_idx > hi_idx:
                    continue
                cpu_indices.extend(list(range(lo_idx, hi_idx + 1)))
            else:
                idx = safe_int(part, -1)
                if idx == -1:
                    continue
                cpu_indices.append(idx)

    cpu_numa_mapping = get_cpu_numa_node_mapping()

    numa_nodes: set[int] = set()
    for cpu_idx in cpu_indices:
        if 0 <= cpu_idx < len(cpu_numa_mapping):
            numa_node = cpu_numa_mapping[cpu_idx]
            if numa_node is not None:
                numa_nodes.add(numa_node)
    if not numa_nodes:
        return ""

    return list_to_range_str(sorted(numa_nodes))


@lru_cache
def map_numa_node_to_cpu_affinity(numa_node: int | str | None) -> str:
    """
    Map NUMA nodes to CPU affinity.

    Args:
        numa_node:
            The NUMA nodes as an integer bitmask or a string (e.g., "0-1,3").

    Returns:
        A comma-separated string of CPU core indices,
        or blank string if no CPU cores are found.

    """
    if numa_node is None:
        return ""

    if isinstance(numa_node, int):
        numa_indices = bitmask_to_list(numa_node)
    else:
        if not numa_node:
            return ""
        numa_indices: list[int] = []
        for part in numa_node.split(","):
            if "-" in part:
                lo, hi = part.split("-")
                lo_idx = safe_int(lo, -1)
                hi_idx = safe_int(hi, -1)
                if lo_idx == -1 or hi_idx == -1 or lo_idx > hi_idx:
                    continue
                numa_indices.extend(list(range(lo_idx, hi_idx + 1)))
            else:
                idx = safe_int(part, -1)
                if idx == -1:
                    continue
                numa_indices.append(idx)

    numa_cpu_mapping = get_numa_node_cpu_mapping()

    cpu_cores: set[int] = set()
    for numa_idx in numa_indices:
        if numa_idx in numa_cpu_mapping:
            cpu_cores.update(numa_cpu_mapping[numa_idx])
    if not cpu_cores:
        return ""

    return list_to_range_str(sorted(cpu_cores))


def bitmask_to_list(bitmask: int, offset: int = 0) -> list[int]:
    """
    Convert a bitmask to a list of set bit indices.

    Args:
        bitmask:
            The bitmask as an integer.
        offset:
            The offset to add to each index.

    Returns:
        A list of indices where the bits are set to 1.

    """
    bits_len = bitmask.bit_length()
    indices = [offset + i for i in range(bits_len) if (bitmask >> i) & 1]
    return indices


def list_to_range_str(indices: list[int]) -> str:
    """
    Convert a list of indices to a comma-separated string with ranges.

    Args:
        indices:
            The list of indices, must be sorted in ascending order.

    Returns:
        A comma-separated string with ranges (e.g., "0,2-4,6").

    """
    if not indices:
        return ""

    if len(indices) == 1:
        return f"{indices[0]}"
    if len(indices) == (indices[-1] - indices[0] + 1):
        return f"{indices[0]}-{indices[-1]}"

    start, end = indices[0], indices[0]
    ranges: list[tuple[int, int]] = []
    for i in indices[1:]:
        if i == end + 1:
            end = i
        else:
            ranges.append((start, end))
            start, end = i, i
    ranges.append((start, end))

    range_str_parts: list[str] = []
    for start, end in ranges:
        if start == end:
            range_str_parts.append(f"{start}")
        else:
            range_str_parts.append(f"{start}-{end}")
    range_str = ",".join(range_str_parts)

    return range_str


def bitmask_to_str(bitmask_list: list) -> str:
    """
    Convert a bitmask to a comma-separated string of set bit indices.

    Args:
        bitmask_list:
            An integer list stores each item in bitmask.

    Returns:
        If a bitmask are contiguous, returns a range string (e.g., "2-5"),
        Otherwise, returns a comma-separated string of indices (e.g., "0,2-4").

    """
    bits_lists = []
    offset = 0
    for bitmask in bitmask_list:
        if bitmask != 0:
            bits_lists.extend(bitmask_to_list(bitmask, offset))
        offset += get_bits_size()

    return list_to_range_str(sorted(bits_lists))
