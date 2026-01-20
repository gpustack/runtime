from pathlib import Path

Healthy = "Healthy"
"""
Healthy means that the device is healthy
"""

Unhealthy = "Unhealthy"
"""
Unhealthy means that the device is unhealthy
"""

Version = "v1beta1"
"""
Version means current version of the API supported by kubelet
"""

DevicePluginPath = Path("/var/lib/kubelet/device-plugins")
"""
DevicePluginPath is the folder the Device Plugin is expecting sockets to be on
Only privileged pods have access to this path
Note: Placeholder until we find a "standard path"
"""

KubeletSocket = DevicePluginPath / "kubelet.sock"
"""
KubeletSocket is the path of the Kubelet registry socket
"""

KubeletPreStartContainerRPCTimeoutInSecs = 30
"""
KubeletPreStartContainerRPCTimeoutInSecs is the timeout duration in secs for PreStartContainer RPC
Timeout duration in secs for PreStartContainer RPC
"""
