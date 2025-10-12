"""
Copyright © 2024 MetaX Integrated Circuits (Shanghai) Co., Ltd. All Rights Reserved.

This software and associated documentation files (hereinafter collectively referred to as
"Software") is a proprietary commercial software developed by MetaX Integrated Circuits
(Shanghai) Co., Ltd. and/or its affiliates (hereinafter collectively referred to as “MetaX”).
The information presented in the Software belongs to MetaX. Without prior written permission
from MetaX, no entity or individual has the right to obtain a copy of the Software to deal in
the Software, including but not limited to use, copy, modify, merge, disclose, publish,
distribute, sublicense, and/or sell copies of the Software or substantial portions of the Software.

The Software is provided for reference only, without warranty of any kind, either express or
implied, including but not limited to the warranty of merchantability, fitness for any purpose
and/or noninfringement. In no case shall MetaX be liable for any claim, damage or other liability
arising from, out of or in connection with the Software.

If the Software need to be used in conjunction with any third-party software or open source
software, the rights to the third-party software or open source software still belong to the
copyright owners. For details, please refer to the respective notices or licenses. Please comply
with the provisions of the relevant notices or licenses. If the open source software licenses
additionally require the disposal of rights related to this Software, please contact MetaX
immediately and obtain MetaX 's written consent.

MetaX reserves the right, at its sole discretion, to change, modify, add or remove portions of the
Software, at any time. MetaX reserves all the right for the final explanation.
"""

from ctypes import *
from functools import wraps
import sys
import os
import string


## enums ##
_mxSmlDeviceBrand_t = c_uint
MXSML_BRAND_UNKNOWN = 0
MXSML_BRAND_N = 1
MXSML_BRAND_C = 2
MXSML_BRAND_G = 3

_mxSmlDeviceVirtualizationMode_t = c_uint
MXSML_VIRTUALIZATION_MODE_NONE = 0  # Represents bare metal
MXSML_VIRTUALIZATION_MODE_PF = 1  # Physical function after virtualization
MXSML_VIRTUALIZATION_MODE_VF = 2  # Virtualized device

_mxSmlVersionUnit_t = c_uint
MXSML_VERSION_BIOS = 0
MXSML_VERSION_DRIVER = 1
MXSML_VERSION_SMP0 = 2
MXSML_VERSION_SMP1 = 3
MXSML_VERSION_CCX0 = 4
MXSML_VERSION_CCX1 = 5
MXSML_VERSION_CCX2 = 6
MXSML_VERSION_CCX3 = 7  # Only valid for N-class device

_mxSmlPmbusUnit_t = c_uint
MXSML_PMBUS_SOC = 0
MXSML_PMBUS_CORE = 1
MXSML_PMBUS_HBM = 2
MXSML_PMBUS_PCIE = 3
MXSML_PMBUS_HBM2 = 4

_mxSmlTemperatureSensors_t = c_uint
MXSML_TEMPERATURE_HOTSPOT = 0  # Chip max temperature
MXSML_TEMPERATURE_HOTLIMIT = 1  # Chip temperature limit
MXSML_TEMPERATURE_SOC = 2  # Power DrMOS soc
MXSML_TEMPERATURE_CORE = 3  # Power DrMOS core
MXSML_TEMPERATURE_CCX_DNOC = 4  # Deprecated. Do not use
MXSML_TEMPERATURE_CSC_FUSE = 5  # Deprecated. Do not use
MXSML_TEMPERATURE_CCX_DLA_VPUE1_ATH = 6  # Deprecated. Do not use
MXSML_TEMPERATURE_VPUE1 = 7  # Deprecated. Do not use
MXSML_TEMPERATURE_VPUE0 = 8  # Deprecated. Do not use
MXSML_TEMPERATURE_ATUL2 = 9  # Deprecated. Do not use
MXSML_TEMPERATURE_DLA1 = 10  # Chip, Valid for N-class device
MXSML_TEMPERATURE_DLA0 = 11  # Chip, Valid for N-class device
MXSML_TEMPERATURE_EMC0 = 12  # Air inlet, Valid for C-class device
MXSML_TEMPERATURE_EMC1 = 13  # Tdiode, Valid for C-class device
MXSML_TEMPERATURE_SGM = 14  # Air outlet, Valid for C-class device

_mxSmlRasIp_t = c_uint
MXSML_RAS_MC = 0
MXSML_RAS_PCIE = 1
MXSML_RAS_FUSE = 2
MXSML_RAS_G2D = 3
MXSML_RAS_INT = 4
MXSML_RAS_HAG = 5
MXSML_RAS_METALK = 6
MXSML_RAS_SMP0 = 7
MXSML_RAS_SMP1 = 8
MXSML_RAS_CCX0 = 9
MXSML_RAS_CCX1 = 10
MXSML_RAS_CCX2 = 11
MXSML_RAS_CCX3 = 12
MXSML_RAS_DLA0 = 13
MXSML_RAS_DLA1 = 14
MXSML_RAS_VPUE0 = 15
MXSML_RAS_VPUE1 = 16
MXSML_RAS_VPUD0 = 17
MXSML_RAS_VPUD1 = 18
MXSML_RAS_VPUD2 = 19
MXSML_RAS_VPUD3 = 20
MXSML_RAS_VPUD4 = 21
MXSML_RAS_VPUD5 = 22
MXSML_RAS_VPUD6 = 23
MXSML_RAS_VPUD7 = 24
MXSML_RAS_DMA0 = 25
MXSML_RAS_DMA2 = 26
MXSML_RAS_DMA2 = 27
MXSML_RAS_DMA3 = 28
MXSML_RAS_DMA4 = 29
MXSML_RAS_MCCTL0 = 30
MXSML_RAS_MCCTL1 = 31
MXSML_RAS_MCCTL2 = 32
MXSML_RAS_MCCTL3 = 33
MXSML_RAS_DHUB1 = 34
MXSML_RAS_DHUB2 = 35
MXSML_RAS_DHUB3 = 36
MXSML_RAS_DHUB4 = 37
MXSML_RAS_DHUB5 = 38
MXSML_RAS_DHUB6 = 39
MXSML_RAS_DHUB7 = 40
MXSML_RAS_ATH = 41
MXSML_RAS_ATUL20 = 42
MXSML_RAS_ATUL21 = 43
MXSML_RAS_XSC = 44
MXSML_RAS_CE = 45

_mxSmlDpmIp_t = c_uint
MXSML_DPM_DLA = 0  # Valid for N-class device
MXSML_DPM_XCORE = 1  # Valid for C-class device
MXSML_DPM_MC = 2
MXSML_DPM_SOC = 3
MXSML_DPM_DNOC = 4
MXSML_DPM_VPUE = 5
MXSML_DPM_VPUD = 6
MXSML_DPM_HBM = 7
MXSML_DPM_G2D = 8  # Valid for N-class device
MXSML_DPM_HBM_POWER = 9
MXSML_DPM_CCX = 10
MXSML_DPM_IP_GROUP = (
    11  # Valid for N-class device, group of soc, dnoc, vpue, vpud, ccx, g2d
)
MXSML_DPM_DMA = 12
MXSML_DPM_CSC = 13
MXSML_DPM_ETH = 14
MXSML_DPM_RESERVED = 15

_mxSmlPciPowerState_t = c_uint
MXSML_POWER_PCI_STATE_D0 = 0
MXSML_POWER_PCI_STATE_D3HOT = 1
MXSML_POWER_PCI_STATE_D3COLD = 2

_mxSmlClockIp_t = c_uint
MXSML_CLOCK_CSC = 0
MXSML_CLOCK_DLA = 1  # Valid for N-class device
MXSML_CLOCK_MC = 2  # Valid for N-class device
MXSML_CLOCK_MC0 = 3  # Valid for C-class device
MXSML_CLOCK_MC1 = 4  # Valid for C-class device
MXSML_CLOCK_VPUE = 5
MXSML_CLOCK_VPUD = 6
MXSML_CLOCK_SOC = 7
MXSML_CLOCK_DNOC = 8
MXSML_CLOCK_G2D = 9  # Valid for N-class device
MXSML_CLOCK_CCX = 10
MXSML_CLOCK_XCORE = 11  # Valid for C-class device

_mxSmlMetaXLinkType_t = c_uint
MXSML_METAXLINK_INPUT = 0
MXSML_METAXLINK_TARGET = 1

_mxSmlGpuTopologyLevel_t = c_uint
MXSML_TOPOLOGY_INTERNAL = 0
MXSML_TOPOLOGY_SINGLE = 1  # All devices that only need traverse a single PCIe switch
MXSML_TOPOLOGY_MULTIPLE = 2  # All devices that need not traverse a host bridge
MXSML_TOPOLOGY_HOSTBRIDGE = 3  # All devices that are connected to the same host bridge
MXSML_TOPOLOGY_NODE = 4  # All devices that are connected to the same NUMA node but possibly multiple host bridges
MXSML_TOPOLOGY_METAXLINK = 5  # All devices that are connected to the same MetaXLink
MXSML_TOPOLOGY_SYSTEM = 6  # All devices in the system
MXSML_TOPOLOGY_UNDEFINED = 7

_mxSmlUsageIp_t = c_uint
MXSML_USAGE_DLA = 0  # Only valid for N-class device
MXSML_USAGE_VPUE = 1
MXSML_USAGE_VPUD = 2
MXSML_USAGE_G2D = 3  # Only valid for N-class device
MXSML_USAGE_XCORE = 4  # Only valid for C-class device

_mxSmlFwIpName_t = c_uint
MXSML_FW_IPNAME_SMP0 = 0
MXSML_FW_IPNAME_SMP1 = 1
MXSML_FW_IPNAME_CCX0 = 2
MXSML_FW_IPNAME_CCX1 = 3
MXSML_FW_IPNAME_CCX2 = 4
MXSML_FW_IPNAME_CCX3 = 5
MXSML_FW_IPNAME_ALL = 6

_mxSmlLoglevel_t = c_uint
MXSML_LOGLEVEL_NONE = 0
MXSML_LOGLEVEL_FATAL = 1
MXSML_LOGLEVEL_ERROR = 2
MXSML_LOGLEVEL_WARN = 3
MXSML_LOGLEVEL_INFO = 4
MXSML_LOGLEVEL_DEBUG = 5
MXSML_LOGLEVEL_VERBOSE = 6
MXSML_LOGLEVEL_UNKNOWN = 7

_mxSmlPciGen_t = c_uint
MXSML_PCI_GEN_1 = 1
MXSML_PCI_GEN_2 = 2
MXSML_PCI_GEN_3 = 3
MXSML_PCI_GEN_4 = 4
MXSML_PCI_GEN_5 = 5

_mxsmlReturn_t = c_uint
MXSML_SUCCESS = 0
MXSML_ERROR_FAILURE = 1
MXSML_ERROR_NO_DEVICE = 2
MXSML_ERROR_OPERATION_NOT_SUPPORT = 3
MXSML_ERROR_SYSFS_ERROR = 4
MXSML_ERROR_SYSFS_WRITE_ERROR = 5
MXSML_ERROR_INVALID_DEVICE_ID = 6
MXSML_ERROR_INVALID_DID_ID = 7
MXSML_ERROR_PERMISSION_DENIED = 8
MXSML_ERROR_INVALID_INPUT = 9
MXSML_ERROR_INSUFFICIENT_SIZE = 10
MXSML_ERROR_RESERVED3 = 11
MXSML_ERROR_IO_CONTROL_FAILURE = 12
MXSML_ERROR_MMAP_FAILURE = 13
MXSML_ERROR_UN_MMAP_FAILURE = 14
MXSML_ERROR_INVALID_INPUT_FOR_MMAP = 15
MXSML_ERROR_RESERVED1 = 16
MXSML_ERROR_RESERVED2 = 17
MXSML_ERROR_TARGET_VF_NOT_FOUND = 18
MXSML_ERROR_INVALID_FREQUENCY = 19
MXSML_ERROR_FLR_NOT_READY = 20
MXSML_ERROR_OPEN_DEVICE_FILE_FAILURE = 21
MXSML_ERROR_CLOSE_DEVICE_FILE_FAILURE = 22
MXSML_ERROR_BUSY_DEVICE = 23
MXSML_ERROR_MMIO_NOT_ENOUGH = 24
MXSML_ERROR_GET_PCI_BRIDGE_FAILURE = 25
MXSML_ERROR_LOAD_DLL_FAILURE = 26

_mxSmlMetaXLinkState_t = c_uint
MXSML_MetaXLink_State_Enabled = 0
MXSML_MetaXLink_State_Smi_Disabled = 1
MXSML_MetaXLink_State_Vf_Disabled = 2
MXSML_MetaXLink_State_GpuNum_Disabled = 3
MXSML_MetaXLink_State_Training_Disabled = 4

_mxSmlMxlkPortState_t = c_uint
MXSML_Mxlk_Port_State_NoTraining = 0
MXSML_Mxlk_Port_State_Up = 1
MXSML_Mxlk_Port_State_Down_Optical_InPlace = 2
MXSML_Mxlk_Port_State_Down_Optical_OutPlace = 3
MXSML_Mxlk_Port_State_Down_Optical_NoUse = 4
MXSML_Mxlk_Port_State_NoUse = 5

_mxSmlPciEventType_t = c_uint
MXSML_Pci_Event_AER_UE = 0
MXSML_Pci_Event_AER_CE = 1
MXSML_Pci_Event_SYNFLD = 2
MXSML_Pci_Event_DBE = 3
MXSML_Pci_Event_MMIO = 4


# buffer size
MXSML_DEVICE_BDF_ID_SIZE = 32  # Guaranteed maximum possible size for BDF ID
MXSML_EEPROM_INFO_SIZE = 1024  # Guaranteed maximum possible size for Eeprom info
MXSML_VERSION_INFO_SIZE = 64  # Guaranteed maximum possible size for Version info
MXSML_VIRTUAL_DEVICE_SIZE = (
    128  # Guaranteed maximum possible size for virtual device ids
)
MXSML_DEVICE_UUID_SIZE = 96  # Guaranteed maximum possible size for UUID
MXSML_DEVICE_NAME_SIZE = 32  # Guaranteed maximum possible size for Device name
MXSML_CHIP_SERIAL_SIZE = 32  # Guaranteed maximum possible size for chip serial
MXSML_BOARD_SERIAL_SIZE = 32  # Guaranteed maximum possible size for board serial
MXSML_OPTICAL_MODULE_INFO_SIZE = (
    32  # Guaranteed maximum possible size for optical module info
)
MXSML_DEVICE_REAL_PATH_SIZE = (
    1024  # Guaranteed maximum possible size for device real path
)
MXSML_VBIOS_BIN_PATH_SIZE = 150  # Guaranteed maximum possible size for file path
DEVICE_UNAVAILABLE_REASON_SIZE = (
    64  # Guaranteed maximum possible size for device unavailable reason
)

MXSML_RAS_ERROR_REG_NUM = 128  # Number of RAS error counter register
MXSML_RAS_STATUS_REG_NUM = 128  # Number of RAS status register

MXSML_METAX_LINK_NUM = 7  # Number of C-class device MetaXLink

MXSML_PROCESS_NAME_SIZE = 64  # Maximum length showed process name
MXSML_MAX_GPU_NUM_USED_BY_PROCESS = (
    64  # Maximum stored GPU information used by a process
)

MXSML_MAX_SMP_NUM = 2  # The max num of smp
MXSML_MAX_CCX_NUM = 4  # The max num of ccx

MXSML_MIN_PCI_DELAY = 1  # Minimum pci delay, unit: ms
MXSML_MAX_PCI_DELAY = 10000  # Maximum pci delay, unit: ms

# Used for mxSmlInitWithFlags
MXSML_INIT_FLAG_NORMAL = 0  # the behavior is the same as mxsmlInit
MXSML_INIT_FLAG_REINIT = 1  # repeated initialization will reinit resources


## MXSML Structure ##
class mxsmlFriendlyObject(object):
    def __init__(self, dictionary):
        for x in dictionary:
            setattr(self, x, dictionary[x])

    def __str__(self):
        return self.__dict__.__str__()


def mxsmlStructToFriendlyObject(struct):
    dictionary = {}
    for field in struct._fields_:
        key = field[0]
        value = getattr(struct, key)
        dictionary[key] = value.decode() if isinstance(value, bytes) else value
    return mxsmlFriendlyObject(dictionary)


# pack the object so it can be passed to the MXSML library
def mxsmlFriendlyObjectToStruct(obj, model):
    for field in model._fields_:
        key = field[0]
        value = obj.__dict__[key]
        setattr(model, key, value.encode())
    return model


class _MxsmlBaseStructure(Structure):
    _format_ = {}
    _default_format_ = "%s"

    def __str__(self):
        attrs = []
        for field in self._fields_:
            attr_name = field[0]
            attr_value = getattr(self, attr_name)
            fmt = self._default_format_
            if attr_name in self._format_:
                fmt = self._format_[attr_name]
            attrs.append(("%s: " + fmt) % (attr_name, attr_value))
        return self.__class__.__name__ + "(" + ", ".join(attrs) + ")"

    def __getattribute__(self, name):
        res = super().__getattribute__(name)
        if isinstance(res, bytes):
            return res.decode()
        return res

    def __setattr__(self, name, value):
        if isinstance(value, str):
            value = value.encode()
        super().__setattr__(name, value)


class c_mxsmlDeviceInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("deviceId", c_uint),
        ("type", c_uint),  # Do not use
        ("bdfId", c_char * MXSML_DEVICE_BDF_ID_SIZE),
        ("gpuId", c_uint),
        ("nodeId", c_uint),
        ("uuid", c_char * MXSML_DEVICE_UUID_SIZE),
        ("brand", _mxSmlDeviceBrand_t),
        ("mode", _mxSmlDeviceVirtualizationMode_t),
        ("deviceName", c_char * MXSML_DEVICE_NAME_SIZE),
    ]


class c_mxSmlVirtualDeviceIds_t(_MxsmlBaseStructure):
    _fields_ = [
        ("number", c_int),
        ("deviceId", c_int * MXSML_VIRTUAL_DEVICE_SIZE),
    ]


class c_mxSmlLimitedDeviceIds_t(c_mxSmlVirtualDeviceIds_t):
    pass


class c_mxsmlMemoryInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("visVramTotal", c_long),
        ("visVramUse", c_long),
        ("vramTotal", c_long),
        ("vramUse", c_long),
        ("xttTotal", c_long),
        ("xttUse", c_long),
    ]
    _default_format_ = "%d KB"


class c_mxSmlPmbusInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("voltage", c_uint),
        ("current", c_uint),
        ("power", c_uint),
    ]
    _format_ = {"voltage": "%d mV", "current": "%d mA", "power": "%d mW"}


class c_mxSmlBoardWayElectricInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("voltage", c_uint),
        ("current", c_uint),
        ("power", c_uint),
    ]
    _format_ = {"voltage": "%d mV", "current": "%d mA", "power": "%d mW"}


class c_mxSmlMxcDpmPerfLevel_t(_MxsmlBaseStructure):
    _fields_ = [
        ("xcore", c_uint),
        ("mc", c_uint),
        ("soc", c_uint),
        ("dnoc", c_uint),
        ("vpue", c_uint),
        ("vpud", c_uint),
        ("ccx", c_uint),
    ]


class c_mxSmlEepromInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("content", c_char * MXSML_EEPROM_INFO_SIZE),
    ]


class c_mxSmlRasErrorRegister_t(_MxsmlBaseStructure):
    _fields_ = [
        ("rasIp", _mxSmlRasIp_t),
        ("registerIndex", c_uint),
        ("rasErrorUe", c_int),
        ("rasErrorCe", c_int),
    ]


class c_mxSmlRasErrorData_t(_MxsmlBaseStructure):
    _fields_ = [
        ("rasErrorRegister", c_mxSmlRasErrorRegister_t * MXSML_RAS_ERROR_REG_NUM),
        ("showRasErrorSize", c_uint),
    ]


class c_mxSmlRasRegister_t(_MxsmlBaseStructure):
    _fields_ = [
        ("rasIp", _mxSmlRasIp_t),
        ("registerIndex", c_uint),
        ("registerData", c_int),
    ]


class c_mxSmlRasStatusData_t(_MxsmlBaseStructure):
    _fields_ = [
        ("rasStatusRegister", c_mxSmlRasRegister_t * MXSML_RAS_STATUS_REG_NUM),
        ("showRasStatusSize", c_uint),
    ]


class c_mxSmlPcieInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("speed", c_float),
        ("width", c_uint),
    ]
    _format_ = {"speed": "%.1f GT/s", "width": "x%d"}


class c_mxSmlCodecStatus_t(_MxsmlBaseStructure):
    _fields_ = [
        ("encoder", c_int),
        ("decoder", c_int),
    ]


class c_mxSmlHbmBandWidth_t(_MxsmlBaseStructure):
    _fields_ = [
        ("hbmBandwidthReqTotal", c_int),
        ("hbmBandwidthRespTotal", c_int),
    ]
    _default_format_ = "%d MBytes/s"


class c_mxSmlPcieThroughput_t(_MxsmlBaseStructure):
    _fields_ = [
        ("rx", c_int),
        ("tx", c_int),
    ]
    _default_format_ = "%d MBytes/s"


class c_mxSmlDmaEngineBandwidth_t(_MxsmlBaseStructure):
    _fields_ = [
        ("readReqBandwidth", c_int),
        ("readRespBandwidth", c_int),
        ("writeReqBandwidth", c_int),
        ("writeRespBandwidth", c_int),
    ]
    _default_format_ = "%d MBytes/s"


class c_mxSmlMetaXLinkBandwidth_t(_MxsmlBaseStructure):
    _fields_ = [
        ("requestBandwidth", c_int),
        ("responseBandwidth", c_int),
    ]
    _default_format_ = "%d MBytes/s"


class c_mxSmlMetaXLinkRemoteInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("deviceId", c_int),
        ("bdfId", c_char * MXSML_DEVICE_BDF_ID_SIZE),
    ]


class c_mxSmlMcmMetaXLinkRemoteInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("deviceId", c_int),
        ("bdfId", c_char * MXSML_DEVICE_BDF_ID_SIZE),
        ("dieId", c_uint),
    ]


class c_mxSmlProcessGpuInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("bdfId", c_char * MXSML_DEVICE_BDF_ID_SIZE),
        ("gpuId", c_uint),
        ("gpuMemoryUsage", c_ulong),
    ]


class c_mxSmlProcessInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("processId", c_uint),
        ("processName", c_char * MXSML_PROCESS_NAME_SIZE),
        ("gpuNumber", c_uint),
        ("processGpuInfo", c_mxSmlProcessGpuInfo_t * MXSML_MAX_GPU_NUM_USED_BY_PROCESS),
    ]


class c_mxSmlProcessGpuInfo_v2_t(_MxsmlBaseStructure):
    _fields_ = [
        ("bdfId", c_char * MXSML_DEVICE_BDF_ID_SIZE),
        ("gpuId", c_uint),
        ("gpuMemoryUsage", c_ulong),
        ("dieId", c_uint),
    ]


class c_mxSmlProcessInfo_v2_t(_MxsmlBaseStructure):
    _fields_ = [
        ("processId", c_uint),
        ("processName", c_char * MXSML_PROCESS_NAME_SIZE),
        ("gpuNumber", c_uint),
        (
            "processGpuInfo",
            c_mxSmlProcessGpuInfo_v2_t * MXSML_MAX_GPU_NUM_USED_BY_PROCESS,
        ),
    ]


class c_mxSmlFwLoglevel_t(_MxsmlBaseStructure):
    _fields_ = [
        ("smpCount", c_uint),
        ("smp", _mxSmlLoglevel_t * MXSML_MAX_SMP_NUM),
        ("ccxCount", c_uint),
        ("ccx", _mxSmlLoglevel_t * MXSML_MAX_CCX_NUM),
    ]


class c_mxSmlMetaXLinkInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("speed", c_float * MXSML_METAX_LINK_NUM),
        ("width", c_uint * MXSML_METAX_LINK_NUM),
    ]


class c_mxSmlSingleMxlkInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("speed", c_float),
        ("width", c_uint),
    ]


class c_mxSmlOpticalModuleStatus_t(_MxsmlBaseStructure):
    _fields_ = [
        ("temperature", c_int),  # unit: degress 0.01C
        ("voltage", c_uint),  # unit: mV
        ("moduleState", c_uint),
        ("dataPathState", c_uint),
        ("rxState", c_uint * 2),  # 6 bytes total
        ("version", c_uint * 2),  # version[0]: major, version[1]: minor
    ]
    _format_ = {"voltage": "%d mV", "dataPathState": "0x%x"}


class c_mxSmlOpticalModuleInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("name", c_char * MXSML_OPTICAL_MODULE_INFO_SIZE),
        ("oui", c_uint * 3),
        ("pn", c_char * MXSML_OPTICAL_MODULE_INFO_SIZE),
        ("rev", c_char * MXSML_OPTICAL_MODULE_INFO_SIZE),
        ("sn", c_char * MXSML_OPTICAL_MODULE_INFO_SIZE),
    ]


class c_mxSmlVbiosUpgradeArg_t(_MxsmlBaseStructure):
    _fields_ = [
        ("timeLimit", c_uint),
        ("vbiosBinPath", c_char * (MXSML_VBIOS_BIN_PATH_SIZE + 1)),
        ("forceUpgrade", c_int),
        ("ret", c_uint),
        ("pfBar0Size", c_int),
        ("vfBar0Size", c_int),
    ]


class c_mxSmlPciEventInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("bitNumber", c_int),
        ("count", c_int),
        ("firstTime", c_char * 20),
        ("name", c_char * 64),
    ]


class c_mxSmlDeviceUnavailableReasonInfo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("unavailableCode", c_int),
        ("unavailableReason", c_char * DEVICE_UNAVAILABLE_REASON_SIZE),
    ]


class c_mxSmlEccErrorCount_t(_MxsmlBaseStructure):
    _fields_ = [
        ("sramCE", c_uint),
        ("sramUE", c_uint),
        ("dramCE", c_uint),
        ("dramUE", c_uint),
        ("retiredPage", c_uint),
    ]


class c_mxSmlMetaXLinkTopo_t(_MxsmlBaseStructure):
    _fields_ = [
        ("topologyId", c_uint),
        ("socketId", c_uint),
        ("dieId", c_uint),
    ]


## MXSML Error ##
class MXSMLError(Exception):
    _valClassMapping = dict()

    def __new__(typ, value):
        """
        Maps value to a proper subclass of MXSMLError.
        See _generateErrorSubclass function for more details
        """
        if typ == MXSMLError:
            typ = MXSMLError._valClassMapping.get(value, typ)
        obj = Exception.__new__(typ)
        obj.value = value
        return obj

    def __str__(self):
        try:
            return str(mxSmlGetErrorString(self.value))
        except MXSMLError:
            return "MXSML Error with code: " + str(self.value)

    def __eq__(self, other):
        return self.value == other.value


def _generateErrorSubclass():
    # Create execption subclass for each MXSML Error
    def generate_new_func(val):
        def new(typ):
            obj = MXSMLError.__new__(typ, val)
            return obj

        return new

    this_module = sys.modules[__name__]
    mxsmlErrorsNames = [x for x in dir(this_module) if x.startswith("MXSML_ERROR_")]
    for err_name in mxsmlErrorsNames:
        class_name = "MXSMLError_" + string.capwords(
            err_name.replace("MXSML_ERROR_", ""), "_"
        ).replace("_", "")
        err_val = getattr(this_module, err_name)
        new_error_class = type(
            class_name, (MXSMLError,), {"__new__": generate_new_func(err_val)}
        )
        new_error_class.__module__ = __name__
        setattr(this_module, class_name, new_error_class)
        MXSMLError._valClassMapping[err_val] = new_error_class


_generateErrorSubclass()

## Lib loading ##
mxSmlLib = None


def get_mxSmlLib():
    return mxSmlLib


def _mxsmlCheckReturn(ret):
    if ret != MXSML_SUCCESS:
        raise MXSMLError(ret)
    return ret


## Function access ##
_mxsmlFunctionPointerCache = {}


def _mxsmlGetFunctionPointer(name):
    if name in _mxsmlFunctionPointerCache:
        return _mxsmlFunctionPointerCache[name]

    global mxSmlLib
    if mxSmlLib == None:
        raise MXSMLError(MXSML_ERROR_FAILURE)

    _mxsmlFunctionPointerCache[name] = getattr(mxSmlLib, name)
    return _mxsmlFunctionPointerCache[name]


## string/bytes conversion for ease of use
def convertStrBytes(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # encoding a str returns bytes
        args = [arg.encode() if isinstance(arg, str) else arg for arg in args]
        res = func(*args, **kwargs)
        if isinstance(res, bytes):
            return res.decode()
        return res

    return wrapper


def _searchMxsmlLibrary():
    for lib in [
        "/opt/mxdriver/lib/libmxsml.so",
        "/opt/maca/lib/libmxsml.so",
        "/opt/mxn100/lib/libmxsml.so",
    ]:
        if os.path.isfile(lib):
            return lib

    libmxsml = "libmxsml.so"
    for root, __, files in os.walk("/opt", followlinks=True):
        if libmxsml in files:
            return os.path.join(os.path.realpath(root), libmxsml)
    return None


def _loadMxsmlLibrary():
    "Load the library if it isn't loaded already"
    global mxSmlLib
    if mxSmlLib != None:
        return

    try:
        path_libmxsml = _searchMxsmlLibrary()
        if not path_libmxsml:
            _mxsmlCheckReturn(MXSML_ERROR_LOAD_DLL_FAILURE)
        else:
            mxSmlLib = CDLL(path_libmxsml)
    except OSError as ose:
        _mxsmlCheckReturn(MXSML_ERROR_LOAD_DLL_FAILURE)
    if mxSmlLib == None:
        _mxsmlCheckReturn(MXSML_ERROR_LOAD_DLL_FAILURE)


## function wrappers ##
def mxSmlInit():
    _loadMxsmlLibrary()
    fn = _mxsmlGetFunctionPointer("mxSmlInit")
    ret = fn()
    _mxsmlCheckReturn(ret)
    return None


def mxSmlInitWithFlags(flags):
    _loadMxsmlLibrary()
    fn = _mxsmlGetFunctionPointer("mxSmlInitWithFlags")
    ret = fn(flags)
    _mxsmlCheckReturn(ret)
    return None


@convertStrBytes
def mxSmlGetMacaVersion():
    version = create_string_buffer(MXSML_VERSION_INFO_SIZE)
    fn = _mxsmlGetFunctionPointer("mxSmlGetMacaVersion")
    ret = fn(byref(version), byref(c_uint(MXSML_VERSION_INFO_SIZE)))
    _mxsmlCheckReturn(ret)
    return version.value


def mxSmlGetDeviceCount():
    fn = _mxsmlGetFunctionPointer("mxSmlGetDeviceCount")
    return fn()


def mxSmlGetPfDeviceCount():
    fn = _mxsmlGetFunctionPointer("mxSmlGetPfDeviceCount")
    return fn()


def mxSmlGetDeviceInfo(device_id):
    device_info = c_mxsmlDeviceInfo_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetDeviceInfo")
    ret = fn(c_uint(device_id), byref(device_info))
    _mxsmlCheckReturn(ret)
    return device_info


def mxSmlGetVirtualDevicesByPhysicalId(device_id):
    devices = c_mxSmlVirtualDeviceIds_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetVirtualDevicesByPhysicalId")
    ret = fn(c_uint(device_id), byref(devices))
    _mxsmlCheckReturn(ret)
    return devices


def mxSmlGetAllLimitedDevices():
    devices = c_mxSmlLimitedDeviceIds_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetAllLimitedDevices")
    ret = fn(byref(devices))
    _mxsmlCheckReturn(ret)
    return devices


def mxSmlGetLimitedDeviceInfo(device_id):
    device_info = c_mxsmlDeviceInfo_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetLimitedDeviceInfo")
    ret = fn(c_uint(device_id), byref(device_info))
    _mxsmlCheckReturn(ret)
    return device_info


@convertStrBytes
def mxSmlGetDeviceVersion(device_id, version_unit):
    version = create_string_buffer(MXSML_VERSION_INFO_SIZE)
    fn = _mxsmlGetFunctionPointer("mxSmlGetDeviceVersion")
    ret = fn(
        c_uint(device_id),
        _mxSmlVersionUnit_t(version_unit),
        byref(version),
        byref(c_uint(MXSML_VERSION_INFO_SIZE)),
    )
    _mxsmlCheckReturn(ret)
    return version.value


@convertStrBytes
def mxSmlGetChipSerial(device_id):
    serial = create_string_buffer(MXSML_CHIP_SERIAL_SIZE)
    fn = _mxsmlGetFunctionPointer("mxSmlGetChipSerial")
    ret = fn(c_uint(device_id), byref(serial), byref(c_uint(MXSML_CHIP_SERIAL_SIZE)))
    _mxsmlCheckReturn(ret)
    return serial.value


def mxSmlGetMemoryInfo(device_id):
    memory_info = c_mxsmlMemoryInfo_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetMemoryInfo")
    ret = fn(c_uint(device_id), byref(memory_info))
    _mxsmlCheckReturn(ret)
    return memory_info


def mxSmlGetPmbusInfo(device_id, pmbus_unit):
    pmbus_info = c_mxSmlPmbusInfo_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetPmbusInfo")
    ret = fn(c_uint(device_id), _mxSmlPmbusUnit_t(pmbus_unit), byref(pmbus_info))
    _mxsmlCheckReturn(ret)
    return pmbus_info


def mxSmlGetTemperatureInfo(device_id, temperature_type):
    temperature = c_int()
    fn = _mxsmlGetFunctionPointer("mxSmlGetTemperatureInfo")
    ret = fn(
        c_uint(device_id),
        _mxSmlTemperatureSensors_t(temperature_type),
        byref(temperature),
    )
    _mxsmlCheckReturn(ret)
    return temperature.value


def mxSmlGetBoardPowerInfo(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlGetBoardPowerInfo")
    size = c_uint(0)
    ret = fn(c_uint(device_id), byref(size), (c_mxSmlBoardWayElectricInfo_t * 0)())

    if ret == MXSML_ERROR_INSUFFICIENT_SIZE:
        power_infos = (c_mxSmlBoardWayElectricInfo_t * size.value)()
        ret = fn(c_uint(device_id), byref(size), byref(power_infos))
        _mxsmlCheckReturn(ret)
        return [info for info in power_infos]
    else:
        raise MXSMLError(ret)


def mxSmlGetDpmMaxPerfLevel(device_id):
    dpm_level = c_mxSmlMxcDpmPerfLevel_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetDpmMaxPerfLevel")
    ret = fn(c_uint(device_id), byref(dpm_level))
    _mxsmlCheckReturn(ret)
    return dpm_level


def mxSmlGetDpmIpMaxPerfLevel(device_id, dpm_ip):
    fn = _mxsmlGetFunctionPointer("mxSmlGetDpmIpMaxPerfLevel")
    level = c_uint(0)
    ret = fn(c_uint(device_id), _mxSmlDpmIp_t(dpm_ip), byref(level))
    _mxsmlCheckReturn(ret)
    return level.value


def mxSmlGetEepromInfo(device_id):
    eeprom_info = c_mxSmlEepromInfo_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetEepromInfo")
    ret = fn(c_uint(device_id), byref(eeprom_info))
    _mxsmlCheckReturn(ret)
    return eeprom_info


@convertStrBytes
def mxSmlGetBoardSerial(device_id):
    serial = create_string_buffer(MXSML_BOARD_SERIAL_SIZE)
    fn = _mxsmlGetFunctionPointer("mxSmlGetBoardSerial")
    ret = fn(c_uint(device_id), byref(serial), byref(c_uint(MXSML_BOARD_SERIAL_SIZE)))
    _mxsmlCheckReturn(ret)
    return serial.value


@convertStrBytes
def mxSmlGetPptableVersion(device_id):
    version = create_string_buffer(MXSML_VERSION_INFO_SIZE)
    fn = _mxsmlGetFunctionPointer("mxSmlGetPptableVersion")
    ret = fn(c_uint(device_id), c_uint(5), byref(version))
    _mxsmlCheckReturn(ret)
    return version.value


def mxSmlGetRasErrorData(device_id):
    ras = c_mxSmlRasErrorData_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetRasErrorData")
    ret = fn(c_uint(device_id), byref(ras))
    _mxsmlCheckReturn(ret)
    return ras


def mxSmlGetRasStatusData(device_id):
    ras = c_mxSmlRasStatusData_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetRasStatusData")
    ret = fn(c_uint(device_id), byref(ras))
    _mxsmlCheckReturn(ret)
    return ras


def mxSmlGetPcieInfo(device_id):
    pcie_info = c_mxSmlPcieInfo_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetPcieInfo")
    ret = fn(c_uint(device_id), byref(pcie_info))
    _mxsmlCheckReturn(ret)
    return pcie_info


def mxSmlGetPcieMaxLinkInfo(device_id):
    pcie_info = c_mxSmlPcieInfo_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetPcieMaxLinkInfo")
    ret = fn(c_uint(device_id), byref(pcie_info))
    _mxsmlCheckReturn(ret)
    return pcie_info


def mxSmlGetPowerStateInfo(device_id, dpm_ip):
    fn = _mxsmlGetFunctionPointer("mxSmlGetPowerStateInfo")
    size = c_uint(0)
    ret = fn(c_uint(device_id), _mxSmlDpmIp_t(dpm_ip), (c_int * 0)(), byref(size))

    if ret == MXSML_ERROR_INSUFFICIENT_SIZE:
        power_states = (c_int * size.value)()
        ret = fn(
            c_uint(device_id), _mxSmlDpmIp_t(dpm_ip), byref(power_states), byref(size)
        )
        _mxsmlCheckReturn(ret)
        return [state for state in power_states]
    else:
        raise MXSMLError(ret)


def mxSmlGetPciPowerState(device_id):
    state = _mxSmlPciPowerState_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetPciPowerState")
    ret = fn(c_uint(device_id), byref(state))
    _mxsmlCheckReturn(ret)
    return state.value


def mxSmlGetCodecStatus(device_id):
    status = c_mxSmlCodecStatus_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetCodecStatus")
    ret = fn(c_uint(device_id), byref(status))
    _mxsmlCheckReturn(ret)
    return status


def mxSmlGetClocks(device_id, clock_ip):
    fn = _mxsmlGetFunctionPointer("mxSmlGetClocks")
    size = c_uint(1)
    clocks = (c_uint * 1)()
    ret = fn(c_uint(device_id), _mxSmlClockIp_t(clock_ip), byref(size), byref(clocks))

    if ret == MXSML_SUCCESS:
        return [clock for clock in clocks]

    elif ret == MXSML_ERROR_INSUFFICIENT_SIZE:
        clocks = (c_uint * size.value)()
        ret = fn(
            c_uint(device_id), _mxSmlClockIp_t(clock_ip), byref(size), byref(clocks)
        )
        _mxsmlCheckReturn(ret)
        return [clock for clock in clocks]

    else:
        raise MXSMLError(ret)


def mxSmlGetHbmBandWidth(device_id):
    bw = c_mxSmlHbmBandWidth_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetHbmBandWidth")
    ret = fn(c_uint(device_id), byref(bw))
    _mxsmlCheckReturn(ret)
    return bw


def mxSmlGetPcieThroughput(device_id):
    through_put = c_mxSmlPcieThroughput_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetPcieThroughput")
    ret = fn(c_uint(device_id), byref(through_put))
    _mxsmlCheckReturn(ret)
    return through_put


def mxSmlGetDmaBandwidth(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlGetDmaBandwidth")
    size = c_uint(10)
    ret = fn(c_uint(device_id), (c_mxSmlDmaEngineBandwidth_t * 10)(), byref(size))

    if ret == MXSML_SUCCESS:
        bandwidths = (c_mxSmlDmaEngineBandwidth_t * size.value)()
        ret = fn(c_uint(device_id), byref(bandwidths), byref(size))
        _mxsmlCheckReturn(ret)
        return [bw for bw in bandwidths]
    else:
        raise MXSMLError(ret)


def mxSmlGetMetaXLinkBandwidth(device_id, mxlk_type):
    size = c_uint(MXSML_METAX_LINK_NUM)
    entry = []
    bw = (c_mxSmlMetaXLinkBandwidth_t * MXSML_METAX_LINK_NUM)(*entry)
    fn = _mxsmlGetFunctionPointer("mxSmlGetMetaXLinkBandwidth")
    ret = fn(
        c_uint(device_id), _mxSmlMetaXLinkType_t(mxlk_type), byref(size), byref(bw)
    )
    _mxsmlCheckReturn(ret)
    return bw


def mxSmlGetMetaXLinkRemoteInfo(device_id, link_id):
    remote = c_mxSmlMetaXLinkRemoteInfo_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetMetaXLinkRemoteInfo")
    ret = fn(c_uint(device_id), c_uint(link_id), byref(remote))
    _mxsmlCheckReturn(ret)
    return remote


def mxSmlGetNumberOfProcess():
    proc_number = c_uint()
    fn = _mxsmlGetFunctionPointer("mxSmlGetNumberOfProcess")
    ret = fn(byref(proc_number))
    _mxsmlCheckReturn(ret)
    return proc_number.value


def mxSmlGetProcessInfo(process_number):
    entry = []
    processes = (c_mxSmlProcessInfo_t * process_number)(*entry)
    fn = _mxsmlGetFunctionPointer("mxSmlGetProcessInfo")
    ret = fn(process_number, byref(processes))
    _mxsmlCheckReturn(ret)
    return processes


def mxSmlGetProcessInfo_v2(process_number):
    entry = []
    processes = (c_mxSmlProcessInfo_v2_t * process_number)(*entry)
    fn = _mxsmlGetFunctionPointer("mxSmlGetProcessInfo_v2")
    ret = fn(process_number, byref(processes))
    _mxsmlCheckReturn(ret)
    return processes


def mxSmlGetSingleGpuProcess(device_id):
    number = c_uint(0)
    fn = _mxsmlGetFunctionPointer("mxSmlGetSingleGpuProcess")
    ret = fn(c_uint(device_id), byref(number), (c_mxSmlProcessInfo_t * 1)())

    if ret == MXSML_SUCCESS:
        # oversize the array incase more processes are created
        number.value = number.value * 2 + 5
        processes = (c_mxSmlProcessInfo_t * number.value)()
        ret = fn(c_uint(device_id), byref(number), byref(processes))
        _mxsmlCheckReturn(ret)

        process_array = []
        for i in range(number.value):
            process_array.append(mxsmlStructToFriendlyObject(processes[i]))
        return process_array
    else:
        # error case
        raise MXSMLError(ret)


def mxSmlGetSingleGpuProcess_v2(device_id):
    number = c_uint(0)
    fn = _mxsmlGetFunctionPointer("mxSmlGetSingleGpuProcess_v2")
    ret = fn(c_uint(device_id), byref(number), (c_mxSmlProcessInfo_v2_t * 1)())

    if ret == MXSML_SUCCESS:
        # oversize the array incase more processes are created
        number.value = number.value * 2 + 5
        processes = (c_mxSmlProcessInfo_v2_t * number.value)()
        ret = fn(c_uint(device_id), byref(number), byref(processes))
        _mxsmlCheckReturn(ret)

        process_array = []
        for i in range(number.value):
            process_array.append(mxsmlStructToFriendlyObject(processes[i]))
        return process_array
    else:
        # error case
        raise MXSMLError(ret)


def mxSmlGetDeviceTopology(device_src, device_dst):
    topo = _mxSmlGpuTopologyLevel_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetDeviceTopology")
    ret = fn(c_uint(device_src), c_uint(device_dst), byref(topo))
    _mxsmlCheckReturn(ret)
    return topo.value


def mxSmlGetDeviceDistance(device_src, device_dst):
    distance = c_uint()
    fn = _mxsmlGetFunctionPointer("mxSmlGetDeviceDistance")
    ret = fn(c_uint(device_src), c_uint(device_dst), byref(distance))
    _mxsmlCheckReturn(ret)
    return distance.value


def mxSmlGetCpuAffinity(device_id, cpu_set_size):
    entry = []
    cpu_set = (c_uint * cpu_set_size)(*entry)
    fn = _mxsmlGetFunctionPointer("mxSmlGetCpuAffinity")
    ret = fn(c_uint(device_id), c_uint(cpu_set_size), byref(cpu_set))
    _mxsmlCheckReturn(ret)
    return cpu_set


def mxSmlGetNodeAffinity(device_id, node_set_size):
    entry = []
    node_set = (c_uint * node_set_size)(*entry)
    fn = _mxsmlGetFunctionPointer("mxSmlGetNodeAffinity")
    ret = fn(c_uint(device_id), c_uint(node_set_size), byref(node_set))
    _mxsmlCheckReturn(ret)
    return node_set


def mxSmlGetPciDelay(device_id):
    delay = c_uint()
    fn = _mxsmlGetFunctionPointer("mxSmlGetPciDelay")
    ret = fn(c_uint(device_id), byref(delay))
    _mxsmlCheckReturn(ret)
    return delay.value


def mxSmlGetDpmIpClockInfo(device_id, dpm_ip):
    fn = _mxsmlGetFunctionPointer("mxSmlGetDpmIpClockInfo")
    size = c_uint(0)
    ret = fn(c_uint(device_id), _mxSmlDpmIp_t(dpm_ip), (c_uint * 1)(), byref(size))

    if ret == MXSML_ERROR_INSUFFICIENT_SIZE:
        clocks = (c_uint * size.value)()
        ret = fn(c_uint(device_id), _mxSmlDpmIp_t(dpm_ip), byref(clocks), byref(size))
        _mxsmlCheckReturn(ret)
        return [clk for clk in clocks]
    else:
        raise MXSMLError(ret)


def mxSmlGetDpmIpVddInfo(device_id, dpm_ip):
    fn = _mxsmlGetFunctionPointer("mxSmlGetDpmIpVddInfo")
    size = c_uint(0)
    ret = fn(c_uint(device_id), _mxSmlDpmIp_t(dpm_ip), (c_uint * 1)(), byref(size))

    if ret == MXSML_ERROR_INSUFFICIENT_SIZE:
        vdds = (c_uint * size.value)()
        ret = fn(c_uint(device_id), _mxSmlDpmIp_t(dpm_ip), byref(vdds), byref(size))
        _mxsmlCheckReturn(ret)
        return [vdd for vdd in vdds]
    else:
        raise MXSMLError(ret)


def mxSmlGetCurrentDpmIpPerfLevel(device_id, dpm_ip):
    level = c_uint()
    fn = _mxsmlGetFunctionPointer("mxSmlGetCurrentDpmIpPerfLevel")
    ret = fn(c_uint(device_id), _mxSmlDpmIp_t(dpm_ip), byref(level))
    _mxsmlCheckReturn(ret)
    return level.value


def mxSmlGetDeviceIpUsage(device_id, usage_ip):
    usage = c_int()
    fn = _mxsmlGetFunctionPointer("mxSmlGetDeviceIpUsage")
    ret = fn(c_uint(device_id), _mxSmlUsageIp_t(usage_ip), byref(usage))
    _mxsmlCheckReturn(ret)
    return usage.value


def mxSmlGetXcoreApUsage(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlGetXcoreApUsage")
    size = c_uint(0)
    dpc_num = c_uint(0)
    ret = fn(c_uint(device_id), (c_uint * 0)(), byref(size), byref(dpc_num))

    if ret == MXSML_ERROR_INSUFFICIENT_SIZE:
        usage_array = (c_uint * size.value)()
        ret = fn(c_uint(device_id), byref(usage_array), byref(size), byref(dpc_num))
        _mxsmlCheckReturn(ret)
        idx_num = (size.value + dpc_num.value - 1) // dpc_num.value
        return [usage_array[i : i + idx_num] for i in range(0, size.value, idx_num)]
    else:
        raise MXSMLError(ret)


def mxSmlGetApUsageToggle(device_id):
    toggle = c_uint()
    fn = _mxsmlGetFunctionPointer("mxSmlGetApUsageToggle")
    ret = fn(c_uint(device_id), byref(toggle))
    _mxsmlCheckReturn(ret)
    return toggle.value


def mxSmlGetFwIpLoglevel(device_id, fw_ip):
    level = _mxSmlLoglevel_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetFwIpLoglevel")
    ret = fn(c_uint(device_id), _mxSmlFwIpName_t(fw_ip), byref(level))
    _mxsmlCheckReturn(ret)
    return level.value


def mxSmlGetFwLoglevel(device_id):
    log_level = c_mxSmlFwLoglevel_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetFwLoglevel")
    ret = fn(c_uint(device_id), byref(log_level))
    _mxsmlCheckReturn(ret)
    return log_level


def mxSmlGetEccState(device_id):
    state = c_uint()
    fn = _mxsmlGetFunctionPointer("mxSmlGetEccState")
    ret = fn(c_uint(device_id), byref(state))
    _mxsmlCheckReturn(ret)
    return state.value


def mxSmlGetMetaXLinkInfo(device_id):
    mxlk_info = c_mxSmlMetaXLinkInfo_t()
    fn = _mxsmlGetFunctionPointer("mxSmlGetMetaXLinkInfo")
    ret = fn(c_uint(device_id), byref(mxlk_info))
    _mxsmlCheckReturn(ret)
    return mxlk_info


def mxSmlGetSriovState(device_id):
    state = c_uint()
    fn = _mxsmlGetFunctionPointer("mxSmlGetSriovState")
    ret = fn(c_uint(device_id), byref(state))
    _mxsmlCheckReturn(ret)
    return state.value


def mxSmlGetDeviceSlot(device_id):
    slot = c_uint()
    fn = _mxsmlGetFunctionPointer("mxSmlGetDeviceSlot")
    ret = fn(c_uint(device_id), byref(slot))
    _mxsmlCheckReturn(ret)
    return slot.value


def mxSmlGetOpticalModuleStatus(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlGetOpticalModuleStatus")
    size = c_uint(0)
    ret = fn(c_uint(device_id), (c_mxSmlOpticalModuleStatus_t * 0)(), byref(size))

    if ret == MXSML_ERROR_INSUFFICIENT_SIZE:
        status_array = (c_mxSmlOpticalModuleStatus_t * size.value)()
        ret = fn(c_uint(device_id), byref(status_array), byref(size))
        _mxsmlCheckReturn(ret)
        return [status for status in status_array]
    else:
        raise MXSMLError(ret)


def mxSmlGetOpticalModuleInfo(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlGetOpticalModuleInfo")
    size = c_uint(0)
    ret = fn(c_uint(device_id), (c_mxSmlOpticalModuleInfo_t * 0)(), byref(size))

    if ret == MXSML_ERROR_INSUFFICIENT_SIZE:
        infos = (c_mxSmlOpticalModuleInfo_t * size.value)()
        ret = fn(c_uint(device_id), byref(infos), byref(size))
        _mxsmlCheckReturn(ret)
        return [info for info in infos]
    else:
        raise MXSMLError(ret)


@convertStrBytes
def mxSmlGetDeviceRealPath(device_id):
    real_path = create_string_buffer(MXSML_DEVICE_REAL_PATH_SIZE)
    fn = _mxsmlGetFunctionPointer("mxSmlGetDeviceRealPath")
    ret = fn(
        c_uint(device_id), byref(real_path), byref(c_uint(MXSML_DEVICE_REAL_PATH_SIZE))
    )
    _mxsmlCheckReturn(ret)
    return real_path.value


def mxSmlGetCurrentClocksThrottleReason(device_id):
    reason = c_ulonglong()
    fn = _mxsmlGetFunctionPointer("mxSmlGetCurrentClocksThrottleReason")
    ret = fn(c_uint(device_id), byref(reason))
    _mxsmlCheckReturn(ret)
    return reason.value


def mxSmlGetBoardPowerLimit(device_id):
    limit = c_uint()
    fn = _mxsmlGetFunctionPointer("mxSmlGetBoardPowerLimit")
    ret = fn(c_uint(device_id), byref(limit))
    _mxsmlCheckReturn(ret)
    return limit.value


def mxSmlSetDpmIpMaxPerfLevel(device_id, dpm_ip, level):
    fn = _mxsmlGetFunctionPointer("mxSmlSetDpmIpMaxPerfLevel")
    ret = fn(c_uint(device_id), _mxSmlDpmIp_t(dpm_ip), c_uint(level))
    _mxsmlCheckReturn(ret)
    return ret


@convertStrBytes
def mxSmlDumpVbios(device_id, time_limit, bin_path):
    fn = _mxsmlGetFunctionPointer("mxSmlDumpVbios")
    fw_ret = c_uint()
    b_bin_path = c_char_p(bin_path)
    ret = fn(c_uint(device_id), c_uint(time_limit), b_bin_path, byref(fw_ret))
    _mxsmlCheckReturn(ret)
    return ret


@convertStrBytes
def mxSmlVbiosUpgrade(device_id, time_limit, bin_path):
    # Will take effect after reboot
    upgrade_arg = c_mxSmlVbiosUpgradeArg_t()
    upgrade_arg.timeLimit = c_uint(time_limit)
    upgrade_arg.vbiosBinPath = bin_path
    upgrade_arg.forceUpgrade = c_int(0)
    fn = _mxsmlGetFunctionPointer("mxSmlVbiosUpgrade")
    ret = fn(c_uint(device_id), byref(upgrade_arg))
    _mxsmlCheckReturn(ret)
    return ret


def mxSmlFunctionLevelReset(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlFunctionLevelReset")
    ret = fn(c_uint(device_id))
    _mxsmlCheckReturn(ret)
    return ret


def mxSmlFunctionLevelResetVfFromPf(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlFunctionLevelResetVfFromPf")
    ret = fn(c_uint(device_id))
    _mxsmlCheckReturn(ret)
    return ret


def mxSmlSetUnlockKey(device_id, key):
    b_key = c_char_p(key.encode("utf-8"))
    fn = _mxsmlGetFunctionPointer("mxSmlSetUnlockKey")
    ret = fn(c_uint(device_id), b_key)
    _mxsmlCheckReturn(ret)
    return ret


def mxSmlSetCpuAffinity(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlSetCpuAffinity")
    ret = fn(c_uint(device_id))
    _mxsmlCheckReturn(ret)
    return ret


def mxSmlClearCpuAffinity(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlClearCpuAffinity")
    ret = fn(c_uint(device_id))
    _mxsmlCheckReturn(ret)
    return ret


def mxSmlReset(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlReset")
    ret = fn(c_uint(device_id))
    _mxsmlCheckReturn(ret)
    return ret


def mxSmlSetPciSpeed(device_id, pci_gen):
    # Will take effect after reboot
    fn = _mxsmlGetFunctionPointer("mxSmlSetPciSpeed")
    ret = fn(c_uint(device_id), _mxSmlPciGen_t(pci_gen))
    _mxsmlCheckReturn(ret)
    return ret


def mxSmlSetPciDelay(device_id, delay):
    fn = _mxsmlGetFunctionPointer("mxSmlSetPciDelay")
    ret = fn(c_uint(device_id), c_uint(delay))
    _mxsmlCheckReturn(ret)
    return ret


def mxSmlSetFwLoglevel(device_id, fw_ip, level):
    fn = _mxsmlGetFunctionPointer("mxSmlSetFwLoglevel")
    ret = fn(c_uint(device_id), _mxSmlFwIpName_t(fw_ip), _mxSmlLoglevel_t(level))
    _mxsmlCheckReturn(ret)
    return ret


def mxSmlSetApUsageToggle(device_id, toggle):
    fn = _mxsmlGetFunctionPointer("mxSmlSetApUsageToggle")
    ret = fn(c_uint(device_id), c_uint(toggle))
    _mxsmlCheckReturn(ret)
    return ret


def mxSmlSetEccState(device_id, state):
    # Will take effect after reboot
    fn = _mxsmlGetFunctionPointer("mxSmlSetEccState")
    ret = fn(c_uint(device_id), c_uint(state))
    _mxsmlCheckReturn(ret)
    return ret


def mxSmlGetMetaXLinkState(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlGetMetaXLinkState")
    mxlkStateCode = _mxSmlMetaXLinkState_t()
    entry = []
    mxlk_state = (c_char * 128)(*entry)
    size = c_uint(128)
    ret = fn(c_uint(device_id), byref(mxlkStateCode), byref(mxlk_state), byref(size))
    _mxsmlCheckReturn(ret)
    return ret


def mxSmlGetMetaXLinkPortState(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlGetMetaXLinkPortState")
    entry = []
    mxlk_port_state = (_mxSmlMxlkPortState_t * 7)(*entry)
    size = c_uint(7)
    ret = fn(c_uint(device_id), byref(mxlk_port_state), byref(size))
    _mxsmlCheckReturn(ret)
    return mxlk_port_state


def mxSmlGetPciMmioState(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlGetPciMmioState")
    state = c_uint()
    ret = fn(c_uint(device_id), byref(state))
    _mxsmlCheckReturn(ret)
    return state.value


def mxSmlGetPciEventInfo(device_id, event_type):
    fn = _mxsmlGetFunctionPointer("mxSmlGetPciEventInfo")
    entry = []
    event_infos = (c_mxSmlPciEventInfo_t * 2)(*entry)
    size = c_uint(2)
    ret = fn(
        c_uint(device_id),
        _mxSmlPciEventType_t(event_type),
        byref(event_infos),
        byref(size),
    )
    _mxsmlCheckReturn(ret)
    return event_infos


def mxSmlGetDeviceIsaVersion(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlGetDeviceIsaVersion")
    isaVersion = c_int(0)
    ret = fn(c_uint(device_id), byref(isaVersion))
    _mxsmlCheckReturn(ret)
    return isaVersion.value


def mxSmlGetDeviceUnavailableReason(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlGetDeviceUnavailableReason")
    reason = c_mxSmlDeviceUnavailableReasonInfo_t()
    ret = fn(c_uint(device_id), byref(reason))
    _mxsmlCheckReturn(ret)
    return reason


def mxSmlGetTotalEccErrors(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlGetTotalEccErrors")
    eccCounts = c_mxSmlEccErrorCount_t()
    ret = fn(c_uint(device_id), byref(eccCounts))
    _mxsmlCheckReturn(ret)
    return mxsmlStructToFriendlyObject(eccCounts)


def mxSmlGetMetaXLinkTopo(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlGetMetaXLinkTopo")
    topoInfo = c_mxSmlMetaXLinkTopo_t()
    ret = fn(c_uint(device_id), byref(topoInfo))
    _mxsmlCheckReturn(ret)
    return mxsmlStructToFriendlyObject(topoInfo)


@convertStrBytes
def mxSmlGetErrorString(result):
    fn = _mxsmlGetFunctionPointer("mxSmlGetErrorString")
    fn.restype = c_char_p  # otherwise return is an int
    ret = fn(_mxsmlReturn_t(result))
    return ret


def mxSmlGetDeviceDieId(device_id):
    fn = _mxsmlGetFunctionPointer("mxSmlGetDeviceDieId")
    die_id = c_uint()
    ret = fn(c_uint(device_id), byref(die_id))
    _mxsmlCheckReturn(ret)
    return die_id.value
