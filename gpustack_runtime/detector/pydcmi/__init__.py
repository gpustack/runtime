##
# Python bindings for the DCMI library
##
from __future__ import annotations

import string
import sys
import threading
from ctypes import *
from functools import wraps
from typing import ClassVar

## C Type mappings ##
## Constants ##
MAX_CHIP_NAME_LEN = 32
TEMPLATE_NAME_LEN = 32
DIE_ID_COUNT = 5
AGENTDRV_PROF_DATA_NUM = 3
DCMI_VDEV_RES_NAME_LEN = 16
DCMI_VDEV_SIZE = 20
DCMI_VDEV_FOR_RESERVE = 32
DCMI_SOC_SPLIT_MAX = 32
DCMI_MAX_EVENT_NAME_LENGTH = 256
DCMI_MAX_EVENT_DATA_LENGTH = 32
DCMI_EVENT_FILTER_FLAG_EVENT_ID = 1 << 0
DCMI_EVENT_FILTER_FLAG_SERVERITY = 1 << 1
DCMI_EVENT_FILTER_FLAG_NODE_TYPE = 1 << 2
DCMI_MAX_EVENT_RESV_LENGTH = 32
HCCS_MAX_PCS_NUM = 16
HCCS_RES_PCS_NUM = 64
IP_ADDR_LIST_LEN = 1024
HCCS_PING_MESH_MAX_NUM = 48
ADDR_MAX_LEN = 16
TOPO_INFO_MAX_LENGTH = 32

## Enums ##
DCMI_IPADDR_TYPE_V4 = 0
DCMI_IPADDR_TYPE_V6 = 1
DCMI_IPADDR_TYPE_ANY = 2

## Enums ##
DCMI_UNIT_TYPE_NPU = 0
DCMI_UNIT_TYPE_MCU = 1
DCMI_UNIT_TYPE_CPU = 2
DCMI_UNIT_TYPE_INVALID = 0xFF

## Enums ##
DCMI_RDFX_DETECT_OK = 0
DCMI_RDFX_DETECT_SOCK_FAIL = 1
DCMI_RDFX_DETECT_RECV_TIMEOUT = 2
DCMI_RDFX_DETECT_UNREACH = 3
DCMI_RDFX_DETECT_TIME_EXCEEDED = 4
DCMI_RDFX_DETECT_FAULT = 5
DCMI_RDFX_DETECT_INIT = 6
DCMI_RDFX_DETECT_THREAD_ERR = 7
DCMI_RDFX_DETECT_IP_SET = 8
DCMI_RDFX_DETECT_MAX = 0xFF

## Enums ##
DCMI_PORT_TYPE_VNIC_PORT = 0
DCMI_PORT_TYPE_ROCE_PORT = 1

## Enums ##
DCMI_MAIN_CMD_DVPP = 0
DCMI_MAIN_CMD_ISP = 1
DCMI_MAIN_CMD_TS_GROUP_NUM = 2
DCMI_MAIN_CMD_CAN = 3
DCMI_MAIN_CMD_UART = 4
DCMI_MAIN_CMD_UPGRADE = 5
DCMI_MAIN_CMD_HCCS = 16
DCMI_MAIN_CMD_TEMP = 50
DCMI_MAIN_CMD_SVM = 51
DCMI_MAIN_CMD_VDEV_MNG = 52
DCMI_MAIN_CMD_SIO = 56
DCMI_MAIN_CMD_DEVICE_SHARE = 0x8001
DCMI_MAIN_CMD_MAX = 57

## Enums ##
DCMI_SVM_SUB_CMD_CREATE = 1
DCMI_VMNG_SUB_CMD_GET_VDEV_RESOURCE = 0
DCMI_VMNG_SUB_CMD_GET_TOTAL_RESOURCE = 1
DCMI_VMNG_SUB_CMD_GET_FREE_RESOURCE = 2
DCMI_EX_COMPUTING_SUB_CMD_TOKEN = 1
DCMI_TS_SUB_CMD_AICORE_UTILIZATION_RATE = 0
DCMI_TS_SUB_CMD_VECTORCORE_UTILIZATION_RATE = 1
DCMI_TS_SUB_CMD_FFTS_TYPE = 2
DCMI_TS_SUB_CMD_SET_FAULT_MASK = 3
DCMI_TS_SUB_CMD_GET_FAULT_MASK = 4

## Enums ##
DCMI_FREQ_TYPE_DDR = 1
DCMI_FREQ_TYPE_CTRLCPU = 2
DCMI_FREQ_TYPE_HBM = 6
DCMI_FREQ_TYPE_AICORE_CURRENT_ = 7
DCMI_FREQ_TYPE_AICORE_MAX = 9
DCMI_FREQ_TYPE_VECTORCORE_CURRENT = 12

## Enums ##
DCMI_RESET_CHANNEL_OUTBAND = 0
DCMI_RESET_CHANNEL_INBAND = 1

## Enums ##
DCMI_BOOT_STATUS_UNINIT = 0
DCMI_BOOT_STATUS_BIOS = 1
DCMI_BOOT_STATUS_OS = 2
DCMI_BOOT_STATUS_FINISH = 3

## Enums ##
DCMI_DEVICE_TYPE_DDR = 0
DCMI_DEVICE_TYPE_SRAM = 1
DCMI_DEVICE_TYPE_HBM = 2
DCMI_DEVICE_TYPE_NPU = 3
DCMI_DEVICE_TYPE_NONE = 0xFF

## Enums ##
DCMI_INPUT_TYPE_MEMORY = 1
DCMI_INPUT_TYPE_AICORE = 2
DCMI_INPUT_TYPE_AICPU = 3
DCMI_INPUT_TYPE_CTRLCPU = 4
DCMI_INPUT_TYPE_MEM_BANDWIDTH = 5
DCMI_INPUT_TYPE_ONCHIP_MEMORY = 6
DCMI_INPUT_TYPE_ONCHIP_MEM_BANDWIDTH = 10

## Enums ##
DCMI_DMS_FAULT_EVENT = 0

## Enums ##
DCMI_DIE_TYPE_NDIE = 0
DCMI_DIE_TYPE_VDIE = 1

## Enums ##
DCMI_TOPO_TYPE_SELF = 0
DCMI_TOPO_TYPE_SYS = 1
DCMI_TOPO_TYPE_PHB = 2
DCMI_TOPO_TYPE_HCCS = 3
DCMI_TOPO_TYPE_PXB = 4
DCMI_TOPO_TYPE_PIX = 5
DCMI_TOPO_TYPE_BUTT = 6  # Unknown
DCMI_TOPO_TYOE_MAX = 7


## Error Codes ##
DCMI_SUCCESS = 0
DCMI_ERROR_INVALID_PARAMETER = -8001
DCMI_ERROR_MEM_OPERATE_FAIL = -8003
DCMI_ERROR_INVALID_DEVICE_ID = -8007
DCMI_ERROR_DEVICE_NOT_EXIST = -8008
DCMI_ERROR_CONFIG_INFO_NOT_EXIST = -8023
DCMI_ERROR_OPER_NOT_PERMITTED = -8002
DCMI_ERROR_NOT_SUPPORT_IN_CONTAINER = -8013
DCMI_ERROR_NOT_SUPPORT = -8255
DCMI_ERROR_TIME_OUT = -8006
DCMI_ERROR_NOT_REDAY = -8012
DCMI_ERROR_IS_UPGRADING = -8017
DCMI_ERROR_RESOURCE_OCCUPIED = -8020
DCMI_ERROR_SECURE_FUN_FAIL = -8004
DCMI_ERROR_INNER_ERR = -8005
DCMI_ERROR_IOCTL_FAIL = -8009
DCMI_ERROR_SEND_MSG_FAIL = -8010
DCMI_ERROR_RECV_MSG_FAIL = -8011
DCMI_ERROR_RESET_FAIL = -8015
DCMI_ERROR_ABORT_OPERATE = -8016
DCMI_ERROR_UNINITIALIZED = -99997
DCMI_ERROR_FUNCTION_NOT_FOUND = -99998
DCMI_ERROR_LIBRARY_NOT_FOUND = -99999

## Lib loading ##
dcmiLib = None
libLoadLock = threading.Lock()


## Error Checking ##
class DCMIError(Exception):
    _valClassMapping: ClassVar[dict] = {}

    _errcode_to_string: ClassVar[dict] = {
        DCMI_ERROR_INVALID_PARAMETER: "Invalid Parameter",
        DCMI_ERROR_MEM_OPERATE_FAIL: "Memory Operation Failed",
        DCMI_ERROR_INVALID_DEVICE_ID: "Invalid Device ID",
        DCMI_ERROR_DEVICE_NOT_EXIST: "Device Not Exist",
        DCMI_ERROR_CONFIG_INFO_NOT_EXIST: "Config Info Not Exist",
        DCMI_ERROR_OPER_NOT_PERMITTED: "Operation Not Permitted",
        DCMI_ERROR_NOT_SUPPORT_IN_CONTAINER: "Not Supported in Container",
        DCMI_ERROR_NOT_SUPPORT: "Not Supported",
        DCMI_ERROR_TIME_OUT: "Time Out",
        DCMI_ERROR_NOT_REDAY: "Not Ready",
        DCMI_ERROR_IS_UPGRADING: "Is Upgrading",
        DCMI_ERROR_RESOURCE_OCCUPIED: "Resource Occupied",
        DCMI_ERROR_SECURE_FUN_FAIL: "Secure Function Failed",
        DCMI_ERROR_INNER_ERR: "Inner Error",
        DCMI_ERROR_IOCTL_FAIL: "Ioctl Failed",
        DCMI_ERROR_SEND_MSG_FAIL: "Send Message Failed",
        DCMI_ERROR_RECV_MSG_FAIL: "Receive Message Failed",
        DCMI_ERROR_RESET_FAIL: "Reset Failed",
        DCMI_ERROR_ABORT_OPERATE: "Abort Operate",
        DCMI_ERROR_UNINITIALIZED: "Library Not Initialized",
        DCMI_ERROR_FUNCTION_NOT_FOUND: "Function Not Found",
        DCMI_ERROR_LIBRARY_NOT_FOUND: "Library Not Found",
    }

    def __new__(cls, value):
        """
        Maps value to a proper subclass of DCMIError.
        See _extractDCMIErrorsAsClasses function for more details.
        """
        if cls == DCMIError:
            cls = DCMIError._valClassMapping.get(value, cls)
        obj = Exception.__new__(cls)
        obj.value = value
        return obj

    def __str__(self):
        try:
            if self.value not in DCMIError._errcode_to_string:
                DCMIError._errcode_to_string[self.value] = (
                    f"Unknown DCMI Error {self.value}"
                )
            return DCMIError._errcode_to_string[self.value]
        except DCMIError:
            return f"DCMI Error with code {self.value}"

    def __eq__(self, other):
        if isinstance(other, DCMIError):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return False


def dcmiExceptionClass(dcmiErrorCode):
    if dcmiErrorCode not in DCMIError._valClassMapping:
        msg = f"DCMI error code {dcmiErrorCode} is not valid"
        raise ValueError(msg)
    return DCMIError._valClassMapping[dcmiErrorCode]


def _extractDCMIErrorsAsClasses():
    """
    Generates a hierarchy of classes on top of DCMIError class.

    Each DCMI Error gets a new DCMIError subclass. This way try,except blocks can filter appropriate
    exceptions more easily.

    DCMIError is a parent class. Each DCMI_ERROR_* gets it's own subclass.
    e.g. DCMI_ERROR_INVALID_PARAMETER will be turned into DCMIError_InvalidParameter.
    """
    this_module = sys.modules[__name__]
    dcmiErrorsNames = [x for x in dir(this_module) if x.startswith("DCMI_ERROR_")]
    for err_name in dcmiErrorsNames:
        # e.g. Turn DCMI_ERROR_INVALID_PARAMETER into DCMIError_InvalidParameter
        class_name = "DCMIError_" + string.capwords(
            err_name.replace("DCMI_ERROR_", ""),
            "_",
        ).replace("_", "")
        err_val = getattr(this_module, err_name)

        def gen_new(val):
            def new(typ, *args):
                obj = DCMIError.__new__(typ, val)
                return obj

            return new

        new_error_class = type(class_name, (DCMIError,), {"__new__": gen_new(err_val)})
        new_error_class.__module__ = __name__
        setattr(this_module, class_name, new_error_class)
        DCMIError._valClassMapping[err_val] = new_error_class


_extractDCMIErrorsAsClasses()


def _dcmiCheckReturn(ret):
    if ret != DCMI_SUCCESS:
        raise DCMIError(ret)
    return ret


## Function access ##
_dcmiGetFunctionPointer_cache = {}


def _dcmiGetFunctionPointer(name):
    global dcmiLib

    if name in _dcmiGetFunctionPointer_cache:
        return _dcmiGetFunctionPointer_cache[name]

    libLoadLock.acquire()
    try:
        if dcmiLib is None:
            raise DCMIError(DCMI_ERROR_UNINITIALIZED)
        try:
            _dcmiGetFunctionPointer_cache[name] = getattr(dcmiLib, name)
            return _dcmiGetFunctionPointer_cache[name]
        except AttributeError:
            raise DCMIError(DCMI_ERROR_FUNCTION_NOT_FOUND)
    finally:
        libLoadLock.release()


## Alternative object
# Allows the object to be printed
# Allows mismatched types to be assigned
#  - like None when the Structure variant requires c_uint
class dcmiFriendlyObject:
    def __init__(self, dictionary):
        for x in dictionary:
            setattr(self, x, dictionary[x])

    def __str__(self):
        return self.__dict__.__str__()


def dcmiStructToFriendlyObject(struct):
    d = {}
    for x in struct._fields_:
        key = x[0]
        value = getattr(struct, key)
        # only need to convert from bytes if bytes, no need to check python version.
        d[key] = value.decode() if isinstance(value, bytes) else value
    obj = dcmiFriendlyObject(d)
    return obj


# pack the object so it can be passed to the DCMI library
def dcmiFriendlyObjectToStruct(obj, model):
    for x in model._fields_:
        key = x[0]
        value = obj.__dict__[key]
        # any c_char_p in python3 needs to be bytes, default encoding works fine.
        setattr(model, key, value.encode())
    return model


## Structure definitions ##
class _PrintableStructure(Structure):
    """
    Abstract class that produces nicer __str__ output than ctypes.Structure.
    """

    _fmt_ = {}

    def __str__(self):
        result = []
        for x in self._fields_:
            key = x[0]
            value = getattr(self, key)
            fmt = "%s"
            if key in self._fmt_:
                fmt = self._fmt_[key]
            elif "<default>" in self._fmt_:
                fmt = self._fmt_["<default>"]
            result.append(("%s: " + fmt) % (key, value))
        return self.__class__.__name__ + "(" + ", ".join(result) + ")"

    def __getattribute__(self, name):
        res = super().__getattribute__(name)
        if isinstance(res, bytes):
            return res.decode()
        return res

    def __setattr__(self, name, value):
        if isinstance(value, str):
            value = value.encode()
        super().__setattr__(name, value)


class c_dcmi_chip_info(_PrintableStructure):
    _fields_: ClassVar = [
        ("chip_type", c_char * MAX_CHIP_NAME_LEN),
        ("chip_name", c_char * MAX_CHIP_NAME_LEN),
        ("chip_ver", c_char * MAX_CHIP_NAME_LEN),
        ("aicore_cnt", c_uint),
    ]


class c_dcmi_chip_info_v2(_PrintableStructure):
    _fields_: ClassVar = [
        ("chip_type", c_char * MAX_CHIP_NAME_LEN),
        ("chip_name", c_char * MAX_CHIP_NAME_LEN),
        ("chip_ver", c_char * MAX_CHIP_NAME_LEN),
        ("aicore_cnt", c_uint),
        ("npu_name", c_char * MAX_CHIP_NAME_LEN),
    ]


class c_dcmi_pcie_info_all(_PrintableStructure):
    _fields_: ClassVar = [
        ("venderid", c_uint),
        ("subvenderid", c_uint),
        ("deviceid", c_uint),
        ("subdeviceid", c_uint),
        ("domain", c_int),
        ("bdf_busid", c_uint),
        ("bdf_deviceid", c_uint),
        ("bdf_funcid", c_uint),
        ("reserve", c_char * 32),
    ]


class c_dcmi_die_id(_PrintableStructure):
    _fields_: ClassVar = [
        ("soc_die", c_uint * DIE_ID_COUNT),
    ]


class c_dcmi_ecc_info(_PrintableStructure):
    _fields_: ClassVar = [
        ("enable_flag", c_int),
        ("single_bit_error_cnt", c_uint),
        ("double_bit_error_cnt", c_uint),
        ("total_single_bit_error_cnt", c_uint),
        ("total_double_bit_error_cnt", c_uint),
        ("single_bit_isolated_pages_cnt", c_uint),
        ("double_bit_isolated_pages_cnt", c_uint),
        ("single_bit_next_isolated_pages_cnt", c_uint),
        ("double_bit_next_isolated_pages_cnt", c_uint),
    ]


class c_dcmi_hbm_info(_PrintableStructure):
    _fields_: ClassVar = [
        ("memory_size", c_ulonglong),
        ("freq", c_uint),
        ("memory_usage", c_ulonglong),
        ("temp", c_int),
        ("bandwith_util_rate", c_uint),
    ]


class c_dcmi_get_memory_info_stru(_PrintableStructure):
    _fields_: ClassVar = [
        ("memory_size", c_ulonglong),
        ("memory_available", c_ulonglong),
        ("freq", c_uint),
        ("hugepagesize", c_ulong),
        ("hugepages_total", c_ulong),
        ("hugepages_free", c_ulong),
        ("utiliza", c_uint),
        ("reserve", c_char * 60),
    ]


class c_dcmi_ip_addr_union(Union):
    _fields_: ClassVar = [
        ("ip6", c_ubyte * 16),
        ("ip4", c_ubyte * 4),
    ]


class c_dcmi_ip_addr(_PrintableStructure):
    _fields_: ClassVar = [
        ("u_addr", c_dcmi_ip_addr_union),
        ("ip_type", c_uint),
    ]

    def __str__(self):
        if self.ip_type == DCMI_IPADDR_TYPE_V4:
            parts = [str(b) for b in self.u_addr.ip4]
            return ".".join(parts)
        if self.ip_type == DCMI_IPADDR_TYPE_V6:
            parts = [
                f"{self.u_addr.ip6[i] << 8 | self.u_addr.ip6[i + 1]:x}"
                for i in range(0, 16, 2)
            ]
            return ":".join(parts)
        return ""


class c_dcmi_base_resource(_PrintableStructure):
    _fields_: ClassVar = [
        ("token", c_ulonglong),
        ("token_max", c_ulonglong),
        ("task_timeout", c_ulonglong),
        ("vfg_id", c_uint),
        ("vip_mode", c_ubyte),
        ("reserved", c_char * (DCMI_VDEV_FOR_RESERVE - 1)),
    ]


class c_dcmi_computing_resource(_PrintableStructure):
    _fields_: ClassVar = [
        ("aic", c_float),
        ("aiv", c_float),
        ("dsa", c_ushort),
        ("rtsq", c_ushort),
        ("acsq", c_ushort),
        ("cdqm", c_ushort),
        ("c_core", c_ushort),
        ("ffts", c_ushort),
        ("sdma", c_ushort),
        ("pcie_dma", c_ushort),
        ("memory_size", c_ulonglong),
        ("event_id", c_uint),
        ("notify_id", c_uint),
        ("stream_id", c_uint),
        ("model_id", c_uint),
        ("topic_schedule_aicpu", c_ushort),
        ("host_ctrl_cpu", c_ushort),
        ("host_aicpu", c_ushort),
        ("device_aicpu", c_ushort),
        ("topic_ctrl_cpu_slot", c_ushort),
        ("vdev_aicore_utilization", c_uint),
        ("vdev_memory_total", c_ulonglong),
        ("vdev_memory_free", c_ulonglong),
        ("reserved", c_char * (DCMI_VDEV_FOR_RESERVE - DCMI_VDEV_SIZE)),
    ]


class c_dcmi_media_resource(_PrintableStructure):
    _fields_: ClassVar = [
        ("jpegd", c_float),
        ("jpege", c_float),
        ("vpc", c_float),
        ("vdec", c_float),
        ("pngd", c_float),
        ("venc", c_float),
        ("reserved", c_char * DCMI_VDEV_FOR_RESERVE),
    ]


class c_dcmi_create_vdev_out(_PrintableStructure):
    _fields_: ClassVar = [
        ("vdev_id", c_uint),
        ("pcie_bus", c_uint),
        ("pcie_device", c_uint),
        ("pcie_func", c_uint),
        ("vfg_id", c_uint),
        ("reserved", c_char * DCMI_VDEV_FOR_RESERVE),
    ]


class c_dcmi_create_vdev_res_stru(_PrintableStructure):
    _fields_: ClassVar = [
        ("vdev_id", c_uint),
        ("vfg_id", c_uint),
        ("template_name", c_char * TEMPLATE_NAME_LEN),
        ("reserved", c_char * 64),
    ]


class c_dcmi_vdev_query_info(_PrintableStructure):
    _fields_: ClassVar = [
        ("name", c_char * DCMI_VDEV_RES_NAME_LEN),
        ("status", c_uint),
        ("is_container_used", c_uint),
        ("vfid", c_uint),
        ("vfg_id", c_uint),
        ("container_id", c_ulonglong),
        ("base", c_dcmi_base_resource),
        ("computing", c_dcmi_computing_resource),
        ("media", c_dcmi_media_resource),
    ]


class c_dcmi_vdev_query_stru(_PrintableStructure):
    _fields_: ClassVar = [
        ("vdev_id", c_uint),
        ("query_info", c_dcmi_vdev_query_info),
    ]


class c_dcmi_soc_free_resource(_PrintableStructure):
    _fields_: ClassVar = [
        ("vfg_num", c_uint),
        ("vfg_bitmap", c_uint),
        ("base", c_dcmi_base_resource),
        ("computing", c_dcmi_computing_resource),
        ("media", c_dcmi_media_resource),
    ]


class c_dcmi_soc_total_resource(_PrintableStructure):
    _fields_: ClassVar = [
        ("vdev_num", c_uint),
        ("vdev_id", c_uint * DCMI_SOC_SPLIT_MAX),
        ("vfg_num", c_uint),
        ("vfg_bitmap", c_uint),
        ("base", c_dcmi_base_resource),
        ("computing", c_dcmi_computing_resource),
        ("media", c_dcmi_media_resource),
    ]


class c_dcmi_spod_info(_PrintableStructure):
    _fields_: ClassVar = [
        ("sdid", c_uint),
        ("scale_type", c_uint),
        ("super_pod_id", c_uint),
        ("server_id", c_uint),
        ("reserve", c_uint * 8),
    ]


class c_dcmi_dms_fault_event(_PrintableStructure):
    _fields_: ClassVar = [
        ("event_id", c_uint),
        ("deviceid", c_ushort),
        ("node_type", c_ubyte),
        ("node_id", c_ubyte),
        ("sub_node_type", c_ubyte),
        ("sub_node_id", c_ubyte),
        ("severity", c_ubyte),
        ("assertion", c_ubyte),
        ("event_serial_num", c_int),
        ("notify_serial_num", c_int),
        ("alarm_raised_time", c_ulonglong),
        ("event_name", c_char * DCMI_MAX_EVENT_NAME_LENGTH),
        ("additional_info", c_char * DCMI_MAX_EVENT_DATA_LENGTH),
        ("resv", c_char * DCMI_MAX_EVENT_RESV_LENGTH),
    ]


class c_dcmi_event(_PrintableStructure):
    _fields_: ClassVar = [
        ("type", c_uint),
        ("event_t", c_dcmi_dms_fault_event),
    ]


class c_dcmi_event_filter(_PrintableStructure):
    _fields_: ClassVar = [
        ("filter_flag", c_ulonglong),
        ("event_id", c_uint),
        ("severity", c_ubyte),
        ("node_type", c_ubyte),
        ("resv", c_char * DCMI_MAX_EVENT_RESV_LENGTH),
    ]


class c_dcmi_proc_mem_info(_PrintableStructure):
    _fields_: ClassVar = [
        ("proc_id", c_int),
        ("proc_mem_usage", c_ulonglong),
    ]


class c_dcmi_board_info(_PrintableStructure):
    _fields_: ClassVar = [
        ("board_id", c_uint),
        ("pcb_id", c_uint),
        ("bom_id", c_uint),
        ("slot_id", c_uint),
    ]


class c_dcmi_pcie_link_bandwidth_info(_PrintableStructure):
    _fields_: ClassVar = [
        ("profiling_time", c_int),
        ("tx_p_bw", c_uint * AGENTDRV_PROF_DATA_NUM),
        ("tx_np_bw", c_uint * AGENTDRV_PROF_DATA_NUM),
        ("tx_cpl_bw", c_uint * AGENTDRV_PROF_DATA_NUM),
        ("tx_np_lantency", c_uint * AGENTDRV_PROF_DATA_NUM),
        ("rx_p_bw", c_uint * AGENTDRV_PROF_DATA_NUM),
        ("rx_np_bw", c_uint * AGENTDRV_PROF_DATA_NUM),
        ("rx_cpl_bw", c_uint * AGENTDRV_PROF_DATA_NUM),
    ]


class c_dcmi_hccs_statistic_info(_PrintableStructure):
    _fields_: ClassVar = [
        ("tx_cnt", c_uint * HCCS_MAX_PCS_NUM),
        ("rx_cnt", c_uint * HCCS_MAX_PCS_NUM),
        ("crc_err_cnt", c_uint * HCCS_MAX_PCS_NUM),
        ("retry_cnt", c_uint * HCCS_MAX_PCS_NUM),
        ("reserved_field_cnt", c_uint * HCCS_RES_PCS_NUM),
    ]


class c_dcmi_hccs_bandwidth_info(_PrintableStructure):
    _fields_: ClassVar = [
        ("profiling_time", c_int),
        ("total_txbw", c_double),
        ("total_rxbw", c_double),
        ("tx_bandwidth", c_double * HCCS_MAX_PCS_NUM),
        ("rx_bandwidth", c_double * HCCS_MAX_PCS_NUM),
    ]


class c_dcmi_sio_crc_err_statistic_info(_PrintableStructure):
    _fields_: ClassVar = [
        ("tx_error_count", c_ushort),
        ("rx_error_count", c_ushort),
        ("reserved", c_char * 8),
    ]


class c_dcmi_hccsping_mesh_operate(_PrintableStructure):
    _fields_: ClassVar = [
        ("dst_addr_list", c_char * IP_ADDR_LIST_LEN),
        ("pkt_size", c_int),
        ("pkt_send_num", c_int),
        ("pkt_interval", c_int),
        ("timeout", c_int),
        ("task_interval", c_int),
        ("task_id", c_int),
    ]


class c_dcmi_hccsping_mesh_info(_PrintableStructure):
    _fields_: ClassVar = [
        ("dst_addr", c_char * HCCS_PING_MESH_MAX_NUM * ADDR_MAX_LEN),
        ("suc_pkt_num", c_uint * HCCS_PING_MESH_MAX_NUM),
        ("fail_pkt_num", c_uint * HCCS_PING_MESH_MAX_NUM),
        ("max_time", c_long * HCCS_PING_MESH_MAX_NUM),
        ("min_time", c_long * HCCS_PING_MESH_MAX_NUM),
        ("avg_time", c_long * HCCS_PING_MESH_MAX_NUM),
        ("tp95_time", c_long * HCCS_PING_MESH_MAX_NUM),
        ("reply_stat_num", c_int * HCCS_PING_MESH_MAX_NUM),
        ("ping_total_num", c_ulonglong * HCCS_PING_MESH_MAX_NUM),
        ("dest_num", c_int),
    ]


## string/bytes conversion for ease of use
def convertStrBytes(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # encoding a str returns bytes in python 2 and 3
        args = [arg.encode() if isinstance(arg, str) else arg for arg in args]
        res = func(*args, **kwargs)
        # In python 2, str and bytes are the same
        # In python 3, str is unicode and should be decoded.
        # Ctypes handles most conversions, this only effects c_char and char arrays.
        if isinstance(res, bytes):
            if isinstance(res, str):
                return res
            return res.decode()
        return res

    return wrapper


def _LoadDcmiLibrary():
    global dcmiLib
    if dcmiLib is None:
        libLoadLock.acquire()
        try:
            if dcmiLib is None:
                if sys.platform.startswith("win"):
                    # DCMI is typically used on Linux, but for completeness,
                    # Windows support would require different path handling.
                    raise DCMIError(DCMI_ERROR_LIBRARY_NOT_FOUND)
                # Linux path
                locs = [
                    "libdcmi.so",
                    "/usr/local/Ascend/driver/lib64/driver/libdcmi.so",
                    "/usr/local/dcmi/libdcmi.so",
                ]
                for loc in locs:
                    try:
                        dcmiLib = CDLL(loc)
                        break
                    except OSError:
                        pass
                if dcmiLib is None:
                    raise DCMIError(DCMI_ERROR_LIBRARY_NOT_FOUND)
        finally:
            libLoadLock.release()


## C function wrappers ##
def dcmi_init():
    _LoadDcmiLibrary()

    # Initialize the library
    fn = _dcmiGetFunctionPointer("dcmi_init")
    ret = fn()
    _dcmiCheckReturn(ret)


def dcmi_get_card_list():
    c_card_num = c_int()
    c_card_list = (c_int * 64)()
    fn = _dcmiGetFunctionPointer("dcmi_get_card_list")
    ret = fn(byref(c_card_num), c_card_list, 64)
    _dcmiCheckReturn(ret)
    return c_card_num.value, list(c_card_list[: c_card_num.value])


def dcmi_get_device_num_in_card(card_id):
    c_device_num = c_int()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_num_in_card")
    ret = fn(card_id, byref(c_device_num))
    _dcmiCheckReturn(ret)
    return c_device_num.value


def dcmi_get_device_id_in_card(card_id):
    c_device_id_max = c_int()
    c_mcu_id = c_int()
    c_cpu_id = c_int()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_id_in_card")
    ret = fn(card_id, byref(c_device_id_max), byref(c_mcu_id), byref(c_cpu_id))
    _dcmiCheckReturn(ret)
    return c_device_id_max.value, c_mcu_id.value, c_cpu_id.value


def dcmi_get_device_type(card_id, device_id):
    c_device_type = c_uint()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_type")
    ret = fn(card_id, device_id, byref(c_device_type))
    _dcmiCheckReturn(ret)
    return c_device_type.value


def dcmi_get_device_pcie_info_v2(card_id, device_id):
    c_pcie_info = c_dcmi_pcie_info_all()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_pcie_info_v2")
    ret = fn(card_id, device_id, byref(c_pcie_info))
    _dcmiCheckReturn(ret)
    return c_pcie_info


def dcmi_get_device_bdf(card_id, device_id):
    c_pcie_info = dcmi_get_device_pcie_info_v2(card_id, device_id)

    domain = c_pcie_info.domain
    bus = c_pcie_info.bdf_busid
    device = c_pcie_info.bdf_deviceid
    function = c_pcie_info.bdf_funcid
    return f"{domain:04x}:{bus:02x}:{device:02x}.{function:x}"


def dcmi_get_device_chip_info(card_id, device_id):
    c_chip_info = c_dcmi_chip_info()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_chip_info")
    ret = fn(card_id, device_id, byref(c_chip_info))
    _dcmiCheckReturn(ret)
    return c_chip_info


def dcmi_get_device_chip_info_v2(card_id, device_id):
    try:
        c_chip_info = c_dcmi_chip_info_v2()
        fn = _dcmiGetFunctionPointer("dcmi_get_device_chip_info_v2")
        ret = fn(card_id, device_id, byref(c_chip_info))
        _dcmiCheckReturn(ret)
    except DCMIError as e:
        if e.value != DCMI_ERROR_FUNCTION_NOT_FOUND:
            raise
        return dcmi_get_device_chip_info(card_id, device_id)
    return c_chip_info


def dcmi_get_device_power_info(card_id, device_id):
    c_power = c_int()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_power_info")
    ret = fn(card_id, device_id, byref(c_power))
    _dcmiCheckReturn(ret)
    return c_power.value


def dcmi_get_device_health(card_id, device_id):
    c_health = c_uint()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_health")
    ret = fn(card_id, device_id, byref(c_health))
    _dcmiCheckReturn(ret)
    return c_health.value


def dcmi_get_device_errorcode_v2(card_id, device_id, list_len=1024):
    c_error_count = c_int()
    c_error_code_list = (c_uint * list_len)()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_errorcode_v2")
    ret = fn(card_id, device_id, byref(c_error_count), c_error_code_list, list_len)
    _dcmiCheckReturn(ret)
    return list(c_error_code_list[: c_error_count.value])


def dcmi_get_device_temperature(card_id, device_id):
    c_temperature = c_int()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_temperature")
    ret = fn(card_id, device_id, byref(c_temperature))
    _dcmiCheckReturn(ret)
    return c_temperature.value


def dcmi_get_device_voltage(card_id, device_id):
    c_voltage = c_uint()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_voltage")
    ret = fn(card_id, device_id, byref(c_voltage))
    _dcmiCheckReturn(ret)
    return c_voltage.value


def dcmi_get_device_ecc_info(card_id, device_id, device_type):
    c_device_ecc_info = c_dcmi_ecc_info()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_ecc_info")
    ret = fn(card_id, device_id, device_type, byref(c_device_ecc_info))
    _dcmiCheckReturn(ret)
    return c_device_ecc_info


def dcmi_get_device_frequency(card_id, device_id, freq_type):
    c_frequency = c_uint()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_frequency")
    ret = fn(card_id, device_id, freq_type, byref(c_frequency))
    _dcmiCheckReturn(ret)
    return c_frequency.value


def dcmi_get_device_hbm_info(card_id, device_id):
    c_hbm_info = c_dcmi_hbm_info()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_hbm_info")
    ret = fn(card_id, device_id, byref(c_hbm_info))
    _dcmiCheckReturn(ret)
    return c_hbm_info


def dcmi_get_device_memory_info_v3(card_id, device_id):
    c_memory_info = c_dcmi_get_memory_info_stru()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_memory_info_v3")
    ret = fn(card_id, device_id, byref(c_memory_info))
    _dcmiCheckReturn(ret)
    return c_memory_info


def dcmi_get_device_utilization_rate(card_id, device_id, input_type):
    c_utilization_rate = c_uint()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_utilization_rate")
    ret = fn(card_id, device_id, input_type, byref(c_utilization_rate))
    _dcmiCheckReturn(ret)
    return c_utilization_rate.value


def dcmi_get_device_info(card_id, device_id, main_cmd, sub_cmd, result):
    c_size = c_uint(sizeof(result))
    fn = _dcmiGetFunctionPointer("dcmi_get_device_info")
    ret = fn(card_id, device_id, main_cmd, sub_cmd, byref(result), byref(c_size))
    _dcmiCheckReturn(ret)


def dcmi_get_device_ip(card_id, device_id, port_type, port_id=0):
    c_ip = c_dcmi_ip_addr()
    c_mask = c_dcmi_ip_addr()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_ip")
    ret = fn(card_id, device_id, port_type, port_id, byref(c_ip), byref(c_mask))
    _dcmiCheckReturn(ret)
    return c_ip, c_mask


def dcmi_get_device_gateway(card_id, device_id, port_type, port_id=0):
    c_gateway = c_dcmi_ip_addr()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_gateway")
    ret = fn(card_id, device_id, port_type, port_id, byref(c_gateway))
    _dcmiCheckReturn(ret)
    return c_gateway


def dcmi_get_device_network_health(card_id, device_id):
    c_result = c_uint()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_network_health")
    ret = fn(card_id, device_id, byref(c_result))
    _dcmiCheckReturn(ret)
    return c_result.value


def dcmi_get_device_logic_id(card_id, device_id):
    c_logic_id = c_uint()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_logic_id")
    ret = fn(byref(c_logic_id), card_id, device_id)
    _dcmiCheckReturn(ret)
    return c_logic_id.value


def dcmi_create_vdevice(card_id, device_id, vdev):
    c_out = c_dcmi_create_vdev_out()
    fn = _dcmiGetFunctionPointer("dcmi_create_vdevice")
    ret = fn(card_id, device_id, byref(vdev), byref(c_out))
    _dcmiCheckReturn(ret)
    return c_out


def dcmi_set_destroy_vdevice(card_id, device_id, vdevid):
    fn = _dcmiGetFunctionPointer("dcmi_set_destroy_vdevice")
    ret = fn(card_id, device_id, vdevid)
    _dcmiCheckReturn(ret)


def dcmi_get_device_phyid_from_logicid(logicid):
    c_phyid = c_uint()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_phyid_from_logicid")
    ret = fn(logicid, byref(c_phyid))
    _dcmiCheckReturn(ret)
    return c_phyid.value


def dcmi_get_device_logicid_from_phyid(phyid):
    c_logicid = c_uint()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_logicid_from_phyid")
    ret = fn(phyid, byref(c_logicid))
    _dcmiCheckReturn(ret)
    return c_logicid.value


def dcmi_get_card_id_device_id_from_logicid(device_logic_id):
    c_card_id = c_int()
    c_device_id = c_int()
    fn = _dcmiGetFunctionPointer("dcmi_get_card_id_device_id_from_logicid")
    ret = fn(byref(c_card_id), byref(c_device_id), device_logic_id)
    _dcmiCheckReturn(ret)
    return (c_card_id.value, c_device_id.value)


def dcmi_get_card_id_device_id_from_phyid(device_phy_id):
    c_card_id = c_int()
    c_device_id = c_int()
    fn = _dcmiGetFunctionPointer("dcmi_get_card_id_device_id_from_phyid")
    ret = fn(byref(c_card_id), byref(c_device_id), device_phy_id)
    _dcmiCheckReturn(ret)
    return (c_card_id.value, c_device_id.value)


def dcmi_get_product_type(card_id, device_id, buf_size=128):
    c_product_type = create_string_buffer(buf_size)
    fn = _dcmiGetFunctionPointer("dcmi_get_product_type")
    ret = fn(card_id, device_id, c_product_type, buf_size)
    _dcmiCheckReturn(ret)
    return c_product_type.value


def dcmi_set_device_reset(card_id, device_id, channel_type):
    fn = _dcmiGetFunctionPointer("dcmi_set_device_reset")
    ret = fn(card_id, device_id, channel_type)
    _dcmiCheckReturn(ret)


def dcmi_get_device_outband_channel_state(card_id, device_id):
    c_channel_state = c_int()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_outband_channel_state")
    ret = fn(card_id, device_id, byref(c_channel_state))
    _dcmiCheckReturn(ret)
    return c_channel_state.value


def dcmi_pre_reset_soc(card_id, device_id):
    fn = _dcmiGetFunctionPointer("dcmi_pre_reset_soc")
    ret = fn(card_id, device_id)
    _dcmiCheckReturn(ret)


def dcmi_rescan_soc(card_id, device_id):
    fn = _dcmiGetFunctionPointer("dcmi_rescan_soc")
    ret = fn(card_id, device_id)
    _dcmiCheckReturn(ret)


def dcmi_get_netdev_brother_device(card_id, device_id):
    c_brother_card_id = c_int()
    fn = _dcmiGetFunctionPointer("dcmi_get_netdev_brother_device")
    ret = fn(card_id, device_id, byref(c_brother_card_id))
    _dcmiCheckReturn(ret)
    return c_brother_card_id.value


def dcmi_get_device_boot_status(card_id, device_id):
    c_boot_status = c_uint()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_boot_status")
    ret = fn(card_id, device_id, byref(c_boot_status))
    _dcmiCheckReturn(ret)
    return c_boot_status.value


def dcmi_subscribe_fault_event(card_id, device_id, filter):
    fn = _dcmiGetFunctionPointer("dcmi_subscribe_fault_event")
    ret = fn(card_id, device_id, byref(filter))
    _dcmiCheckReturn(ret)


def dcmi_get_npu_work_mode(card_id):
    c_work_mode = c_ubyte()
    fn = _dcmiGetFunctionPointer("dcmi_get_npu_work_mode")
    ret = fn(card_id, byref(c_work_mode))
    _dcmiCheckReturn(ret)
    return c_work_mode.value


def dcmi_get_device_die_v2(card_id, device_id, input_type):
    c_die_id = c_dcmi_die_id()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_die_v2")
    ret = fn(card_id, device_id, input_type, byref(c_die_id))
    _dcmiCheckReturn(ret)
    return " ".join([hex(i)[2:] for i in c_die_id.soc_die])


def dcmi_get_device_resource_info(card_id, device_id, proc_num):
    c_proc_info = c_dcmi_proc_mem_info()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_resource_info")
    ret = fn(card_id, device_id, byref(c_proc_info), byref(proc_num))
    _dcmiCheckReturn(ret)
    return c_proc_info


def dcmi_get_device_board_info(card_id, device_id):
    c_board_info = c_dcmi_board_info()
    fn = _dcmiGetFunctionPointer("dcmi_get_device_board_info")
    ret = fn(card_id, device_id, byref(c_board_info))
    _dcmiCheckReturn(ret)
    return c_board_info


def dcmi_get_pcie_link_bandwidth_info(card_id, device_id):
    c_pcie_link_bandwidth_info = c_dcmi_pcie_link_bandwidth_info()
    fn = _dcmiGetFunctionPointer("dcmi_get_pcie_link_bandwidth_info")
    ret = fn(card_id, device_id, byref(c_pcie_link_bandwidth_info))
    _dcmiCheckReturn(ret)
    return c_pcie_link_bandwidth_info


@convertStrBytes
def dcmi_get_driver_version():
    c_driver_ver = create_string_buffer(64)
    fn = _dcmiGetFunctionPointer("dcmi_get_driver_version")
    ret = fn(c_driver_ver, c_uint(64))
    _dcmiCheckReturn(ret)
    return c_driver_ver.value


@convertStrBytes
def dcmi_get_dcmi_version():
    c_dcmi_ver = create_string_buffer(32)
    fn = _dcmiGetFunctionPointer("dcmi_get_dcmi_version")
    ret = fn(c_dcmi_ver, c_uint(32))
    _dcmiCheckReturn(ret)
    return c_dcmi_ver.value


def dcmi_get_mainboard_id(card_id, device_id):
    c_mainboard_id = c_uint()
    fn = _dcmiGetFunctionPointer("dcmi_get_mainboard_id")
    ret = fn(card_id, device_id, byref(c_mainboard_id))
    _dcmiCheckReturn(ret)
    return c_mainboard_id.value


def dcmi_get_hccs_link_bandwidth_info(card_id, device_id):
    c_hccs_bandwidth_info = c_dcmi_hccs_bandwidth_info()
    fn = _dcmiGetFunctionPointer("dcmi_get_hccs_link_bandwidth_info")
    ret = fn(card_id, device_id, byref(c_hccs_bandwidth_info))
    _dcmiCheckReturn(ret)
    return c_hccs_bandwidth_info


def dcmi_start_hccsping_mesh(card_id, device_id, port_id, hccsping_mesh):
    fn = _dcmiGetFunctionPointer("dcmi_start_hccsping_mesh")
    ret = fn(card_id, device_id, port_id, byref(hccsping_mesh))
    _dcmiCheckReturn(ret)


def dcmi_stop_hccsping_mesh(card_id, device_id, port_id, task_id):
    fn = _dcmiGetFunctionPointer("dcmi_stop_hccsping_mesh")
    ret = fn(card_id, device_id, port_id, task_id)
    _dcmiCheckReturn(ret)


def dcmi_get_hccsping_mesh_info(card_id, device_id, port_id, task_id):
    c_hccsping_mesh_reply = c_dcmi_hccsping_mesh_info()
    fn = _dcmiGetFunctionPointer("dcmi_get_hccsping_mesh_info")
    ret = fn(card_id, device_id, port_id, task_id, byref(c_hccsping_mesh_reply))
    _dcmiCheckReturn(ret)
    return c_hccsping_mesh_reply


def dcmi_get_hccsping_mesh_state(card_id, device_id, port_id, task_id):
    c_state = c_uint()
    fn = _dcmiGetFunctionPointer("dcmi_get_hccsping_mesh_state")
    ret = fn(card_id, device_id, port_id, task_id, byref(c_state))
    _dcmiCheckReturn(ret)
    return c_state.value


def dcmi_get_spod_node_status(card_id, device_id, sdid):
    c_status = c_uint()
    fn = _dcmiGetFunctionPointer("dcmi_get_spod_node_status")
    ret = fn(card_id, device_id, sdid, byref(c_status))
    _dcmiCheckReturn(ret)
    return c_status.value


def dcmi_set_spod_node_status(card_id, device_id, sdid, status):
    fn = _dcmiGetFunctionPointer("dcmi_set_spod_node_status")
    ret = fn(card_id, device_id, sdid, status)
    _dcmiCheckReturn(ret)


def dcmi_get_topo_info_by_device_id(card_id1, device_id1, card_id2, device_id2):
    c_topo_info = c_int()
    fn = _dcmiGetFunctionPointer("dcmi_get_topo_info_by_device_id")
    ret = fn(card_id1, device_id1, card_id2, device_id2, byref(c_topo_info))
    _dcmiCheckReturn(ret)
    return c_topo_info.value


@convertStrBytes
def dcmi_get_affinity_cpu_info_by_device_id(card_id, device_id):
    c_cpu_info = create_string_buffer(TOPO_INFO_MAX_LENGTH)
    c_cpu_info_len = c_int()
    fn = _dcmiGetFunctionPointer("dcmi_get_affinity_cpu_info_by_device_id")
    ret = fn(card_id, device_id, c_cpu_info, byref(c_cpu_info_len))
    _dcmiCheckReturn(ret)
    return c_cpu_info.value
