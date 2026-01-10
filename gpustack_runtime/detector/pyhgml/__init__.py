##
# Python bindings for the HGML library
##
import string
import sys
import threading
from ctypes import *
from functools import wraps
from pathlib import Path

## C Type mappings ##
## Enums
_hgmlEnableState_t = c_uint
HGML_FEATURE_DISABLED = 0
HGML_FEATURE_ENABLED = 1

_hgmlBrandType_t = c_uint
HGML_BRAND_UNKNOWN = 0
HGML_BRAND_BEETHOVEN = 14
HGML_BRAND_COUNT = 15

_hgmlTemperatureThresholds_t = c_uint
HGML_TEMPERATURE_THRESHOLD_SHUTDOWN = 0
HGML_TEMPERATURE_THRESHOLD_SLOWDOWN = 1
HGML_TEMPERATURE_THRESHOLD_MEM_MAX = 2
HGML_TEMPERATURE_THRESHOLD_GPU_MAX = 3
HGML_TEMPERATURE_THRESHOLD_ACOUSTIC_MIN = 4
HGML_TEMPERATURE_THRESHOLD_ACOUSTIC_CURR = 5
HGML_TEMPERATURE_THRESHOLD_ACOUSTIC_MAX = 6
HGML_TEMPERATURE_THRESHOLD_COUNT = 7

_hgmlTemperatureSensors_t = c_uint
HGML_TEMPERATURE_GPU = 0
HGML_TEMPERATURE_COUNT = 1

_hgmlComputeMode_t = c_uint
HGML_COMPUTEMODE_DEFAULT = 0
HGML_COMPUTEMODE_EXCLUSIVE_THREAD = 1  ## Support Removed
HGML_COMPUTEMODE_PROHIBITED = 2
HGML_COMPUTEMODE_EXCLUSIVE_PROCESS = 3
HGML_COMPUTEMODE_COUNT = 4

_hgmlMemoryLocation_t = c_uint
HGML_MEMORY_LOCATION_L1_CACHE = 0
HGML_MEMORY_LOCATION_L2_CACHE = 1
HGML_MEMORY_LOCATION_DRAM = 2
HGML_MEMORY_LOCATION_DEVICE_MEMORY = 2
HGML_MEMORY_LOCATION_REGISTER_FILE = 3
HGML_MEMORY_LOCATION_TEXTURE_MEMORY = 4
HGML_MEMORY_LOCATION_TEXTURE_SHM = 5
HGML_MEMORY_LOCATION_CBU = 6
HGML_MEMORY_LOCATION_SRAM = 7
HGML_MEMORY_LOCATION_COUNT = 8

HGML_ICNLINK_MAX_LINKS = 18

_hgmlIcnLinkErrorCounter_t = c_uint
HGML_ICNLINK_ERROR_DL_REPLAY = 0
HGML_ICNLINK_ERROR_DL_RECOVERY = 1
HGML_ICNLINK_ERROR_DL_CRC_FLIT = 2
HGML_ICNLINK_ERROR_DL_CRC_DATA = 3
HGML_ICNLINK_ERROR_DL_ECC_DATA = 4
HGML_ICNLINK_ERROR_COUNT = 5

_hgmlIcnLinkCapability_t = c_uint
HGML_ICNLINK_CAP_P2P_SUPPORTED = 0
HGML_ICNLINK_CAP_SYSMEM_ACCESS = 1
HGML_ICNLINK_CAP_P2P_ATOMICS = 2
HGML_ICNLINK_CAP_SYSMEM_ATOMICS = 3
HGML_ICNLINK_CAP_SLI_BRIDGE = 4
HGML_ICNLINK_CAP_VALID = 5
HGML_ICNLINK_CAP_COUNT = 6

_hgmlIcnLinkUtilizationCountPktTypes_t = c_uint
HGML_ICNLINK_COUNTER_PKTFILTER_NOP = 0x1
HGML_ICNLINK_COUNTER_PKTFILTER_READ = 0x2
HGML_ICNLINK_COUNTER_PKTFILTER_WRITE = 0x4
HGML_ICNLINK_COUNTER_PKTFILTER_RATOM = 0x8
HGML_ICNLINK_COUNTER_PKTFILTER_NRATOM = 0x10
HGML_ICNLINK_COUNTER_PKTFILTER_FLUSH = 0x20
HGML_ICNLINK_COUNTER_PKTFILTER_RESPDATA = 0x40
HGML_ICNLINK_COUNTER_PKTFILTER_RESPNODATA = 0x80
HGML_ICNLINK_COUNTER_PKTFILTER_ALL = 0xFF

_hgmlIcnLinkUtilizationCountUnits_t = c_uint
HGML_ICNLINK_COUNTER_UNIT_CYCLES = 0
HGML_ICNLINK_COUNTER_UNIT_PACKETS = 1
HGML_ICNLINK_COUNTER_UNIT_BYTES = 2
HGML_ICNLINK_COUNTER_UNIT_RESERVED = 3
HGML_ICNLINK_COUNTER_UNIT_COUNT = 4

_hgmlIcnLinkDeviceType_t = c_uint
HGML_ICNLINK_DEVICE_TYPE_PPU = 0x00
HGML_ICNLINK_DEVICE_TYPE_SWITCH = 0x02
HGML_ICNLINK_DEVICE_TYPE_UNKNOWN = 0xFF

# These are deprecated, instead use _hgmlMemoryErrorType_t
_hgmlEccBitType_t = c_uint
HGML_SINGLE_BIT_ECC = 0
HGML_DOUBLE_BIT_ECC = 1
HGML_ECC_ERROR_TYPE_COUNT = 2

_hgmlEccCounterType_t = c_uint
HGML_VOLATILE_ECC = 0
HGML_AGGREGATE_ECC = 1
HGML_ECC_COUNTER_TYPE_COUNT = 2

_hgmlMemoryErrorType_t = c_uint
HGML_MEMORY_ERROR_TYPE_CORRECTED = 0
HGML_MEMORY_ERROR_TYPE_UNCORRECTED = 1
HGML_MEMORY_ERROR_TYPE_COUNT = 2

_hgmlClockType_t = c_uint
HGML_CLOCK_GRAPHICS = 0
HGML_CLOCK_SM = 1
HGML_CLOCK_MEM = 2
HGML_CLOCK_VIDEO = 3
HGML_CLOCK_COUNT = 4

_hgmlClockId_t = c_uint
HGML_CLOCK_ID_CURRENT = 0
HGML_CLOCK_ID_APP_CLOCK_TARGET = 1
HGML_CLOCK_ID_APP_CLOCK_DEFAULT = 2
HGML_CLOCK_ID_CUSTOMER_BOOST_MAX = 3
HGML_CLOCK_ID_COUNT = 4

_hgmlDriverModel_t = c_uint
HGML_DRIVER_WDDM = 0
HGML_DRIVER_WDM = 1
HGML_DRIVER_MCDM = 2

HGML_MAX_GPU_PERF_PSTATES = 16

_hgmlPstates_t = c_uint
HGML_PSTATE_0 = 0
HGML_PSTATE_1 = 1
HGML_PSTATE_2 = 2
HGML_PSTATE_3 = 3
HGML_PSTATE_4 = 4
HGML_PSTATE_5 = 5
HGML_PSTATE_6 = 6
HGML_PSTATE_7 = 7
HGML_PSTATE_8 = 8
HGML_PSTATE_9 = 9
HGML_PSTATE_10 = 10
HGML_PSTATE_11 = 11
HGML_PSTATE_12 = 12
HGML_PSTATE_13 = 13
HGML_PSTATE_14 = 14
HGML_PSTATE_15 = 15
HGML_PSTATE_UNKNOWN = 32

_hgmlInforomObject_t = c_uint
HGML_INFOROM_OEM = 0
HGML_INFOROM_ECC = 1
HGML_INFOROM_POWER = 2
HGML_INFOROM_COUNT = 3

_hgmlReturn_t = c_uint
HGML_SUCCESS = 0
HGML_ERROR_UNINITIALIZED = 1
HGML_ERROR_INVALID_ARGUMENT = 2
HGML_ERROR_NOT_SUPPORTED = 3
HGML_ERROR_NO_PERMISSION = 4
HGML_ERROR_ALREADY_INITIALIZED = 5
HGML_ERROR_NOT_FOUND = 6
HGML_ERROR_INSUFFICIENT_SIZE = 7
HGML_ERROR_INSUFFICIENT_POWER = 8
HGML_ERROR_DRIVER_NOT_LOADED = 9
HGML_ERROR_TIMEOUT = 10
HGML_ERROR_IRQ_ISSUE = 11
HGML_ERROR_LIBRARY_NOT_FOUND = 12
HGML_ERROR_FUNCTION_NOT_FOUND = 13
HGML_ERROR_CORRUPTED_INFOROM = 14
HGML_ERROR_GPU_IS_LOST = 15
HGML_ERROR_RESET_REQUIRED = 16
HGML_ERROR_OPERATING_SYSTEM = 17
HGML_ERROR_LIB_RM_VERSION_MISMATCH = 18
HGML_ERROR_IN_USE = 19
HGML_ERROR_MEMORY = 20
HGML_ERROR_NO_DATA = 21
HGML_ERROR_VGPU_ECC_NOT_SUPPORTED = 22
HGML_ERROR_INSUFFICIENT_RESOURCES = 23
HGML_ERROR_FREQ_NOT_SUPPORTED = 24
HGML_ERROR_ARGUMENT_VERSION_MISMATCH = 25
HGML_ERROR_DEPRECATED = 26
HGML_ERROR_NOT_READY = 27
HGML_ERROR_GPU_NOT_FOUND = 28
HGML_ERROR_UNKNOWN = 999

_hgmlFanState_t = c_uint
HGML_FAN_NORMAL = 0
HGML_FAN_FAILED = 1

_hgmlFanControlPolicy_t = c_uint
HGML_FAN_POLICY_TEMPERATURE_CONTINOUS_SW = 0
HGML_FAN_POLICY_MANUAL = 1

_hgmlLedColor_t = c_uint
HGML_LED_COLOR_GREEN = 0
HGML_LED_COLOR_AMBER = 1

_hgmlGpuOperationMode_t = c_uint
HGML_GOM_ALL_ON = 0
HGML_GOM_COMPUTE = 1
HGML_GOM_LOW_DP = 2

_hgmlPageRetirementCause_t = c_uint
HGML_PAGE_RETIREMENT_CAUSE_MULTIPLE_SINGLE_BIT_ECC_ERRORS = 0
HGML_PAGE_RETIREMENT_CAUSE_DOUBLE_BIT_ECC_ERROR = 1
HGML_PAGE_RETIREMENT_CAUSE_COUNT = 2

_hgmlRestrictedAPI_t = c_uint
HGML_RESTRICTED_API_SET_APPLICATION_CLOCKS = 0
HGML_RESTRICTED_API_SET_AUTO_BOOSTED_CLOCKS = 1
HGML_RESTRICTED_API_COUNT = 2

_hgmlBridgeChipType_t = c_uint
HGML_BRIDGE_CHIP_PLX = 0
HGML_BRIDGE_CHIP_BRO4 = 1
HGML_MAX_PHYSICAL_BRIDGE = 128

_hgmlValueType_t = c_uint
HGML_VALUE_TYPE_DOUBLE = 0
HGML_VALUE_TYPE_UNSIGNED_INT = 1
HGML_VALUE_TYPE_UNSIGNED_LONG = 2
HGML_VALUE_TYPE_UNSIGNED_LONG_LONG = 3
HGML_VALUE_TYPE_SIGNED_LONG_LONG = 4
HGML_VALUE_TYPE_SIGNED_INT = 5
HGML_VALUE_TYPE_COUNT = 6

_hgmlPerfPolicyType_t = c_uint
HGML_PERF_POLICY_POWER = 0
HGML_PERF_POLICY_THERMAL = 1
HGML_PERF_POLICY_SYNC_BOOST = 2
HGML_PERF_POLICY_BOARD_LIMIT = 3
HGML_PERF_POLICY_LOW_UTILIZATION = 4
HGML_PERF_POLICY_RELIABILITY = 5
HGML_PERF_POLICY_TOTAL_APP_CLOCKS = 10
HGML_PERF_POLICY_TOTAL_BASE_CLOCKS = 11
HGML_PERF_POLICY_COUNT = 12

_hgmlEncoderQueryType_t = c_uint
HGML_ENCODER_QUERY_H264 = 0
HGML_ENCODER_QUERY_HEVC = 1
HGML_ENCODER_QUERY_AV1 = 2
HGML_ENCODER_QUERY_UNKNOWN = 255

_hgmlFBCSessionType_t = c_uint
HGML_FBC_SESSION_TYPE_UNKNOWN = 0
HGML_FBC_SESSION_TYPE_TOSYS = 1
HGML_FBC_SESSION_TYPE_CUDA = 2
HGML_FBC_SESSION_TYPE_VID = 3
HGML_FBC_SESSION_TYPE_HWENC = 4

_hgmlDetachGpuState_t = c_uint
HGML_DETACH_GPU_KEEP = 0
HGML_DETACH_GPU_REMOVE = 1

_hgmlPcieLinkState_t = c_uint
HGML_PCIE_LINK_KEEP = 0
HGML_PCIE_LINK_SHUT_DOWN = 1

_hgmlSamplingType_t = c_uint
HGML_TOTAL_POWER_SAMPLES = 0
HGML_GPU_UTILIZATION_SAMPLES = 1
HGML_MEMORY_UTILIZATION_SAMPLES = 2
HGML_ENC_UTILIZATION_SAMPLES = 3
HGML_DEC_UTILIZATION_SAMPLES = 4
HGML_PROCESSOR_CLK_SAMPLES = 5
HGML_MEMORY_CLK_SAMPLES = 6
HGML_MODULE_POWER_SAMPLES = 7
HGML_SAMPLINGTYPE_COUNT = 8

_hgmlPcieUtilCounter_t = c_uint
HGML_PCIE_UTIL_TX_BYTES = 0
HGML_PCIE_UTIL_RX_BYTES = 1
HGML_PCIE_UTIL_COUNT = 2

_hgmlGpuTopologyLevel_t = c_uint
HGML_TOPOLOGY_INTERNAL = 0
HGML_TOPOLOGY_SINGLE = 10
HGML_TOPOLOGY_MULTIPLE = 20
HGML_TOPOLOGY_HOSTBRIDGE = 30
HGML_TOPOLOGY_NODE = 40
HGML_TOPOLOGY_SYSTEM = 50

_hgmlGpuP2PCapsIndex_t = c_uint
HGML_P2P_CAPS_INDEX_READ = (0,)
HGML_P2P_CAPS_INDEX_WRITE = 1
HGML_P2P_CAPS_INDEX_ICNLINK = 2
HGML_P2P_CAPS_INDEX_ATOMICS = 3
#
# HGML_P2P_CAPS_INDEX_PROP is deprecated.
# Use HGML_P2P_CAPS_INDEX_PCI instead.
#
HGML_P2P_CAPS_INDEX_PROP = 4
HGML_P2P_CAPS_INDEX_UNKNOWN = 5

_hgmlGpuP2PStatus_t = c_uint
HGML_P2P_STATUS_OK = 0
HGML_P2P_STATUS_CHIPSET_NOT_SUPPORTED = 1
HGML_P2P_STATUS_GPU_NOT_SUPPORTED = 2
HGML_P2P_STATUS_IOH_TOPOLOGY_NOT_SUPPORTED = 3
HGML_P2P_STATUS_DISABLED_BY_REGKEY = 4
HGML_P2P_STATUS_NOT_SUPPORTED = 5
HGML_P2P_STATUS_UNKNOWN = 6

# PCI bus Types
_hgmlBusType_t = c_uint
HGML_BUS_TYPE_UNKNOWN = 0
HGML_BUS_TYPE_PCI = 1
HGML_BUS_TYPE_PCIE = 2
HGML_BUS_TYPE_FPCI = 3
HGML_BUS_TYPE_AGP = 4

_hgmlPowerSource_t = c_uint
HGML_POWER_SOURCE_AC = 0x00000000
HGML_POWER_SOURCE_BATTERY = 0x00000001
HGML_POWER_SOURCE_UNDERSIZED = 0x00000002

_hgmlAdaptiveClockInfoStatus_t = c_uint
HGML_ADAPTIVE_CLOCKING_INFO_STATUS_DISABLED = 0x00000000
HGML_ADAPTIVE_CLOCKING_INFO_STATUS_ENABLED = 0x00000001

_hgmlClockLimitId_t = c_uint
HGML_CLOCK_LIMIT_ID_RANGE_START = 0xFFFFFF00
HGML_CLOCK_LIMIT_ID_TDP = 0xFFFFFF01
HGML_CLOCK_LIMIT_ID_UNLIMITED = 0xFFFFFF02

_hgmlPcieLinkMaxSpeed_t = c_uint
HGML_PCIE_LINK_MAX_SPEED_INVALID = 0x00000000
HGML_PCIE_LINK_MAX_SPEED_2500MBPS = 0x00000001
HGML_PCIE_LINK_MAX_SPEED_5000MBPS = 0x00000002
HGML_PCIE_LINK_MAX_SPEED_8000MBPS = 0x00000003
HGML_PCIE_LINK_MAX_SPEED_16000MBPS = 0x00000004
HGML_PCIE_LINK_MAX_SPEED_32000MBPS = 0x00000005
HGML_PCIE_LINK_MAX_SPEED_64000MBPS = 0x00000006

_hgmlAffinityScope_t = c_uint
HGML_AFFINITY_SCOPE_NODE = 0
HGML_AFFINITY_SCOPE_SOCKET = 1

_hgmlDeviceGpuRecoveryAction_t = c_uint
HGML_GPU_RECOVERY_ACTION_NONE = 0
HGML_GPU_RECOVERY_ACTION_GPU_RESET = 1
HGML_GPU_RECOVERY_ACTION_NODE_REBOOT = 2
HGML_GPU_RECOVERY_ACTION_DRAIN_P2P = 3
HGML_GPU_RECOVERY_ACTION_DRAIN_AND_RESET = 4
HGML_GPU_RECOVERY_ACTION_GPU_RESET_BUS = 5

# C preprocessor defined values
hgmlFlagDefault = 0
hgmlFlagForce = 1
HGML_INIT_FLAG_NO_GPUS = 1
HGML_INIT_FLAG_NO_ATTACH = 2

HGML_MAX_GPC_COUNT = 32

# buffer size
HGML_DEVICE_INFOROM_VERSION_BUFFER_SIZE = 16
HGML_DEVICE_UUID_BUFFER_SIZE = 80
HGML_DEVICE_UUID_V2_BUFFER_SIZE = 96
HGML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE = 80
HGML_SYSTEM_HGML_VERSION_BUFFER_SIZE = 80
HGML_DEVICE_NAME_BUFFER_SIZE = 64
HGML_DEVICE_NAME_V2_BUFFER_SIZE = 96
HGML_DEVICE_SERIAL_BUFFER_SIZE = 30
HGML_DEVICE_PART_NUMBER_BUFFER_SIZE = 80
HGML_DEVICE_GPU_PART_NUMBER_BUFFER_SIZE = 80
HGML_DEVICE_VBIOS_VERSION_BUFFER_SIZE = 32
HGML_DEVICE_PCI_BUS_ID_BUFFER_SIZE = 32
HGML_DEVICE_PCI_BUS_ID_BUFFER_V2_SIZE = 16
HGML_GRID_LICENSE_BUFFER_SIZE = 128
HGML_VGPU_NAME_BUFFER_SIZE = 64
HGML_VGPU_METADATA_OPAQUE_DATA_SIZE = 4
HGML_VGPU_PGPU_METADATA_OPAQUE_DATA_SIZE = 4
HGML_GRID_LICENSE_FEATURE_MAX_COUNT = 3

# Format strings
HGML_DEVICE_PCI_BUS_ID_LEGACY_FMT = "%04X:%02X:%02X.0"
HGML_DEVICE_PCI_BUS_ID_FMT = "%08X:%02X:%02X.0"

HGML_VALUE_NOT_AVAILABLE_ulonglong = c_ulonglong(-1)
HGML_VALUE_NOT_AVAILABLE_uint = c_uint(-1)

"""
 Field Identifiers.

 All Identifiers pertain to a device. Each ID is only used once and is guaranteed never to change.
"""
HGML_FI_DEV_ECC_CURRENT = 1  # Current ECC mode. 1=Active. 0=Inactive
HGML_FI_DEV_ECC_PENDING = 2  # Pending ECC mode. 1=Active. 0=Inactive

# ECC Count Totals
HGML_FI_DEV_ECC_SBE_VOL_TOTAL = 3  # Total single bit volatile ECC errors
HGML_FI_DEV_ECC_DBE_VOL_TOTAL = 4  # Total double bit volatile ECC errors
HGML_FI_DEV_ECC_SBE_AGG_TOTAL = 5  # Total single bit aggregate (persistent) ECC errors
HGML_FI_DEV_ECC_DBE_AGG_TOTAL = 6  # Total double bit aggregate (persistent) ECC errors
# Individual ECC locations
HGML_FI_DEV_ECC_SBE_VOL_L1 = 7  # L1 cache single bit volatile ECC errors
HGML_FI_DEV_ECC_DBE_VOL_L1 = 8  # L1 cache double bit volatile ECC errors
HGML_FI_DEV_ECC_SBE_VOL_L2 = 9  # L2 cache single bit volatile ECC errors
HGML_FI_DEV_ECC_DBE_VOL_L2 = 10  # L2 cache double bit volatile ECC errors
HGML_FI_DEV_ECC_SBE_VOL_DEV = 11  # Device memory single bit volatile ECC errors
HGML_FI_DEV_ECC_DBE_VOL_DEV = 12  # Device memory double bit volatile ECC errors
HGML_FI_DEV_ECC_SBE_VOL_REG = 13  # Register file single bit volatile ECC errors
HGML_FI_DEV_ECC_DBE_VOL_REG = 14  # Register file double bit volatile ECC errors
HGML_FI_DEV_ECC_SBE_VOL_TEX = 15  # Texture memory single bit volatile ECC errors
HGML_FI_DEV_ECC_DBE_VOL_TEX = 16  # Texture memory double bit volatile ECC errors
HGML_FI_DEV_ECC_DBE_VOL_CBU = 17  # CBU double bit volatile ECC errors
HGML_FI_DEV_ECC_SBE_AGG_L1 = 18  # L1 cache single bit aggregate (persistent) ECC errors
HGML_FI_DEV_ECC_DBE_AGG_L1 = 19  # L1 cache double bit aggregate (persistent) ECC errors
HGML_FI_DEV_ECC_SBE_AGG_L2 = 20  # L2 cache single bit aggregate (persistent) ECC errors
HGML_FI_DEV_ECC_DBE_AGG_L2 = 21  # L2 cache double bit aggregate (persistent) ECC errors
HGML_FI_DEV_ECC_SBE_AGG_DEV = (
    22  # Device memory single bit aggregate (persistent) ECC errors
)
HGML_FI_DEV_ECC_DBE_AGG_DEV = (
    23  # Device memory double bit aggregate (persistent) ECC errors
)
HGML_FI_DEV_ECC_SBE_AGG_REG = (
    24  # Register File single bit aggregate (persistent) ECC errors
)
HGML_FI_DEV_ECC_DBE_AGG_REG = (
    25  # Register File double bit aggregate (persistent) ECC errors
)
HGML_FI_DEV_ECC_SBE_AGG_TEX = (
    26  # Texture memory single bit aggregate (persistent) ECC errors
)
HGML_FI_DEV_ECC_DBE_AGG_TEX = (
    27  # Texture memory double bit aggregate (persistent) ECC errors
)
HGML_FI_DEV_ECC_DBE_AGG_CBU = 28  # CBU double bit aggregate ECC errors

# Page Retirement
HGML_FI_DEV_RETIRED_SBE = 29  # Number of retired pages because of single bit errors
HGML_FI_DEV_RETIRED_DBE = 30  # Number of retired pages because of double bit errors
HGML_FI_DEV_RETIRED_PENDING = 31  # If any pages are pending retirement. 1=yes. 0=no.

# IcnLink Flit Error Counters
HGML_FI_DEV_ICNLINK_CRC_FLIT_ERROR_COUNT_L0 = (
    32  # NVLink flow control CRC  Error Counter for Lane 0
)
HGML_FI_DEV_ICNLINK_CRC_FLIT_ERROR_COUNT_L1 = (
    33  # NVLink flow control CRC  Error Counter for Lane 1
)
HGML_FI_DEV_ICNLINK_CRC_FLIT_ERROR_COUNT_L2 = (
    34  # NVLink flow control CRC  Error Counter for Lane 2
)
HGML_FI_DEV_ICNLINK_CRC_FLIT_ERROR_COUNT_L3 = (
    35  # NVLink flow control CRC  Error Counter for Lane 3
)
HGML_FI_DEV_ICNLINK_CRC_FLIT_ERROR_COUNT_L4 = (
    36  # NVLink flow control CRC  Error Counter for Lane 4
)
HGML_FI_DEV_ICNLINK_CRC_FLIT_ERROR_COUNT_L5 = (
    37  # NVLink flow control CRC  Error Counter for Lane 5
)
HGML_FI_DEV_ICNLINK_CRC_FLIT_ERROR_COUNT_TOTAL = (
    38  # NVLink flow control CRC  Error Counter total for all Lanes
)

# IcnLink CRC Data Error Counters
HGML_FI_DEV_ICNLINK_CRC_DATA_ERROR_COUNT_L0 = (
    39  # NVLink data CRC Error Counter for Lane 0
)
HGML_FI_DEV_ICNLINK_CRC_DATA_ERROR_COUNT_L1 = (
    40  # NVLink data CRC Error Counter for Lane 1
)
HGML_FI_DEV_ICNLINK_CRC_DATA_ERROR_COUNT_L2 = (
    41  # NVLink data CRC Error Counter for Lane 2
)
HGML_FI_DEV_ICNLINK_CRC_DATA_ERROR_COUNT_L3 = (
    42  # NVLink data CRC Error Counter for Lane 3
)
HGML_FI_DEV_ICNLINK_CRC_DATA_ERROR_COUNT_L4 = (
    43  # NVLink data CRC Error Counter for Lane 4
)
HGML_FI_DEV_ICNLINK_CRC_DATA_ERROR_COUNT_L5 = (
    44  # NVLink data CRC Error Counter for Lane 5
)
HGML_FI_DEV_ICNLINK_CRC_DATA_ERROR_COUNT_TOTAL = (
    45  # IcnLink data CRC Error Counter total for all Lanes
)

# IcnLink Replay Error Counters
HGML_FI_DEV_ICNLINK_REPLAY_ERROR_COUNT_L0 = 46  # NVLink Replay Error Counter for Lane 0
HGML_FI_DEV_ICNLINK_REPLAY_ERROR_COUNT_L1 = 47  # NVLink Replay Error Counter for Lane 1
HGML_FI_DEV_ICNLINK_REPLAY_ERROR_COUNT_L2 = 48  # NVLink Replay Error Counter for Lane 2
HGML_FI_DEV_ICNLINK_REPLAY_ERROR_COUNT_L3 = 49  # NVLink Replay Error Counter for Lane 3
HGML_FI_DEV_ICNLINK_REPLAY_ERROR_COUNT_L4 = 50  # NVLink Replay Error Counter for Lane 4
HGML_FI_DEV_ICNLINK_REPLAY_ERROR_COUNT_L5 = 51  # NVLink Replay Error Counter for Lane 5
HGML_FI_DEV_ICNLINK_REPLAY_ERROR_COUNT_TOTAL = (
    52  # NVLink Replay Error Counter total for all Lanes
)

# IcnLink Recovery Error Counters
HGML_FI_DEV_ICNLINK_RECOVERY_ERROR_COUNT_L0 = (
    53  # NVLink Recovery Error Counter for Lane 0
)
HGML_FI_DEV_ICNLINK_RECOVERY_ERROR_COUNT_L1 = (
    54  # NVLink Recovery Error Counter for Lane 1
)
HGML_FI_DEV_ICNLINK_RECOVERY_ERROR_COUNT_L2 = (
    55  # NVLink Recovery Error Counter for Lane 2
)
HGML_FI_DEV_ICNLINK_RECOVERY_ERROR_COUNT_L3 = (
    56  # NVLink Recovery Error Counter for Lane 3
)
HGML_FI_DEV_ICNLINK_RECOVERY_ERROR_COUNT_L4 = (
    57  # NVLink Recovery Error Counter for Lane 4
)
HGML_FI_DEV_ICNLINK_RECOVERY_ERROR_COUNT_L5 = (
    58  # NVLink Recovery Error Counter for Lane 5
)
HGML_FI_DEV_ICNLINK_RECOVERY_ERROR_COUNT_TOTAL = (
    59  # NVLink Recovery Error Counter total for all Lanes
)

# IcnLink Bandwidth Counters
HGML_FI_DEV_ICNLINK_BANDWIDTH_C0_L0 = (
    60  # NVLink Bandwidth Counter for Counter Set 0, Lane 0
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C0_L1 = (
    61  # NVLink Bandwidth Counter for Counter Set 0, Lane 1
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C0_L2 = (
    62  # NVLink Bandwidth Counter for Counter Set 0, Lane 2
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C0_L3 = (
    63  # NVLink Bandwidth Counter for Counter Set 0, Lane 3
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C0_L4 = (
    64  # NVLink Bandwidth Counter for Counter Set 0, Lane 4
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C0_L5 = (
    65  # NVLink Bandwidth Counter for Counter Set 0, Lane 5
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C0_TOTAL = (
    66  # NVLink Bandwidth Counter Total for Counter Set 0, All Lanes
)

# IcnLink Bandwidth Counters
HGML_FI_DEV_ICNLINK_BANDWIDTH_C1_L0 = (
    67  # NVLink Bandwidth Counter for Counter Set 1, Lane 0
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C1_L1 = (
    68  # NVLink Bandwidth Counter for Counter Set 1, Lane 1
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C1_L2 = (
    69  # NVLink Bandwidth Counter for Counter Set 1, Lane 2
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C1_L3 = (
    70  # NVLink Bandwidth Counter for Counter Set 1, Lane 3
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C1_L4 = (
    71  # NVLink Bandwidth Counter for Counter Set 1, Lane 4
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C1_L5 = (
    72  # NVLink Bandwidth Counter for Counter Set 1, Lane 5
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C1_TOTAL = (
    73  # NVLink Bandwidth Counter Total for Counter Set 1, All Lanes
)

# Perf Policy Counters
HGML_FI_DEV_PERF_POLICY_POWER = 74  # Perf Policy Counter for Power Policy
HGML_FI_DEV_PERF_POLICY_THERMAL = 75  # Perf Policy Counter for Thermal Policy
HGML_FI_DEV_PERF_POLICY_SYNC_BOOST = 76  # Perf Policy Counter for Sync boost Policy
HGML_FI_DEV_PERF_POLICY_BOARD_LIMIT = 77  # Perf Policy Counter for Board Limit
HGML_FI_DEV_PERF_POLICY_LOW_UTILIZATION = (
    78  # Perf Policy Counter for Low GPU Utilization Policy
)
HGML_FI_DEV_PERF_POLICY_RELIABILITY = 79  # Perf Policy Counter for Reliability Policy
HGML_FI_DEV_PERF_POLICY_TOTAL_APP_CLOCKS = (
    80  # Perf Policy Counter for Total App Clock Policy
)
HGML_FI_DEV_PERF_POLICY_TOTAL_BASE_CLOCKS = (
    81  # Perf Policy Counter for Total Base Clocks Policy
)

# Memory temperatures
HGML_FI_DEV_MEMORY_TEMP = 82  # Memory temperature for the device

# Energy Counter
HGML_FI_DEV_TOTAL_ENERGY_CONSUMPTION = (
    83  # Total energy consumption for the GPU in mJ since the driver was last reloaded
)

# NVLink Speed
HGML_FI_DEV_ICNLINK_SPEED_MBPS_L0 = 84
HGML_FI_DEV_ICNLINK_SPEED_MBPS_L1 = 85
HGML_FI_DEV_ICNLINK_SPEED_MBPS_L2 = 86
HGML_FI_DEV_ICNLINK_SPEED_MBPS_L3 = 87
HGML_FI_DEV_ICNLINK_SPEED_MBPS_L4 = 88
HGML_FI_DEV_ICNLINK_SPEED_MBPS_L5 = 89
HGML_FI_DEV_ICNLINK_SPEED_MBPS_COMMON = 90

# NVLink Link Count
HGML_FI_DEV_ICNLINK_LINK_COUNT = 91

# Page Retirement pending fields
HGML_FI_DEV_RETIRED_PENDING_SBE = 92
HGML_FI_DEV_RETIRED_PENDING_DBE = 93

# PCIe replay and replay rollover counters
HGML_FI_DEV_PCIE_REPLAY_COUNTER = 94
HGML_FI_DEV_PCIE_REPLAY_ROLLOVER_COUNTER = 95

# IcnLink Flit Error Counters
HGML_FI_DEV_ICNLINK_CRC_FLIT_ERROR_COUNT_L6 = (
    96  # NVLink flow control CRC  Error Counter for Lane 6
)
HGML_FI_DEV_ICNLINK_CRC_FLIT_ERROR_COUNT_L7 = (
    97  # NVLink flow control CRC  Error Counter for Lane 7
)
HGML_FI_DEV_ICNLINK_CRC_FLIT_ERROR_COUNT_L8 = (
    98  # NVLink flow control CRC  Error Counter for Lane 8
)
HGML_FI_DEV_ICNLINK_CRC_FLIT_ERROR_COUNT_L9 = (
    99  # NVLink flow control CRC  Error Counter for Lane 9
)
HGML_FI_DEV_ICNLINK_CRC_FLIT_ERROR_COUNT_L10 = (
    100  # NVLink flow control CRC  Error Counter for Lane 10
)
HGML_FI_DEV_ICNLINK_CRC_FLIT_ERROR_COUNT_L11 = (
    101  # NVLink flow control CRC  Error Counter for Lane 11
)

# IcnLink CRC Data Error Counters
HGML_FI_DEV_ICNLINK_CRC_DATA_ERROR_COUNT_L6 = (
    102  # NVLink data CRC Error Counter for Lane 6
)
HGML_FI_DEV_ICNLINK_CRC_DATA_ERROR_COUNT_L7 = (
    103  # NVLink data CRC Error Counter for Lane 7
)
HGML_FI_DEV_ICNLINK_CRC_DATA_ERROR_COUNT_L8 = (
    104  # NVLink data CRC Error Counter for Lane 8
)
HGML_FI_DEV_ICNLINK_CRC_DATA_ERROR_COUNT_L9 = (
    105  # NVLink data CRC Error Counter for Lane 9
)
HGML_FI_DEV_ICNLINK_CRC_DATA_ERROR_COUNT_L10 = (
    106  # NVLink data CRC Error Counter for Lane 10
)
HGML_FI_DEV_ICNLINK_CRC_DATA_ERROR_COUNT_L11 = (
    107  # NVLink data CRC Error Counter for Lane 11
)

# IcnLink Replay Error Counters
HGML_FI_DEV_ICNLINK_REPLAY_ERROR_COUNT_L6 = (
    108  # NVLink Replay Error Counter for Lane 6
)
HGML_FI_DEV_ICNLINK_REPLAY_ERROR_COUNT_L7 = (
    109  # NVLink Replay Error Counter for Lane 7
)
HGML_FI_DEV_ICNLINK_REPLAY_ERROR_COUNT_L8 = (
    110  # NVLink Replay Error Counter for Lane 8
)
HGML_FI_DEV_ICNLINK_REPLAY_ERROR_COUNT_L9 = (
    111  # NVLink Replay Error Counter for Lane 9
)
HGML_FI_DEV_ICNLINK_REPLAY_ERROR_COUNT_L10 = (
    112  # NVLink Replay Error Counter for Lane 10
)
HGML_FI_DEV_ICNLINK_REPLAY_ERROR_COUNT_L11 = (
    113  # NVLink Replay Error Counter for Lane 11
)

# IcnLink Recovery Error Counters
HGML_FI_DEV_ICNLINK_RECOVERY_ERROR_COUNT_L6 = (
    114  # NVLink Recovery Error Counter for Lane 6
)
HGML_FI_DEV_ICNLINK_RECOVERY_ERROR_COUNT_L7 = (
    115  # NVLink Recovery Error Counter for Lane 7
)
HGML_FI_DEV_ICNLINK_RECOVERY_ERROR_COUNT_L8 = (
    116  # NVLink Recovery Error Counter for Lane 8
)
HGML_FI_DEV_ICNLINK_RECOVERY_ERROR_COUNT_L9 = (
    117  # NVLink Recovery Error Counter for Lane 9
)
HGML_FI_DEV_ICNLINK_RECOVERY_ERROR_COUNT_L10 = (
    118  # NVLink Recovery Error Counter for Lane 10
)
HGML_FI_DEV_ICNLINK_RECOVERY_ERROR_COUNT_L11 = (
    119  # NVLink Recovery Error Counter for Lane 11
)

# IcnLink Bandwidth Counters
HGML_FI_DEV_ICNLINK_BANDWIDTH_C0_L6 = (
    120  # NVLink Bandwidth Counter for Counter Set 0, Lane 6
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C0_L7 = (
    121  # NVLink Bandwidth Counter for Counter Set 0, Lane 7
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C0_L8 = (
    122  # NVLink Bandwidth Counter for Counter Set 0, Lane 8
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C0_L9 = (
    123  # NVLink Bandwidth Counter for Counter Set 0, Lane 9
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C0_L10 = (
    124  # NVLink Bandwidth Counter for Counter Set 0, Lane 10
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C0_L11 = (
    125  # NVLink Bandwidth Counter for Counter Set 0, Lane 11
)

# IcnLink Bandwidth Counters
HGML_FI_DEV_ICNLINK_BANDWIDTH_C1_L6 = (
    126  # NVLink Bandwidth Counter for Counter Set 1, Lane 6
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C1_L7 = (
    127  # NVLink Bandwidth Counter for Counter Set 1, Lane 7
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C1_L8 = (
    128  # NVLink Bandwidth Counter for Counter Set 1, Lane 8
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C1_L9 = (
    129  # NVLink Bandwidth Counter for Counter Set 1, Lane 9
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C1_L10 = (
    130  # NVLink Bandwidth Counter for Counter Set 1, Lane 10
)
HGML_FI_DEV_ICNLINK_BANDWIDTH_C1_L11 = (
    131  # NVLink Bandwidth Counter for Counter Set 1, Lane 11
)

# NVLink Speed
HGML_FI_DEV_ICNLINK_SPEED_MBPS_L6 = 132
HGML_FI_DEV_ICNLINK_SPEED_MBPS_L7 = 133
HGML_FI_DEV_ICNLINK_SPEED_MBPS_L8 = 134
HGML_FI_DEV_ICNLINK_SPEED_MBPS_L9 = 135
HGML_FI_DEV_ICNLINK_SPEED_MBPS_L10 = 136
HGML_FI_DEV_ICNLINK_SPEED_MBPS_L11 = 137

# NVLink Throughput Counters
HGML_FI_DEV_ICNLINK_THROUGHPUT_DATA_TX = 138  # NVLink TX Data throughput in KiB
HGML_FI_DEV_ICNLINK_THROUGHPUT_DATA_RX = 139  # NVLink RX Data throughput in KiB
HGML_FI_DEV_ICNLINK_THROUGHPUT_RAW_TX = 140  # NVLink TX Data + protocol overhead in KiB
HGML_FI_DEV_ICNLINK_THROUGHPUT_RAW_RX = 141  # NVLink RX Data + protocol overhead in KiB

# Row Remapper
HGML_FI_DEV_REMAPPED_COR = 142
HGML_FI_DEV_REMAPPED_UNC = 143
HGML_FI_DEV_REMAPPED_PENDING = 144
HGML_FI_DEV_REMAPPED_FAILURE = 145

# Remote device NVLink ID
HGML_FI_DEV_ICNLINK_REMOTE_ICNLINK_ID = 146

# Number of NVLinks connected to NVSwitch
HGML_FI_DEV_NVSWITCH_CONNECTED_LINK_COUNT = 147

# IcnLink ECC Data Error Counters
HGML_FI_DEV_ICNLINK_ECC_DATA_ERROR_COUNT_L0 = (
    148  # < NVLink data ECC Error Counter for Link 0
)
HGML_FI_DEV_ICNLINK_ECC_DATA_ERROR_COUNT_L1 = (
    149  # < NVLink data ECC Error Counter for Link 1
)
HGML_FI_DEV_ICNLINK_ECC_DATA_ERROR_COUNT_L2 = (
    150  # < NVLink data ECC Error Counter for Link 2
)
HGML_FI_DEV_ICNLINK_ECC_DATA_ERROR_COUNT_L3 = (
    151  # < NVLink data ECC Error Counter for Link 3
)
HGML_FI_DEV_ICNLINK_ECC_DATA_ERROR_COUNT_L4 = (
    152  # < NVLink data ECC Error Counter for Link 4
)
HGML_FI_DEV_ICNLINK_ECC_DATA_ERROR_COUNT_L5 = (
    153  # < NVLink data ECC Error Counter for Link 5
)
HGML_FI_DEV_ICNLINK_ECC_DATA_ERROR_COUNT_L6 = (
    154  # < NVLink data ECC Error Counter for Link 6
)
HGML_FI_DEV_ICNLINK_ECC_DATA_ERROR_COUNT_L7 = (
    155  # < NVLink data ECC Error Counter for Link 7
)
HGML_FI_DEV_ICNLINK_ECC_DATA_ERROR_COUNT_L8 = (
    156  # < NVLink data ECC Error Counter for Link 8
)
HGML_FI_DEV_ICNLINK_ECC_DATA_ERROR_COUNT_L9 = (
    157  # < NVLink data ECC Error Counter for Link 9
)
HGML_FI_DEV_ICNLINK_ECC_DATA_ERROR_COUNT_L10 = (
    158  # < NVLink data ECC Error Counter for Link 10
)
HGML_FI_DEV_ICNLINK_ECC_DATA_ERROR_COUNT_L11 = (
    159  # < NVLink data ECC Error Counter for Link 11
)
HGML_FI_DEV_ICNLINK_ECC_DATA_ERROR_COUNT_TOTAL = (
    160  # < IcnLink data ECC Error Counter total for all Links
)

HGML_FI_DEV_ICNLINK_ERROR_DL_REPLAY = 161
HGML_FI_DEV_ICNLINK_ERROR_DL_RECOVERY = 162
HGML_FI_DEV_ICNLINK_ERROR_DL_CRC = 163
HGML_FI_DEV_ICNLINK_GET_SPEED = 164
HGML_FI_DEV_ICNLINK_GET_STATE = 165
HGML_FI_DEV_ICNLINK_GET_VERSION = 166

HGML_FI_DEV_ICNLINK_GET_POWER_STATE = 167
HGML_FI_DEV_ICNLINK_GET_POWER_THRESHOLD = 168

HGML_FI_DEV_PCIE_L0_TO_RECOVERY_COUNTER = 169

HGML_FI_DEV_C2C_LINK_COUNT = 170
HGML_FI_DEV_C2C_LINK_GET_STATUS = 171
HGML_FI_DEV_C2C_LINK_GET_MAX_BW = 172

HGML_FI_DEV_PCIE_COUNT_CORRECTABLE_ERRORS = 173
HGML_FI_DEV_PCIE_COUNT_NAKS_RECEIVED = 174
HGML_FI_DEV_PCIE_COUNT_RECEIVER_ERROR = 175
HGML_FI_DEV_PCIE_COUNT_BAD_TLP = 176
HGML_FI_DEV_PCIE_COUNT_NAKS_SENT = 177
HGML_FI_DEV_PCIE_COUNT_BAD_DLLP = 178
HGML_FI_DEV_PCIE_COUNT_NON_FATAL_ERROR = 179
HGML_FI_DEV_PCIE_COUNT_FATAL_ERROR = 180
HGML_FI_DEV_PCIE_COUNT_UNSUPPORTED_REQ = 181
HGML_FI_DEV_PCIE_COUNT_LCRC_ERROR = 182
HGML_FI_DEV_PCIE_COUNT_LANE_ERROR = 183

HGML_FI_DEV_IS_RESETLESS_MIG_SUPPORTED = 184

HGML_FI_DEV_POWER_AVERAGE = 185
HGML_FI_DEV_POWER_INSTANT = 186
HGML_FI_DEV_POWER_MIN_LIMIT = 187
HGML_FI_DEV_POWER_MAX_LIMIT = 188
HGML_FI_DEV_POWER_DEFAULT_LIMIT = 189
HGML_FI_DEV_POWER_CURRENT_LIMIT = 190
HGML_FI_DEV_ENERGY = 191
HGML_FI_DEV_POWER_REQUESTED_LIMIT = 192

HGML_FI_DEV_TEMPERATURE_SHUTDOWN_TLIMIT = 193
HGML_FI_DEV_TEMPERATURE_SLOWDOWN_TLIMIT = 194
HGML_FI_DEV_TEMPERATURE_MEM_MAX_TLIMIT = 195
HGML_FI_DEV_TEMPERATURE_GPU_MAX_TLIMIT = 196

HGML_FI_DEV_ICNLINK_CABLE_STATUS = 500
HGML_FI_DEV_ICNLINK_LANE_WIDTH = 501
HGML_FI_DEV_ICNLINK_LINKUP_COUNT = 502
HGML_FI_DEV_ICNLINK_LINKDOWN_COUNT = 503
HGML_FI_DEV_ICNLINK_FECC_ERROR_COUNT = 504
HGML_FI_DEV_ICNLINK_FECU_ERROR_COUNT = 505
HGML_FI_DEV_ICNLINK_PACKET_ERROR_TX = 506
HGML_FI_DEV_ICNLINK_PACKET_ERROR_RX = 507
HGML_FI_DEV_ICNLINK_PACKET_TOTAL_TX = 508
HGML_FI_DEV_ICNLINK_PACKET_TOTAL_RX = 509

HGML_FI_DEV_CORE_UTILIZATION = 510
HGML_FI_DEV_AUTO_RESET_STATUS = 511
HGML_FI_DEV_ICNLINK_PHYSICAL_PORT = 512
HGML_FI_DEV_OVERCLOCKING_MODE = 513
HGML_FI_DEV_HBM_VENDOR = 514
HGML_FI_DEV_BASE_CLOCK = 515
HGML_FI_DEV_REAR_ID = 516

HGML_FI_DEV_XID_PPU_RESET = 517
HGML_FI_DEV_XID_OS_REBOOT = 518
HGML_FI_DEV_XID_COLD_REBOOT = 519

HGML_FI_DEV_PCM_ENABLED = 520
HGML_FI_DEV_GPM_ENABLED = 521
HGML_FI_DEV_TIDE_MODE_STATUS = 522
HGML_FI_DEV_MPS_MODE_STATUS = 523

HGML_FI_MAX = 524  # One greater than the largest field ID defined above

## Enums needed for the method hgmlDeviceGetVirtualizationMode and hgmlDeviceSetVirtualizationMode
HGML_GPU_VIRTUALIZATION_MODE_NONE = 0  # Represents Bare Metal GPU
HGML_GPU_VIRTUALIZATION_MODE_PASSTHROUGH = (
    1  # Device is associated with GPU-Passthorugh
)
HGML_GPU_VIRTUALIZATION_MODE_VGPU = (
    2  # Device is associated with vGPU inside virtual machine.
)
HGML_GPU_VIRTUALIZATION_MODE_HOST_VGPU = (
    3  # Device is associated with VGX hypervisor in vGPU mode
)
HGML_GPU_VIRTUALIZATION_MODE_HOST_VSGA = (
    4  # Device is associated with VGX hypervisor in vSGA mode
)

## Lib loading ##
hgmlLib = None
libLoadLock = threading.Lock()
_hgmlLib_refcount = 0  # Incremented on each hgmlInit and decremented on hgmlShutdown

## vGPU Management
_hgmlVgpuTypeId_t = c_uint
_hgmlVgpuInstance_t = c_uint

_hgmlVgpuVmIdType_t = c_uint
HGML_VGPU_VM_ID_DOMAIN_ID = 0
HGML_VGPU_VM_ID_UUID = 1

_hgmlGridLicenseFeatureCode_t = c_uint
HGML_GRID_LICENSE_FEATURE_CODE_UNKNOWN = 0
HGML_GRID_LICENSE_FEATURE_CODE_VGPU = 1

_hgmlVgpuCapability_t = c_uint
HGML_VGPU_CAP_ICNLINK_P2P = 0  # vGPU P2P over NVLink is supported
HGML_VGPU_CAP_GPUDIRECT = 1  # GPUDirect capability is supported
HGML_VGPU_CAP_MULTI_VGPU_EXCLUSIVE = (
    2  # vGPU profile cannot be mixed with other vGPU profiles in same VM
)
HGML_VGPU_CAP_EXCLUSIVE_TYPE = (
    3  # vGPU profile cannot run on a GPU alongside other profiles of different type
)
HGML_VGPU_CAP_EXCLUSIVE_SIZE = (
    4  # vGPU profile cannot run on a GPU alongside other profiles of different size
)
HGML_VGPU_CAP_COUNT = 5

_hgmlVgpuDriverCapability_t = c_uint
HGML_VGPU_DRIVER_CAP_HETEROGENEOUS_MULTI_VGPU = (
    0  # Supports mixing of different vGPU profiles within one guest VM
)
HGML_VGPU_DRIVER_CAP_COUNT = 1

_hgmlDeviceVgpuCapability_t = c_uint
HGML_DEVICE_VGPU_CAP_FRACTIONAL_MULTI_VGPU = 0  # Query whether the fractional vGPU profiles on this GPU can be used in multi-vGPU configurations
HGML_DEVICE_VGPU_CAP_HETEROGENEOUS_TIMESLICE_PROFILES = 1  # Query whether the GPU supports concurrent execution of timesliced vGPU profiles of differing types
HGML_DEVICE_VGPU_CAP_HETEROGENEOUS_TIMESLICE_SIZES = 2  # Query whether the GPU supports concurrent execution of timesliced vGPU profiles of differing framebuffer sizes
HGML_DEVICE_VGPU_CAP_READ_DEVICE_BUFFER_BW = 3  # Query the GPU's read_device_buffer expected bandwidth capacity in megabytes per second
HGML_DEVICE_VGPU_CAP_WRITE_DEVICE_BUFFER_BW = 4  # Query the GPU's write_device_buffer expected bandwidth capacity in megabytes per second
HGML_DEVICE_VGPU_CAP_COUNT = 5

_hgmlVgpuGuestInfoState_t = c_uint
HGML_VGPU_INSTANCE_GUEST_INFO_STATE_UNINITIALIZED = 0
HGML_VGPU_INSTANCE_GUEST_INFO_STATE_INITIALIZED = 1

_hgmlVgpuVmCompatibility_t = c_uint
HGML_VGPU_VM_COMPATIBILITY_NONE = 0x0
HGML_VGPU_VM_COMPATIBILITY_COLD = 0x1
HGML_VGPU_VM_COMPATIBILITY_HIBERNATE = 0x2
HGML_VGPU_VM_COMPATIBILITY_SLEEP = 0x4
HGML_VGPU_VM_COMPATIBILITY_LIVE = 0x8

_hgmlVgpuPgpuCompatibilityLimitCode_t = c_uint
HGML_VGPU_COMPATIBILITY_LIMIT_NONE = 0x0
HGML_VGPU_COMPATIBILITY_LIMIT_HOST_DRIVER = 0x1
HGML_VGPU_COMPATIBILITY_LIMIT_GUEST_DRIVER = 0x2
HGML_VGPU_COMPATIBILITY_LIMIT_GPU = 0x4
HGML_VGPU_COMPATIBILITY_LIMIT_OTHER = 0x80000000

_hgmlHostVgpuMode_t = c_uint
HGML_HOST_VGPU_MODE_NON_SRIOV = 0
HGML_HOST_VGPU_MODE_SRIOV = 1

_hgmlConfComputeGpusReadyState_t = c_uint
HGML_CC_ACCEPTING_CLIENT_REQUESTS_FALSE = 0
HGML_CC_ACCEPTING_CLIENT_REQUESTS_TRUE = 1

_hgmlConfComputeGpuCaps_t = c_uint
HGML_CC_SYSTEM_GPUS_CC_NOT_CAPABLE = 0
HGML_CC_SYSTEM_GPUS_CC_CAPABLE = 1

_hgmlConfComputeCpuCaps_t = c_uint
HGML_CC_SYSTEM_CPU_CAPS_NONE = 0
HGML_CC_SYSTEM_CPU_CAPS_AMD_SEV = 1
HGML_CC_SYSTEM_CPU_CAPS_INTEL_TDX = 2

_hgmlConfComputeDevToolsMode_t = c_uint
HGML_CC_SYSTEM_DEVTOOLS_MODE_OFF = 0
HGML_CC_SYSTEM_DEVTOOLS_MODE_ON = 1

HGML_CC_SYSTEM_ENVIRONMENT_UNAVAILABLE = 0
HGML_CC_SYSTEM_ENVIRONMENT_SIM = 1
HGML_CC_SYSTEM_ENVIRONMENT_PROD = 2

_hgmlConfComputeCcFeature_t = c_uint
HGML_CC_SYSTEM_FEATURE_DISABLED = 0
HGML_CC_SYSTEM_FEATURE_ENABLED = 1


# GSP firmware
HGML_GSP_FIRMWARE_VERSION_BUF_SIZE = 0x40


class HGMLLibraryMismatchError(Exception):
    pass


## Error Checking ##
class HGMLError(Exception):
    _valClassMapping = {}
    # List of currently known error codes
    _errcode_to_string = {
        HGML_ERROR_UNINITIALIZED: "Uninitialized",
        HGML_ERROR_INVALID_ARGUMENT: "Invalid Argument",
        HGML_ERROR_NOT_SUPPORTED: "Not Supported",
        HGML_ERROR_NO_PERMISSION: "Insufficient Permissions",
        HGML_ERROR_ALREADY_INITIALIZED: "Already Initialized",
        HGML_ERROR_NOT_FOUND: "Not Found",
        HGML_ERROR_INSUFFICIENT_SIZE: "Insufficient Size",
        HGML_ERROR_INSUFFICIENT_POWER: "Insufficient External Power",
        HGML_ERROR_DRIVER_NOT_LOADED: "Driver Not Loaded",
        HGML_ERROR_TIMEOUT: "Timeout",
        HGML_ERROR_IRQ_ISSUE: "Interrupt Request Issue",
        HGML_ERROR_LIBRARY_NOT_FOUND: "HGML Shared Library Not Found",
        HGML_ERROR_FUNCTION_NOT_FOUND: "Function Not Found",
        HGML_ERROR_CORRUPTED_INFOROM: "Corrupted infoROM",
        HGML_ERROR_GPU_IS_LOST: "GPU is lost",
        HGML_ERROR_RESET_REQUIRED: "GPU requires restart",
        HGML_ERROR_OPERATING_SYSTEM: "The operating system has blocked the request.",
        HGML_ERROR_LIB_RM_VERSION_MISMATCH: "RM has detected an HGML/RM version mismatch.",
        HGML_ERROR_MEMORY: "Insufficient Memory",
        HGML_ERROR_UNKNOWN: "Unknown Error",
    }

    def __new__(typ, value):
        """
        Maps value to a proper subclass of HGMLError.
        See _extractHGMLErrorsAsClasses function for more details.
        """
        if typ == HGMLError:
            typ = HGMLError._valClassMapping.get(value, typ)
        obj = Exception.__new__(typ)
        obj.value = value
        return obj

    def __str__(self):
        try:
            if self.value not in HGMLError._errcode_to_string:
                HGMLError._errcode_to_string[self.value] = str(
                    hgmlErrorString(self.value)
                )
            return HGMLError._errcode_to_string[self.value]
        except HGMLError:
            return "HGML Error with code %d" % self.value

    def __eq__(self, other):
        return self.value == other.value


def hgmlExceptionClass(hgmlErrorCode):
    if hgmlErrorCode not in HGMLError._valClassMapping:
        msg = f"hgmlErrorCode {hgmlErrorCode} is not valid"
        raise ValueError(msg)
    return HGMLError._valClassMapping[hgmlErrorCode]


def _extractHGMLErrorsAsClasses():
    """
    Generates a hierarchy of classes on top of HGMLError class.

    Each HGML Error gets a new HGMLError subclass. This way try,except blocks can filter appropriate
    exceptions more easily.

    HGMLError is a parent class. Each HGML_ERROR_* gets it's own subclass.
    e.g. HGML_ERROR_ALREADY_INITIALIZED will be turned into HGMLError_AlreadyInitialized
    """
    this_module = sys.modules[__name__]
    hgmlErrorsNames = [x for x in dir(this_module) if x.startswith("HGML_ERROR_")]
    for err_name in hgmlErrorsNames:
        # e.g. Turn HGML_ERROR_ALREADY_INITIALIZED into HGMLError_AlreadyInitialized
        class_name = "HGMLError_" + string.capwords(
            err_name.replace("HGML_ERROR_", ""), "_"
        ).replace("_", "")
        err_val = getattr(this_module, err_name)

        def gen_new(val):
            def new(typ, *args):
                obj = HGMLError.__new__(typ, val)
                return obj

            return new

        new_error_class = type(class_name, (HGMLError,), {"__new__": gen_new(err_val)})
        new_error_class.__module__ = __name__
        setattr(this_module, class_name, new_error_class)
        HGMLError._valClassMapping[err_val] = new_error_class


_extractHGMLErrorsAsClasses()


def _hgmlCheckReturn(ret):
    if ret != HGML_SUCCESS:
        raise HGMLError(ret)
    return ret


## Function access ##
_hgmlGetFunctionPointer_cache = {}  # function pointers are cached to prevent unnecessary libLoadLock locking


def _hgmlGetFunctionPointer(name):
    global hgmlLib

    if name in _hgmlGetFunctionPointer_cache:
        return _hgmlGetFunctionPointer_cache[name]

    libLoadLock.acquire()
    try:
        # ensure library was loaded
        if hgmlLib is None:
            raise HGMLError(HGML_ERROR_UNINITIALIZED)
        try:
            _hgmlGetFunctionPointer_cache[name] = getattr(hgmlLib, name)
            return _hgmlGetFunctionPointer_cache[name]
        except AttributeError:
            raise HGMLError(HGML_ERROR_FUNCTION_NOT_FOUND)
    finally:
        # lock is always freed
        libLoadLock.release()


## Alternative object
# Allows the object to be printed
# Allows mismatched types to be assigned
#  - like None when the Structure variant requires c_uint
class hgmlFriendlyObject:
    def __init__(self, dictionary):
        for x in dictionary:
            setattr(self, x, dictionary[x])

    def __str__(self):
        return self.__dict__.__str__()


def hgmlStructToFriendlyObject(struct):
    d = {}
    for x in struct._fields_:
        key = x[0]
        value = getattr(struct, key)
        # only need to convert from bytes if bytes, no need to check python version.
        d[key] = value.decode() if isinstance(value, bytes) else value
    obj = hgmlFriendlyObject(d)
    return obj


# pack the object so it can be passed to the HGML library
def hgmlFriendlyObjectToStruct(obj, model):
    for x in model._fields_:
        key = x[0]
        value = obj.__dict__[key]
        # any c_char_p in python3 needs to be bytes, default encoding works fine.
        setattr(model, key, value.encode())
    return model


## Unit structures
class struct_c_hgmlUnit_t(Structure):
    pass  # opaque handle


c_hgmlUnit_t = POINTER(struct_c_hgmlUnit_t)


class _PrintableStructure(Structure):
    """
    Abstract class that produces nicer __str__ output than ctypes.Structure.
    e.g. instead of:
      >>> print str(obj)
      <class_name object at 0x7fdf82fef9e0>
    this class will print
      class_name(field_name: formatted_value, field_name: formatted_value).

    _fmt_ dictionary of <str _field_ name> -> <str format>
    e.g. class that has _field_ 'hex_value', c_uint could be formatted with
      _fmt_ = {"hex_value" : "%08X"}
    to produce nicer output.
    Default fomratting string for all fields can be set with key "<default>" like:
      _fmt_ = {"<default>" : "%d MHz"} # e.g all values are numbers in MHz.
    If not set it's assumed to be just "%s"

    Exact format of returned str from this class is subject to change in the future.
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
        # need to convert bytes to unicode for python3 don't need to for python2
        # Python 2 strings are of both str and bytes
        # Python 3 strings are not of type bytes
        # ctypes should convert everything to the correct values otherwise
        if isinstance(res, bytes):
            if isinstance(res, str):
                return res
            return res.decode()
        return res

    def __setattr__(self, name, value):
        if isinstance(value, str):
            # encoding a python2 string returns the same value, since python2 strings are bytes already
            # bytes passed in python3 will be ignored.
            value = value.encode()
        super().__setattr__(name, value)


class c_hgmlUnitInfo_t(_PrintableStructure):
    _fields_ = [
        ("name", c_char * 96),
        ("id", c_char * 96),
        ("serial", c_char * 96),
        ("firmwareVersion", c_char * 96),
    ]


class c_hgmlC2cModeInfo_v1_t(_PrintableStructure):
    _fields_ = [
        ("isC2cEnabled", c_uint),
    ]


hgmlC2cModeInfo_v1 = 0x1000008


class c_hgmlLedState_t(_PrintableStructure):
    _fields_ = [
        ("cause", c_char * 256),
        ("color", _hgmlLedColor_t),
    ]


class c_hgmlPSUInfo_t(_PrintableStructure):
    _fields_ = [
        ("state", c_char * 256),
        ("current", c_uint),
        ("voltage", c_uint),
        ("power", c_uint),
    ]


class c_hgmlUnitFanInfo_t(_PrintableStructure):
    _fields_ = [
        ("speed", c_uint),
        ("state", _hgmlFanState_t),
    ]


class c_hgmlUnitFanSpeeds_t(_PrintableStructure):
    _fields_ = [
        ("fans", c_hgmlUnitFanInfo_t * 24),
        ("count", c_uint),
    ]


## Device structures
class struct_c_hgmlDevice_t(Structure):
    pass  # opaque handle


c_hgmlDevice_t = POINTER(struct_c_hgmlDevice_t)


# Legacy pciInfo used for _v1 and _v2
class hgmlPciInfo_v2_t(_PrintableStructure):
    _fields_ = [
        ("busId", c_char * HGML_DEVICE_PCI_BUS_ID_BUFFER_V2_SIZE),
        ("domain", c_uint),
        ("bus", c_uint),
        ("device", c_uint),
        ("pciDeviceId", c_uint),
        # Added in 2.285
        ("pciSubSystemId", c_uint),
        ("reserved0", c_uint),
        ("reserved1", c_uint),
        ("reserved2", c_uint),
        ("reserved3", c_uint),
    ]
    _fmt_ = {
        "domain": "0x%04X",
        "bus": "0x%02X",
        "device": "0x%02X",
        "pciDeviceId": "0x%08X",
        "pciSubSystemId": "0x%08X",
    }


class hgmlPciInfo_t(_PrintableStructure):
    _fields_ = [
        # Moved to the new busId location below
        ("busIdLegacy", c_char * HGML_DEVICE_PCI_BUS_ID_BUFFER_V2_SIZE),
        ("domain", c_uint),
        ("bus", c_uint),
        ("device", c_uint),
        ("pciDeviceId", c_uint),
        # Added in 2.285
        ("pciSubSystemId", c_uint),
        # New busId replaced the long deprecated and reserved fields with a
        # field of the same size in 9.0
        ("busId", c_char * HGML_DEVICE_PCI_BUS_ID_BUFFER_SIZE),
    ]
    _fmt_ = {
        "domain": "0x%08X",
        "bus": "0x%02X",
        "device": "0x%02X",
        "pciDeviceId": "0x%08X",
        "pciSubSystemId": "0x%08X",
    }


class c_hgmlExcludedDeviceInfo_t(_PrintableStructure):
    _fields_ = [
        ("pci", hgmlPciInfo_t),
        ("uuid", c_char * HGML_DEVICE_UUID_BUFFER_SIZE),
    ]


class hgmlIcnLinkUtilizationControl_t(_PrintableStructure):
    _fields_ = [
        ("units", _hgmlIcnLinkUtilizationCountUnits_t),
        ("pktfilter", _hgmlIcnLinkUtilizationCountPktTypes_t),
    ]


class c_hgmlMemory_t(_PrintableStructure):
    _fields_ = [
        ("total", c_ulonglong),
        ("free", c_ulonglong),
        ("used", c_ulonglong),
    ]
    _fmt_ = {"<default>": "%d B"}


class c_hgmlMemory_v2_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),
        ("total", c_ulonglong),
        ("reserved", c_ulonglong),
        ("free", c_ulonglong),
        ("used", c_ulonglong),
    ]
    _fmt_ = {"<default>": "%d B"}


hgmlMemory_v2 = 0x02000028


class c_hgmlBAR1Memory_t(_PrintableStructure):
    _fields_ = [
        ("bar1Total", c_ulonglong),
        ("bar1Free", c_ulonglong),
        ("bar1Used", c_ulonglong),
    ]
    _fmt_ = {"<default>": "%d B"}


class hgmlClkMonFaultInfo_t(Structure):
    _fields_ = [
        ("clkApiDomain", c_uint),
        ("clkDomainFaultMask", c_uint),
    ]


MAX_CLK_DOMAINS = 32


class hgmlClkMonStatus_t(Structure):
    _fields_ = [
        ("bGlobalStatus", c_uint),
        ("clkMonListSize", c_uint),
        ("clkMonList", hgmlClkMonFaultInfo_t * MAX_CLK_DOMAINS),
    ]


# On Windows with the WDDM driver, usedGpuMemory is reported as None
# Code that processes this structure should check for None, I.E.
#
# if (info.usedGpuMemory == None):
#     # TODO handle the error
#     pass
# else:
#    print("Using %d MiB of memory" % (info.usedGpuMemory / 1024 / 1024))
# endif
#
# See HGML documentation for more information
class c_hgmlProcessInfo_v2_t(_PrintableStructure):
    _fields_ = [
        ("pid", c_uint),
        ("usedGpuMemory", c_ulonglong),
        ("gpuInstanceId", c_uint),
        ("computeInstanceId", c_uint),
    ]
    _fmt_ = {"usedGpuMemory": "%d B"}


c_hgmlProcessInfo_v3_t = c_hgmlProcessInfo_v2_t

c_hgmlProcessInfo_t = c_hgmlProcessInfo_v3_t

_hgmlProcessMode_t = c_uint
HGML_PROCESS_MODE_COMPUTE = 0
HGML_PROCESS_MODE_GRAPHICS = 1
HGML_PROCESS_MODE_MPS = 2


class c_hgmlProcessDetail_v1_t(Structure):
    _fields_ = [
        ("pid", c_uint),
        ("usedGpuMemory", c_ulonglong),
        ("gpuInstanceId", c_uint),
        ("computeInstanceId", c_uint),
        ("usedGpuCcProtectedMemory", c_ulonglong),
    ]


class c_hgmlProcessDetailList_v1_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),
        ("mode", _hgmlProcessMode_t),
        ("numProcArrayEntries", c_uint),
        ("procArray", POINTER(c_hgmlProcessDetail_v1_t)),
    ]
    _fmt_ = {"numProcArrayEntries": "%d B"}


c_hgmlProcessDetailList_t = c_hgmlProcessDetailList_v1_t

hgmlProcessDetailList_v1 = 0x1000018


class c_hgmlBridgeChipInfo_t(_PrintableStructure):
    _fields_ = [
        ("type", _hgmlBridgeChipType_t),
        ("fwVersion", c_uint),
    ]


class c_hgmlBridgeChipHierarchy_t(_PrintableStructure):
    _fields_ = [
        ("bridgeCount", c_uint),
        ("bridgeChipInfo", c_hgmlBridgeChipInfo_t * 128),
    ]


class c_hgmlEccErrorCounts_t(_PrintableStructure):
    _fields_ = [
        ("l1Cache", c_ulonglong),
        ("l2Cache", c_ulonglong),
        ("deviceMemory", c_ulonglong),
        ("registerFile", c_ulonglong),
    ]


class c_hgmlUtilization_t(_PrintableStructure):
    _fields_ = [
        ("gpu", c_uint),
        ("memory", c_uint),
    ]
    _fmt_ = {"<default>": "%d %%"}


# Added in 2.285
class c_hgmlHwbcEntry_t(_PrintableStructure):
    _fields_ = [
        ("hwbcId", c_uint),
        ("firmwareVersion", c_char * 32),
    ]


class c_hgmlValue_t(Union):
    _fields_ = [
        ("dVal", c_double),
        ("uiVal", c_uint),
        ("ulVal", c_ulong),
        ("ullVal", c_ulonglong),
        ("sllVal", c_longlong),
        ("siVal", c_int),
        ("usVal", c_ushort),
    ]


class c_hgmlSample_t(_PrintableStructure):
    _fields_ = [
        ("timeStamp", c_ulonglong),
        ("sampleValue", c_hgmlValue_t),
    ]


class c_hgmlViolationTime_t(_PrintableStructure):
    _fields_ = [
        ("referenceTime", c_ulonglong),
        ("violationTime", c_ulonglong),
    ]


class c_hgmlFieldValue_t(_PrintableStructure):
    _fields_ = [
        ("fieldId", c_uint32),
        ("scopeId", c_uint32),
        ("timestamp", c_int64),
        ("latencyUsec", c_int64),
        ("valueType", _hgmlValueType_t),
        ("hgmlReturn", _hgmlReturn_t),
        ("value", c_hgmlValue_t),
    ]


class c_hgmlVgpuInstanceUtilizationSample_t(_PrintableStructure):
    _fields_ = [
        ("vgpuInstance", _hgmlVgpuInstance_t),
        ("timeStamp", c_ulonglong),
        ("smUtil", c_hgmlValue_t),
        ("memUtil", c_hgmlValue_t),
        ("encUtil", c_hgmlValue_t),
        ("decUtil", c_hgmlValue_t),
    ]


class c_hgmlVgpuProcessUtilizationSample_t(_PrintableStructure):
    _fields_ = [
        ("vgpuInstance", _hgmlVgpuInstance_t),
        ("pid", c_uint),
        ("processName", c_char * HGML_VGPU_NAME_BUFFER_SIZE),
        ("timeStamp", c_ulonglong),
        ("smUtil", c_uint),
        ("memUtil", c_uint),
        ("encUtil", c_uint),
        ("decUtil", c_uint),
    ]


class c_hgmlVgpuLicenseExpiry_t(_PrintableStructure):
    _fields_ = [
        ("year", c_uint32),
        ("month", c_uint16),
        ("day", c_uint16),
        ("hour", c_uint16),
        ("min", c_uint16),
        ("sec", c_uint16),
        ("status", c_uint8),
    ]


class c_hgmlVgpuLicenseInfo_t(_PrintableStructure):
    _fields_ = [
        ("isLicensed", c_uint8),
        ("licenseExpiry", c_hgmlVgpuLicenseExpiry_t),
        ("currentState", c_uint),
    ]


class c_hgmlEncoderSession_t(_PrintableStructure):
    _fields_ = [
        ("sessionId", c_uint),
        ("pid", c_uint),
        ("vgpuInstance", _hgmlVgpuInstance_t),
        ("codecType", c_uint),
        ("hResolution", c_uint),
        ("vResolution", c_uint),
        ("averageFps", c_uint),
        ("encodeLatency", c_uint),
    ]


class c_hgmlProcessUtilizationSample_t(_PrintableStructure):
    _fields_ = [
        ("pid", c_uint),
        ("timeStamp", c_ulonglong),
        ("smUtil", c_uint),
        ("memUtil", c_uint),
        ("encUtil", c_uint),
        ("decUtil", c_uint),
    ]


class c_hgmlGridLicenseExpiry_t(_PrintableStructure):
    _fields_ = [
        ("year", c_uint32),
        ("month", c_uint16),
        ("day", c_uint16),
        ("hour", c_uint16),
        ("min", c_uint16),
        ("sec", c_uint16),
        ("status", c_uint8),
    ]


class c_hgmlGridLicensableFeature_t(_PrintableStructure):
    _fields_ = [
        ("featureCode", _hgmlGridLicenseFeatureCode_t),
        ("featureState", c_uint),
        ("licenseInfo", c_char * HGML_GRID_LICENSE_BUFFER_SIZE),
        ("productName", c_char * HGML_GRID_LICENSE_BUFFER_SIZE),
        ("featureEnabled", c_uint),
        ("licenseExpiry", c_hgmlGridLicenseExpiry_t),
    ]


class c_hgmlGridLicensableFeatures_t(_PrintableStructure):
    _fields_ = [
        ("isGridLicenseSupported", c_int),
        ("licensableFeaturesCount", c_uint),
        (
            "gridLicensableFeatures",
            c_hgmlGridLicensableFeature_t * HGML_GRID_LICENSE_FEATURE_MAX_COUNT,
        ),
    ]


HGML_ICNLINK_FIRMWARE_UCODE_TYPE_MSE = 0x1
HGML_ICNLINK_FIRMWARE_UCODE_TYPE_NETIR = 0x2
HGML_ICNLINK_FIRMWARE_UCODE_TYPE_NETIR_UPHY = 0x3
HGML_ICNLINK_FIRMWARE_UCODE_TYPE_NETIR_CLN = 0x4
HGML_ICNLINK_FIRMWARE_UCODE_TYPE_NETIR_DLN = 0x5
HGML_ICNLINK_FIRMWARE_VERSION_LENGTH = 100


class c_hgmlNvlinkFirmwareVersion_t(_PrintableStructure):
    _fields_ = [
        ("ucodeType", c_uint8),
        ("major", c_uint),
        ("minor", c_uint),
        ("subMinor", c_uint),
    ]


class c_hgmlNvlinkFirmwareInfo_t(_PrintableStructure):
    _fields_ = [
        (
            "firmwareVersion",
            c_hgmlNvlinkFirmwareVersion_t * HGML_ICNLINK_FIRMWARE_VERSION_LENGTH,
        ),
        ("numValidEntries", c_uint),
    ]


hgmlIcnLinkInfo_v2 = 0x200064C


class c_hgmlIcnLinkInfo_v2_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),
        ("isNvleEnabled", c_uint),
        ("firmwareInfo", c_hgmlNvlinkFirmwareInfo_t),
    ]

    def __init__(self):
        super().__init__(version=hgmlIcnLinkInfo_v2)


## Event structures
class struct_c_hgmlEventSet_t(Structure):
    pass  # opaque handle


c_hgmlEventSet_t = POINTER(struct_c_hgmlEventSet_t)

hgmlEventTypeSingleBitEccError = 0x0000000000000001
hgmlEventTypeDoubleBitEccError = 0x0000000000000002
hgmlEventTypePState = 0x0000000000000004
hgmlEventTypeXidCriticalError = 0x0000000000000008
hgmlEventTypeClock = 0x0000000000000010
hgmlEventTypePowerSourceChange = 0x0000000000000080
hgmlEventMigConfigChange = 0x0000000000000100
hgmlEventTypeSingleBitEccErrorStorm = 0x0000000000000200
hgmlEventTypeDramRetirementEvent = 0x0000000000000400
hgmlEventTypeDramRetirementFailure = 0x0000000000000800
hgmlEventTypeNonFatalPoisonError = 0x0000000000001000
hgmlEventTypeFatalPoisonError = 0x0000000000002000
hgmlEventTypeGpuUnavailableError = 0x0000000000004000
hgmlEventTypeGpuRecoveryAction = 0x0000000000008000
hgmlEventTypeNone = 0x0000000000000000
hgmlEventTypeAll = (
    hgmlEventTypeNone
    | hgmlEventTypeSingleBitEccError
    | hgmlEventTypeDoubleBitEccError
    | hgmlEventTypePState
    | hgmlEventTypeClock
    | hgmlEventTypePowerSourceChange
    | hgmlEventTypeXidCriticalError
    | hgmlEventMigConfigChange
    | hgmlEventTypeSingleBitEccErrorStorm
    | hgmlEventTypeDramRetirementEvent
    | hgmlEventTypeDramRetirementFailure
    | hgmlEventTypeNonFatalPoisonError
    | hgmlEventTypeFatalPoisonError
    | hgmlEventTypeGpuUnavailableError
    | hgmlEventTypeGpuRecoveryAction
)

## Clock Event Reasons defines
hgmlClocksEventReasonGpuIdle = 0x0000000000000001
hgmlClocksEventReasonApplicationsClocksSetting = 0x0000000000000002
hgmlClocksEventReasonUserDefinedClocks = hgmlClocksEventReasonApplicationsClocksSetting  # deprecated, use hgmlClocksEventReasonApplicationsClocksSetting
hgmlClocksEventReasonSwPowerCap = 0x0000000000000004
hgmlClocksEventReasonHwSlowdown = 0x0000000000000008
hgmlClocksEventReasonSyncBoost = 0x0000000000000010
hgmlClocksEventReasonSwThermalSlowdown = 0x0000000000000020
hgmlClocksEventReasonHwThermalSlowdown = 0x0000000000000040
hgmlClocksEventReasonHwPowerBrakeSlowdown = 0x0000000000000080
hgmlClocksEventReasonDisplayClockSetting = 0x0000000000000100
hgmlClocksEventReasonNone = 0x0000000000000000
hgmlClocksEventReasonAll = (
    hgmlClocksEventReasonNone
    | hgmlClocksEventReasonGpuIdle
    | hgmlClocksEventReasonApplicationsClocksSetting
    | hgmlClocksEventReasonSwPowerCap
    | hgmlClocksEventReasonHwSlowdown
    | hgmlClocksEventReasonSyncBoost
    | hgmlClocksEventReasonSwThermalSlowdown
    | hgmlClocksEventReasonHwThermalSlowdown
    | hgmlClocksEventReasonHwPowerBrakeSlowdown
    | hgmlClocksEventReasonDisplayClockSetting
)

## Following have been deprecated
hgmlClocksThrottleReasonGpuIdle = 0x0000000000000001
hgmlClocksThrottleReasonApplicationsClocksSetting = 0x0000000000000002
hgmlClocksThrottleReasonUserDefinedClocks = hgmlClocksThrottleReasonApplicationsClocksSetting  # deprecated, use hgmlClocksThrottleReasonApplicationsClocksSetting
hgmlClocksThrottleReasonSwPowerCap = 0x0000000000000004
hgmlClocksThrottleReasonHwSlowdown = 0x0000000000000008
hgmlClocksThrottleReasonSyncBoost = 0x0000000000000010
hgmlClocksThrottleReasonSwThermalSlowdown = 0x0000000000000020
hgmlClocksThrottleReasonHwThermalSlowdown = 0x0000000000000040
hgmlClocksThrottleReasonHwPowerBrakeSlowdown = 0x0000000000000080
hgmlClocksThrottleReasonDisplayClockSetting = 0x0000000000000100
hgmlClocksThrottleReasonNone = 0x0000000000000000
hgmlClocksThrottleReasonAll = (
    hgmlClocksThrottleReasonNone
    | hgmlClocksThrottleReasonGpuIdle
    | hgmlClocksThrottleReasonApplicationsClocksSetting
    | hgmlClocksThrottleReasonSwPowerCap
    | hgmlClocksThrottleReasonHwSlowdown
    | hgmlClocksThrottleReasonSyncBoost
    | hgmlClocksThrottleReasonSwThermalSlowdown
    | hgmlClocksThrottleReasonHwThermalSlowdown
    | hgmlClocksThrottleReasonHwPowerBrakeSlowdown
    | hgmlClocksThrottleReasonDisplayClockSetting
)


class c_hgmlEventData_t(_PrintableStructure):
    _fields_ = [
        ("device", c_hgmlDevice_t),
        ("eventType", c_ulonglong),
        ("eventData", c_ulonglong),
        ("gpuInstanceId", c_uint),
        ("computeInstanceId", c_uint),
    ]
    _fmt_ = {"eventType": "0x%08X"}


class c_hgmlAccountingStats_t(_PrintableStructure):
    _fields_ = [
        ("gpuUtilization", c_uint),
        ("memoryUtilization", c_uint),
        ("maxMemoryUsage", c_ulonglong),
        ("time", c_ulonglong),
        ("startTime", c_ulonglong),
        ("isRunning", c_uint),
        ("reserved", c_uint * 5),
    ]


class c_hgmlVgpuVersion_t(Structure):
    _fields_ = [
        ("minVersion", c_uint),
        ("maxVersion", c_uint),
    ]


class c_hgmlVgpuMetadata_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),
        ("revision", c_uint),
        ("guestInfoState", _hgmlVgpuGuestInfoState_t),
        ("guestDriverVersion", c_char * HGML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE),
        ("hostDriverVersion", c_char * HGML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE),
        ("reserved", c_uint * 6),
        ("vgpuVirtualizationCaps", c_uint),
        ("guestVgpuVersion", c_uint),
        ("opaqueDataSize", c_uint),
        ("opaqueData", c_char * HGML_VGPU_METADATA_OPAQUE_DATA_SIZE),
    ]


class c_hgmlVgpuPgpuMetadata_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),
        ("revision", c_uint),
        ("hostDriverVersion", c_char * HGML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE),
        ("pgpuVirtualizationCaps", c_uint),
        ("reserved", c_uint * 5),
        ("hostSupportedVgpuRange", c_hgmlVgpuVersion_t),
        ("opaqueDataSize", c_uint),
        ("opaqueData", c_char * HGML_VGPU_PGPU_METADATA_OPAQUE_DATA_SIZE),
    ]


class c_hgmlVgpuPgpuCompatibility_t(Structure):
    _fields_ = [
        ("vgpuVmCompatibility", _hgmlVgpuVmCompatibility_t),
        ("compatibilityLimitCode", _hgmlVgpuPgpuCompatibilityLimitCode_t),
    ]


## vGPU scheduler policy defines
HGML_VGPU_SCHEDULER_POLICY_UNKNOWN = 0
HGML_VGPU_SCHEDULER_POLICY_BEST_EFFORT = 1
HGML_VGPU_SCHEDULER_POLICY_EQUAL_SHARE = 2
HGML_VGPU_SCHEDULER_POLICY_FIXED_SHARE = 3

## Supported vGPU scheduler policy count
HGML_SUPPORTED_VGPU_SCHEDULER_POLICY_COUNT = 3

HGML_SCHEDULER_SW_MAX_LOG_ENTRIES = 200

HGML_VGPU_SCHEDULER_ARR_DEFAULT = 0
HGML_VGPU_SCHEDULER_ARR_DISABLE = 1
HGML_VGPU_SCHEDULER_ARR_ENABLE = 2

HGML_VGPU_SCHEDULER_ENGINE_TYPE_GRAPHICS = 1


class c_hgmlVgpuSchedDataWithARR_t(_PrintableStructure):
    _fields_ = [
        ("avgFactor", c_uint),
        ("timeslice", c_uint),
    ]


class c_hgmlVgpuSchedData_t(_PrintableStructure):
    _fields_ = [
        ("timeslice", c_uint),
    ]


class c_hgmlVgpuSchedulerParams_t(Union):
    _fields_ = [
        ("vgpuSchedDataWithARR", c_hgmlVgpuSchedDataWithARR_t),
        ("vgpuSchedData", c_hgmlVgpuSchedData_t),
    ]


class c_hgmlVgpuSchedulerLogEntry_t(_PrintableStructure):
    _fields_ = [
        ("timestamp", c_ulonglong),
        ("timeRunTotal", c_ulonglong),
        ("timeRun", c_ulonglong),
        ("swRunlistId", c_uint),
        ("targetTimeSlice", c_ulonglong),
        ("cumulativePreemptionTime", c_ulonglong),
    ]


class c_hgmlVgpuSchedulerLog_t(_PrintableStructure):
    _fields_ = [
        ("engineId", c_uint),
        ("schedulerPolicy", c_uint),
        ("arrMode", c_uint),
        ("schedulerParams", c_hgmlVgpuSchedulerParams_t),
        ("entriesCount", c_uint),
        (
            "logEntries",
            c_hgmlVgpuSchedulerLogEntry_t * HGML_SCHEDULER_SW_MAX_LOG_ENTRIES,
        ),
    ]


class c_hgmlVgpuSchedulerGetState_t(_PrintableStructure):
    _fields_ = [
        ("schedulerPolicy", c_uint),
        ("arrMode", c_uint),
        ("schedulerParams", c_hgmlVgpuSchedulerParams_t),
    ]


class c_hgmlVgpuSchedSetDataWithARR_t(_PrintableStructure):
    _fields_ = [
        ("avgFactor", c_uint),
        ("frequency", c_uint),
    ]


class c_hgmlVgpuSchedSetData_t(_PrintableStructure):
    _fields_ = [
        ("timeslice", c_uint),
    ]


class c_hgmlVgpuSchedulerSetParams_t(Union):
    _fields_ = [
        ("vgpuSchedDataWithARR", c_hgmlVgpuSchedSetDataWithARR_t),
        ("vgpuSchedData", c_hgmlVgpuSchedSetData_t),
    ]


class c_hgmlVgpuSchedulerSetState_t(_PrintableStructure):
    _fields_ = [
        ("schedulerPolicy", c_uint),
        ("enableARRMode", c_uint),
        ("schedulerParams", c_hgmlVgpuSchedulerSetParams_t),
    ]


class c_hgmlVgpuSchedulerCapabilities_t(_PrintableStructure):
    _fields_ = [
        ("supportedSchedulers", c_uint * HGML_SUPPORTED_VGPU_SCHEDULER_POLICY_COUNT),
        ("maxTimeslice", c_uint),
        ("minTimeslice", c_uint),
        ("isArrModeSupported", c_uint),
        ("maxFrequencyForARR", c_uint),
        ("minFrequencyForARR", c_uint),
        ("maxAvgFactorForARR", c_uint),
        ("minAvgFactorForARR", c_uint),
    ]


class c_hgmlVgpuTypeIdInfo_v1_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),
        ("vgpuCount", c_uint),
        ("vgpuTypeIds", POINTER(c_uint)),
    ]


hgmlVgpuTypeIdInfo_v1 = 0x1000010


class c_hgmlVgpuSchedulerState_v1_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),
        ("engineId", c_uint),
        ("schedulerPolicy", c_uint),
        ("enableARRMode", c_uint),
        ("schedulerParams", c_hgmlVgpuSchedulerSetParams_t),
    ]


hgmlVgpuSchedulerState_v1 = 0x1000018


class c_hgmlVgpuSchedulerStateInfo_v1_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),  # input
        ("engineId", c_uint),  # input. One of HGML_ENGINE_TYPE*
        ("schedulerPolicy", c_uint),  # output
        ("arrMode", c_uint),  # output
        ("schedulerParams", c_hgmlVgpuSchedulerParams_t),  # output
    ]


hgmlVgpuSchedulerStateInfo_v1 = 0x1000018


class c_hgmlVgpuSchedulerLogInfo_v1_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),  # input
        ("engineId", c_uint),  # input. One of HGML_ENGINE_TYPE*
        ("schedulerPolicy", c_uint),  # output
        ("arrMode", c_uint),  # output
        ("schedulerParams", c_hgmlVgpuSchedulerParams_t),  # output
        ("entriesCount", c_uint),  # output
        (
            "logEntries",
            c_hgmlVgpuSchedulerLogEntry_t * HGML_SCHEDULER_SW_MAX_LOG_ENTRIES,
        ),  # output
    ]


hgmlVgpuSchedulerLogInfo_v1 = 0x10025A0


class c_hgmlVgpuCreatablePlacementInfo_v1_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),
        ("vgpuTypeId", c_uint),
        ("count", c_uint),
        ("placementIds", POINTER(c_uint)),
        ("placementSize", c_uint),
    ]


hgmlVgpuCreatablePlacementInfo_v1 = 0x1000020


class c_hgmlFBCStats_t(Structure):
    _fields_ = [
        ("sessionsCount", c_uint),
        ("averageFPS", c_uint),
        ("averageLatency", c_uint),
    ]


class c_hgmlFBCSession_t(_PrintableStructure):
    _fields_ = [
        ("sessionId", c_uint),
        ("pid", c_uint),
        ("vgpuInstance", _hgmlVgpuInstance_t),
        ("displayOrdinal", c_uint),
        ("sessionType", c_uint),
        ("sessionFlags", c_uint),
        ("hMaxResolution", c_uint),
        ("vMaxResolution", c_uint),
        ("hResolution", c_uint),
        ("vResolution", c_uint),
        ("averageFPS", c_uint),
        ("averageLatency", c_uint),
    ]


HGML_DEVICE_MIG_DISABLE = 0x0
HGML_DEVICE_MIG_ENABLE = 0x1

HGML_GPU_INSTANCE_PROFILE_1_SLICE = 0x0
HGML_GPU_INSTANCE_PROFILE_2_SLICE = 0x1
HGML_GPU_INSTANCE_PROFILE_3_SLICE = 0x2
HGML_GPU_INSTANCE_PROFILE_4_SLICE = 0x3
HGML_GPU_INSTANCE_PROFILE_7_SLICE = 0x4
HGML_GPU_INSTANCE_PROFILE_8_SLICE = 0x5
HGML_GPU_INSTANCE_PROFILE_6_SLICE = 0x6
HGML_GPU_INSTANCE_PROFILE_1_SLICE_REV1 = 0x7
HGML_GPU_INSTANCE_PROFILE_2_SLICE_REV1 = 0x8
HGML_GPU_INSTANCE_PROFILE_1_SLICE_REV2 = 0x9

HGML_GPU_INSTANCE_PROFILE_2_CE = 0xA
HGML_GPU_INSTANCE_PROFILE_4_CE = 0xB
HGML_GPU_INSTANCE_PROFILE_8_CE = 0xC
HGML_GPU_INSTANCE_PROFILE_16_CE = 0xD
HGML_GPU_INSTANCE_PROFILE_17_CE = 0xE

HGML_GPU_INSTANCE_PROFILE_COUNT = 0xF


class c_hgmlGpuInstancePlacement_t(Structure):
    _fields_ = [
        ("start", c_uint),
        ("size", c_uint),
    ]


class c_hgmlGpuInstanceProfileInfo_t(Structure):
    _fields_ = [
        ("id", c_uint),
        ("isP2pSupported", c_uint),
        ("sliceCount", c_uint),
        ("instanceCount", c_uint),
        ("multiprocessorCount", c_uint),
        ("copyEngineCount", c_uint),
        ("decoderCount", c_uint),
        ("encoderCount", c_uint),
        ("jpegCount", c_uint),
        ("ofaCount", c_uint),
        ("memorySizeMB", c_ulonglong),
    ]


hgmlGpuInstanceProfileInfo_v2 = 0x02000098


class c_hgmlGpuInstanceProfileInfo_v2_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),
        ("id", c_uint),
        ("isP2pSupported", c_uint),
        ("sliceCount", c_uint),
        ("instanceCount", c_uint),
        ("multiprocessorCount", c_uint),
        ("copyEngineCount", c_uint),
        ("decoderCount", c_uint),
        ("encoderCount", c_uint),
        ("jpegCount", c_uint),
        ("ofaCount", c_uint),
        ("memorySizeMB", c_ulonglong),
        ("name", c_char * HGML_DEVICE_NAME_V2_BUFFER_SIZE),
    ]

    def __init__(self):
        super().__init__(version=hgmlGpuInstanceProfileInfo_v2)


class c_hgmlGpuInstanceInfo_t(Structure):
    _fields_ = [
        ("device", c_hgmlDevice_t),
        ("id", c_uint),
        ("profileId", c_uint),
        ("placement", c_hgmlGpuInstancePlacement_t),
    ]


class struct_c_hgmlGpuInstance_t(Structure):
    pass  # opaque handle


c_hgmlGpuInstance_t = POINTER(struct_c_hgmlGpuInstance_t)

HGML_COMPUTE_INSTANCE_PROFILE_1_SLICE = 0x0
HGML_COMPUTE_INSTANCE_PROFILE_2_SLICE = 0x1
HGML_COMPUTE_INSTANCE_PROFILE_3_SLICE = 0x2
HGML_COMPUTE_INSTANCE_PROFILE_4_SLICE = 0x3
HGML_COMPUTE_INSTANCE_PROFILE_7_SLICE = 0x4
HGML_COMPUTE_INSTANCE_PROFILE_8_SLICE = 0x5
HGML_COMPUTE_INSTANCE_PROFILE_6_SLICE = 0x6
HGML_COMPUTE_INSTANCE_PROFILE_1_SLICE_REV1 = 0x7

HGML_COMPUTE_INSTANCE_PROFILE_1_CU = 0x8
HGML_COMPUTE_INSTANCE_PROFILE_2_CU = 0x9
HGML_COMPUTE_INSTANCE_PROFILE_3_CU = 0xA

HGML_COMPUTE_INSTANCE_PROFILE_1_CE = 0xB
HGML_COMPUTE_INSTANCE_PROFILE_2_CE = 0xC
HGML_COMPUTE_INSTANCE_PROFILE_3_CE = 0xD
HGML_COMPUTE_INSTANCE_PROFILE_4_CE = 0xE
HGML_COMPUTE_INSTANCE_PROFILE_5_CE = 0xF
HGML_COMPUTE_INSTANCE_PROFILE_6_CE = 0x10
HGML_COMPUTE_INSTANCE_PROFILE_7_CE = 0x11
HGML_COMPUTE_INSTANCE_PROFILE_8_CE = 0x12
HGML_COMPUTE_INSTANCE_PROFILE_9_CE = 0x13
HGML_COMPUTE_INSTANCE_PROFILE_10_CE = 0x14
HGML_COMPUTE_INSTANCE_PROFILE_11_CE = 0x15
HGML_COMPUTE_INSTANCE_PROFILE_12_CE = 0x16
HGML_COMPUTE_INSTANCE_PROFILE_13_CE = 0x17
HGML_COMPUTE_INSTANCE_PROFILE_14_CE = 0x18
HGML_COMPUTE_INSTANCE_PROFILE_15_CE = 0x19
HGML_COMPUTE_INSTANCE_PROFILE_16_CE = 0x1A
HGML_COMPUTE_INSTANCE_PROFILE_17_CE = 0x1B

HGML_COMPUTE_INSTANCE_PROFILE_COUNT = 0x8

HGML_COMPUTE_INSTANCE_ENGINE_PROFILE_SHARED = 0x0
HGML_COMPUTE_INSTANCE_ENGINE_PROFILE_COUNT = 0x1


class c_hgmlComputeInstancePlacement_t(Structure):
    _fields_ = [
        ("start", c_uint),
        ("size", c_uint),
    ]


class c_hgmlComputeInstanceProfileInfo_t(Structure):
    _fields_ = [
        ("id", c_uint),
        ("sliceCount", c_uint),
        ("instanceCount", c_uint),
        ("multiprocessorCount", c_uint),
        ("sharedCopyEngineCount", c_uint),
        ("sharedDecoderCount", c_uint),
        ("sharedEncoderCount", c_uint),
        ("sharedJpegCount", c_uint),
        ("sharedOfaCount", c_uint),
    ]


hgmlComputeInstanceProfileInfo_v2 = 0x02000088


class c_hgmlComputeInstanceProfileInfo_v2_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),
        ("id", c_uint),
        ("sliceCount", c_uint),
        ("instanceCount", c_uint),
        ("multiprocessorCount", c_uint),
        ("sharedCopyEngineCount", c_uint),
        ("sharedDecoderCount", c_uint),
        ("sharedEncoderCount", c_uint),
        ("sharedJpegCount", c_uint),
        ("sharedOfaCount", c_uint),
        ("name", c_char * HGML_DEVICE_NAME_V2_BUFFER_SIZE),
    ]

    def __init__(self):
        super().__init__(version=hgmlComputeInstanceProfileInfo_v2)


class c_hgmlComputeInstanceInfo_t(Structure):
    _fields_ = [
        ("device", c_hgmlDevice_t),
        ("gpuInstance", c_hgmlGpuInstance_t),
        ("id", c_uint),
        ("profileId", c_uint),
        ("placement", c_hgmlComputeInstancePlacement_t),
    ]


HGML_MAX_GPU_UTILIZATIONS = 8
HGML_GPU_UTILIZATION_DOMAIN_GPU = 0
HGML_GPU_UTILIZATION_DOMAIN_FB = 1
HGML_GPU_UTILIZATION_DOMAIN_VID = 2
HGML_GPU_UTILIZATION_DOMAIN_BUS = 3


class c_hgmlGpuDynamicPstatesUtilization_t(Structure):
    _fields_ = [
        ("bIsPresent", c_uint, 1),
        ("percentage", c_uint),
        ("incThreshold", c_uint),
        ("decThreshold", c_uint),
    ]


class c_hgmlGpuDynamicPstatesInfo_t(Structure):
    _fields_ = [
        ("flags", c_uint),
        (
            "utilization",
            c_hgmlGpuDynamicPstatesUtilization_t * HGML_MAX_GPU_UTILIZATIONS,
        ),
    ]


HGML_MAX_THERMAL_SENSORS_PER_GPU = 3

HGML_THERMAL_TARGET_NONE = 0
HGML_THERMAL_TARGET_UNKNOWN = -1

HGML_THERMAL_CONTROLLER_NONE = 0
HGML_THERMAL_CONTROLLER_UNKNOWN = -1


class c_hgmlGpuThermalSensor_t(Structure):
    _fields_ = [
        ("controller", c_int),
        ("defaultMinTemp", c_int),
        ("defaultMaxTemp", c_int),
        ("currentTemp", c_int),
        ("target", c_int),
    ]


class c_hgmlGpuThermalSettings_t(Structure):
    _fields_ = [
        ("count", c_uint),
        ("sensor", c_hgmlGpuThermalSensor_t * HGML_MAX_THERMAL_SENSORS_PER_GPU),
    ]


class struct_c_hgmlComputeInstance_t(Structure):
    pass  # opaque handle


c_hgmlComputeInstance_t = POINTER(struct_c_hgmlComputeInstance_t)


class c_hgmlDeviceAttributes(Structure):
    _fields_ = [
        ("multiprocessorCount", c_uint),
        ("sharedCopyEngineCount", c_uint),
        ("sharedDecoderCount", c_uint),
        ("sharedEncoderCount", c_uint),
        ("sharedJpegCount", c_uint),
        ("sharedOfaCount", c_uint),
        ("gpuInstanceSliceCount", c_uint),
        ("computeInstanceSliceCount", c_uint),
        ("memorySizeMB", c_ulonglong),
    ]


class c_hgmlRowRemapperHistogramValues(Structure):
    _fields_ = [
        ("max", c_uint),
        ("high", c_uint),
        ("partial", c_uint),
        ("low", c_uint),
        ("none", c_uint),
    ]


HGML_GPU_CERT_CHAIN_SIZE = 0x1000
HGML_GPU_ATTESTATION_CERT_CHAIN_SIZE = 0x1400
HGML_CC_GPU_CEC_NONCE_SIZE = 0x20
HGML_CC_GPU_ATTESTATION_REPORT_SIZE = 0x2000
HGML_CC_GPU_CEC_ATTESTATION_REPORT_SIZE = 0x1000
HGML_CC_CEC_ATTESTATION_REPORT_NOT_PRESENT = 0
HGML_CC_CEC_ATTESTATION_REPORT_PRESENT = 1


class c_hgmlConfComputeSystemState_t(Structure):
    _fields_ = [
        ("environment", c_uint),
        ("ccFeature", c_uint),
        ("devToolsMode", c_uint),
    ]


class c_hgmlConfComputeSystemCaps_t(Structure):
    _fields_ = [
        ("cpuCaps", c_uint),
        ("gpusCaps", c_uint),
    ]


class c_hgmlConfComputeMemSizeInfo_t(Structure):
    _fields_ = [
        ("protectedMemSizeKib", c_ulonglong),
        ("unprotectedMemSizeKib", c_ulonglong),
    ]


class c_hgmlConfComputeGpuCertificate_t(Structure):
    _fields_ = [
        ("certChainSize", c_uint),
        ("attestationCertChainSize", c_uint),
        ("certChain", c_uint8 * HGML_GPU_CERT_CHAIN_SIZE),
        ("attestationCertChain", c_uint8 * HGML_GPU_ATTESTATION_CERT_CHAIN_SIZE),
    ]


class c_hgmlConfComputeGpuAttestationReport_t(Structure):
    _fields_ = [
        ("isCecAttestationReportPresent", c_uint),  # output
        ("attestationReportSize", c_uint),  # output
        ("cecAttestationReportSize", c_uint),  # output
        (
            "nonce",
            c_uint8 * HGML_CC_GPU_CEC_NONCE_SIZE,
        ),  # input: spdm supports 32 bytes on nonce
        ("attestationReport", c_uint8 * HGML_CC_GPU_ATTESTATION_REPORT_SIZE),  # output
        (
            "cecAttestationReport",
            c_uint8 * HGML_CC_GPU_CEC_ATTESTATION_REPORT_SIZE,
        ),  # output
    ]


## string/bytes conversion for ease of use
def convertStrBytes(func):
    """
    In python 3, strings are unicode instead of bytes, and need to be converted for ctypes
    Args from caller: (1, 'string', <__main__.c_hgmlDevice_t at 0xFFFFFFFF>)
    Args passed to function: (1, b'string', <__main__.c_hgmlDevice_t at 0xFFFFFFFF)>.
    ----
    Returned from function: b'returned string'
    Returned to caller: 'returned string'
    """

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
    return func


def throwOnVersionMismatch(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except HGMLError_FunctionNotFound:
            raise HGMLLibraryMismatchError(
                "Unversioned function called and the "
                "pyHGML version does not match the HGML lib version. "
                "Either use matching pyHGML and HGML lib versions or "
                "use a versioned function such as " + func.__name__ + "_v2",
            )

    return wrapper


## C function wrappers ##
def hgmlInitWithFlags(flags):
    _LoadHgmlLibrary()

    #
    # Initialize the library
    #
    fn = _hgmlGetFunctionPointer("hgmlInitWithFlags")
    ret = fn(flags)
    _hgmlCheckReturn(ret)

    # Atomically update refcount
    global _hgmlLib_refcount
    libLoadLock.acquire()
    _hgmlLib_refcount += 1
    libLoadLock.release()


def hgmlInit():
    hgmlInitWithFlags(0)


def _LoadHgmlLibrary():
    """
    Load the library if it isn't loaded already.
    """
    global hgmlLib

    if hgmlLib is None:
        # lock to ensure only one caller loads the library
        libLoadLock.acquire()
        try:
            # ensure the library still isn't loaded
            if hgmlLib is None:
                if sys.platform.startswith("win"):
                    # Do not support Windows yet.
                    raise HGMLError(HGML_ERROR_LIBRARY_NOT_FOUND)
                # Linux path
                locs = [
                    "libhgml.so",
                    [
                        str(Path(__file__).resolve().parent / "libuki.so"),
                        str(Path(__file__).resolve().parent / "libhgml.so"),
                    ],
                ]
                for loc in locs:
                    try:
                        if isinstance(loc, str):
                            hgmlLib = CDLL(loc)
                        else:
                            for pre_loc in loc[:-1]:
                                CDLL(pre_loc)
                            hgmlLib = CDLL(loc[-1])
                        break
                    except OSError:
                        pass
                if hgmlLib is None:
                    raise HGMLError(HGML_ERROR_LIBRARY_NOT_FOUND)
        finally:
            # lock is always freed
            libLoadLock.release()


def hgmlShutdown():
    #
    # Leave the library loaded, but shutdown the interface
    #
    fn = _hgmlGetFunctionPointer("hgmlShutdown")
    ret = fn()
    _hgmlCheckReturn(ret)

    # Atomically update refcount
    global _hgmlLib_refcount
    libLoadLock.acquire()
    if _hgmlLib_refcount > 0:
        _hgmlLib_refcount -= 1
    libLoadLock.release()


# Added in 2.285
@convertStrBytes
def hgmlErrorString(result):
    fn = _hgmlGetFunctionPointer("hgmlErrorString")
    fn.restype = c_char_p  # otherwise return is an int
    ret = fn(result)
    return ret


# Added in 2.285
@convertStrBytes
def hgmlSystemGetHGMLVersion():
    c_version = create_string_buffer(HGML_SYSTEM_HGML_VERSION_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlSystemGetHGMLVersion")
    ret = fn(c_version, c_uint(HGML_SYSTEM_HGML_VERSION_BUFFER_SIZE))
    _hgmlCheckReturn(ret)
    return c_version.value


def hgmlSystemGetHggcDriverVersion():
    c_cuda_version = c_int()
    fn = _hgmlGetFunctionPointer("hgmlSystemGetHggcDriverVersion")
    ret = fn(byref(c_cuda_version))
    _hgmlCheckReturn(ret)
    return c_cuda_version.value


def hgmlSystemGetHggcDriverVersion_v2():
    c_cuda_version = c_int()
    fn = _hgmlGetFunctionPointer("hgmlSystemGetHggcDriverVersion_v2")
    ret = fn(byref(c_cuda_version))
    _hgmlCheckReturn(ret)
    return c_cuda_version.value


# Added in 2.285
@convertStrBytes
def hgmlSystemGetProcessName(pid):
    c_name = create_string_buffer(1024)
    fn = _hgmlGetFunctionPointer("hgmlSystemGetProcessName")
    ret = fn(c_uint(pid), c_name, c_uint(1024))
    _hgmlCheckReturn(ret)
    return c_name.value


@convertStrBytes
def hgmlSystemGetDriverVersion():
    c_version = create_string_buffer(HGML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlSystemGetDriverVersion")
    ret = fn(c_version, c_uint(HGML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE))
    _hgmlCheckReturn(ret)
    return c_version.value


# Added in 2.285
def hgmlSystemGetHicVersion():
    c_count = c_uint(0)
    hics = None
    fn = _hgmlGetFunctionPointer("hgmlSystemGetHicVersion")

    # get the count
    ret = fn(byref(c_count), None)

    # this should only fail with insufficient size
    if ret not in (HGML_SUCCESS, HGML_ERROR_INSUFFICIENT_SIZE):
        raise HGMLError(ret)

    # If there are no hics
    if c_count.value == 0:
        return []

    hic_array = c_hgmlHwbcEntry_t * c_count.value
    hics = hic_array()
    ret = fn(byref(c_count), hics)
    _hgmlCheckReturn(ret)
    return hics


## Unit get functions
def hgmlUnitGetCount():
    c_count = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlUnitGetCount")
    ret = fn(byref(c_count))
    _hgmlCheckReturn(ret)
    return c_count.value


def hgmlUnitGetHandleByIndex(index):
    c_index = c_uint(index)
    unit = c_hgmlUnit_t()
    fn = _hgmlGetFunctionPointer("hgmlUnitGetHandleByIndex")
    ret = fn(c_index, byref(unit))
    _hgmlCheckReturn(ret)
    return unit


def hgmlUnitGetUnitInfo(unit):
    c_info = c_hgmlUnitInfo_t()
    fn = _hgmlGetFunctionPointer("hgmlUnitGetUnitInfo")
    ret = fn(unit, byref(c_info))
    _hgmlCheckReturn(ret)
    return c_info


def hgmlUnitGetLedState(unit):
    c_state = c_hgmlLedState_t()
    fn = _hgmlGetFunctionPointer("hgmlUnitGetLedState")
    ret = fn(unit, byref(c_state))
    _hgmlCheckReturn(ret)
    return c_state


def hgmlUnitGetPsuInfo(unit):
    c_info = c_hgmlPSUInfo_t()
    fn = _hgmlGetFunctionPointer("hgmlUnitGetPsuInfo")
    ret = fn(unit, byref(c_info))
    _hgmlCheckReturn(ret)
    return c_info


def hgmlUnitGetTemperature(unit, type):
    c_temp = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlUnitGetTemperature")
    ret = fn(unit, c_uint(type), byref(c_temp))
    _hgmlCheckReturn(ret)
    return c_temp.value


def hgmlUnitGetFanSpeedInfo(unit):
    c_speeds = c_hgmlUnitFanSpeeds_t()
    fn = _hgmlGetFunctionPointer("hgmlUnitGetFanSpeedInfo")
    ret = fn(unit, byref(c_speeds))
    _hgmlCheckReturn(ret)
    return c_speeds


# added to API
def hgmlUnitGetDeviceCount(unit):
    c_count = c_uint(0)
    # query the unit to determine device count
    fn = _hgmlGetFunctionPointer("hgmlUnitGetDevices")
    ret = fn(unit, byref(c_count), None)
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        ret = HGML_SUCCESS
    _hgmlCheckReturn(ret)
    return c_count.value


def hgmlUnitGetDevices(unit):
    c_count = c_uint(hgmlUnitGetDeviceCount(unit))
    device_array = c_hgmlDevice_t * c_count.value
    c_devices = device_array()
    fn = _hgmlGetFunctionPointer("hgmlUnitGetDevices")
    ret = fn(unit, byref(c_count), c_devices)
    _hgmlCheckReturn(ret)
    return c_devices


## Device get functions
def hgmlDeviceGetCount():
    c_count = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetCount_v2")
    ret = fn(byref(c_count))
    _hgmlCheckReturn(ret)
    return c_count.value


def hgmlDeviceGetHandleByIndex(index):
    c_index = c_uint(index)
    device = c_hgmlDevice_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetHandleByIndex_v2")
    ret = fn(c_index, byref(device))
    _hgmlCheckReturn(ret)
    return device


# Deprecated
@convertStrBytes
def hgmlDeviceGetHandleBySerial(serial):
    c_serial = c_char_p(serial)
    device = c_hgmlDevice_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetHandleBySerial")
    ret = fn(c_serial, byref(device))
    _hgmlCheckReturn(ret)
    return device


@convertStrBytes
def hgmlDeviceGetHandleByUUID(uuid):
    c_uuid = c_char_p(uuid)
    device = c_hgmlDevice_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetHandleByUUID")
    ret = fn(c_uuid, byref(device))
    _hgmlCheckReturn(ret)
    return device


@convertStrBytes
def hgmlDeviceGetHandleByPciBusId(pciBusId):
    c_busId = c_char_p(pciBusId)
    device = c_hgmlDevice_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetHandleByPciBusId_v2")
    ret = fn(c_busId, byref(device))
    _hgmlCheckReturn(ret)
    return device


@convertStrBytes
def hgmlDeviceGetName(handle):
    c_name = create_string_buffer(HGML_DEVICE_NAME_V2_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetName")
    ret = fn(handle, c_name, c_uint(HGML_DEVICE_NAME_V2_BUFFER_SIZE))
    _hgmlCheckReturn(ret)
    return c_name.value


def hgmlDeviceGetBoardId(handle):
    c_id = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetBoardId")
    ret = fn(handle, byref(c_id))
    _hgmlCheckReturn(ret)
    return c_id.value


def hgmlDeviceGetMultiGpuBoard(handle):
    c_multiGpu = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMultiGpuBoard")
    ret = fn(handle, byref(c_multiGpu))
    _hgmlCheckReturn(ret)
    return c_multiGpu.value


def hgmlDeviceGetBrand(handle):
    c_type = _hgmlBrandType_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetBrand")
    ret = fn(handle, byref(c_type))
    _hgmlCheckReturn(ret)
    return c_type.value


def hgmlDeviceGetC2cModeInfoV1(handle):
    c_info = c_hgmlC2cModeInfo_v1_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetC2cModeInfoV")
    ret = fn(handle, byref(c_info))
    _hgmlCheckReturn(ret)
    return c_info


def hgmlDeviceGetC2cModeInfoV(handle):
    return hgmlDeviceGetC2cModeInfoV1(handle)


@convertStrBytes
def hgmlDeviceGetBoardPartNumber(handle):
    c_part_number = create_string_buffer(HGML_DEVICE_PART_NUMBER_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetBoardPartNumber")
    ret = fn(handle, c_part_number, c_uint(HGML_DEVICE_PART_NUMBER_BUFFER_SIZE))
    _hgmlCheckReturn(ret)
    return c_part_number.value


@convertStrBytes
def hgmlDeviceGetSerial(handle):
    c_serial = create_string_buffer(HGML_DEVICE_SERIAL_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetSerial")
    ret = fn(handle, c_serial, c_uint(HGML_DEVICE_SERIAL_BUFFER_SIZE))
    _hgmlCheckReturn(ret)
    return c_serial.value


def hgmlDeviceGetModuleId(handle, moduleId=c_uint()):
    isReference = type(moduleId) is not c_uint
    moduleIdRef = moduleId if isReference else byref(moduleId)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetModuleId")
    ret = fn(handle, moduleIdRef)
    if isReference:
        return ret
    _hgmlCheckReturn(ret)
    return moduleId.value


def hgmlDeviceGetMemoryAffinity(handle, nodeSetSize, scope):
    affinity_array = c_ulonglong * nodeSetSize
    c_affinity = affinity_array()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMemoryAffinity")
    ret = fn(handle, nodeSetSize, byref(c_affinity), _hgmlAffinityScope_t(scope))
    _hgmlCheckReturn(ret)
    return c_affinity


def hgmlDeviceGetCpuAffinityWithinScope(handle, cpuSetSize, scope):
    affinity_array = c_ulonglong * cpuSetSize
    c_affinity = affinity_array()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetCpuAffinityWithinScope")
    ret = fn(handle, cpuSetSize, byref(c_affinity), _hgmlAffinityScope_t(scope))
    _hgmlCheckReturn(ret)
    return c_affinity


def hgmlDeviceGetCpuAffinity(handle, cpuSetSize):
    affinity_array = c_ulonglong * cpuSetSize
    c_affinity = affinity_array()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetCpuAffinity")
    ret = fn(handle, cpuSetSize, byref(c_affinity))
    _hgmlCheckReturn(ret)
    return c_affinity


def hgmlDeviceSetCpuAffinity(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetCpuAffinity")
    ret = fn(handle)
    _hgmlCheckReturn(ret)


def hgmlDeviceClearCpuAffinity(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceClearCpuAffinity")
    ret = fn(handle)
    _hgmlCheckReturn(ret)


def hgmlDeviceGetMinorNumber(handle):
    c_minor_number = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMinorNumber")
    ret = fn(handle, byref(c_minor_number))
    _hgmlCheckReturn(ret)
    return c_minor_number.value


@convertStrBytes
def hgmlDeviceGetUUID(handle):
    c_uuid = create_string_buffer(HGML_DEVICE_UUID_V2_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetUUID")
    ret = fn(handle, c_uuid, c_uint(HGML_DEVICE_UUID_V2_BUFFER_SIZE))
    _hgmlCheckReturn(ret)
    return c_uuid.value


@convertStrBytes
def hgmlDeviceGetInforomVersion(handle, infoRomObject):
    c_version = create_string_buffer(HGML_DEVICE_INFOROM_VERSION_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetInforomVersion")
    ret = fn(
        handle,
        _hgmlInforomObject_t(infoRomObject),
        c_version,
        c_uint(HGML_DEVICE_INFOROM_VERSION_BUFFER_SIZE),
    )
    _hgmlCheckReturn(ret)
    return c_version.value


# Added in 4.304
@convertStrBytes
def hgmlDeviceGetInforomImageVersion(handle):
    c_version = create_string_buffer(HGML_DEVICE_INFOROM_VERSION_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetInforomImageVersion")
    ret = fn(handle, c_version, c_uint(HGML_DEVICE_INFOROM_VERSION_BUFFER_SIZE))
    _hgmlCheckReturn(ret)
    return c_version.value


# Added in 4.304
def hgmlDeviceGetInforomConfigurationChecksum(handle):
    c_checksum = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetInforomConfigurationChecksum")
    ret = fn(handle, byref(c_checksum))
    _hgmlCheckReturn(ret)
    return c_checksum.value


# Added in 4.304
def hgmlDeviceValidateInforom(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceValidateInforom")
    ret = fn(handle)
    _hgmlCheckReturn(ret)


def hgmlDeviceGetLastBBXFlushTime(handle):
    c_timestamp = c_ulonglong()
    c_durationUs = c_ulong()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetLastBBXFlushTime")
    ret = fn(handle, byref(c_timestamp), byref(c_durationUs))
    _hgmlCheckReturn(ret)
    return [c_timestamp.value, c_durationUs.value]


def hgmlDeviceGetDisplayMode(handle):
    c_mode = _hgmlEnableState_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetDisplayMode")
    ret = fn(handle, byref(c_mode))
    _hgmlCheckReturn(ret)
    return c_mode.value


def hgmlDeviceGetDisplayActive(handle):
    c_mode = _hgmlEnableState_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetDisplayActive")
    ret = fn(handle, byref(c_mode))
    _hgmlCheckReturn(ret)
    return c_mode.value


def hgmlDeviceGetPersistenceMode(handle):
    c_state = _hgmlEnableState_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPersistenceMode")
    ret = fn(handle, byref(c_state))
    _hgmlCheckReturn(ret)
    return c_state.value


def hgmlDeviceGetPciInfo_v3(handle):
    c_info = hgmlPciInfo_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPciInfo_v3")
    ret = fn(handle, byref(c_info))
    _hgmlCheckReturn(ret)
    return c_info


def hgmlDeviceGetPciInfo(handle):
    return hgmlDeviceGetPciInfo_v3(handle)


def hgmlDeviceGetClockInfo(handle, type):
    c_clock = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetClockInfo")
    ret = fn(handle, _hgmlClockType_t(type), byref(c_clock))
    _hgmlCheckReturn(ret)
    return c_clock.value


# Added in 2.285
def hgmlDeviceGetMaxClockInfo(handle, type):
    c_clock = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMaxClockInfo")
    ret = fn(handle, _hgmlClockType_t(type), byref(c_clock))
    _hgmlCheckReturn(ret)
    return c_clock.value


# Added in 4.304
# Deprecated
def hgmlDeviceGetApplicationsClock(handle, type):
    c_clock = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetApplicationsClock")
    ret = fn(handle, _hgmlClockType_t(type), byref(c_clock))
    _hgmlCheckReturn(ret)
    return c_clock.value


def hgmlDeviceGetMaxCustomerBoostClock(handle, type):
    c_clock = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMaxCustomerBoostClock")
    ret = fn(handle, _hgmlClockType_t(type), byref(c_clock))
    _hgmlCheckReturn(ret)
    return c_clock.value


def hgmlDeviceGetClock(handle, type, id):
    c_clock = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetClock")
    ret = fn(handle, _hgmlClockType_t(type), _hgmlClockId_t(id), byref(c_clock))
    _hgmlCheckReturn(ret)
    return c_clock.value


# Added in 5.319
# Deprecated
def hgmlDeviceGetDefaultApplicationsClock(handle, type):
    c_clock = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetDefaultApplicationsClock")
    ret = fn(handle, _hgmlClockType_t(type), byref(c_clock))
    _hgmlCheckReturn(ret)
    return c_clock.value


# Added in 4.304
def hgmlDeviceGetSupportedMemoryClocks(handle):
    # first call to get the size
    c_count = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetSupportedMemoryClocks")
    ret = fn(handle, byref(c_count), None)

    if ret == HGML_SUCCESS:
        # special case, no clocks
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        clocks_array = c_uint * c_count.value
        c_clocks = clocks_array()

        # make the call again
        ret = fn(handle, byref(c_count), c_clocks)
        _hgmlCheckReturn(ret)

        procs = []
        for i in range(c_count.value):
            procs.append(c_clocks[i])

        return procs
    # error case
    raise HGMLError(ret)


# Added in 4.304
def hgmlDeviceGetSupportedGraphicsClocks(handle, memoryClockMHz):
    # first call to get the size
    c_count = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetSupportedGraphicsClocks")
    ret = fn(handle, c_uint(memoryClockMHz), byref(c_count), None)

    if ret == HGML_SUCCESS:
        # special case, no clocks
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        clocks_array = c_uint * c_count.value
        c_clocks = clocks_array()

        # make the call again
        ret = fn(handle, c_uint(memoryClockMHz), byref(c_count), c_clocks)
        _hgmlCheckReturn(ret)

        procs = []
        for i in range(c_count.value):
            procs.append(c_clocks[i])

        return procs
    # error case
    raise HGMLError(ret)


def hgmlDeviceGetFanSpeed(handle):
    c_speed = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetFanSpeed")
    ret = fn(handle, byref(c_speed))
    _hgmlCheckReturn(ret)
    return c_speed.value


def hgmlDeviceGetFanSpeed_v2(handle, fan):
    c_speed = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetFanSpeed_v2")
    ret = fn(handle, fan, byref(c_speed))
    _hgmlCheckReturn(ret)
    return c_speed.value


def hgmlDeviceGetTargetFanSpeed(handle, fan):
    c_speed = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetTargetFanSpeed")
    ret = fn(handle, fan, byref(c_speed))
    _hgmlCheckReturn(ret)
    return c_speed.value


def hgmlDeviceGetNumFans(device):
    c_numFans = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetNumFans")
    ret = fn(device, byref(c_numFans))
    _hgmlCheckReturn(ret)
    return c_numFans.value


def hgmlDeviceSetDefaultFanSpeed_v2(handle, index):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetDefaultFanSpeed_v2")
    ret = fn(handle, index)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlDeviceGetMinMaxFanSpeed(handle, minSpeed=c_uint(), maxSpeed=c_uint()):
    isReference = (type(minSpeed) is not c_uint) or (type(maxSpeed) is not c_uint)
    minSpeedRef = minSpeed if isReference else byref(minSpeed)
    maxSpeedRef = maxSpeed if isReference else byref(maxSpeed)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMinMaxFanSpeed")
    ret = fn(handle, minSpeedRef, maxSpeedRef)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS if isReference else [minSpeed.value, maxSpeed.value]


def hgmlDeviceGetFanControlPolicy_v2(handle, fan, fanControlPolicy=c_uint()):
    isReference = type(fanControlPolicy) is not c_uint
    fanControlPolicyRef = fanControlPolicy if isReference else byref(fanControlPolicy)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetFanControlPolicy_v2")
    ret = fn(handle, fan, fanControlPolicyRef)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS if isReference else fanControlPolicy.value


def hgmlDeviceSetFanControlPolicy(handle, fan, fanControlPolicy):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetFanControlPolicy")
    ret = fn(handle, fan, _hgmlFanControlPolicy_t(fanControlPolicy))
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlDeviceGetTemperature(handle, sensor):
    c_temp = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetTemperature")
    ret = fn(handle, _hgmlTemperatureSensors_t(sensor), byref(c_temp))
    _hgmlCheckReturn(ret)
    return c_temp.value


def hgmlDeviceGetTemperatureThreshold(handle, threshold):
    c_temp = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetTemperatureThreshold")
    ret = fn(handle, _hgmlTemperatureThresholds_t(threshold), byref(c_temp))
    _hgmlCheckReturn(ret)
    return c_temp.value


def hgmlDeviceSetTemperatureThreshold(handle, threshold, temp):
    c_temp = c_uint()
    c_temp.value = temp
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetTemperatureThreshold")
    ret = fn(handle, _hgmlTemperatureThresholds_t(threshold), byref(c_temp))
    _hgmlCheckReturn(ret)


# DEPRECATED use hgmlDeviceGetPerformanceState
def hgmlDeviceGetPowerState(handle):
    c_pstate = _hgmlPstates_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPowerState")
    ret = fn(handle, byref(c_pstate))
    _hgmlCheckReturn(ret)
    return c_pstate.value


def hgmlDeviceGetPerformanceState(handle):
    c_pstate = _hgmlPstates_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPerformanceState")
    ret = fn(handle, byref(c_pstate))
    _hgmlCheckReturn(ret)
    return c_pstate.value


# Deprecated
def hgmlDeviceGetPowerManagementMode(handle):
    c_pcapMode = _hgmlEnableState_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPowerManagementMode")
    ret = fn(handle, byref(c_pcapMode))
    _hgmlCheckReturn(ret)
    return c_pcapMode.value


def hgmlDeviceGetPowerManagementLimit(handle):
    c_limit = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPowerManagementLimit")
    ret = fn(handle, byref(c_limit))
    _hgmlCheckReturn(ret)
    return c_limit.value


# Added in 4.304
def hgmlDeviceGetPowerManagementLimitConstraints(handle):
    c_minLimit = c_uint()
    c_maxLimit = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPowerManagementLimitConstraints")
    ret = fn(handle, byref(c_minLimit), byref(c_maxLimit))
    _hgmlCheckReturn(ret)
    return [c_minLimit.value, c_maxLimit.value]


# Added in 4.304
def hgmlDeviceGetPowerManagementDefaultLimit(handle):
    c_limit = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPowerManagementDefaultLimit")
    ret = fn(handle, byref(c_limit))
    _hgmlCheckReturn(ret)
    return c_limit.value


# Added in 331
def hgmlDeviceGetEnforcedPowerLimit(handle):
    c_limit = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetEnforcedPowerLimit")
    ret = fn(handle, byref(c_limit))
    _hgmlCheckReturn(ret)
    return c_limit.value


def hgmlDeviceGetPowerUsage(handle):
    c_watts = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPowerUsage")
    ret = fn(handle, byref(c_watts))
    _hgmlCheckReturn(ret)
    return c_watts.value


def hgmlDeviceGetTotalEnergyConsumption(handle):
    c_millijoules = c_uint64()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetTotalEnergyConsumption")
    ret = fn(handle, byref(c_millijoules))
    _hgmlCheckReturn(ret)
    return c_millijoules.value


# Added in 4.304
def hgmlDeviceGetGpuOperationMode(handle):
    c_currState = _hgmlGpuOperationMode_t()
    c_pendingState = _hgmlGpuOperationMode_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGpuOperationMode")
    ret = fn(handle, byref(c_currState), byref(c_pendingState))
    _hgmlCheckReturn(ret)
    return [c_currState.value, c_pendingState.value]


# Added in 4.304
def hgmlDeviceGetCurrentGpuOperationMode(handle):
    return hgmlDeviceGetGpuOperationMode(handle)[0]


# Added in 4.304
def hgmlDeviceGetPendingGpuOperationMode(handle):
    return hgmlDeviceGetGpuOperationMode(handle)[1]


def hgmlDeviceGetMemoryInfo(handle, version=None):
    if not version:
        c_memory = c_hgmlMemory_t()
        fn = _hgmlGetFunctionPointer("hgmlDeviceGetMemoryInfo")
    else:
        c_memory = c_hgmlMemory_v2_t()
        c_memory.version = version
        fn = _hgmlGetFunctionPointer("hgmlDeviceGetMemoryInfo_v2")
    ret = fn(handle, byref(c_memory))
    _hgmlCheckReturn(ret)
    return c_memory


def hgmlDeviceGetBAR1MemoryInfo(handle):
    c_bar1_memory = c_hgmlBAR1Memory_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetBAR1MemoryInfo")
    ret = fn(handle, byref(c_bar1_memory))
    _hgmlCheckReturn(ret)
    return c_bar1_memory


def hgmlDeviceGetComputeMode(handle):
    c_mode = _hgmlComputeMode_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetComputeMode")
    ret = fn(handle, byref(c_mode))
    _hgmlCheckReturn(ret)
    return c_mode.value


def hgmlDeviceGetHggcComputeCapability(handle):
    c_major = c_int()
    c_minor = c_int()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetHggcComputeCapability")
    ret = fn(handle, byref(c_major), byref(c_minor))
    _hgmlCheckReturn(ret)
    return (c_major.value, c_minor.value)


def hgmlDeviceGetEccMode(handle):
    c_currState = _hgmlEnableState_t()
    c_pendingState = _hgmlEnableState_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetEccMode")
    ret = fn(handle, byref(c_currState), byref(c_pendingState))
    _hgmlCheckReturn(ret)
    return [c_currState.value, c_pendingState.value]


# added to API
def hgmlDeviceGetCurrentEccMode(handle):
    return hgmlDeviceGetEccMode(handle)[0]


# added to API
def hgmlDeviceGetPendingEccMode(handle):
    return hgmlDeviceGetEccMode(handle)[1]


def hgmlDeviceGetDefaultEccMode(handle):
    c_defaultState = _hgmlEnableState_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetDefaultEccMode")
    ret = fn(handle, byref(c_defaultState))
    _hgmlCheckReturn(ret)
    return [c_defaultState.value]


def hgmlDeviceGetTotalEccErrors(handle, errorType, counterType):
    c_count = c_ulonglong()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetTotalEccErrors")
    ret = fn(
        handle,
        _hgmlMemoryErrorType_t(errorType),
        _hgmlEccCounterType_t(counterType),
        byref(c_count),
    )
    _hgmlCheckReturn(ret)
    return c_count.value


# This is deprecated, instead use hgmlDeviceGetMemoryErrorCounter
def hgmlDeviceGetDetailedEccErrors(handle, errorType, counterType):
    c_counts = c_hgmlEccErrorCounts_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetDetailedEccErrors")
    ret = fn(
        handle,
        _hgmlMemoryErrorType_t(errorType),
        _hgmlEccCounterType_t(counterType),
        byref(c_counts),
    )
    _hgmlCheckReturn(ret)
    return c_counts


# Added in 4.304
def hgmlDeviceGetMemoryErrorCounter(handle, errorType, counterType, locationType):
    c_count = c_ulonglong()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMemoryErrorCounter")
    ret = fn(
        handle,
        _hgmlMemoryErrorType_t(errorType),
        _hgmlEccCounterType_t(counterType),
        _hgmlMemoryLocation_t(locationType),
        byref(c_count),
    )
    _hgmlCheckReturn(ret)
    return c_count.value


def hgmlDeviceGetUtilizationRates(handle):
    c_util = c_hgmlUtilization_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetUtilizationRates")
    ret = fn(handle, byref(c_util))
    _hgmlCheckReturn(ret)
    return c_util


def hgmlDeviceGetEncoderUtilization(handle):
    c_util = c_uint()
    c_samplingPeriod = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetEncoderUtilization")
    ret = fn(handle, byref(c_util), byref(c_samplingPeriod))
    _hgmlCheckReturn(ret)
    return [c_util.value, c_samplingPeriod.value]


def hgmlDeviceGetDecoderUtilization(handle):
    c_util = c_uint()
    c_samplingPeriod = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetDecoderUtilization")
    ret = fn(handle, byref(c_util), byref(c_samplingPeriod))
    _hgmlCheckReturn(ret)
    return [c_util.value, c_samplingPeriod.value]


def hgmlDeviceGetJpgUtilization(handle):
    c_util = c_uint()
    c_samplingPeriod = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetJpgUtilization")
    ret = fn(handle, byref(c_util), byref(c_samplingPeriod))
    _hgmlCheckReturn(ret)
    return [c_util.value, c_samplingPeriod.value]


def hgmlDeviceGetOfaUtilization(handle):
    c_util = c_uint()
    c_samplingPeriod = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetOfaUtilization")
    ret = fn(handle, byref(c_util), byref(c_samplingPeriod))
    _hgmlCheckReturn(ret)
    return [c_util.value, c_samplingPeriod.value]


def hgmlDeviceGetPcieReplayCounter(handle):
    c_replay = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPcieReplayCounter")
    ret = fn(handle, byref(c_replay))
    _hgmlCheckReturn(ret)
    return c_replay.value


def hgmlDeviceGetDriverModel(handle):
    c_currModel = _hgmlDriverModel_t()
    c_pendingModel = _hgmlDriverModel_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetDriverModel")
    ret = fn(handle, byref(c_currModel), byref(c_pendingModel))
    _hgmlCheckReturn(ret)
    return [c_currModel.value, c_pendingModel.value]


# added to API
def hgmlDeviceGetCurrentDriverModel(handle):
    return hgmlDeviceGetDriverModel(handle)[0]


# added to API
def hgmlDeviceGetPendingDriverModel(handle):
    return hgmlDeviceGetDriverModel(handle)[1]


# Added in 2.285
@convertStrBytes
def hgmlDeviceGetVbiosVersion(handle):
    c_version = create_string_buffer(HGML_DEVICE_VBIOS_VERSION_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetVbiosVersion")
    ret = fn(handle, c_version, c_uint(HGML_DEVICE_VBIOS_VERSION_BUFFER_SIZE))
    _hgmlCheckReturn(ret)
    return c_version.value


# Added in 2.285
def hgmlDeviceGetComputeRunningProcesses_v2(handle):
    # first call to get the size
    c_count = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetComputeRunningProcesses_v2")
    ret = fn(handle, byref(c_count), None)
    if ret == HGML_SUCCESS:
        # special case, no running processes
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        # oversize the array incase more processes are created
        c_count.value = c_count.value * 2 + 5
        proc_array = c_hgmlProcessInfo_v2_t * c_count.value
        c_procs = proc_array()
        # make the call again
        ret = fn(handle, byref(c_count), c_procs)
        _hgmlCheckReturn(ret)
        procs = []
        for i in range(c_count.value):
            # use an alternative struct for this object
            obj = hgmlStructToFriendlyObject(c_procs[i])
            if obj.usedGpuMemory == HGML_VALUE_NOT_AVAILABLE_ulonglong.value:
                # special case for WDDM on Windows, see comment above
                obj.usedGpuMemory = None
            procs.append(obj)
        return procs
    # error case
    raise HGMLError(ret)


# Added in 2.285
def hgmlDeviceGetComputeRunningProcesses_v3(handle):
    # first call to get the size
    c_count = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetComputeRunningProcesses_v3")
    ret = fn(handle, byref(c_count), None)

    if ret == HGML_SUCCESS:
        # special case, no running processes
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        # oversize the array incase more processes are created
        c_count.value = c_count.value * 2 + 5
        proc_array = c_hgmlProcessInfo_v3_t * c_count.value
        c_procs = proc_array()

        # make the call again
        ret = fn(handle, byref(c_count), c_procs)
        _hgmlCheckReturn(ret)

        procs = []
        for i in range(c_count.value):
            # use an alternative struct for this object
            obj = hgmlStructToFriendlyObject(c_procs[i])
            if obj.usedGpuMemory == HGML_VALUE_NOT_AVAILABLE_ulonglong.value:
                # special case for WDDM on Windows, see comment above
                obj.usedGpuMemory = None
            procs.append(obj)

        return procs
    # error case
    raise HGMLError(ret)


@throwOnVersionMismatch
def hgmlDeviceGetComputeRunningProcesses(handle):
    return hgmlDeviceGetComputeRunningProcesses_v3(handle)


def hgmlDeviceGetGraphicsRunningProcesses_v2(handle):
    # first call to get the size
    c_count = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGraphicsRunningProcesses_v2")
    ret = fn(handle, byref(c_count), None)
    if ret == HGML_SUCCESS:
        # special case, no running processes
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        # oversize the array incase more processes are created
        c_count.value = c_count.value * 2 + 5
        proc_array = c_hgmlProcessInfo_v2_t * c_count.value
        c_procs = proc_array()
        # make the call again
        ret = fn(handle, byref(c_count), c_procs)
        _hgmlCheckReturn(ret)
        procs = []
        for i in range(c_count.value):
            # use an alternative struct for this object
            obj = hgmlStructToFriendlyObject(c_procs[i])
            if obj.usedGpuMemory == HGML_VALUE_NOT_AVAILABLE_ulonglong.value:
                # special case for WDDM on Windows, see comment above
                obj.usedGpuMemory = None
            procs.append(obj)
        return procs
    # error case
    raise HGMLError(ret)


def hgmlDeviceGetGraphicsRunningProcesses_v3(handle):
    # first call to get the size
    c_count = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGraphicsRunningProcesses_v3")
    ret = fn(handle, byref(c_count), None)

    if ret == HGML_SUCCESS:
        # special case, no running processes
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        # oversize the array incase more processes are created
        c_count.value = c_count.value * 2 + 5
        proc_array = c_hgmlProcessInfo_v3_t * c_count.value
        c_procs = proc_array()

        # make the call again
        ret = fn(handle, byref(c_count), c_procs)
        _hgmlCheckReturn(ret)

        procs = []
        for i in range(c_count.value):
            # use an alternative struct for this object
            obj = hgmlStructToFriendlyObject(c_procs[i])
            if obj.usedGpuMemory == HGML_VALUE_NOT_AVAILABLE_ulonglong.value:
                # special case for WDDM on Windows, see comment above
                obj.usedGpuMemory = None
            procs.append(obj)

        return procs
    # error case
    raise HGMLError(ret)


@throwOnVersionMismatch
def hgmlDeviceGetGraphicsRunningProcesses(handle):
    return hgmlDeviceGetGraphicsRunningProcesses_v3(handle)


@throwOnVersionMismatch
def hgmlDeviceGetMPSComputeRunningProcesses(handle):
    return hgmlDeviceGetMPSComputeRunningProcesses_v3(handle)


def hgmlDeviceGetMPSComputeRunningProcesses_v2(handle):
    # first call to get the size
    c_count = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMPSComputeRunningProcesses_v2")
    ret = fn(handle, byref(c_count), None)

    if ret == HGML_SUCCESS:
        # special case, no running processes
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        # oversize the array incase more processes are created
        c_count.value = c_count.value * 2 + 5
        proc_array = c_hgmlProcessInfo_v2_t * c_count.value
        c_procs = proc_array()

        # make the call again
        ret = fn(handle, byref(c_count), c_procs)
        _hgmlCheckReturn(ret)

        procs = []
        for i in range(c_count.value):
            # use an alternative struct for this object
            obj = hgmlStructToFriendlyObject(c_procs[i])
            if obj.usedGpuMemory == HGML_VALUE_NOT_AVAILABLE_ulonglong.value:
                # special case for WDDM on Windows, see comment above
                obj.usedGpuMemory = None
            procs.append(obj)

        return procs
    # error case
    raise HGMLError(ret)


def hgmlDeviceGetMPSComputeRunningProcesses_v3(handle):
    # first call to get the size
    c_count = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMPSComputeRunningProcesses_v3")
    ret = fn(handle, byref(c_count), None)

    if ret == HGML_SUCCESS:
        # special case, no running processes
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        # oversize the array incase more processes are created
        c_count.value = c_count.value * 2 + 5
        proc_array = c_hgmlProcessInfo_v3_t * c_count.value
        c_procs = proc_array()

        # make the call again
        ret = fn(handle, byref(c_count), c_procs)
        _hgmlCheckReturn(ret)

        procs = []
        for i in range(c_count.value):
            # use an alternative struct for this object
            obj = hgmlStructToFriendlyObject(c_procs[i])
            if obj.usedGpuMemory == HGML_VALUE_NOT_AVAILABLE_ulonglong.value:
                # special case for WDDM on Windows, see comment above
                obj.usedGpuMemory = None
            procs.append(obj)

        return procs
    # error case
    raise HGMLError(ret)


def hgmlDeviceGetRunningProcessDetailList(handle, version, mode):
    c_processDetailList = c_hgmlProcessDetailList_t()
    c_processDetailList.version = version
    c_processDetailList.mode = mode

    fn = _hgmlGetFunctionPointer("hgmlDeviceGetRunningProcessDetailList")

    # first call to get the size
    ret = fn(handle, byref(c_processDetailList))
    if ret == HGML_SUCCESS:
        # special case, no running processes
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        c_procs = c_hgmlProcessDetail_v1_t * c_processDetailList.numProcArrayEntries
        c_processDetailList.procArray = cast(
            (c_procs)(), POINTER(c_hgmlProcessDetail_v1_t)
        )

        # make the call again
        ret = fn(handle, byref(c_processDetailList))
        _hgmlCheckReturn(ret)

        procs = []
        for i in range(c_processDetailList.numProcArrayEntries):
            # use an alternative struct for this object
            obj = c_processDetailList.procArray[i]
            if obj.usedGpuMemory == HGML_VALUE_NOT_AVAILABLE_ulonglong.value:
                obj.usedGpuMemory = None
            if obj.usedGpuCcProtectedMemory == HGML_VALUE_NOT_AVAILABLE_ulonglong.value:
                obj.usedGpuCcProtectedMemory = None
            procs.append(obj)

        return procs
    # error case
    raise HGMLError(ret)


def hgmlDeviceGetAutoBoostedClocksEnabled(handle):
    c_isEnabled = _hgmlEnableState_t()
    c_defaultIsEnabled = _hgmlEnableState_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetAutoBoostedClocksEnabled")
    ret = fn(handle, byref(c_isEnabled), byref(c_defaultIsEnabled))
    _hgmlCheckReturn(ret)
    return [c_isEnabled.value, c_defaultIsEnabled.value]
    # Throws HGML_ERROR_NOT_SUPPORTED if hardware doesn't support setting auto boosted clocks


## Set functions
def hgmlUnitSetLedState(unit, color):
    fn = _hgmlGetFunctionPointer("hgmlUnitSetLedState")
    ret = fn(unit, _hgmlLedColor_t(color))
    _hgmlCheckReturn(ret)


def hgmlDeviceSetPersistenceMode(handle, mode):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetPersistenceMode")
    ret = fn(handle, _hgmlEnableState_t(mode))
    _hgmlCheckReturn(ret)


def hgmlDeviceSetComputeMode(handle, mode):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetComputeMode")
    ret = fn(handle, _hgmlComputeMode_t(mode))
    _hgmlCheckReturn(ret)


def hgmlDeviceSetEccMode(handle, mode):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetEccMode")
    ret = fn(handle, _hgmlEnableState_t(mode))
    _hgmlCheckReturn(ret)


def hgmlDeviceClearEccErrorCounts(handle, counterType):
    fn = _hgmlGetFunctionPointer("hgmlDeviceClearEccErrorCounts")
    ret = fn(handle, _hgmlEccCounterType_t(counterType))
    _hgmlCheckReturn(ret)


def hgmlDeviceSetDriverModel(handle, model):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetDriverModel")
    ret = fn(handle, _hgmlDriverModel_t(model))
    _hgmlCheckReturn(ret)


def hgmlDeviceSetAutoBoostedClocksEnabled(handle, enabled):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetAutoBoostedClocksEnabled")
    ret = fn(handle, _hgmlEnableState_t(enabled))
    _hgmlCheckReturn(ret)
    # Throws HGML_ERROR_NOT_SUPPORTED if hardware doesn't support setting auto boosted clocks


def hgmlDeviceSetDefaultAutoBoostedClocksEnabled(handle, enabled, flags):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetDefaultAutoBoostedClocksEnabled")
    ret = fn(handle, _hgmlEnableState_t(enabled), c_uint(flags))
    _hgmlCheckReturn(ret)
    # Throws HGML_ERROR_NOT_SUPPORTED if hardware doesn't support setting auto boosted clocks


def hgmlDeviceSetGpuLockedClocks(handle, minGpuClockMHz, maxGpuClockMHz):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetGpuLockedClocks")
    ret = fn(handle, c_uint(minGpuClockMHz), c_uint(maxGpuClockMHz))
    _hgmlCheckReturn(ret)


def hgmlDeviceResetGpuLockedClocks(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceResetGpuLockedClocks")
    ret = fn(handle)
    _hgmlCheckReturn(ret)


def hgmlDeviceSetMemoryLockedClocks(handle, minMemClockMHz, maxMemClockMHz):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetMemoryLockedClocks")
    ret = fn(handle, c_uint(minMemClockMHz), c_uint(maxMemClockMHz))
    _hgmlCheckReturn(ret)


def hgmlDeviceResetMemoryLockedClocks(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceResetMemoryLockedClocks")
    ret = fn(handle)
    _hgmlCheckReturn(ret)


def hgmlDeviceGetClkMonStatus(handle, c_clkMonInfo=hgmlClkMonStatus_t()):
    isReference = type(c_clkMonInfo) is not hgmlClkMonStatus_t
    c_clkMonInfoRef = c_clkMonInfo if isReference else byref(c_clkMonInfo)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetClkMonStatus")
    ret = fn(handle, c_clkMonInfoRef)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS if isReference else c_clkMonInfo


# Added in 4.304
# Deprecated
def hgmlDeviceSetApplicationsClocks(handle, maxMemClockMHz, maxGraphicsClockMHz):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetApplicationsClocks")
    ret = fn(handle, c_uint(maxMemClockMHz), c_uint(maxGraphicsClockMHz))
    _hgmlCheckReturn(ret)


# Added in 4.304
# Deprecated
def hgmlDeviceResetApplicationsClocks(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceResetApplicationsClocks")
    ret = fn(handle)
    _hgmlCheckReturn(ret)


# Added in 4.304
def hgmlDeviceSetPowerManagementLimit(handle, limit):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetPowerManagementLimit")
    ret = fn(handle, c_uint(limit))
    _hgmlCheckReturn(ret)


# Added in 4.304
def hgmlDeviceSetGpuOperationMode(handle, mode):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetGpuOperationMode")
    ret = fn(handle, _hgmlGpuOperationMode_t(mode))
    _hgmlCheckReturn(ret)


# Added in 2.285
def hgmlEventSetCreate():
    fn = _hgmlGetFunctionPointer("hgmlEventSetCreate")
    eventSet = c_hgmlEventSet_t()
    ret = fn(byref(eventSet))
    _hgmlCheckReturn(ret)
    return eventSet


# Added in 2.285
def hgmlDeviceRegisterEvents(handle, eventTypes, eventSet):
    fn = _hgmlGetFunctionPointer("hgmlDeviceRegisterEvents")
    ret = fn(handle, c_ulonglong(eventTypes), eventSet)
    _hgmlCheckReturn(ret)


# Added in 2.285
def hgmlDeviceGetSupportedEventTypes(handle):
    c_eventTypes = c_ulonglong()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetSupportedEventTypes")
    ret = fn(handle, byref(c_eventTypes))
    _hgmlCheckReturn(ret)
    return c_eventTypes.value


# raises HGML_ERROR_TIMEOUT exception on timeout
def hgmlEventSetWait_v2(eventSet, timeoutms):
    fn = _hgmlGetFunctionPointer("hgmlEventSetWait_v2")
    data = c_hgmlEventData_t()
    ret = fn(eventSet, byref(data), c_uint(timeoutms))
    _hgmlCheckReturn(ret)
    return data


def hgmlEventSetWait(eventSet, timeoutms):
    return hgmlEventSetWait_v2(eventSet, timeoutms)


# Added in 2.285
def hgmlEventSetFree(eventSet):
    fn = _hgmlGetFunctionPointer("hgmlEventSetFree")
    ret = fn(eventSet)
    _hgmlCheckReturn(ret)


# Added in 3.295
def hgmlDeviceOnSameBoard(handle1, handle2):
    fn = _hgmlGetFunctionPointer("hgmlDeviceOnSameBoard")
    onSameBoard = c_int()
    ret = fn(handle1, handle2, byref(onSameBoard))
    _hgmlCheckReturn(ret)
    return onSameBoard.value != 0


# Added in 3.295
def hgmlDeviceGetCurrPcieLinkGeneration(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetCurrPcieLinkGeneration")
    gen = c_uint()
    ret = fn(handle, byref(gen))
    _hgmlCheckReturn(ret)
    return gen.value


# Added in 3.295
def hgmlDeviceGetMaxPcieLinkGeneration(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMaxPcieLinkGeneration")
    gen = c_uint()
    ret = fn(handle, byref(gen))
    _hgmlCheckReturn(ret)
    return gen.value


# Added in 3.295
def hgmlDeviceGetCurrPcieLinkWidth(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetCurrPcieLinkWidth")
    width = c_uint()
    ret = fn(handle, byref(width))
    _hgmlCheckReturn(ret)
    return width.value


# Added in 3.295
def hgmlDeviceGetMaxPcieLinkWidth(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMaxPcieLinkWidth")
    width = c_uint()
    ret = fn(handle, byref(width))
    _hgmlCheckReturn(ret)
    return width.value


def hgmlDeviceGetGpuMaxPcieLinkGeneration(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGpuMaxPcieLinkGeneration")
    gen = c_uint()
    ret = fn(handle, byref(gen))
    _hgmlCheckReturn(ret)
    return gen.value


# Added in 4.304
# Deprecated
def hgmlDeviceGetSupportedClocksThrottleReasons(handle):
    c_reasons = c_ulonglong()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetSupportedClocksThrottleReasons")
    ret = fn(handle, byref(c_reasons))
    _hgmlCheckReturn(ret)
    return c_reasons.value


def hgmlDeviceGetSupportedClocksEventReasons(handle):
    c_reasons = c_ulonglong()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetSupportedClocksEventReasons")
    ret = fn(handle, byref(c_reasons))
    _hgmlCheckReturn(ret)
    return c_reasons.value


# Added in 4.304
# Deprecated
def hgmlDeviceGetCurrentClocksThrottleReasons(handle):
    c_reasons = c_ulonglong()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetCurrentClocksThrottleReasons")
    ret = fn(handle, byref(c_reasons))
    _hgmlCheckReturn(ret)
    return c_reasons.value


def hgmlDeviceGetCurrentClocksEventReasons(handle):
    c_reasons = c_ulonglong()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetCurrentClocksEventReasons")
    ret = fn(handle, byref(c_reasons))
    _hgmlCheckReturn(ret)
    return c_reasons.value


# Added in 5.319
def hgmlDeviceGetIndex(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetIndex")
    c_index = c_uint()
    ret = fn(handle, byref(c_index))
    _hgmlCheckReturn(ret)
    return c_index.value


# Added in 5.319
def hgmlDeviceGetAccountingMode(handle):
    c_mode = _hgmlEnableState_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetAccountingMode")
    ret = fn(handle, byref(c_mode))
    _hgmlCheckReturn(ret)
    return c_mode.value


def hgmlDeviceSetAccountingMode(handle, mode):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetAccountingMode")
    ret = fn(handle, _hgmlEnableState_t(mode))
    _hgmlCheckReturn(ret)


def hgmlDeviceClearAccountingPids(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceClearAccountingPids")
    ret = fn(handle)
    _hgmlCheckReturn(ret)


def hgmlDeviceGetAccountingStats(handle, pid):
    stats = c_hgmlAccountingStats_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetAccountingStats")
    ret = fn(handle, c_uint(pid), byref(stats))
    _hgmlCheckReturn(ret)
    if stats.maxMemoryUsage == HGML_VALUE_NOT_AVAILABLE_ulonglong.value:
        # special case for WDDM on Windows, see comment above
        stats.maxMemoryUsage = None
    return stats


def hgmlDeviceGetAccountingPids(handle):
    count = c_uint(hgmlDeviceGetAccountingBufferSize(handle))
    pids = (c_uint * count.value)()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetAccountingPids")
    ret = fn(handle, byref(count), pids)
    _hgmlCheckReturn(ret)
    return list(map(int, pids[0 : count.value]))


def hgmlDeviceGetAccountingBufferSize(handle):
    bufferSize = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetAccountingBufferSize")
    ret = fn(handle, byref(bufferSize))
    _hgmlCheckReturn(ret)
    return int(bufferSize.value)


def hgmlDeviceGetRetiredPages(device, sourceFilter):
    c_source = _hgmlPageRetirementCause_t(sourceFilter)
    c_count = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetRetiredPages")

    # First call will get the size
    ret = fn(device, c_source, byref(c_count), None)

    # this should only fail with insufficient size
    if ret not in (HGML_SUCCESS, HGML_ERROR_INSUFFICIENT_SIZE):
        raise HGMLError(ret)

    # call again with a buffer
    # oversize the array for the rare cases where additional pages
    # are retired between HGML calls
    c_count.value = c_count.value * 2 + 5
    page_array = c_ulonglong * c_count.value
    c_pages = page_array()
    ret = fn(device, c_source, byref(c_count), c_pages)
    _hgmlCheckReturn(ret)
    return list(map(int, c_pages[0 : c_count.value]))


def hgmlDeviceGetRetiredPages_v2(device, sourceFilter):
    c_source = _hgmlPageRetirementCause_t(sourceFilter)
    c_count = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetRetiredPages_v2")

    # First call will get the size
    ret = fn(device, c_source, byref(c_count), None)

    # this should only fail with insufficient size
    if ret not in (HGML_SUCCESS, HGML_ERROR_INSUFFICIENT_SIZE):
        raise HGMLError(ret)

    # call again with a buffer
    # oversize the array for the rare cases where additional pages
    # are retired between HGML calls
    c_count.value = c_count.value * 2 + 5
    page_array = c_ulonglong * c_count.value
    c_pages = page_array()
    times_array = c_ulonglong * c_count.value
    c_times = times_array()
    ret = fn(device, c_source, byref(c_count), c_pages, c_times)
    _hgmlCheckReturn(ret)
    return [
        {"address": int(c_pages[i]), "timestamp": int(c_times[i])}
        for i in range(c_count.value)
    ]


def hgmlDeviceGetRetiredPagesPendingStatus(device):
    c_pending = _hgmlEnableState_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetRetiredPagesPendingStatus")
    ret = fn(device, byref(c_pending))
    _hgmlCheckReturn(ret)
    return int(c_pending.value)


def hgmlDeviceGetAPIRestriction(device, apiType):
    c_permission = _hgmlEnableState_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetAPIRestriction")
    ret = fn(device, _hgmlRestrictedAPI_t(apiType), byref(c_permission))
    _hgmlCheckReturn(ret)
    return int(c_permission.value)


def hgmlDeviceSetAPIRestriction(handle, apiType, isRestricted):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetAPIRestriction")
    ret = fn(handle, _hgmlRestrictedAPI_t(apiType), _hgmlEnableState_t(isRestricted))
    _hgmlCheckReturn(ret)


def hgmlDeviceGetBridgeChipInfo(handle):
    bridgeHierarchy = c_hgmlBridgeChipHierarchy_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetBridgeChipInfo")
    ret = fn(handle, byref(bridgeHierarchy))
    _hgmlCheckReturn(ret)
    return bridgeHierarchy


def hgmlDeviceGetSamples(device, sampling_type, timeStamp):
    c_sampling_type = _hgmlSamplingType_t(sampling_type)
    c_time_stamp = c_ulonglong(timeStamp)
    c_sample_count = c_uint(0)
    c_sample_value_type = _hgmlValueType_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetSamples")

    ## First Call gets the size
    ret = fn(
        device,
        c_sampling_type,
        c_time_stamp,
        byref(c_sample_value_type),
        byref(c_sample_count),
        None,
    )

    # Stop if this fails
    if ret != HGML_SUCCESS:
        raise HGMLError(ret)

    sampleArray = c_sample_count.value * c_hgmlSample_t
    c_samples = sampleArray()
    ret = fn(
        device,
        c_sampling_type,
        c_time_stamp,
        byref(c_sample_value_type),
        byref(c_sample_count),
        c_samples,
    )
    _hgmlCheckReturn(ret)
    return (c_sample_value_type.value, c_samples[0 : c_sample_count.value])


# Deprecated
def hgmlDeviceGetViolationStatus(device, perfPolicyType):
    c_perfPolicy_type = _hgmlPerfPolicyType_t(perfPolicyType)
    c_violTime = c_hgmlViolationTime_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetViolationStatus")

    ## Invoke the method to get violation time
    ret = fn(device, c_perfPolicy_type, byref(c_violTime))
    _hgmlCheckReturn(ret)
    return c_violTime


def hgmlDeviceGetPcieThroughput(device, counter):
    c_util = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPcieThroughput")
    ret = fn(device, _hgmlPcieUtilCounter_t(counter), byref(c_util))
    _hgmlCheckReturn(ret)
    return c_util.value


def hgmlSystemGetTopologyGpuSet(cpuNumber):
    c_count = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlSystemGetTopologyGpuSet")

    # First call will get the size
    ret = fn(cpuNumber, byref(c_count), None)

    if ret != HGML_SUCCESS:
        raise HGMLError(ret)
    # call again with a buffer
    device_array = c_hgmlDevice_t * c_count.value
    c_devices = device_array()
    ret = fn(cpuNumber, byref(c_count), c_devices)
    _hgmlCheckReturn(ret)
    return list(c_devices[0 : c_count.value])


def hgmlDeviceGetTopologyNearestGpus(device, level):
    c_count = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetTopologyNearestGpus")

    # First call will get the size
    ret = fn(device, level, byref(c_count), None)

    if ret != HGML_SUCCESS:
        raise HGMLError(ret)

    # call again with a buffer
    device_array = c_hgmlDevice_t * c_count.value
    c_devices = device_array()
    ret = fn(device, level, byref(c_count), c_devices)
    _hgmlCheckReturn(ret)
    return list(c_devices[0 : c_count.value])


def hgmlDeviceGetTopologyCommonAncestor(device1, device2):
    c_level = _hgmlGpuTopologyLevel_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetTopologyCommonAncestor")
    ret = fn(device1, device2, byref(c_level))
    _hgmlCheckReturn(ret)
    return c_level.value


# Deprecated
def hgmlDeviceGetIcnLinkUtilizationCounter(device, link, counter):
    c_rxcounter = c_ulonglong()
    c_txcounter = c_ulonglong()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetIcnLinkUtilizationCounter")
    ret = fn(device, link, counter, byref(c_rxcounter), byref(c_txcounter))
    _hgmlCheckReturn(ret)
    return (c_rxcounter.value, c_txcounter.value)


# Deprecated
def hgmlDeviceFreezeIcnLinkUtilizationCounter(device, link, counter, freeze):
    fn = _hgmlGetFunctionPointer("hgmlDeviceFreezeIcnLinkUtilizationCounter")
    ret = fn(device, link, counter, freeze)
    _hgmlCheckReturn(ret)


# Deprecated
def hgmlDeviceResetIcnLinkUtilizationCounter(device, link, counter):
    fn = _hgmlGetFunctionPointer("hgmlDeviceResetIcnLinkUtilizationCounter")
    ret = fn(device, link, counter)
    _hgmlCheckReturn(ret)


# Deprecated
def hgmlDeviceSetIcnLinkUtilizationControl(device, link, counter, control, reset):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetIcnLinkUtilizationControl")
    ret = fn(device, link, counter, byref(control), reset)
    _hgmlCheckReturn(ret)


# Deprecated
def hgmlDeviceGetIcnLinkUtilizationControl(device, link, counter):
    c_control = hgmlIcnLinkUtilizationControl_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetIcnLinkUtilizationControl")
    ret = fn(device, link, counter, byref(c_control))
    _hgmlCheckReturn(ret)
    return c_control


def hgmlDeviceGetIcnLinkCapability(device, link, capability):
    c_capResult = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetIcnLinkCapability")
    ret = fn(device, link, capability, byref(c_capResult))
    _hgmlCheckReturn(ret)
    return c_capResult.value


def hgmlDeviceGetIcnLinkErrorCounter(device, link, counter):
    c_result = c_ulonglong()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetIcnLinkErrorCounter")
    ret = fn(device, link, counter, byref(c_result))
    _hgmlCheckReturn(ret)
    return c_result.value


def hgmlDeviceResetIcnLinkErrorCounters(device, link):
    fn = _hgmlGetFunctionPointer("hgmlDeviceResetIcnLinkErrorCounters")
    ret = fn(device, link)
    _hgmlCheckReturn(ret)


def hgmlDeviceGetIcnLinkRemotePciInfo(device, link):
    c_pci = hgmlPciInfo_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetIcnLinkRemotePciInfo_v2")
    ret = fn(device, link, byref(c_pci))
    _hgmlCheckReturn(ret)
    return c_pci


def hgmlDeviceGetIcnLinkRemoteDeviceType(handle, link):
    c_type = _hgmlIcnLinkDeviceType_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetIcnLinkRemoteDeviceType")
    ret = fn(handle, link, byref(c_type))
    _hgmlCheckReturn(ret)
    return c_type.value


def hgmlDeviceGetIcnLinkState(device, link):
    c_isActive = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetIcnLinkState")
    ret = fn(device, link, byref(c_isActive))
    _hgmlCheckReturn(ret)
    return c_isActive.value


def hgmlDeviceGetIcnLinkVersion(device, link):
    c_version = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetIcnLinkVersion")
    ret = fn(device, link, byref(c_version))
    _hgmlCheckReturn(ret)
    return c_version.value


def hgmlDeviceModifyDrainState(pciInfo, newState):
    fn = _hgmlGetFunctionPointer("hgmlDeviceModifyDrainState")
    ret = fn(pointer(pciInfo), newState)
    _hgmlCheckReturn(ret)


def hgmlDeviceQueryDrainState(pciInfo):
    c_newState = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceQueryDrainState")
    ret = fn(pointer(pciInfo), byref(c_newState))
    _hgmlCheckReturn(ret)
    return c_newState.value


def hgmlDeviceRemoveGpu(pciInfo):
    fn = _hgmlGetFunctionPointer("hgmlDeviceRemoveGpu")
    ret = fn(pointer(pciInfo))
    _hgmlCheckReturn(ret)


def hgmlDeviceDiscoverGpus(pciInfo):
    fn = _hgmlGetFunctionPointer("hgmlDeviceDiscoverGpus")
    ret = fn(pointer(pciInfo))
    _hgmlCheckReturn(ret)


def hgmlDeviceGetFieldValues(handle, fieldIds):
    values_arr = c_hgmlFieldValue_t * len(fieldIds)
    values = values_arr()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetFieldValues")

    for i, fieldId in enumerate(fieldIds):
        try:
            (values[i].fieldId, values[i].scopeId) = fieldId
        except TypeError:
            values[i].fieldId = fieldId

    ret = fn(handle, c_int32(len(fieldIds)), byref(values))
    _hgmlCheckReturn(ret)
    return values


def hgmlDeviceClearFieldValues(handle, fieldIds):
    values_arr = c_hgmlFieldValue_t * len(fieldIds)
    values = values_arr()
    fn = _hgmlGetFunctionPointer("hgmlDeviceClearFieldValues")

    for i, fieldId in enumerate(fieldIds):
        try:
            (values[i].fieldId, values[i].scopeId) = fieldId
        except TypeError:
            values[i].fieldId = fieldId

    ret = fn(handle, c_int32(len(fieldIds)), byref(values))
    _hgmlCheckReturn(ret)
    return values


def hgmlDeviceGetVirtualizationMode(handle):
    c_virtualization_mode = c_ulonglong()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetVirtualizationMode")
    ret = fn(handle, byref(c_virtualization_mode))
    _hgmlCheckReturn(ret)
    return c_virtualization_mode.value


def hgmlDeviceSetVirtualizationMode(handle, virtualization_mode):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetVirtualizationMode")
    return fn(handle, virtualization_mode)


def hgmlGetVgpuDriverCapabilities(capability):
    c_capResult = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlGetVgpuDriverCapabilities")
    ret = fn(_hgmlVgpuDriverCapability_t(capability), byref(c_capResult))
    _hgmlCheckReturn(ret)
    return c_capResult.value


def hgmlDeviceGetVgpuCapabilities(handle, capability):
    c_capResult = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetVgpuCapabilities")
    ret = fn(handle, _hgmlDeviceVgpuCapability_t(capability), byref(c_capResult))
    _hgmlCheckReturn(ret)
    return c_capResult.value


def hgmlDeviceGetSupportedVgpus(handle):
    # first call to get the size
    c_vgpu_count = c_uint(0)

    fn = _hgmlGetFunctionPointer("hgmlDeviceGetSupportedVgpus")
    ret = fn(handle, byref(c_vgpu_count), None)

    if ret == HGML_SUCCESS:
        # special case, no supported vGPUs
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        vgpu_type_ids_array = _hgmlVgpuTypeId_t * c_vgpu_count.value
        c_vgpu_type_ids = vgpu_type_ids_array()

        # make the call again
        ret = fn(handle, byref(c_vgpu_count), c_vgpu_type_ids)
        _hgmlCheckReturn(ret)
        vgpus = []
        for i in range(c_vgpu_count.value):
            vgpus.append(c_vgpu_type_ids[i])
        return vgpus
    # error case
    raise HGMLError(ret)


def hgmlDeviceGetCreatableVgpus(handle):
    # first call to get the size
    c_vgpu_count = c_uint(0)

    fn = _hgmlGetFunctionPointer("hgmlDeviceGetCreatableVgpus")
    ret = fn(handle, byref(c_vgpu_count), None)

    if ret == HGML_SUCCESS:
        # special case, no supported vGPUs
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        vgpu_type_ids_array = _hgmlVgpuTypeId_t * c_vgpu_count.value
        c_vgpu_type_ids = vgpu_type_ids_array()

        # make the call again
        ret = fn(handle, byref(c_vgpu_count), c_vgpu_type_ids)
        _hgmlCheckReturn(ret)
        vgpus = []
        for i in range(c_vgpu_count.value):
            vgpus.append(c_vgpu_type_ids[i])
        return vgpus
    # error case
    raise HGMLError(ret)


def hgmlVgpuTypeGetGpuInstanceProfileId(vgpuTypeId):
    c_profile_id = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuTypeGetGpuInstanceProfileId")
    ret = fn(vgpuTypeId, byref(c_profile_id))
    _hgmlCheckReturn(ret)
    return c_profile_id.value


@convertStrBytes
def hgmlVgpuTypeGetClass(vgpuTypeId):
    c_class = create_string_buffer(HGML_DEVICE_NAME_BUFFER_SIZE)
    c_buffer_size = c_uint(HGML_DEVICE_NAME_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlVgpuTypeGetClass")
    ret = fn(vgpuTypeId, c_class, byref(c_buffer_size))
    _hgmlCheckReturn(ret)
    return c_class.value


@convertStrBytes
def hgmlVgpuTypeGetName(vgpuTypeId):
    c_name = create_string_buffer(HGML_DEVICE_NAME_BUFFER_SIZE)
    c_buffer_size = c_uint(HGML_DEVICE_NAME_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlVgpuTypeGetName")
    ret = fn(vgpuTypeId, c_name, byref(c_buffer_size))
    _hgmlCheckReturn(ret)
    return c_name.value


def hgmlVgpuTypeGetDeviceID(vgpuTypeId):
    c_device_id = c_ulonglong(0)
    c_subsystem_id = c_ulonglong(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuTypeGetDeviceID")
    ret = fn(vgpuTypeId, byref(c_device_id), byref(c_subsystem_id))
    _hgmlCheckReturn(ret)
    return (c_device_id.value, c_subsystem_id.value)


def hgmlVgpuTypeGetFramebufferSize(vgpuTypeId):
    c_fb_size = c_ulonglong(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuTypeGetFramebufferSize")
    ret = fn(vgpuTypeId, byref(c_fb_size))
    _hgmlCheckReturn(ret)
    return c_fb_size.value


def hgmlVgpuTypeGetNumDisplayHeads(vgpuTypeId):
    c_num_heads = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuTypeGetNumDisplayHeads")
    ret = fn(vgpuTypeId, byref(c_num_heads))
    _hgmlCheckReturn(ret)
    return c_num_heads.value


def hgmlVgpuTypeGetResolution(vgpuTypeId):
    c_xdim = c_uint(0)
    c_ydim = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuTypeGetResolution")
    ret = fn(vgpuTypeId, 0, byref(c_xdim), byref(c_ydim))
    _hgmlCheckReturn(ret)
    return (c_xdim.value, c_ydim.value)


@convertStrBytes
def hgmlVgpuTypeGetLicense(vgpuTypeId):
    c_license = create_string_buffer(HGML_GRID_LICENSE_BUFFER_SIZE)
    c_buffer_size = c_uint(HGML_GRID_LICENSE_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlVgpuTypeGetLicense")
    ret = fn(vgpuTypeId, c_license, c_buffer_size)
    _hgmlCheckReturn(ret)
    return c_license.value


def hgmlVgpuTypeGetFrameRateLimit(vgpuTypeId):
    c_frl_config = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuTypeGetFrameRateLimit")
    ret = fn(vgpuTypeId, byref(c_frl_config))
    _hgmlCheckReturn(ret)
    return c_frl_config.value


def hgmlVgpuTypeGetMaxInstances(handle, vgpuTypeId):
    c_max_instances = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuTypeGetMaxInstances")
    ret = fn(handle, vgpuTypeId, byref(c_max_instances))
    _hgmlCheckReturn(ret)
    return c_max_instances.value


def hgmlVgpuTypeGetMaxInstancesPerVm(vgpuTypeId):
    c_max_instances_per_vm = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuTypeGetMaxInstancesPerVm")
    ret = fn(vgpuTypeId, byref(c_max_instances_per_vm))
    _hgmlCheckReturn(ret)
    return c_max_instances_per_vm.value


def hgmlDeviceGetActiveVgpus(handle):
    # first call to get the size
    c_vgpu_count = c_uint(0)

    fn = _hgmlGetFunctionPointer("hgmlDeviceGetActiveVgpus")
    ret = fn(handle, byref(c_vgpu_count), None)

    if ret == HGML_SUCCESS:
        # special case, no active vGPUs
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        vgpu_instance_array = _hgmlVgpuInstance_t * c_vgpu_count.value
        c_vgpu_instances = vgpu_instance_array()

        # make the call again
        ret = fn(handle, byref(c_vgpu_count), c_vgpu_instances)
        _hgmlCheckReturn(ret)
        vgpus = []
        for i in range(c_vgpu_count.value):
            vgpus.append(c_vgpu_instances[i])
        return vgpus
    # error case
    raise HGMLError(ret)


@convertStrBytes
def hgmlVgpuInstanceGetVmID(vgpuInstance):
    c_vm_id = create_string_buffer(HGML_DEVICE_UUID_BUFFER_SIZE)
    c_buffer_size = c_uint(HGML_GRID_LICENSE_BUFFER_SIZE)
    c_vm_id_type = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetVmID")
    ret = fn(vgpuInstance, byref(c_vm_id), c_buffer_size, byref(c_vm_id_type))
    _hgmlCheckReturn(ret)
    return (c_vm_id.value, c_vm_id_type.value)


@convertStrBytes
def hgmlVgpuInstanceGetUUID(vgpuInstance):
    c_uuid = create_string_buffer(HGML_DEVICE_UUID_BUFFER_SIZE)
    c_buffer_size = c_uint(HGML_DEVICE_UUID_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetUUID")
    ret = fn(vgpuInstance, byref(c_uuid), c_buffer_size)
    _hgmlCheckReturn(ret)
    return c_uuid.value


@convertStrBytes
def hgmlVgpuInstanceGetMdevUUID(vgpuInstance):
    c_uuid = create_string_buffer(HGML_DEVICE_UUID_BUFFER_SIZE)
    c_buffer_size = c_uint(HGML_DEVICE_UUID_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetMdevUUID")
    ret = fn(vgpuInstance, byref(c_uuid), c_buffer_size)
    _hgmlCheckReturn(ret)
    return c_uuid.value


@convertStrBytes
def hgmlVgpuInstanceGetVmDriverVersion(vgpuInstance):
    c_driver_version = create_string_buffer(HGML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE)
    c_buffer_size = c_uint(HGML_SYSTEM_DRIVER_VERSION_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetVmDriverVersion")
    ret = fn(vgpuInstance, byref(c_driver_version), c_buffer_size)
    _hgmlCheckReturn(ret)
    return c_driver_version.value


# Deprecated
def hgmlVgpuInstanceGetLicenseStatus(vgpuInstance):
    c_license_status = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetLicenseStatus")
    ret = fn(vgpuInstance, byref(c_license_status))
    _hgmlCheckReturn(ret)
    return c_license_status.value


def hgmlVgpuInstanceGetLicenseInfo_v2(vgpuInstance):
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetLicenseInfo_v2")
    c_license_info = c_hgmlVgpuLicenseInfo_t()
    ret = fn(vgpuInstance, byref(c_license_info))
    _hgmlCheckReturn(ret)
    return c_license_info


def hgmlVgpuInstanceGetLicenseInfo(vgpuInstance):
    return hgmlVgpuInstanceGetLicenseInfo_v2(vgpuInstance)


def hgmlVgpuInstanceGetFrameRateLimit(vgpuInstance):
    c_frl = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetFrameRateLimit")
    ret = fn(vgpuInstance, byref(c_frl))
    _hgmlCheckReturn(ret)
    return c_frl.value


def hgmlVgpuInstanceGetEccMode(vgpuInstance):
    c_mode = _hgmlEnableState_t()
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetEccMode")
    ret = fn(vgpuInstance, byref(c_mode))
    _hgmlCheckReturn(ret)
    return c_mode.value


def hgmlVgpuInstanceGetType(vgpuInstance):
    c_vgpu_type = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetType")
    ret = fn(vgpuInstance, byref(c_vgpu_type))
    _hgmlCheckReturn(ret)
    return c_vgpu_type.value


def hgmlVgpuInstanceGetEncoderCapacity(vgpuInstance):
    c_encoder_capacity = c_ulonglong(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetEncoderCapacity")
    ret = fn(vgpuInstance, byref(c_encoder_capacity))
    _hgmlCheckReturn(ret)
    return c_encoder_capacity.value


def hgmlVgpuInstanceSetEncoderCapacity(vgpuInstance, encoder_capacity):
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceSetEncoderCapacity")
    return fn(vgpuInstance, encoder_capacity)


def hgmlVgpuInstanceGetFbUsage(vgpuInstance):
    c_fb_usage = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetFbUsage")
    ret = fn(vgpuInstance, byref(c_fb_usage))
    _hgmlCheckReturn(ret)
    return c_fb_usage.value


def hgmlVgpuTypeGetCapabilities(vgpuTypeId, capability):
    c_cap_result = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuTypeGetCapabilities")
    ret = fn(vgpuTypeId, _hgmlVgpuCapability_t(capability), byref(c_cap_result))
    _hgmlCheckReturn(ret)
    return c_cap_result.value


def hgmlVgpuInstanceGetGpuInstanceId(vgpuInstance):
    c_id = c_uint(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetGpuInstanceId")
    ret = fn(vgpuInstance, byref(c_id))
    _hgmlCheckReturn(ret)
    return c_id.value


@convertStrBytes
def hgmlVgpuInstanceGetGpuPciId(vgpuInstance):
    c_vgpuPciId = create_string_buffer(HGML_DEVICE_PCI_BUS_ID_BUFFER_SIZE)
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetGpuPciId")
    ret = fn(
        vgpuInstance, c_vgpuPciId, byref(c_uint(HGML_DEVICE_PCI_BUS_ID_BUFFER_SIZE))
    )
    _hgmlCheckReturn(ret)
    return c_vgpuPciId.value


def hgmlDeviceGetVgpuUtilization(handle, timeStamp):
    # first call to get the size
    c_vgpu_count = c_uint(0)
    c_time_stamp = c_ulonglong(timeStamp)
    c_sample_value_type = _hgmlValueType_t()

    fn = _hgmlGetFunctionPointer("hgmlDeviceGetVgpuUtilization")
    ret = fn(
        handle, c_time_stamp, byref(c_sample_value_type), byref(c_vgpu_count), None
    )

    if ret == HGML_SUCCESS:
        # special case, no active vGPUs
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        sampleArray = c_vgpu_count.value * c_hgmlVgpuInstanceUtilizationSample_t
        c_samples = sampleArray()

        # make the call again
        ret = fn(
            handle,
            c_time_stamp,
            byref(c_sample_value_type),
            byref(c_vgpu_count),
            c_samples,
        )
        _hgmlCheckReturn(ret)

        return c_samples[0 : c_vgpu_count.value]
    # error case
    raise HGMLError(ret)


def hgmlDeviceGetP2PStatus(device1, device2, p2pIndex):
    c_p2pstatus = _hgmlGpuP2PStatus_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetP2PStatus")
    ret = fn(device1, device2, p2pIndex, byref(c_p2pstatus))
    _hgmlCheckReturn(ret)
    return c_p2pstatus.value


def hgmlDeviceGetGridLicensableFeatures_v4(handle):
    c_get_grid_licensable_features = c_hgmlGridLicensableFeatures_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGridLicensableFeatures_v4")
    ret = fn(handle, byref(c_get_grid_licensable_features))
    _hgmlCheckReturn(ret)

    return c_get_grid_licensable_features


def hgmlDeviceGetGridLicensableFeatures(handle):
    return hgmlDeviceGetGridLicensableFeatures_v4(handle)


def hgmlDeviceGetGspFirmwareVersion(handle, version=None):
    isUserDefined = version is not None
    if not isUserDefined:
        version = (c_char * HGML_GSP_FIRMWARE_VERSION_BUF_SIZE)()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGspFirmwareVersion")
    ret = fn(handle, version)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS if isUserDefined else version.value


def hgmlDeviceGetGspFirmwareMode(handle, isEnabled=c_uint(), defaultMode=c_uint()):
    isReference = type(isEnabled) is not c_uint
    isEnabledRef = isEnabled if isReference else byref(isEnabled)
    defaultModeRef = defaultMode if isReference else byref(defaultMode)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGspFirmwareMode")
    ret = fn(handle, isEnabledRef, defaultModeRef)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS if isReference else [isEnabled.value, defaultMode.value]


def hgmlDeviceGetEncoderCapacity(handle, encoderQueryType):
    c_encoder_capacity = c_ulonglong(0)
    c_encoderQuery_type = _hgmlEncoderQueryType_t(encoderQueryType)

    fn = _hgmlGetFunctionPointer("hgmlDeviceGetEncoderCapacity")
    ret = fn(handle, c_encoderQuery_type, byref(c_encoder_capacity))
    _hgmlCheckReturn(ret)
    return c_encoder_capacity.value


def hgmlDeviceGetVgpuProcessUtilization(handle, timeStamp):
    # first call to get the size
    c_vgpu_count = c_uint(0)
    c_time_stamp = c_ulonglong(timeStamp)

    fn = _hgmlGetFunctionPointer("hgmlDeviceGetVgpuProcessUtilization")
    ret = fn(handle, c_time_stamp, byref(c_vgpu_count), None)

    if ret == HGML_SUCCESS:
        # special case, no active vGPUs
        return []
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        sampleArray = c_vgpu_count.value * c_hgmlVgpuProcessUtilizationSample_t
        c_samples = sampleArray()

        # make the call again
        ret = fn(handle, c_time_stamp, byref(c_vgpu_count), c_samples)
        _hgmlCheckReturn(ret)

        return c_samples[0 : c_vgpu_count.value]
    # error case
    raise HGMLError(ret)


def hgmlDeviceGetEncoderStats(handle):
    c_encoderCount = c_ulonglong(0)
    c_encodeFps = c_ulonglong(0)
    c_encoderLatency = c_ulonglong(0)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetEncoderStats")
    ret = fn(handle, byref(c_encoderCount), byref(c_encodeFps), byref(c_encoderLatency))
    _hgmlCheckReturn(ret)
    return (c_encoderCount.value, c_encodeFps.value, c_encoderLatency.value)


def hgmlDeviceGetEncoderSessions(handle):
    # first call to get the size
    c_session_count = c_uint(0)

    fn = _hgmlGetFunctionPointer("hgmlDeviceGetEncoderSessions")
    ret = fn(handle, byref(c_session_count), None)

    if ret == HGML_SUCCESS:
        if c_session_count.value != 0:
            # typical case
            session_array = c_hgmlEncoderSession_t * c_session_count.value
            c_sessions = session_array()

            # make the call again
            ret = fn(handle, byref(c_session_count), c_sessions)
            _hgmlCheckReturn(ret)
            sessions = []
            for i in range(c_session_count.value):
                sessions.append(c_sessions[i])
            return sessions
        return []  # no active sessions
    # error case
    raise HGMLError(ret)


def hgmlDeviceGetFBCStats(handle):
    c_fbcStats = c_hgmlFBCStats_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetFBCStats")
    ret = fn(handle, byref(c_fbcStats))
    _hgmlCheckReturn(ret)
    return c_fbcStats


def hgmlDeviceGetFBCSessions(handle):
    # first call to get the size
    c_session_count = c_uint(0)

    fn = _hgmlGetFunctionPointer("hgmlDeviceGetFBCSessions")
    ret = fn(handle, byref(c_session_count), None)

    if ret == HGML_SUCCESS:
        if c_session_count.value != 0:
            # typical case
            session_array = c_hgmlFBCSession_t * c_session_count.value
            c_sessions = session_array()

            # make the call again
            ret = fn(handle, byref(c_session_count), c_sessions)
            _hgmlCheckReturn(ret)
            sessions = []
            for i in range(c_session_count.value):
                sessions.append(c_sessions[i])
            return sessions
        return []  # no active sessions
    # error case
    raise HGMLError(ret)


def hgmlVgpuInstanceGetEncoderStats(vgpuInstance):
    c_encoderCount = c_ulonglong(0)
    c_encodeFps = c_ulonglong(0)
    c_encoderLatency = c_ulonglong(0)
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetEncoderStats")
    ret = fn(
        vgpuInstance, byref(c_encoderCount), byref(c_encodeFps), byref(c_encoderLatency)
    )
    _hgmlCheckReturn(ret)
    return (c_encoderCount.value, c_encodeFps.value, c_encoderLatency.value)


def hgmlVgpuInstanceGetEncoderSessions(vgpuInstance):
    # first call to get the size
    c_session_count = c_uint(0)

    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetEncoderSessions")
    ret = fn(vgpuInstance, byref(c_session_count), None)

    if ret == HGML_SUCCESS:
        if c_session_count.value != 0:
            # typical case
            session_array = c_hgmlEncoderSession_t * c_session_count.value
            c_sessions = session_array()

            # make the call again
            ret = fn(vgpuInstance, byref(c_session_count), c_sessions)
            _hgmlCheckReturn(ret)
            sessions = []
            for i in range(c_session_count.value):
                sessions.append(c_sessions[i])
            return sessions
        return []  # no active sessions
    # error case
    raise HGMLError(ret)


def hgmlVgpuInstanceGetFBCStats(vgpuInstance):
    c_fbcStats = c_hgmlFBCStats_t()
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetFBCStats")
    ret = fn(vgpuInstance, byref(c_fbcStats))
    _hgmlCheckReturn(ret)
    return c_fbcStats


def hgmlVgpuInstanceGetFBCSessions(vgpuInstance):
    # first call to get the size
    c_session_count = c_uint(0)

    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetFBCSessions")
    ret = fn(vgpuInstance, byref(c_session_count), None)

    if ret == HGML_SUCCESS:
        if c_session_count.value != 0:
            # typical case
            session_array = c_hgmlFBCSession_t * c_session_count.value
            c_sessions = session_array()

            # make the call again
            ret = fn(vgpuInstance, byref(c_session_count), c_sessions)
            _hgmlCheckReturn(ret)
            sessions = []
            for i in range(c_session_count.value):
                sessions.append(c_sessions[i])
            return sessions
        return []  # no active sessions
    # error case
    raise HGMLError(ret)


def hgmlDeviceGetProcessUtilization(handle, timeStamp):
    # first call to get the size
    c_count = c_uint(0)
    c_time_stamp = c_ulonglong(timeStamp)

    fn = _hgmlGetFunctionPointer("hgmlDeviceGetProcessUtilization")
    ret = fn(handle, None, byref(c_count), c_time_stamp)

    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        # typical case
        sampleArray = c_count.value * c_hgmlProcessUtilizationSample_t
        c_samples = sampleArray()

        # make the call again
        ret = fn(handle, c_samples, byref(c_count), c_time_stamp)
        _hgmlCheckReturn(ret)

        return c_samples[0 : c_count.value]
    # error case
    raise HGMLError(ret)


def hgmlVgpuInstanceGetMetadata(vgpuInstance):
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetMetadata")
    c_vgpuMetadata = c_hgmlVgpuMetadata_t()
    c_bufferSize = c_uint(0)
    # Make the first HGML API call to get the c_bufferSize value.
    # We have already allocated required buffer above.
    ret = fn(vgpuInstance, byref(c_vgpuMetadata), byref(c_bufferSize))
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        ret = fn(vgpuInstance, byref(c_vgpuMetadata), byref(c_bufferSize))
        _hgmlCheckReturn(ret)
    else:
        raise HGMLError(ret)
    return c_vgpuMetadata


def hgmlDeviceGetVgpuMetadata(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetVgpuMetadata")
    c_vgpuPgpuMetadata = c_hgmlVgpuPgpuMetadata_t()
    c_bufferSize = c_uint(0)
    # Make the first HGML API call to get the c_bufferSize value.
    # We have already allocated required buffer above.
    ret = fn(handle, byref(c_vgpuPgpuMetadata), byref(c_bufferSize))
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        ret = fn(handle, byref(c_vgpuPgpuMetadata), byref(c_bufferSize))
        _hgmlCheckReturn(ret)
    else:
        raise HGMLError(ret)
    return c_vgpuPgpuMetadata


def hgmlGetVgpuCompatibility(vgpuMetadata, pgpuMetadata):
    fn = _hgmlGetFunctionPointer("hgmlGetVgpuCompatibility")
    c_vgpuPgpuCompatibility = c_hgmlVgpuPgpuCompatibility_t()
    ret = fn(byref(vgpuMetadata), byref(pgpuMetadata), byref(c_vgpuPgpuCompatibility))
    _hgmlCheckReturn(ret)
    return c_vgpuPgpuCompatibility


@convertStrBytes
def hgmlDeviceGetPgpuMetadataString(handle):
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPgpuMetadataString")
    c_pgpuMetadata = create_string_buffer(HGML_VGPU_PGPU_METADATA_OPAQUE_DATA_SIZE)
    c_bufferSize = c_uint(0)
    # Make the first HGML API call to get the c_bufferSize value.
    # We have already allocated required buffer above.
    ret = fn(handle, byref(c_pgpuMetadata), byref(c_bufferSize))
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        ret = fn(handle, byref(c_pgpuMetadata), byref(c_bufferSize))
        _hgmlCheckReturn(ret)
    else:
        raise HGMLError(ret)
    return (c_pgpuMetadata.value, c_bufferSize.value)


def hgmlDeviceGetVgpuSchedulerLog(handle):
    c_vgpu_sched_log = c_hgmlVgpuSchedulerLog_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetVgpuSchedulerLog")
    ret = fn(handle, byref(c_vgpu_sched_log))
    _hgmlCheckReturn(ret)
    return c_vgpu_sched_log


def hgmlDeviceGetVgpuSchedulerState(handle):
    c_vgpu_sched_state = c_hgmlVgpuSchedulerGetState_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetVgpuSchedulerState")
    ret = fn(handle, byref(c_vgpu_sched_state))
    _hgmlCheckReturn(ret)
    return c_vgpu_sched_state


def hgmlDeviceGetVgpuSchedulerCapabilities(handle):
    c_vgpu_sched_caps = c_hgmlVgpuSchedulerCapabilities_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetVgpuSchedulerCapabilities")
    ret = fn(handle, byref(c_vgpu_sched_caps))
    _hgmlCheckReturn(ret)
    return c_vgpu_sched_caps


def hgmlDeviceSetVgpuSchedulerState(handle, sched_state):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetVgpuSchedulerState")
    ret = fn(handle, byref(sched_state))
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlSetVgpuVersion(vgpuVersion):
    fn = _hgmlGetFunctionPointer("hgmlSetVgpuVersion")
    ret = fn(byref(vgpuVersion))
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlGetVgpuVersion(supported=None, current=None):
    isUserDefined = (supported is not None) or (current is not None)
    if not isUserDefined:
        supported = c_hgmlVgpuVersion_t()
        current = c_hgmlVgpuVersion_t()
    fn = _hgmlGetFunctionPointer("hgmlGetVgpuVersion")
    ret = fn(byref(supported), byref(current))
    _hgmlCheckReturn(ret)
    return (
        HGML_SUCCESS
        if isUserDefined
        else [
            (supported.minVersion, supported.maxVersion),
            (current.minVersion, current.maxVersion),
        ]
    )


def hgmlVgpuInstanceGetAccountingMode(vgpuInstance):
    c_mode = _hgmlEnableState_t()
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetAccountingMode")
    ret = fn(vgpuInstance, byref(c_mode))
    _hgmlCheckReturn(ret)
    return c_mode.value


def hgmlVgpuInstanceGetAccountingPids(vgpuInstance):
    c_pidCount = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetAccountingPids")
    ret = fn(vgpuInstance, byref(c_pidCount), None)
    if ret == HGML_ERROR_INSUFFICIENT_SIZE:
        sampleArray = c_pidCount.value * c_uint
        c_pidArray = sampleArray()
        ret = fn(vgpuInstance, byref(c_pidCount), byref(c_pidArray))
        _hgmlCheckReturn(ret)
    else:
        raise HGMLError(ret)
    return (c_pidCount, c_pidArray)


def hgmlVgpuInstanceGetAccountingStats(vgpuInstance, pid):
    c_accountingStats = c_hgmlAccountingStats_t()
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceGetAccountingStats")
    ret = fn(vgpuInstance, pid, byref(c_accountingStats))
    _hgmlCheckReturn(ret)
    return c_accountingStats


def hgmlVgpuInstanceClearAccountingPids(vgpuInstance):
    fn = _hgmlGetFunctionPointer("hgmlVgpuInstanceClearAccountingPids")
    ret = fn(vgpuInstance)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlGetExcludedDeviceCount():
    c_count = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlGetExcludedDeviceCount")
    ret = fn(byref(c_count))
    _hgmlCheckReturn(ret)
    return c_count.value


def hgmlGetExcludedDeviceInfoByIndex(index):
    c_index = c_uint(index)
    info = c_hgmlExcludedDeviceInfo_t()
    fn = _hgmlGetFunctionPointer("hgmlGetExcludedDeviceInfoByIndex")
    ret = fn(c_index, byref(info))
    _hgmlCheckReturn(ret)
    return info


def hgmlDeviceGetHostVgpuMode(handle):
    c_host_vgpu_mode = _hgmlHostVgpuMode_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetHostVgpuMode")
    ret = fn(handle, byref(c_host_vgpu_mode))
    _hgmlCheckReturn(ret)
    return c_host_vgpu_mode.value


def hgmlDeviceSetMigMode(device, mode):
    c_activationStatus = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetMigMode")
    ret = fn(device, mode, byref(c_activationStatus))
    _hgmlCheckReturn(ret)
    return c_activationStatus.value


def hgmlDeviceGetMigMode(device):
    c_currentMode = c_uint()
    c_pendingMode = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMigMode")
    ret = fn(device, byref(c_currentMode), byref(c_pendingMode))
    _hgmlCheckReturn(ret)
    return [c_currentMode.value, c_pendingMode.value]


def hgmlDeviceGetGpuInstanceProfileInfo(device, profile, version=2):
    if version == 2:
        c_info = c_hgmlGpuInstanceProfileInfo_v2_t()
        fn = _hgmlGetFunctionPointer("hgmlDeviceGetGpuInstanceProfileInfoV")
    elif version == 1:
        c_info = c_hgmlGpuInstanceProfileInfo_t()
        fn = _hgmlGetFunctionPointer("hgmlDeviceGetGpuInstanceProfileInfo")
    else:
        raise HGMLError(HGML_ERROR_FUNCTION_NOT_FOUND)
    ret = fn(device, profile, byref(c_info))
    _hgmlCheckReturn(ret)
    return c_info


# Define function alias for the API exposed by HGML
hgmlDeviceGetGpuInstanceProfileInfoV = hgmlDeviceGetGpuInstanceProfileInfo


def hgmlDeviceGetGpuInstanceRemainingCapacity(device, profileId):
    c_count = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGpuInstanceRemainingCapacity")
    ret = fn(device, profileId, byref(c_count))
    _hgmlCheckReturn(ret)
    return c_count.value


def hgmlDeviceGetGpuInstancePossiblePlacements(
    device, profileId, placementsRef, countRef
):
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGpuInstancePossiblePlacements_v2")
    ret = fn(device, profileId, placementsRef, countRef)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlDeviceCreateGpuInstance(device, profileId):
    c_instance = c_hgmlGpuInstance_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceCreateGpuInstance")
    ret = fn(device, profileId, byref(c_instance))
    _hgmlCheckReturn(ret)
    return c_instance


def hgmlDeviceCreateGpuInstanceWithPlacement(device, profileId, placement):
    c_instance = c_hgmlGpuInstance_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceCreateGpuInstanceWithPlacement")
    ret = fn(device, profileId, placement, byref(c_instance))
    _hgmlCheckReturn(ret)
    return c_instance


def hgmlGpuInstanceDestroy(gpuInstance):
    fn = _hgmlGetFunctionPointer("hgmlGpuInstanceDestroy")
    ret = fn(gpuInstance)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlDeviceGetGpuInstances(device, profileId, gpuInstancesRef, countRef):
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGpuInstances")
    ret = fn(device, profileId, gpuInstancesRef, countRef)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlDeviceGetGpuInstanceById(device, gpuInstanceId):
    c_instance = c_hgmlGpuInstance_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGpuInstanceById")
    ret = fn(device, gpuInstanceId, byref(c_instance))
    _hgmlCheckReturn(ret)
    return c_instance


def hgmlGpuInstanceGetInfo(gpuInstance):
    c_info = c_hgmlGpuInstanceInfo_t()
    fn = _hgmlGetFunctionPointer("hgmlGpuInstanceGetInfo")
    ret = fn(gpuInstance, byref(c_info))
    _hgmlCheckReturn(ret)
    return c_info


def hgmlGpuInstanceGetComputeInstanceProfileInfo(
    device, profile, engProfile, version=2
):
    if version == 2:
        c_info = c_hgmlComputeInstanceProfileInfo_v2_t()
        fn = _hgmlGetFunctionPointer("hgmlGpuInstanceGetComputeInstanceProfileInfoV")
    elif version == 1:
        c_info = c_hgmlComputeInstanceProfileInfo_t()
        fn = _hgmlGetFunctionPointer("hgmlGpuInstanceGetComputeInstanceProfileInfo")
    else:
        raise HGMLError(HGML_ERROR_FUNCTION_NOT_FOUND)
    ret = fn(device, profile, engProfile, byref(c_info))
    _hgmlCheckReturn(ret)
    return c_info


# Define function alias for the API exposed by HGML
hgmlGpuInstanceGetComputeInstanceProfileInfoV = (
    hgmlGpuInstanceGetComputeInstanceProfileInfo
)


def hgmlGpuInstanceGetComputeInstanceRemainingCapacity(gpuInstance, profileId):
    c_count = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlGpuInstanceGetComputeInstanceRemainingCapacity")
    ret = fn(gpuInstance, profileId, byref(c_count))
    _hgmlCheckReturn(ret)
    return c_count.value


def hgmlGpuInstanceGetComputeInstancePossiblePlacements(
    gpuInstance, profileId, placementsRef, countRef
):
    fn = _hgmlGetFunctionPointer("hgmlGpuInstanceGetComputeInstancePossiblePlacements")
    ret = fn(gpuInstance, profileId, placementsRef, countRef)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlGpuInstanceCreateComputeInstance(gpuInstance, profileId):
    c_instance = c_hgmlComputeInstance_t()
    fn = _hgmlGetFunctionPointer("hgmlGpuInstanceCreateComputeInstance")
    ret = fn(gpuInstance, profileId, byref(c_instance))
    _hgmlCheckReturn(ret)
    return c_instance


def hgmlGpuInstanceCreateComputeInstanceWithPlacement(
    gpuInstance, profileId, placement
):
    c_instance = c_hgmlComputeInstance_t()
    fn = _hgmlGetFunctionPointer("hgmlGpuInstanceCreateComputeInstanceWithPlacement")
    ret = fn(gpuInstance, profileId, placement, byref(c_instance))
    _hgmlCheckReturn(ret)
    return c_instance


def hgmlComputeInstanceDestroy(computeInstance):
    fn = _hgmlGetFunctionPointer("hgmlComputeInstanceDestroy")
    ret = fn(computeInstance)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlGpuInstanceGetComputeInstances(
    gpuInstance, profileId, computeInstancesRef, countRef
):
    fn = _hgmlGetFunctionPointer("hgmlGpuInstanceGetComputeInstances")
    ret = fn(gpuInstance, profileId, computeInstancesRef, countRef)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlGpuInstanceGetComputeInstanceById(gpuInstance, computeInstanceId):
    c_instance = c_hgmlComputeInstance_t()
    fn = _hgmlGetFunctionPointer("hgmlGpuInstanceGetComputeInstanceById")
    ret = fn(gpuInstance, computeInstanceId, byref(c_instance))
    _hgmlCheckReturn(ret)
    return c_instance


def hgmlComputeInstanceGetInfo_v2(computeInstance):
    c_info = c_hgmlComputeInstanceInfo_t()
    fn = _hgmlGetFunctionPointer("hgmlComputeInstanceGetInfo_v2")
    ret = fn(computeInstance, byref(c_info))
    _hgmlCheckReturn(ret)
    return c_info


def hgmlComputeInstanceGetInfo(computeInstance):
    return hgmlComputeInstanceGetInfo_v2(computeInstance)


def hgmlDeviceIsMigDeviceHandle(device):
    c_isMigDevice = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceIsMigDeviceHandle")
    ret = fn(device, byref(c_isMigDevice))
    _hgmlCheckReturn(ret)
    return c_isMigDevice


def hgmlDeviceGetGpuInstanceId(device):
    c_gpuInstanceId = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGpuInstanceId")
    ret = fn(device, byref(c_gpuInstanceId))
    _hgmlCheckReturn(ret)
    return c_gpuInstanceId.value


def hgmlDeviceGetComputeInstanceId(device):
    c_computeInstanceId = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetComputeInstanceId")
    ret = fn(device, byref(c_computeInstanceId))
    _hgmlCheckReturn(ret)
    return c_computeInstanceId.value


def hgmlDeviceGetMaxMigDeviceCount(device):
    c_count = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMaxMigDeviceCount")
    ret = fn(device, byref(c_count))
    _hgmlCheckReturn(ret)
    return c_count.value


def hgmlDeviceGetMigDeviceHandleByIndex(device, index):
    c_index = c_uint(index)
    migDevice = c_hgmlDevice_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMigDeviceHandleByIndex")
    ret = fn(device, c_index, byref(migDevice))
    _hgmlCheckReturn(ret)
    return migDevice


def hgmlDeviceGetDeviceHandleFromMigDeviceHandle(migDevice):
    device = c_hgmlDevice_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetDeviceHandleFromMigDeviceHandle")
    ret = fn(migDevice, byref(device))
    _hgmlCheckReturn(ret)
    return device


def hgmlDeviceGetAttributes_v2(device):
    c_attrs = c_hgmlDeviceAttributes()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetAttributes_v2")
    ret = fn(device, byref(c_attrs))
    _hgmlCheckReturn(ret)
    return c_attrs


def hgmlDeviceGetAttributes(device):
    return hgmlDeviceGetAttributes_v2(device)


def hgmlDeviceGetRemappedRows(device):
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetRemappedRows")
    c_corr = c_uint()
    c_unc = c_uint()
    c_bpending = c_uint()
    c_bfailure = c_uint()
    ret = fn(device, byref(c_corr), byref(c_unc), byref(c_bpending), byref(c_bfailure))
    _hgmlCheckReturn(ret)
    return (c_corr.value, c_unc.value, c_bpending.value, c_bfailure.value)


def hgmlDeviceGetRowRemapperHistogram(device):
    c_vals = c_hgmlRowRemapperHistogramValues()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetRowRemapperHistogram")
    ret = fn(device, byref(c_vals))
    _hgmlCheckReturn(ret)
    return c_vals


def hgmlDeviceGetBusType(device):
    c_busType = _hgmlBusType_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetBusType")
    ret = fn(device, byref(c_busType))
    _hgmlCheckReturn(ret)
    return c_busType.value


def hgmlDeviceGetIrqNum(device):
    c_irqNum = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetIrqNum")
    ret = fn(device, byref(c_irqNum))
    _hgmlCheckReturn(ret)
    return c_irqNum.value


def hgmlDeviceGetNumGpuCores(device):
    c_numCores = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetNumGpuCores")
    ret = fn(device, byref(c_numCores))
    _hgmlCheckReturn(ret)
    return c_numCores.value


def hgmlDeviceGetPowerSource(device):
    c_powerSource = _hgmlPowerSource_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPowerSource")
    ret = fn(device, byref(c_powerSource))
    _hgmlCheckReturn(ret)
    return c_powerSource.value


def hgmlDeviceGetMemoryBusWidth(device):
    c_memBusWidth = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMemoryBusWidth")
    ret = fn(device, byref(c_memBusWidth))
    _hgmlCheckReturn(ret)
    return c_memBusWidth.value


def hgmlDeviceGetPcieLinkMaxSpeed(device):
    c_speed = _hgmlPcieLinkMaxSpeed_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPcieLinkMaxSpeed")
    ret = fn(device, byref(c_speed))
    _hgmlCheckReturn(ret)
    return c_speed.value


def hgmlDeviceGetAdaptiveClockInfoStatus(device):
    c_adaptiveClockInfoStatus = _hgmlAdaptiveClockInfoStatus_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetAdaptiveClockInfoStatus")
    ret = fn(device, byref(c_adaptiveClockInfoStatus))
    _hgmlCheckReturn(ret)
    return c_adaptiveClockInfoStatus.value


def hgmlDeviceGetPcieSpeed(device):
    c_speed = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetPcieSpeed")
    ret = fn(device, byref(c_speed))
    _hgmlCheckReturn(ret)
    return c_speed.value


def hgmlDeviceGetDynamicPstatesInfo(
    device, c_dynamicpstatesinfo=c_hgmlGpuDynamicPstatesInfo_t()
):
    isReference = type(c_dynamicpstatesinfo) is not c_hgmlGpuDynamicPstatesInfo_t
    dynamicpstatesinfoRef = (
        c_dynamicpstatesinfo if isReference else byref(c_dynamicpstatesinfo)
    )

    fn = _hgmlGetFunctionPointer("hgmlDeviceGetDynamicPstatesInfo")
    ret = fn(device, dynamicpstatesinfoRef)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS if isReference else c_dynamicpstatesinfo


def hgmlDeviceSetFanSpeed_v2(handle, index, speed):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetFanSpeed_v2")
    ret = fn(handle, index, speed)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlDeviceGetThermalSettings(
    device, sensorindex, c_thermalsettings=c_hgmlGpuThermalSettings_t()
):
    isReference = type(c_thermalsettings) is not c_hgmlGpuThermalSettings_t
    thermalsettingsRef = c_thermalsettings if isReference else byref(c_thermalsettings)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetThermalSettings")
    ret = fn(device, sensorindex, thermalsettingsRef)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS if isReference else c_thermalsettings.sensor[:]


def hgmlDeviceGetMinMaxClockOfPState(
    device, clockType, pstate, minClockMHz=c_uint(), maxClockMHz=c_uint()
):
    isReference = (type(minClockMHz) is not c_uint) or (type(maxClockMHz) is not c_uint)
    minClockMHzRef = minClockMHz if isReference else byref(minClockMHz)
    maxClockMHzRef = maxClockMHz if isReference else byref(maxClockMHz)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMinMaxClockOfPState")
    ret = fn(
        device,
        _hgmlClockType_t(clockType),
        _hgmlClockType_t(pstate),
        minClockMHzRef,
        maxClockMHzRef,
    )
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS if isReference else (minClockMHz.value, maxClockMHz.value)


def hgmlDeviceGetSupportedPerformanceStates(device):
    pstates = []
    c_count = c_uint(HGML_MAX_GPU_PERF_PSTATES)
    c_size = sizeof(c_uint) * c_count.value

    # NOTE: use 'c_uint' to represent the size of the hgmlPstate_t enumeration.
    pstates_array = _hgmlPstates_t * c_count.value
    c_pstates = pstates_array()

    fn = _hgmlGetFunctionPointer("hgmlDeviceGetSupportedPerformanceStates")
    ret = fn(device, c_pstates, c_size)
    _hgmlCheckReturn(ret)

    for value in c_pstates:
        if value != HGML_PSTATE_UNKNOWN:
            pstates.append(value)

    return pstates


def hgmlDeviceGetGpcClkVfOffset(device):
    offset = c_int32()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGpcClkVfOffset")
    ret = fn(device, byref(offset))
    _hgmlCheckReturn(ret)
    return offset.value


# Deprecated
def hgmlDeviceSetGpcClkVfOffset(device, offset):
    c_offset = c_int32(offset)
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetGpcClkVfOffset")
    ret = fn(device, c_offset)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlDeviceGetGpcClkMinMaxVfOffset(device, minOffset=c_int(), maxOffset=c_int()):
    isReference = (type(minOffset) is not c_int) or (type(maxOffset) is not c_int)
    minOffsetRef = minOffset if isReference else byref(minOffset)
    maxOffsetRef = maxOffset if isReference else byref(maxOffset)
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGpcClkMinMaxVfOffset")
    ret = fn(device, minOffsetRef, maxOffsetRef)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS if isReference else (minOffset.value, maxOffset.value)


def hgmlDeviceGetMemClkVfOffset(device):
    offset = c_int32()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMemClkVfOffset")
    ret = fn(device, byref(offset))
    _hgmlCheckReturn(ret)
    return offset.value


# Deprecated
def hgmlDeviceSetMemClkVfOffset(device, offset):
    c_offset = c_int32(offset)
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetMemClkVfOffset")
    ret = fn(device, c_offset)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlDeviceGetMemClkMinMaxVfOffset(device, minOffset=c_int(), maxOffset=c_int()):
    isReference = (type(minOffset) is not c_int) or (type(maxOffset) is not c_int)
    minOffsetRef = minOffset if isReference else byref(minOffset)
    maxOffsetRef = maxOffset if isReference else byref(maxOffset)

    fn = _hgmlGetFunctionPointer("hgmlDeviceGetMemClkMinMaxVfOffset")
    ret = fn(device, minOffsetRef, maxOffsetRef)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS if isReference else (minOffset.value, maxOffset.value)


def hgmlSystemSetConfComputeGpusReadyState(state):
    c_state = c_uint(state)
    fn = _hgmlGetFunctionPointer("hgmlSystemSetConfComputeGpusReadyState")
    ret = fn(c_state)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlSystemGetConfComputeGpusReadyState():
    c_state = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlSystemGetConfComputeGpusReadyState")
    ret = fn(byref(c_state))
    _hgmlCheckReturn(ret)
    return c_state.value


def hgmlSystemGetConfComputeCapabilities():
    c_ccSysCaps = c_hgmlConfComputeSystemCaps_t()
    fn = _hgmlGetFunctionPointer("hgmlSystemGetConfComputeCapabilities")
    ret = fn(byref(c_ccSysCaps))
    _hgmlCheckReturn(ret)
    return c_ccSysCaps


def hgmlSystemGetConfComputeState():
    c_state = c_hgmlConfComputeSystemState_t()
    fn = _hgmlGetFunctionPointer("hgmlSystemGetConfComputeState")
    ret = fn(byref(c_state))
    _hgmlCheckReturn(ret)
    return c_state


def hgmlDeviceSetConfComputeUnprotectedMemSize(device, c_ccMemSize):
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetConfComputeUnprotectedMemSize")
    ret = fn(device, c_ccMemSize)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlDeviceGetConfComputeMemSizeInfo(device):
    c_ccMemSize = c_hgmlConfComputeMemSizeInfo_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetConfComputeMemSizeInfo")
    ret = fn(device, byref(c_ccMemSize))
    _hgmlCheckReturn(ret)
    return c_ccMemSize


def hgmlDeviceGetConfComputeProtectedMemoryUsage(device):
    c_memory = c_hgmlMemory_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetConfComputeProtectedMemoryUsage")
    ret = fn(device, byref(c_memory))
    _hgmlCheckReturn(ret)
    return c_memory


def hgmlDeviceGetConfComputeGpuCertificate(device):
    c_cert = c_hgmlConfComputeGpuCertificate_t()
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetConfComputeGpuCertificate")
    ret = fn(device, byref(c_cert))
    _hgmlCheckReturn(ret)
    return c_cert


def hgmlDeviceGetConfComputeGpuAttestationReport(device, c_nonce):
    c_attestReport = c_hgmlConfComputeGpuAttestationReport_t()
    c_nonce_arr = (c_uint8 * len(c_nonce))(*(c_nonce))
    c_attestReport.nonce = c_nonce_arr
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetConfComputeGpuAttestationReport")
    ret = fn(device, byref(c_attestReport))
    _hgmlCheckReturn(ret)
    return c_attestReport


## GPM ##
#########

## Enums/defines

#### GPM Metric Identifiers
HGML_GPM_METRIC_GRAPHICS_UTIL = 1
HGML_GPM_METRIC_SM_UTIL = 2
HGML_GPM_METRIC_SM_OCCUPANCY = 3
HGML_GPM_METRIC_INTEGER_UTIL = 4
HGML_GPM_METRIC_ANY_TENSOR_UTIL = 5
HGML_GPM_METRIC_DFMA_TENSOR_UTIL = 6
HGML_GPM_METRIC_HMMA_TENSOR_UTIL = 7
HGML_GPM_METRIC_IMMA_TENSOR_UTIL = 9
HGML_GPM_METRIC_DRAM_BW_UTIL = 10
HGML_GPM_METRIC_FP64_UTIL = 11
HGML_GPM_METRIC_FP32_UTIL = 12
HGML_GPM_METRIC_FP16_UTIL = 13
HGML_GPM_METRIC_PCIE_TX_PER_SEC = 20
HGML_GPM_METRIC_PCIE_RX_PER_SEC = 21
HGML_GPM_METRIC_NVDEC_0_UTIL = 30
HGML_GPM_METRIC_NVDEC_1_UTIL = 31
HGML_GPM_METRIC_NVDEC_2_UTIL = 32
HGML_GPM_METRIC_NVDEC_3_UTIL = 33
HGML_GPM_METRIC_NVDEC_4_UTIL = 34
HGML_GPM_METRIC_NVDEC_5_UTIL = 35
HGML_GPM_METRIC_NVDEC_6_UTIL = 36
HGML_GPM_METRIC_NVDEC_7_UTIL = 37
HGML_GPM_METRIC_NVJPG_0_UTIL = 40
HGML_GPM_METRIC_NVJPG_1_UTIL = 41
HGML_GPM_METRIC_NVJPG_2_UTIL = 42
HGML_GPM_METRIC_NVJPG_3_UTIL = 43
HGML_GPM_METRIC_NVJPG_4_UTIL = 44
HGML_GPM_METRIC_NVJPG_5_UTIL = 45
HGML_GPM_METRIC_NVJPG_6_UTIL = 46
HGML_GPM_METRIC_NVJPG_7_UTIL = 47
HGML_GPM_METRIC_NVOFA_0_UTIL = 50
HGML_GPM_METRIC_NVOFA_1_UTIL = 51
HGML_GPM_METRIC_ICNLINK_TOTAL_RX_PER_SEC = 60
HGML_GPM_METRIC_ICNLINK_TOTAL_TX_PER_SEC = 61
HGML_GPM_METRIC_ICNLINK_L0_RX_PER_SEC = 62
HGML_GPM_METRIC_ICNLINK_L0_TX_PER_SEC = 63
HGML_GPM_METRIC_ICNLINK_L1_RX_PER_SEC = 64
HGML_GPM_METRIC_ICNLINK_L1_TX_PER_SEC = 65
HGML_GPM_METRIC_ICNLINK_L2_RX_PER_SEC = 66
HGML_GPM_METRIC_ICNLINK_L2_TX_PER_SEC = 67
HGML_GPM_METRIC_ICNLINK_L3_RX_PER_SEC = 68
HGML_GPM_METRIC_ICNLINK_L3_TX_PER_SEC = 69
HGML_GPM_METRIC_ICNLINK_L4_RX_PER_SEC = 70
HGML_GPM_METRIC_ICNLINK_L4_TX_PER_SEC = 71
HGML_GPM_METRIC_ICNLINK_L5_RX_PER_SEC = 72
HGML_GPM_METRIC_ICNLINK_L5_TX_PER_SEC = 73
HGML_GPM_METRIC_ICNLINK_L6_RX_PER_SEC = 74
HGML_GPM_METRIC_ICNLINK_L6_TX_PER_SEC = 75
HGML_GPM_METRIC_ICNLINK_L7_RX_PER_SEC = 76
HGML_GPM_METRIC_ICNLINK_L7_TX_PER_SEC = 77
HGML_GPM_METRIC_ICNLINK_L8_RX_PER_SEC = 78
HGML_GPM_METRIC_ICNLINK_L8_TX_PER_SEC = 79
HGML_GPM_METRIC_ICNLINK_L9_RX_PER_SEC = 80
HGML_GPM_METRIC_ICNLINK_L9_TX_PER_SEC = 81
HGML_GPM_METRIC_ICNLINK_L10_RX_PER_SEC = 82
HGML_GPM_METRIC_ICNLINK_L10_TX_PER_SEC = 83
HGML_GPM_METRIC_ICNLINK_L11_RX_PER_SEC = 84
HGML_GPM_METRIC_ICNLINK_L11_TX_PER_SEC = 85
HGML_GPM_METRIC_ICNLINK_L12_RX_PER_SEC = 86
HGML_GPM_METRIC_ICNLINK_L12_TX_PER_SEC = 87
HGML_GPM_METRIC_ICNLINK_L13_RX_PER_SEC = 88
HGML_GPM_METRIC_ICNLINK_L13_TX_PER_SEC = 89
HGML_GPM_METRIC_ICNLINK_L14_RX_PER_SEC = 90
HGML_GPM_METRIC_ICNLINK_L14_TX_PER_SEC = 91
HGML_GPM_METRIC_ICNLINK_L15_RX_PER_SEC = 92
HGML_GPM_METRIC_ICNLINK_L15_TX_PER_SEC = 93
HGML_GPM_METRIC_ICNLINK_L16_RX_PER_SEC = 94
HGML_GPM_METRIC_ICNLINK_L16_TX_PER_SEC = 95
HGML_GPM_METRIC_ICNLINK_L17_RX_PER_SEC = 96
HGML_GPM_METRIC_ICNLINK_L17_TX_PER_SEC = 97

HGML_GPM_METRIC_KSD_HIT_RATE = 200
HGML_GPM_METRIC_KVD_HIT_RATE = 201
HGML_GPM_METRIC_L2_HIT_RATE = 202
HGML_GPM_METRIC_LLC_HIT_RATE = 203
HGML_GPM_METRIC_MAX = 250


## Structs


class c_hgmlUnitInfo_t(_PrintableStructure):
    _fields_ = [
        ("name", c_char * 96),
        ("id", c_char * 96),
        ("serial", c_char * 96),
        ("firmwareVersion", c_char * 96),
    ]


class struct_c_hgmlGpmSample_t(Structure):
    pass  # opaque handle


c_hgmlGpmSample_t = POINTER(struct_c_hgmlGpmSample_t)


class c_metricInfo_t(Structure):
    _fields_ = [
        ("shortName", c_char_p),
        ("longName", c_char_p),
        ("unit", c_char_p),
    ]


class c_hgmlGpmMetric_t(_PrintableStructure):
    _fields_ = [
        ("metricId", c_uint),
        ("hgmlReturn", _hgmlReturn_t),
        ("value", c_double),
        ("metricInfo", c_metricInfo_t),
    ]


class c_hgmlGpmMetricsGet_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),
        ("numMetrics", c_uint),
        ("sample1", c_hgmlGpmSample_t),
        ("sample2", c_hgmlGpmSample_t),
        ("metrics", c_hgmlGpmMetric_t * HGML_GPM_METRIC_MAX),
    ]


HGML_GPM_METRICS_GET_VERSION = 1


class c_hgmlGpmSupport_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),
        ("isSupportedDevice", c_uint),
    ]


HGML_GPM_SUPPORT_VERSION = 1


## Functions


def hgmlGpmMetricsGet(metricsGet):
    fn = _hgmlGetFunctionPointer("hgmlGpmMetricsGet")
    ret = fn(byref(metricsGet))
    _hgmlCheckReturn(ret)
    return metricsGet


def hgmlGpmSampleFree(gpmSample):
    fn = _hgmlGetFunctionPointer("hgmlGpmSampleFree")
    ret = fn(gpmSample)
    _hgmlCheckReturn(ret)


def hgmlGpmSampleAlloc():
    gpmSample = c_hgmlGpmSample_t()
    fn = _hgmlGetFunctionPointer("hgmlGpmSampleAlloc")
    ret = fn(byref(gpmSample))
    _hgmlCheckReturn(ret)
    return gpmSample


def hgmlGpmSampleGet(device, gpmSample):
    fn = _hgmlGetFunctionPointer("hgmlGpmSampleGet")
    ret = fn(device, gpmSample)
    _hgmlCheckReturn(ret)
    return gpmSample


def hgmlGpmMigSampleGet(device, gpuInstanceId, gpmSample):
    fn = _hgmlGetFunctionPointer("hgmlGpmMigSampleGet")
    ret = fn(device, gpuInstanceId, gpmSample)
    _hgmlCheckReturn(ret)
    return gpmSample


def hgmlGpmQueryDeviceSupport(device):
    gpmSupport = c_hgmlGpmSupport_t()
    gpmSupport.version = HGML_GPM_SUPPORT_VERSION
    fn = _hgmlGetFunctionPointer("hgmlGpmQueryDeviceSupport")
    ret = fn(device, byref(gpmSupport))
    _hgmlCheckReturn(ret)
    return gpmSupport


def hgmlGpmSetStreamingEnabled(device, state):
    c_state = c_uint(state)
    fn = _hgmlGetFunctionPointer("hgmlGpmSetStreamingEnabled")
    ret = fn(device, c_state)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlGpmQueryIfStreamingEnabled(device):
    c_state = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlGpmQueryIfStreamingEnabled")
    ret = fn(device, byref(c_state))
    _hgmlCheckReturn(ret)
    return c_state.value


# Low Power Structure and Function

HGML_ICNLINK_POWER_STATE_HIGH_SPEED = 0x0
HGML_ICNLINK_POWER_STATE_LOW = 0x1

HGML_ICNLINK_LOW_POWER_THRESHOLD_MIN = 0x1
HGML_ICNLINK_LOW_POWER_THRESHOLD_MAX = 0x1FFF
HGML_ICNLINK_LOW_POWER_THRESHOLD_RESET = 0xFFFFFFFF
HGML_ICNLINK_LOW_POWER_THRESHOLD_DEFAULT = HGML_ICNLINK_LOW_POWER_THRESHOLD_RESET


class c_hgmlIcnLinkPowerThres_t(Structure):
    _fields_ = [
        ("lowPwrThreshold", c_uint),
    ]


def hgmlDeviceSetIcnLinkDeviceLowPowerThreshold(device, l1threshold):
    c_info = c_hgmlIcnLinkPowerThres_t()
    c_info.lowPwrThreshold = l1threshold
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetIcnLinkDeviceLowPowerThreshold")
    ret = fn(device, byref(c_info))
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


HGML_GPU_FABRIC_UUID_LEN = 16

_hgmlGpuFabricState_t = c_uint
HGML_GPU_FABRIC_STATE_NOT_SUPPORTED = 0
HGML_GPU_FABRIC_STATE_NOT_STARTED = 1
HGML_GPU_FABRIC_STATE_IN_PROGRESS = 2
HGML_GPU_FABRIC_STATE_COMPLETED = 3


class c_hgmlGpuFabricInfo_t(_PrintableStructure):
    _fields_ = [
        ("clusterUuid", c_uint8 * HGML_DEVICE_UUID_BUFFER_SIZE),
        ("status", _hgmlReturn_t),
        ("cliqueId", c_uint32),
        ("state", _hgmlGpuFabricState_t),
    ]


def hgmlDeviceGetGpuFabricInfo(device, gpuFabricInfo):
    fn = _hgmlGetFunctionPointer("hgmlDeviceGetGpuFabricInfo")
    ret = fn(device, gpuFabricInfo)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


######################
## Enums/defines
#### HGML GPU ICNLINK BW MODE
HGML_GPU_ICNLINK_BW_MODE_FULL = 0x0
HGML_GPU_ICNLINK_BW_MODE_OFF = 0x1
HGML_GPU_ICNLINK_BW_MODE_MIN = 0x2
HGML_GPU_ICNLINK_BW_MODE_HALF = 0x3
HGML_GPU_ICNLINK_BW_MODE_3QUARTER = 0x4
HGML_GPU_ICNLINK_BW_MODE_COUNT = 0x5


def hgmlSystemSetIcnLinkBwMode(mode):
    fn = _hgmlGetFunctionPointer("hgmlSystemSetIcnLinkBwMode")
    ret = fn(mode)
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS


def hgmlSystemGetIcnLinkBwMode():
    mode = c_uint()
    fn = _hgmlGetFunctionPointer("hgmlSystemGetIcnLinkBwMode")
    ret = fn(byref(mode))
    _hgmlCheckReturn(ret)
    return mode.value


_hgmlPowerScopeType_t = c_uint
HGML_POWER_SCOPE_GPU = 0
HGML_POWER_SCOPE_MODULE = 1
HGML_POWER_SCOPE_MEMORY = 2


class c_hgmlPowerValue_v2_t(_PrintableStructure):
    _fields_ = [
        ("version", c_uint),
        ("powerScope", _hgmlPowerScopeType_t),
        ("powerValueMw", c_uint),
    ]
    _fmt_ = {"<default>": "%d B"}


hgmlPowerValue_v2 = 0x0200000C


def hgmlDeviceSetPowerManagementLimit_v2(
    device, powerScope, powerLimit, version=hgmlPowerValue_v2
):
    c_powerScope = _hgmlPowerScopeType_t(powerScope)
    c_powerValue = c_hgmlPowerValue_v2_t()
    c_powerValue.version = c_uint(version)
    c_powerValue.powerScope = c_powerScope
    c_powerValue.powerValueMw = c_uint(powerLimit)
    fn = _hgmlGetFunctionPointer("hgmlDeviceSetPowerManagementLimit_v2")
    ret = fn(device, byref(c_powerValue))
    _hgmlCheckReturn(ret)
    return HGML_SUCCESS
