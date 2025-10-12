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

from .mxsml import *

_mxSmlMcmTemperatureSensors_t = c_uint
_mxSmlMcmPmbusUnit_t = c_uint
_mxSmlMcmVersionUnit_t = c_uint
_mxSmlClockIp_t = c_uint
_mxSmlUsageIp_t = c_uint
_mxSmlDpmIp_t = c_uint
_mxSmlFwIpName_t = c_uint
_mxSmlLoglevel_t = c_uint
_mxSmlMetaXLinkType_t = c_uint

MAX_DIE_NUM = 2  # Guaranteed maximum die count on one device

_mxsmlMcmFunctionPointerCache = {}


def _mxsmlMcmGetFunctionPointer(name):
    if name in _mxsmlMcmFunctionPointerCache:
        return _mxsmlMcmFunctionPointerCache[name]

    _mxSmlLib = get_mxSmlLib()
    if _mxSmlLib == None:
        raise MXSMLError(MXSML_ERROR_FAILURE)

    _mxsmlMcmFunctionPointerCache[name] = getattr(_mxSmlLib, name)
    return _mxsmlMcmFunctionPointerCache[name]


def _mxsmlMcmCheckReturn(ret):
    if ret != MXSML_SUCCESS:
        raise MXSMLError(ret)
    return ret


def mxSmlGetDeviceDieCount(device_id):
    die_count = c_uint()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDeviceDieCount")
    ret = fn(c_uint(device_id), byref(die_count))
    _mxsmlMcmCheckReturn(ret)
    return die_count.value


def mxSmlGetDieMemoryInfo(device_id, die_id):
    memory_info = c_mxsmlMemoryInfo_t()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieMemoryInfo")
    ret = fn(c_uint(device_id), c_uint(die_id), byref(memory_info))
    _mxsmlMcmCheckReturn(ret)
    return memory_info


def mxSmlGetDieTemperatureInfo(device_id, die_id, temperature_type):
    temperature = c_int()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieTemperatureInfo")
    ret = fn(
        c_uint(device_id),
        c_uint(die_id),
        _mxSmlMcmTemperatureSensors_t(temperature_type),
        byref(temperature),
    )
    _mxsmlMcmCheckReturn(ret)
    return temperature.value


def mxSmlGetDiePmbusInfo(device_id, die_id, pmbus_unit):
    pmbus_info = c_mxSmlPmbusInfo_t()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDiePmbusInfo")
    ret = fn(
        c_uint(device_id),
        c_uint(die_id),
        _mxSmlMcmPmbusUnit_t(pmbus_unit),
        byref(pmbus_info),
    )
    _mxsmlMcmCheckReturn(ret)
    return pmbus_info


@convertStrBytes
def mxSmlGetDeviceDieVersion(device_id, dieId, version_unit):
    version = create_string_buffer(MXSML_VERSION_INFO_SIZE)
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDeviceDieVersion")
    ret = fn(
        c_uint(device_id),
        c_uint(dieId),
        _mxSmlMcmVersionUnit_t(version_unit),
        byref(version),
        byref(c_uint(MXSML_VERSION_INFO_SIZE)),
    )
    _mxsmlMcmCheckReturn(ret)
    return version.value


def mxSmlGetDieClocks(device_id, die_id, clock_ip):
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieClocks")
    size = c_uint(1)
    clocks = (c_uint * 1)()
    ret = fn(
        c_uint(device_id),
        c_uint(die_id),
        _mxSmlClockIp_t(clock_ip),
        byref(size),
        byref(clocks),
    )

    if ret == MXSML_SUCCESS:
        return [clock for clock in clocks]

    elif ret == MXSML_ERROR_INSUFFICIENT_SIZE:
        clocks = (c_uint * size.value)()
        ret = fn(
            c_uint(device_id),
            c_uint(die_id),
            _mxSmlClockIp_t(clock_ip),
            byref(size),
            byref(clocks),
        )
        _mxsmlMcmCheckReturn(ret)
        return [clock for clock in clocks]

    else:
        raise MXSMLError(ret)


def mxSmlGetDieChipSerial(device_id, die_id):
    serial = create_string_buffer(MXSML_CHIP_SERIAL_SIZE)
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieChipSerial")
    ret = fn(
        c_uint(device_id),
        c_uint(die_id),
        byref(serial),
        byref(c_uint(MXSML_CHIP_SERIAL_SIZE)),
    )
    _mxsmlMcmCheckReturn(ret)
    return serial.value


def mxSmlGetDieApUsageToggle(device_id, die_id):
    toggle = c_uint()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieApUsageToggle")
    ret = fn(c_uint(device_id), c_uint(die_id), byref(toggle))
    _mxsmlMcmCheckReturn(ret)
    return toggle.value


def mxSmlGetDieDpmIpClockInfo(device_id, die_id, dpm_ip):
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieDpmIpClockInfo")
    size = c_uint(0)
    ret = fn(
        c_uint(device_id),
        c_uint(die_id),
        _mxSmlDpmIp_t(dpm_ip),
        (c_uint * 1)(),
        byref(size),
    )

    if ret == MXSML_ERROR_INSUFFICIENT_SIZE:
        clocks = (c_uint * size.value)()
        ret = fn(
            c_uint(device_id),
            c_uint(die_id),
            _mxSmlDpmIp_t(dpm_ip),
            byref(clocks),
            byref(size),
        )
        _mxsmlMcmCheckReturn(ret)
        return [clk for clk in clocks]
    else:
        raise MXSMLError(ret)


def mxSmlGetDieDpmIpVddInfo(device_id, die_id, dpm_ip):
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieDpmIpVddInfo")
    size = c_uint(0)
    ret = fn(
        c_uint(device_id),
        c_uint(die_id),
        _mxSmlDpmIp_t(dpm_ip),
        (c_uint * 1)(),
        byref(size),
    )

    if ret == MXSML_ERROR_INSUFFICIENT_SIZE:
        vdds = (c_uint * size.value)()
        ret = fn(
            c_uint(device_id),
            c_uint(die_id),
            _mxSmlDpmIp_t(dpm_ip),
            byref(vdds),
            byref(size),
        )
        _mxsmlMcmCheckReturn(ret)
        return [vdd for vdd in vdds]
    else:
        raise MXSMLError(ret)


def mxSmlGetCurrentDieDpmIpPerfLevel(device_id, die_id, dpm_ip):
    level = c_uint()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetCurrentDieDpmIpPerfLevel")
    ret = fn(c_uint(device_id), c_uint(die_id), _mxSmlDpmIp_t(dpm_ip), byref(level))
    _mxsmlMcmCheckReturn(ret)
    return level.value


def mxSmlSetDieUnlockKey(device_id, die_id, key):
    b_key = c_char_p(key.encode("utf-8"))
    fn = _mxsmlMcmGetFunctionPointer("mxSmlSetDieUnlockKey")
    ret = fn(c_uint(device_id), c_uint(die_id), b_key)
    _mxsmlMcmCheckReturn(ret)
    return ret


def mxSmlGetDieIpUsage(device_id, die_id, usage_ip):
    usage = c_int()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieIpUsage")
    ret = fn(c_uint(device_id), c_uint(die_id), _mxSmlUsageIp_t(usage_ip), byref(usage))
    _mxsmlMcmCheckReturn(ret)
    return usage.value


def mxSmlGetDieDpmMaxPerfLevel(device_id, die_id):
    dpm_level = c_mxSmlMxcDpmPerfLevel_t()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieDpmMaxPerfLevel")
    ret = fn(c_uint(device_id), c_uint(die_id), byref(dpm_level))
    _mxsmlMcmCheckReturn(ret)
    return dpm_level


def mxSmlGetDieDpmIpMaxPerfLevel(device_id, die_id, dpm_ip):
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieDpmIpMaxPerfLevel")
    level = c_uint(0)
    ret = fn(c_uint(device_id), c_uint(die_id), _mxSmlDpmIp_t(dpm_ip), byref(level))
    _mxsmlMcmCheckReturn(ret)
    return level.value


def mxSmlGetDieXcoreApUsage(device_id, die_id):
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieXcoreApUsage")
    size = c_uint(0)
    dpc_num = c_uint(0)
    ret = fn(
        c_uint(device_id), c_uint(die_id), (c_uint * 0)(), byref(size), byref(dpc_num)
    )

    if ret == MXSML_ERROR_INSUFFICIENT_SIZE:
        usage_array = (c_uint * size.value)()
        ret = fn(
            c_uint(device_id),
            c_uint(die_id),
            byref(usage_array),
            byref(size),
            byref(dpc_num),
        )
        _mxsmlMcmCheckReturn(ret)
        idx_num = (size.value + dpc_num.value - 1) // dpc_num.value
        return [usage_array[i : i + idx_num] for i in range(0, size.value, idx_num)]
    else:
        raise MXSMLError(ret)


def mxSmlSetDieDpmIpMaxPerfLevel(device_id, die_id, dpm_ip, level):
    fn = _mxsmlMcmGetFunctionPointer("mxSmlSetDieDpmIpMaxPerfLevel")
    ret = fn(c_uint(device_id), c_uint(die_id), _mxSmlDpmIp_t(dpm_ip), c_uint(level))
    _mxsmlMcmCheckReturn(ret)
    return ret


def mxSmlGetDieCodecStatus(device_id, die_id):
    status = c_mxSmlCodecStatus_t()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieCodecStatus")
    ret = fn(c_uint(device_id), c_uint(die_id), byref(status))
    _mxsmlMcmCheckReturn(ret)
    return status


def mxSmlGetDieEepromInfo(device_id, die_id):
    eeprom_info = c_mxSmlEepromInfo_t()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieEepromInfo")
    ret = fn(c_uint(device_id), c_uint(die_id), byref(eeprom_info))
    _mxsmlMcmCheckReturn(ret)
    return eeprom_info


@convertStrBytes
def mxSmlDieVbiosUpgrade(device_id, die_id, time_limit, bin_path):
    # Will take effect after reboot
    upgrade_arg = c_mxSmlVbiosUpgradeArg_t()
    upgrade_arg.timeLimit = c_uint(time_limit)
    upgrade_arg.vbiosBinPath = bin_path
    upgrade_arg.forceUpgrade = c_int(0)
    fn = _mxsmlMcmGetFunctionPointer("mxSmlDieVbiosUpgrade")
    ret = fn(c_uint(device_id), c_uint(die_id), byref(upgrade_arg))
    _mxsmlMcmCheckReturn(ret)
    return ret


def mxSmlGetDieEccState(device_id, die_id):
    state = c_uint()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieEccState")
    ret = fn(c_uint(device_id), c_uint(die_id), byref(state))
    _mxsmlMcmCheckReturn(ret)
    return state.value


def mxSmlSetDieEccState(device_id, die_id, state):
    # Will take effect after reboot
    fn = _mxsmlMcmGetFunctionPointer("mxSmlSetDieEccState")
    ret = fn(c_uint(device_id), c_uint(die_id), c_uint(state))
    _mxsmlMcmCheckReturn(ret)
    return ret


def mxSmlGetDiePowerStateInfo(device_id, die_id, dpm_ip):
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDiePowerStateInfo")
    size = c_uint(0)
    ret = fn(
        c_uint(device_id),
        c_uint(die_id),
        _mxSmlDpmIp_t(dpm_ip),
        (c_int * 0)(),
        byref(size),
    )

    if ret == MXSML_ERROR_INSUFFICIENT_SIZE:
        power_states = (c_int * size.value)()
        ret = fn(
            c_uint(device_id),
            c_uint(die_id),
            _mxSmlDpmIp_t(dpm_ip),
            byref(power_states),
            byref(size),
        )
        _mxsmlMcmCheckReturn(ret)
        return [state for state in power_states]
    else:
        raise MXSMLError(ret)


def mxSmlGetDieHbmBandWidth(device_id, die_id):
    bw = c_mxSmlHbmBandWidth_t()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieHbmBandWidth")
    ret = fn(c_uint(device_id), c_uint(die_id), byref(bw))
    _mxsmlMcmCheckReturn(ret)
    return bw


@convertStrBytes
def mxSmlGetDiePptableVersion(device_id, die_id):
    version = create_string_buffer(MXSML_VERSION_INFO_SIZE)
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDiePptableVersion")
    ret = fn(c_uint(device_id), c_uint(die_id), c_uint(5), byref(version))
    _mxsmlMcmCheckReturn(ret)
    return version.value


def mxSmlGetDieFwLoglevel(device_id, die_id):
    log_level = c_mxSmlFwLoglevel_t()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieFwLoglevel")
    ret = fn(c_uint(device_id), c_uint(die_id), byref(log_level))
    _mxsmlMcmCheckReturn(ret)
    return log_level


def mxSmlSetDieFwLoglevel(device_id, die_id, fw_ip, level):
    fn = _mxsmlMcmGetFunctionPointer("mxSmlSetDieFwLoglevel")
    ret = fn(
        c_uint(device_id),
        c_uint(die_id),
        _mxSmlFwIpName_t(fw_ip),
        _mxSmlLoglevel_t(level),
    )
    _mxsmlMcmCheckReturn(ret)
    return ret


def mxSmlGetDieFwIpLoglevel(device_id, dieId, fw_ip):
    level = _mxSmlLoglevel_t()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieFwIpLoglevel")
    ret = fn(c_uint(device_id), c_uint(dieId), _mxSmlFwIpName_t(fw_ip), byref(level))
    _mxsmlMcmCheckReturn(ret)
    return level.value


def mxSmlGetDieMetaXLinkBandwidth(device_id, dieId, metaxlink_type):
    size = c_uint(MXSML_METAX_LINK_NUM + 1)
    entry = []
    bw = (c_mxSmlMetaXLinkBandwidth_t * (MXSML_METAX_LINK_NUM + 1))(*entry)
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieMetaXLinkBandwidth")
    ret = fn(
        c_uint(device_id),
        c_uint(dieId),
        _mxSmlMetaXLinkType_t(metaxlink_type),
        byref(size),
        byref(bw),
    )
    _mxsmlMcmCheckReturn(ret)
    return bw


def mxSmlGetDieRasStatusData(device_id, dieId):
    data = c_mxSmlRasStatusData_t()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieRasStatusData")
    ret = fn(c_uint(device_id), c_uint(dieId), byref(data))
    _mxsmlMcmCheckReturn(ret)
    return data


def mxSmlGetDieRasErrorData(device_id, dieId):
    data = c_mxSmlRasErrorData_t()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieRasErrorData")
    ret = fn(c_uint(device_id), c_uint(dieId), byref(data))
    _mxsmlMcmCheckReturn(ret)
    return data


def mxSmlGetDieMetaXLinkInfo(device_id, dieId):
    size = c_uint(8)
    entry = []
    metaxlink_info = (c_mxSmlSingleMxlkInfo_t * 8)(*entry)
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieMetaXLinkInfo")
    ret = fn(c_uint(device_id), c_uint(dieId), byref(metaxlink_info), byref(size))
    _mxsmlMcmCheckReturn(ret)
    return metaxlink_info


def mxSmlGetDieMetaXLinkRemoteInfo(device_id, die_id, link_id):
    link_remote_info = c_mxSmlMcmMetaXLinkRemoteInfo_t()
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieMetaXLinkRemoteInfo")
    ret = fn(
        c_uint(device_id), c_uint(die_id), c_uint(link_id), byref(link_remote_info)
    )
    _mxsmlMcmCheckReturn(ret)
    return link_remote_info


def mxSmlGetDieUnavailableReason(device_id, die_id):
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieUnavailableReason")
    reason = c_mxSmlDeviceUnavailableReasonInfo_t()
    ret = fn(c_uint(device_id), c_uint(die_id), byref(reason))
    _mxsmlMcmCheckReturn(ret)
    return reason


def mxSmlGetDieTotalEccErrors(device_id, die_id):
    fn = _mxsmlMcmGetFunctionPointer("mxSmlGetDieTotalEccErrors")
    eccCounts = c_mxSmlEccErrorCount_t(0)
    ret = fn(c_uint(device_id), c_uint(die_id), byref(eccCounts))
    _mxsmlMcmCheckReturn(ret)
    return mxsmlStructToFriendlyObject(eccCounts)
