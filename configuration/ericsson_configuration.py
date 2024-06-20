# -*- coding:utf-8 -*-
import math

from configuration.common_configuration import map4gFrqPtToBand
from configuration.huawei_configuration import is_4g_freq

G5_CELL_IDENTITY = 'CGI'
G4_CELL_IDENTITY = '本地小区标识'
g5_base_cols = ['地市', 'MeContext',  'CGI', '频段', '厂家', '共址类型', '覆盖类型', '覆盖场景', '区域类别', '物理站编号']
g4_base_cols = ['地市', 'MeContext', 'CGI', '频段', '厂家', '共址类型', '覆盖类型', '覆盖场景', '区域类别', '物理站编号']


def map_eri_freq_pt(df, frequency_param, g4_freq_band_dict):
    all_freq = df[frequency_param].unique().tolist()
    #爱立信5G频点与其他两家厂商不同，先判断是否4G频点
    if is_4g_freq(all_freq, g4_freq_band_dict):
        df[frequency_param] = df[frequency_param].apply(map4gFrqPtToBand,
                                                        args=(g4_freq_band_dict,))
    else:
        # n1是指其他厂商的
        df[frequency_param] = df[frequency_param].map(eri_4g_map_band)
    return df


def eri_4g_map_band(x):
    if 'nan' == str(x):
        return math.nan
    if x[0] == '1':
        return '700M'
    elif x[0] == '5':
        return '2.6G'
    else:
        return '4.9G'
