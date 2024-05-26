# -*- coding:utf-8 -*-

from pandas import DataFrame

from configuration import huawei_configuration, zte_configuration


def freq_param_map(df, manufacturer, system, frequency_param, g4_freq_band_dict, command) -> DataFrame:
    if manufacturer == '华为':
        huawei_configuration.map_huawei_freq_pt(df, system, frequency_param, g4_freq_band_dict)
    elif manufacturer == '中兴':
        df = zte_configuration.map_zte_freq_pt(df,  frequency_param, command)

    df.rename(columns={frequency_param: '对端频带'}, inplace=True)

    return df
