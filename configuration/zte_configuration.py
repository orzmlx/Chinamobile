# -*- coding:utf-8 -*-
import math
import logging
import re
import pandas as pd
from pandas import DataFrame
import os
from python_calamine.pandas import pandas_monkeypatch

from configuration.common_configuration import is_4g_freq, g4_band_dict
from utils import common_utils

logging.basicConfig(format='%(asctime)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S', level=logging.INFO)

G4_CELL_IDENTITY = None
G5_CELL_IDENTITY = "CGI"
g5_base_cols = ['地市', 'CGI', '工作频段', '频段', '厂家', '共址类型', '覆盖类型', '覆盖场景', '区域类别', '物理站编号']
g4_base_cols = ['地市', 'CGI', '频段', '厂家', '共址类型', '覆盖类型', '覆盖场景', '区域类别', '物理站编号']


def judge_freq_relation_by_filename(filename, freq_param):
    if filename.find('NRtoNR') >= 0:
        return '5G'
    elif filename.find('NRtoLTE') >= 0:
        return '5G' if freq_param == 'ServingFrequency' else '4G'


def map_zte_freq_pt(df, g4_freq_band_dict, frequency_param):
    if 'ServingFrequency' != frequency_param:
        all_freq = df[frequency_param].unique().tolist()
        # 爱立信5G频点与其他两家厂商不同，先判断是否4G频点
        if is_4g_freq(all_freq, g4_freq_band_dict):

            df[frequency_param] = df[frequency_param].apply(zte_4g_map_band)
        else:
            # n1是指其他厂商的
            df[frequency_param] = df[frequency_param].map(zte_5g_map_band)
        return df[(df[frequency_param] != '其他频段')]

    return df


def zte_5g_map_band(x):
    if 'nan' == str(x):
        return math.nan
    x = float(x)
    if 762 < x <= 792:
        return '700M'
    elif 2519 < x <= 2666:
        return '2.6G'
    elif 4804 < x <= 4920:
        return '4.9G'
    else:
        return '其他频段'


def zte_4g_map_band(x):
    if 'nan' == str(x):
        return math.nan
    x = int(float(x))
    if 1809 < x <= 1830 or x == 3:
        return 'FDD1800'
    elif 1881 < x <= 1910 or x == 39:
        return 'F'
    elif 2016 < x <= 2020 or x == 34:
        return 'A'
    elif 2319 < x <= 2365 or x == 40:
        return 'E'
    elif 2524 < x <= 2665 or x == 41 or x == 38:
        return 'D'
    elif 934 < x <= 949 or x == 8:
        return 'FDD900'
    else:
        return '其他频段'


def _depart_voice_param(row):
    value = str(row['calculation&基于覆盖语音切换&异频切换门限&系统内'])
    pattern = r'^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$'
    if 'nan' == value:
        return math.nan, math.nan, math.nan, math.nan, math.nan
    # 正数为A3，一个负数为A4，2个负数，第一个为A51，另外一个A52
    if bool(re.match(pattern, value)):
        # 是浮点数字符串
        if value[0] == '-':
            return 'A4', math.nan, value, math.nan, math.nan
        else:
            return 'A3', value, math.nan, math.nan, math.nan
    else:
        if value.find('/'):
            row_splits = value.split('/')
            if len(row_splits) == 2:
                return 'A5', math.nan, math.nan, row_splits[0], row_splits[1]
            else:
                logging.info('语音数据分离出现未定义的数据' + value)
                return math.nan, math.nan, math.nan, math.nan, math.nan
        else:
            logging.info('语音数据分离出现未定义的数据{0}'.format(value))
            return None, None, None, None, None


def _depart_ldn(row):
    ldn = ['Scell&CUEUtranCellFDDLTE/CUEUtranCellTDDLTE&ldn&LDN', 'Scell&CUEUtranCellFDDLTE&ldn&LDN']
    extract = None
    for i in ldn:
        if i in row.index.tolist():
            value = row[i]
            pattern = 'ENBCUCPFunction=([^,]+)'
            extract = re.findall(pattern, value)[0]
            break
    return extract


def _depart_params(row: str):
    voice_param = [None, None, None, None, None]
    if 'calculation&基于覆盖语音切换&异频切换门限&系统内' in row:
        voice_param = list(_depart_voice_param(row))
    ldn_number = _depart_ldn(row)
    voice_param.append(ldn_number)
    return voice_param


def depart_params(df: DataFrame):
    """
        从一列中分离出其他参数，完全写死
    """
    # if 'calculation&基于覆盖语音切换&异频切换门限&系统内' in df.columns.tolist():
    # 使用result_type不能是对series使用apply
    df[['切换事件', '覆盖A3门限-语音', '覆盖A4门限-语音', '覆盖A5门限1-语音', '覆盖A5门限2-语音', 'ldn']] = \
        df.apply(_depart_params, axis=1, result_type='expand')


def parse_node_id(data, supplementary_cgi_df):
    node_id_list = []
    node_id = data[0]
    network_id = data[1]
    if node_id.find('-') >= 0:
        node_splits = node_id.split('-')
        for sp in node_splits:
            if sp.find('_') >= 0:
                sp_res = sp.split('_')
                node_id_list.extend(sp_res)
            else:
                node_id_list.append(sp)
        node_id_list.remove('460')
        node_id_list.remove('00')
        if len(node_id_list) == 1:
            node_id = node_id_list[0]
    node_id = str(int(float(node_id)))
    if '1' == node_id:
        network_id = str(int(float(network_id))) if common_utils.is_float(network_id) else network_id
        identity = supplementary_cgi_df[supplementary_cgi_df['网元ID'] == network_id]['eNodeB标识'].iloc[0]
        return identity
    return node_id


def add_cgi(df, filename, supplementary_cgi_df):
    composition_dict = zte_4g_composition()
    composition = composition_dict[filename]
    if composition is None:
        raise Exception(filename + "没有配置如何配置CGI列")
    df[composition[0]] = df[[composition[0], composition[2]]].apply(parse_node_id, args=(supplementary_cgi_df,), axis=1)
    # node_id = df[composition[0]].iloc[0]
    # node_id = parse_node_id(node_id)
    df['CGI'] = '460-00-' + df[composition[0]].astype(float).astype(int).astype(str) + '-' + \
                df[composition[1]].astype(float).astype(int).astype(str)


def zte_4g_composition():
    '''
        第三列用来获取补充CGI
    '''
    return {
        "l_itran_selection_headline": ('ldn',
                                       'Scell&CUEUtranCellFDDLTE/CUEUtranCellTDDLTE&cellLocalId&小区标识',),
        'l_itran_reselection_headline': ('ldn',
                                         'Scell&CUEUtranCellFDDLTE/CUEUtranCellTDDLTE&cellLocalId&小区标识',
                                         'Scell&CUEUtranCellFDDLTE/CUEUtranCellTDDLTE&ManagedElement&网元ID'
                                         ),
        'l_itran_handover_headline': ('ldn',
                                      'Scell&CUEUtranCellFDDLTE&cellLocalId&小区标识',
                                      'Scell&CUEUtranCellFDDLTE&ManagedElement&网元ID'),
        'l_u31_selection_headline': (
            'Scell&EUtranCellFDD/EUtranCellTDD&ENBFunctionFDD/ENBFunctionTDD&LTEFDDID/LTETDDID',
            'Scell&EUtranCellFDD/EUtranCellTDD&cellLocalId&小区标识'),
        'l_u31_reselection_headline': (
            'Scell&EUtranCellFDD/EUtranCellTDD&ENBFunctionFDD/ENBFunctionTDD&LTEFDDID/LTETDDID',
            'Scell&EUtranCellFDD/EUtranCellTDD&cellLocalId&小区标识', 'Scell&EUtranCellFDD/EUtranCellTDD&MEID&网元ID'),
        'l_u31_handover_headline': (
            'Scell&EUtranCellFDD/EUtranCellTDD&ENBFunctionFDD/ENBFunctionTDD&LTEFDDID/TD-LTEID',
            'Scell&EUtranCellFDD/EUtranCellTDD&cellLocalId&小区标识', 'Scell&EUtranCellFDD/EUtranCellTDD&MEID&网元ID')
    }


def zte_extra_manage(path):
    '''
        返回处理过的参数，防止后续重复处理
    '''
    base_name = os.path.basename(path)
    if base_name.find('NRtoNR_IntraFCoverMobility') >= 0:
        logging.info('开始对' + path + '文件中的参数进行额外数据修正')
        df = pd.read_csv(path, low_memory=False)
        first = df.iloc[0]
        df = df.iloc[1:]
        df.apply(coverMobilityCtrl_interFRatA2Strategy, axis=1)
        df.loc[0] = first
        df = pd.concat([first, df], axis=1).reset_index(drop=True)
        df.to_csv(path, index=False, encoding='utf_8_sig')



def if_judge(if_param, main_param, else_param, row):
    if str(row[if_param]) == '0':
        row[main_param] = row[else_param]


def coverMobilityCtrl_interFRatA2Strategy(row):
    if_judge('CoverMobilityCtrl_interFRatA2Strategy', 'InterFHoA1A2_rsrpThresholdA1', 'InterFRATHoA1A2_rsrpThresholdA1',
             row)
    if_judge('CoverMobilityCtrl_interFRatA2Strategy', 'InterFHoA1A2_rsrpThresholdA2', 'InterFRATHoA1A2_rsrpThresholdA2',
             row)
    if_judge('CoverMobilityCtrl_interFRatA2Strategy', 'InterRatHoA1A2_rsrpThresholdA1',
             'InterFRATHoA1A2_rsrpThresholdA1', row)
    if_judge('CoverMobilityCtrl_interFRatA2Strategy', 'InterRatHoA1A2_rsrpThresholdA2',
             'InterFRATHoA1A2_rsrpThresholdA2', row)
