# -*- coding:utf-8 -*-
import math
import logging
import re

from pandas import DataFrame

logging.basicConfig(format='%(asctime)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S', level=logging.INFO)

G4_CELL_IDENTITY = None
G5_CELL_IDENTITY = "CGI"
g5_base_cols = ['地市', 'CGI', '工作频段', '频段', '厂家', '共址类型', '覆盖类型', '覆盖场景', '区域类别', '物理站编号']
g4_base_cols = ['地市', 'CGI', '频段', '厂家', '共址类型', '覆盖类型', '覆盖场景', '区域类别', '物理站编号']
g4_band_dict = {
    'FDD1800': {'1300', '1250', '1350', '1301', '1425', '1400', '1375', '1275', '1475', '1450', '1506', '1825', '1650',
                '1850', '1401', '1401'},
    'FDD900': {'3591', '3589', '3641', '3590', '3615', '3637'},
    'F': {'38400', '38544', '38350', '38496', '38375', '38352', '38325', '38550', '38499', '38351', '38500',
          '38525', '38402', '38494', '38521', '38475', '38396'},
    'E': {'39292', '38950', '39148', '39123', '39150', '39300', '38952', '39294', '39273', '39496', '39295', '38953',
          '39346', '39250', '39450', '38752', '39151', '38750', '39448', '39325', '39544', '39050'},
    'D': {'40936', '41332', '41134', '38098', '37900', '41340', '40940', '41336', '41140', '41138', '40938', '40342',
          '40540', '38100', '40140', '39940', '37902', '40340', '40937', '41334', '40738', '40738', '40684', '40711',
          '40861', '40866', '37950', '40144'},
    'A': {'36251', '36275', '36250'}}


def judge_freq_relation_by_filename(filename, freq_param):
    if filename.find('NRtoNR') >= 0:
        return '5G'
    elif filename.find('NRtoLTE') >= 0:
        return '5G' if freq_param == 'ServingFrequency' else '4G'


def map_zte_freq_pt(df, system, frequency_param):
    # if 'ServingFrequency' in df.columns.tolist():
    #     df['ServingFrequency'] = df['ServingFrequency'].apply(zte_5g_map_band)
    if 'ServingFrequency' != frequency_param:
        # system = judge_freq_relation_by_filename(command, frequency_param)
        if system == '5G':
            df[frequency_param] = df[frequency_param].apply(zte_5g_map_band)
        elif system == '4G':
            df[frequency_param] = df[frequency_param].apply(zte_4g_map_band)
    return df


def zte_5g_map_band(x):
    if 'nan' == str(x):
        return math.nan
    x = float(x)
    if 762 < x <= 792:
        return '700'
    elif 2519 < x <= 2666:
        return '2.6G'
    elif 4804 < x <= 4920:
        return '4.9G'
    else:
        return '未知频段'


def zte_4g_map_band(x):
    if 'nan' == str(x):
        return math.nan
    x = float(x)
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
        return '未知频段'


def _depart_voice_param(row):
    value = row['calculation&基于覆盖语音切换&异频切换门限&系统内']
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
        if i in row:
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


def add_cgi(df, filename):
    composition_dict = zte_4g_composition()
    composition = composition_dict[filename]
    if composition is None:
        raise Exception(filename + "没有配置如何配置CGI列")

    df['CGI'] = '460-00' + '-' + df[composition[0]].astype(float).astype(int).astype(str) + \
                '-' + df[composition[1]].astype(float).astype(int).astype(str)


def zte_4g_composition():
    return {
        "l_itran_selection_headline": ('ldn',
                                       'Scell&CUEUtranCellFDDLTE/CUEUtranCellTDDLTE&cellLocalId&小区标识'),
        'l_itran_reselection_headline': ('ldn',
                                         'Scell&CUEUtranCellFDDLTE/CUEUtranCellTDDLTE&cellLocalId&小区标识'
                                         ),
        'l_itran_handover_headline': ('ldn',
                                      'Scell&CUEUtranCellFDDLTE&cellLocalId&小区标识'),
        'l_u31_selection_headline': (
            'Scell&EUtranCellFDD/EUtranCellTDD&ENBFunctionFDD/ENBFunctionTDD&LTEFDDID/LTETDDID',
            'Scell&EUtranCellFDD/EUtranCellTDD&cellLocalId&小区标识'),
        'l_u31_reselection_headline': (
            'Scell&EUtranCellFDD/EUtranCellTDD&ENBFunctionFDD/ENBFunctionTDD&LTEFDDID/LTETDDID',
            'Scell&EUtranCellFDD/EUtranCellTDD&cellLocalId&小区标识'),
        'l_u31_handover_headline': (
            'Scell&EUtranCellFDD/EUtranCellTDD&ENBFunctionFDD/ENBFunctionTDD&LTEFDDID/TD-LTEID',
            'Scell&EUtranCellFDD/EUtranCellTDD&cellLocalId&小区标识')
    }
