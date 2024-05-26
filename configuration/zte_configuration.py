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
band_dict = {'FDD-1800': {'1425', '1450', '1301', '1300', '1350', '1250', '1400', '1375', '1275', '1475'},
             'FDD-900': {'3591', '3589', '3641', '3590'},
             'F': {'38525', '38351', '38375', '38544', '38499', '38350', '38402', '38496', '38396', '38352', '38550',
                   '38500', '38325', '38400'},
             'E': {'40936', '39148', '38953', '39150', '39450', '38400', '39295', '39300', '39346', '39151', '39294',
                   '39448', '38952', '38750', '39292', '39496', '38752', '39250', '38950', '39050'},
             'D': {'40936', '38098', '40938', '40340', '40540', '40940', '39940', '38100', '36275', '41334', '37902',
                   '40342', '41336', '37900', '41332', '41140', '41340', '41138', '40937', '41134', '40140', '40144'},
             'A': {'36251', '39148', '36275', '36250'}}


def judge_freq_relation_by_filename(filename, freq_param):
    if filename.find('NRtoNR') >= 0:
        return '5G'
    elif filename.find('NRtoLTE') >= 0:
        return '5G' if freq_param == 'ServingFrequency' else '4G'


def map_zte_freq_pt(df, frequency_param, command):
    # if 'ServingFrequency' in df.columns.tolist():
    #     df['ServingFrequency'] = df['ServingFrequency'].apply(zte_5g_map_band)
    if 'ServingFrequency' != frequency_param:
        system = judge_freq_relation_by_filename(command, frequency_param)
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
    if 1809 < x <= 1830:
        return 'FDD-1800'
    elif 1881 < x <= 1910:
        return 'F'
    elif 2016 < x <= 2020:
        return 'A'
    elif 2319 < x <= 2365:
        return 'E'
    elif 2524 < x <= 2665:
        return 'D'
    elif 934 < x <= 949:
        return 'FDD-900'
    else:
        return '未知频段'


def depart_voice_params(row: str):
    value = row['calculation|基于覆盖语音切换|异频切换门限|系统内']
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


def depart_params(df: DataFrame):
    """
        从一列中分离出其他参数，完全写死
    """
    if 'calculation|基于覆盖语音切换|异频切换门限|系统内' in df.columns.tolist():
        # 使用result_type不能是对series使用apply
        df[['切换事件', '覆盖A3门限-语音', '覆盖A4门限-语音', '覆盖A5门限1-语音', '覆盖A5门限2-语音']] = \
            df.apply(depart_voice_params, axis=1, result_type='expand')


def add_cgi(df, filename):
    composition_dict = zte_4g_composition()
    composition = composition_dict[filename]
    if composition is None:
        raise Exception(filename + "没有配置如何配置CGI列")

    df['CGI'] = '460-00' + '-' + df[composition[0]].astype(float).astype(int).astype(str) + \
                '-' + df[composition[1]].astype(float).astype(int).astype(str)


def zte_4g_composition():
    return {"l_itran_selection_headline": ('Scell|CUEUtranCellFDDLTE/CUEUtranCellTDDLTE|ManagedElement|网元ID',
                                           'Scell|CUEUtranCellFDDLTE/CUEUtranCellTDDLTE|cellLocalId|小区标识'),
            'l_itran_reselection_headline': ('Scell|CUEUtranCellFDDLTE/CUEUtranCellTDDLTE|ManagedElement|网元ID',
                                             'Scell|CUEUtranCellFDDLTE/CUEUtranCellTDDLTE|cellLocalId|小区标识'
                                             ),
            'l_itran_handover_headline': ('Scell|CUEUtranCellFDDLTE|ManagedElement|网元ID',
                                          'Scell|CUEUtranCellFDDLTE|cellLocalId|小区标识'),
            'l_u31_selection_headline': (
                'Scell|EUtranCellFDD/EUtranCellTDD|ENBFunctionFDD/ENBFunctionTDD|LTEFDDID/LTETDDID',
                'Scell|EUtranCellFDD/EUtranCellTDD|cellLocalId|小区标识'),
            'l_u31_reselection_headline': (
                'Scell|EUtranCellFDD/EUtranCellTDD|ENBFunctionFDD/ENBFunctionTDD|LTEFDDID/LTETDDID',
                'Scell|EUtranCellFDD/EUtranCellTDD|cellLocalId|小区标识'),
            'l_u31_handover_headline': (
                'Scell|EUtranCellFDD/EUtranCellTDD|ENBFunctionFDD/ENBFunctionTDD|LTEFDDID/TD-LTEID',
                'Scell|EUtranCellFDD/EUtranCellTDD|cellLocalId|小区标识')
            }
