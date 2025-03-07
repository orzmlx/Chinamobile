# -*- coding:utf-8 -*-

import os

from utils import common_utils

def is_input_valid(df, check_columns, value_check_column, valid_values):
    if df.empty:
        return False
    return check_columns_valid(df, check_columns) and check_columns_first_row(df, value_check_column, valid_values)


def check_columns_valid(df, demand_cols):
    return all(item in df.columns.tolist() for item in demand_cols)


def check_columns_first_row(df, column_name, valid_values):
    if df.empty:
        return False
    return df.iloc[0][column_name] in valid_values


def is_4g_common_valid(df):
    return is_input_valid(df, ['地市名称', '工作频段'], '工作频段', ['A频段', 'D频段', 'E频段', 'F频段', 'FDD'])


def is_5g_common_valid(df):
    return is_input_valid(df, ['CGI', '工作频段', '地市'], '工作频段', ['NR-700', 'NR-C', 'NR-D'])


def is_5g_site_info_valid(df):
    return is_input_valid(df, ['制式', '物理站编号', '地市'], '制式', ['5G'])


def is_4g_site_info_valid(df):
    return is_input_valid(df, ['制式', '物理站编号', '地市'], '制式', ['4G'])


def is_raw_data_valid(path, manufacturer):
    if manufacturer is None:
        return False
    if os.path.isdir(path):
        files = []
        if manufacturer == '华为':
            files = common_utils.find_file(path, '.txt')
        elif manufacturer == '中兴':
            files = common_utils.find_file(path, '.xlsx')
        if len(files) == 0:
            return False
        return True
    return False
