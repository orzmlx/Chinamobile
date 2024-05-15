# -*- coding: utf-8 -*-

import importlib
import os
import pathlib
import re
import sys
import logging
import pandas as pd
from openpyxl import Workbook

importlib.reload(sys)


def get_action_charactors():
    # 这里在爱立信数据时，eq改成equal
    return [':', 'match', 'equal', 'extract']


def get_action_names():
    return {'删除整行', '合并', '整体筛选', '包含筛选'}


def union_action(df, action_tuples_list, action_name, new_col_name, index):
    if action_name == '删除整列':
        action_df = action_delete(df, action_tuples_list, index, axis=1)
    elif action_name == '删除整行':
        action_df = action_delete(df, action_tuples_list, index)
    elif action_name == '合并':
        if new_col_name is None:
            raise Exception("列合并操作没有定义新列的名称,出错行号:" + str(index))
        action_df = action_columns_merge(df, action_tuples_list, new_col_name, index)
    elif action_name == '筛选':
        action_df = action_filter(df, action_tuples_list, index)
    else:
        raise Exception("没有定义的操作名:" + action_name, "出错行号:" + str(index))
    return action_df


def find_operators(str):
    result = []
    operators = get_action_charactors()
    for op in operators:
        if str.find(op) >= 0:
            result.append(op)
    return result


def filter(df, operator, col_name, range):
    if operator == 'match':
        filter_df = df[col_name].str.contains(range, flags=re.IGNORECASE)
    elif operator == ':':

        filter_df = df if range == '整体' else df[df[col_name].str.contains(range, flags=re.IGNORECASE)]

    elif operator == 'equal':
        filter_df = df[df[col_name].str.lower() == range.lower()]
    else:
        raise Exception("未定义的操作符:" + operator)
    return filter_df


def action_delete(df, action_tuples, index, axis=0):
    """
        删除就是过滤的逻辑，删除过滤后的结果
    """
    for tuple in action_tuples:
        col_name = tuple[0].strip()
        range = tuple[1].strip()
        operator = tuple[2].strip()
        if operator is None:
            raise Exception("删除操作,但是缺少算子,出错行数:" + str(index))
        # 筛选出来的df,全部在原df中通过Index删除
        filter_df = filter(df, operator, col_name, range)
        index = filter_df.index.tolist()
        df.drop(index=index, axis=axis, inplace=True)
    return df


def split_column(x, pattern, operator, index):
    try:
        return re.search(pattern, x).group() if operator == 'match' else re.findall(pattern, x)[0]
    except Exception as e:
        raise Exception('正则表达式:' + pattern + "没有匹配到任何字符串,出错行数:" + str(index))
    # if match_res is None:
    #     raise Exception('正则表达式:' + pattern + "没有匹配到任何字符串,出错行数:" + str(index))
    # else:
    #     return match_res.group()


def action_columns_merge(df, action_tuples_list, new_column_name, index):
    constant = None
    df[new_column_name] = ""
    math_way = False
    math_operator = None

    for action_tuple in action_tuples_list:
        col_name = action_tuple[0].strip() if action_tuple[0] is not None else None
        if 'add' == col_name or 'subtract' == col_name:
            math_way = True
            math_operator = col_name
            df[new_column_name] = 0
            continue
        action_range = action_tuple[1].strip() if action_tuple[1] is not None else None
        operator = action_tuple[2].strip() if action_tuple[2] is not None else None
        if operator == 'match' or operator == 'extract':
            try:
                df[col_name + "#"] = df[col_name].apply(split_column, args=(action_range, operator, index))
            except Exception as e:
                raise Exception("请检查正则表达式的正确性," + "出错行数:" + str(index), e)
            if constant is not None:
                df[col_name + "#"] = constant + df[col_name + "#"]
                constant = None
            # cols.append(col_name + "#")
            # # 规整列的顺序,确保新的列在最后一个
            # df = df[cols]
        elif operator == ':' and action_range == '整体':
            df[col_name + "#"] = df[col_name].apply(str)
            if constant is not None:
                df[col_name + "#"] = constant + df[col_name + "#"]
                constant = None
            # 规整列的顺序,,确保新的列在最后一个
            # cols.append(col_name + "#")
            # df = df[cols]
        elif operator is None:  # 如果operator是None,那么该数据是常量，
            constant = constant + col_name if constant is not None else col_name
        else:
            raise Exception("未定义的操作符:" + operator + "出错行数:" + str(index))
    cols = df.columns.tolist()
    if math_way:
        try:
            left_operator_col = action_tuples_list[0][0]
            right_operator_col = action_tuples_list[2][0]
            df[left_operator_col + "#"].fillna(value=0, inplace=True)
            df[right_operator_col + "#"].fillna(value=0, inplace=True)
            df[left_operator_col + "#"] = df[left_operator_col + "#"].apply(float)
            df[right_operator_col + "#"] = df[right_operator_col + "#"].apply(float)
            df[new_column_name] = df[left_operator_col + "#"] + df[
                right_operator_col + "#"] if math_operator == 'add' else \
                df[left_operator_col + "#"] - df[right_operator_col + "#"]
            df.drop([left_operator_col + "#"], axis=1, inplace=True)
            df.drop([right_operator_col + "#"], axis=1, inplace=True)
        except:
            logging.info("列之间算术计算错误")
    else:
        for c in cols:
            if c.find('#') >= 0:
                df[new_column_name] = df[new_column_name] + df[c]
                df.drop([c], axis=1, inplace=True)
        if constant is not None:
            df[new_column_name] = df[new_column_name] + constant
    return df


def action_filter(df, action_tuples, index):
    filter_df = df
    for tuple in action_tuples:
        col_name = tuple[0].strip()
        range = tuple[1].strip()
        operator = tuple[2].strip()
        if operator is None:
            raise Exception("删除操作,但是缺少算子,出错行数:" + str(index))
        filter_df = filter(filter_df, operator, col_name, range)
    return filter_df


def get_csv_summary(path):
    wb = Workbook()
    # 创建第一个sheet
    sheet = wb.active
    items = pathlib.Path(path).rglob('*')
    for item in items:
        item = str(item)
        df = pd.read_csv(item, encoding='utf8')
        # df = df.to_pandas()
        if df.empty:
            raise Exception(item + '中数据为空')
        f_name = os.path.split(item)[1].split(".")[0]
        df_cols = [f_name, len(df)]
        first_col = [f_name, len(df)]
        df_cols.extend(df.columns.tolist())
        first_col.extend(df.iloc[1].tolist())
        sheet.append(df_cols)
        sheet.append(first_col)
    wb.save(os.path.join(path, 'summary.csv'))


if __name__ == "__main__":
    path = 'C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\zte\\20240229\\5G'
    get_csv_summary(
        "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\zte\\20240322\\5G\\RANCM-5G互操作参数-fxx-chaxun-20240319165154"
        "-ITBBU-ITRAN-PNF_V5\\temp")
