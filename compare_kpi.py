# -*- coding:utf-8 -*-

import pandas as pd
import copy
import os
from python_calamine.pandas import pandas_monkeypatch
from tqdm import tqdm

from utils import common_utils


def select_data_by_date(base_df, df, on_index, date_offset, date_index):
    copy_base_df = copy.deepcopy(base_df)
    copy_base_df = copy_base_df.merge(df, on=[on_index], how='left')
    # copy_base_df[date_index] = pd.to_datetime(copy_base_df[date_index], format='%Y-%m-%d')
    # copy_base_df['调整日期'] = pd.to_datetime(copy_base_df['调整日期'], format='%Y-%m-%d')
    before_df = copy_base_df[copy_base_df[date_index] == copy_base_df['调整日期'] - pd.Timedelta(days=7)]
    after_df = copy_base_df[copy_base_df[date_index] == copy_base_df['调整日期'] + pd.Timedelta(days=7)]
    return before_df, after_df


def select_data_by_date_range(base_df, df, on_index, date_offset, date_index):
    copy_base_df = copy.deepcopy(base_df)
    copy_base_df = copy_base_df.merge(df, on=[on_index], how='left')
    # copy_base_df[date_index] = pd.to_datetime(copy_base_df[date_index], format='%Y-%m-%d')
    # copy_base_df['调整日期'] = pd.to_datetime(copy_base_df['调整日期'], format='%Y-%m-%d')
    before_df = copy_base_df[
        copy_base_df[date_index].between(copy_base_df['调整日期'] - pd.Timedelta(days=7) - pd.Timedelta(days=date_offset),
                                         copy_base_df['调整日期'] - pd.Timedelta(days=7))]
    after_df = copy_base_df[
        copy_base_df[date_index].between(copy_base_df['调整日期'] + pd.Timedelta(days=7),
                                         copy_base_df['调整日期'] + pd.Timedelta(days=7) + pd.Timedelta(days=date_offset))]
    return before_df, after_df


def read_raw_data(path, filter_map, use_cols, on_index, date_index):
    file_df = pd.read_csv(path, usecols=use_cols + [on_index, date_index], encoding='gbk')
    file_df[date_index] = pd.to_datetime(file_df[date_index], format='%Y-%m-%d')
    if len(filter_map) == 0:
        return file_df
    for key, value in filter_map.items():
        # condition = key + '==' + value
        file_df = file_df[file_df[key] == value]
    return file_df


def parse_kpi(func, base_df, files, use_cols, on_index, date_index, date_offset, out_path, filter_map):
    dir = os.path.dirname(out_path)
    if not os.path.exists(dir):
        os.mkdir(dir)
    before_df_list = []
    after_df_list = []
    file_number = len(files)
    if on_index == '小区CGI':
        base_df.rename(columns={'CGI': '小区CGI'}, inplace=True)
    for i in tqdm(range(file_number)):
        file = files[i]
        file_df = read_raw_data(file, filter_map, use_cols, on_index, date_index)
        # file_df = pd.read_csv(file, usecols=use_cols + [on_index, date_index], encoding='gbk')
        before_df, after_df = func(base_df, file_df[use_cols + [on_index, date_index]], on_index, date_offset,
                                   date_index)
        before_df = get_avg(before_df, base_df, use_cols, on_index, date_index)
        after_df = get_avg(after_df, base_df, use_cols, on_index, date_index)
        before_df_list.append(before_df)
        after_df_list.append(after_df)
    # 合并所有筛选过后的文件
    before_concat_df = concat_dfs(before_df_list)
    after_concat_df = concat_dfs(after_df_list)
    rename_by_suffix(before_concat_df, use_cols, 'before')
    rename_by_suffix(after_concat_df, use_cols, 'after')
    before_used_cols = [i + '_before' for i in use_cols]
    after_used_cols = [i + '_after' for i in use_cols]
    before_final_df = get_avg(before_concat_df, base_df, before_used_cols, on_index, date_index)
    before_final_df.drop_duplicates(keep='first', inplace=True)
    after_final_df = get_avg(after_concat_df, base_df, after_used_cols, on_index, date_index)
    after_final_df.drop_duplicates(keep='first', inplace=True)
    # res = base_df.merge(before_final_df, on=[on_index, base_date_index], how='left')
    # res = res.merge(after_final_df, on=[on_index, base_date_index], how='left')
    res = after_final_df.merge(before_final_df, on=base_df.columns.tolist(), how='outer')
    res.to_csv(out_path, index=False, encoding='utf_8_sig')
    return res


def rename_by_suffix(df, cols, suffix):
    for c in cols:
        df.rename(columns={c: c + '_' + suffix}, inplace=True)


def concat_dfs(df_list):
    res = pd.DataFrame()
    df_number = len(df_list)
    for i in tqdm(range(df_number)):
        df = df_list[i]
        if df.empty:
            continue
        res = df if res.empty else pd.concat([res, df], axis=0)
    return res


def get_avg(df, base_df, use_cols, on, date_index):
    if df.empty:
        return df
    copy_base_df = copy.deepcopy(base_df)
    cols_number = len(use_cols)
    # 这里简单过滤非数字类型列
    if cols_number == 1:
        columns = df.columns.tolist()
        if date_index in columns:
            columns.remove(date_index)
        # columns.remove(date_index) if date_index in columns else pass
        return df[columns]
    for i in tqdm(range(cols_number)):
        col = use_cols[i]
        filter_df = df[[col, on, base_date_index]]
        filter_df.dropna(subset=[col], axis=0, inplace=True)
        gp_df = filter_df.groupby([on, base_date_index])[col, on, base_date_index].mean(col).reset_index()
        # base_df有重复项
        copy_base_df = copy_base_df.merge(gp_df, on=[on, base_date_index], how='left')
        copy_base_df.drop_duplicates(inplace=True, keep='first')
    copy_base_df.dropna(subset=use_cols, how='all', inplace=True)
    return copy_base_df


def merge_res(path, base_df, system):
    res = copy.deepcopy(base_df)
    # res = pd.DataFrame()
    base_columns = base_df.columns.tolist()
    summary_csvs = common_utils.find_file(path, 'summary.csv')
    if len(summary_csvs) == 1:
        return
    for file in summary_csvs:
        f_df = pd.read_csv(file)
        f_df[base_date_index] = pd.to_datetime(f_df[base_date_index], format='%Y-%m-%d')
        if '小区CGI' in f_df.columns.tolist():
            f_df.rename(columns={'小区CGI': 'CGI'}, inplace=True)
        res = res.merge(f_df, how='left', on=base_columns)
        # res = f_df if res.empty else res.merge(f_df, how='left', on=base_columns)
    out_path = os.path.join(path, system + '_summary.csv')
    res.to_csv(out_path, index=False, encoding='utf_8_sig')


if __name__ == "__main__":
    base_date_index = '调整日期'
    pandas_monkeypatch()
    on = 'CGI'
    on_1 = '小区CGI'
    date_col = '数据开始时间'
    date_col_1 = '开始时间'
    work_dir = 'C:\\Users\\orzmlx\\Desktop\\chinamobile\\ye'
    index_file_path = 'C:\\Users\\orzmlx\\Desktop\\chinamobile\\45G互操作策略调整小区清单汇总-0607 - 剔除3月前(1).xlsx'
    g4_performance_path = 'C:\\Users\\orzmlx\\Desktop\\chinamobile\\ye\\4G性能指标'
    g4_performance_cols = ['总流量(MB)(MB)']
    g5_performance_path = 'C:\\Users\\orzmlx\\Desktop\\chinamobile\\ye\\5G性能指标'
    g5_performance_cols = ['PDCP上行业务字节数(Mbyte)', 'PDCP下行业务字节数(Mbyte)', '小区UE上行速率小于1Mbps的样本数(个)',
                           '小区UE上行速率低于1Mbps样本比例(%)', '小区UE下行速率小于5Mbps的样本数(个)', '小区UE下行速率低于5Mbps样本比例(%)']
    g5_high_load_path = 'C:\\Users\\orzmlx\\Desktop\\chinamobile\\取数\\5G高负荷'
    g5_high_load_cols = ['问题类型']
    g4_high_load_path = 'C:\\Users\\orzmlx\\Desktop\\chinamobile\\取数\\4G高负荷'
    g4_high_load_cols = ['问题类型（省内）']

    g4_traffic_path = 'C:\\Users\\orzmlx\\Desktop\\chinamobile\\取数\\4G流量'
    g4_traffic_cols = ['上行流量(MB)(MB)', '下行流量(MB)(MB)', '总流量(MB)(MB)']
    g4_traffic_files = common_utils.find_file(g4_traffic_path, '.csv')
    g5_high_load_files = common_utils.find_file(g5_high_load_path, '.csv')
    g4_high_load_files = common_utils.find_file(g4_high_load_path, '.csv')

    g4_performance_files = common_utils.find_file(g4_performance_path, '.csv')
    g5_performance_files = common_utils.find_file(g5_performance_path, '.csv')
    g4_base_df = pd.read_excel(index_file_path, sheet_name='4G', engine='calamine')
    g4_base_df[base_date_index] = pd.to_datetime(g4_base_df[base_date_index], format='%Y-%m-%d')

    g5_base_df = pd.read_excel(index_file_path, sheet_name='5G', engine='calamine')
    g5_base_df[base_date_index] = pd.to_datetime(g5_base_df[base_date_index], format='%Y-%m-%d')

    # g4_base_df.drop_duplicates(inplace=True,keep='first')
    # g5_base_df.drop_duplicates(inplace=True, keep='first')
    g5_performance_out_path = os.path.join(work_dir, '5G', '5G性能指标_summary.csv')
    g4_performance_out_path = os.path.join(work_dir, '4G', '4G性能指标_summary.csv')
    g5_high_load_out_path = os.path.join(work_dir, '5G', '5G高负荷_summary.csv')
    g4_high_load_out_path = os.path.join(work_dir, '4G', '4G高负荷_summary.csv')
    g4_traffic_out_path = os.path.join(work_dir, '4G', '4G流量_summary.csv')
    # parse_kpi(select_data_by_date_range, g5_base_df, g5_performance_files, g5_performance_cols, on, date_col, 3,
    #           g5_performance_out_path, {})
    # merge_res( os.path.join(work_dir, '5G'),g5_base_df)

    # parse_kpi(select_data_by_date_range, g4_base_df, g4_performance_files, g4_performance_cols, on_1, date_col_1, 3,
    #           g4_performance_out_path, {})
    # g5_high_load_filter_map = {'问题类型': '高负荷小区'}
    # parse_kpi(select_data_by_date, g5_base_df, g5_high_load_files, g5_high_load_cols, on_1, date_col_1, 3,
    #           g5_high_load_out_path, g5_high_load_filter_map)

    # g4_high_load_filter_map = {'问题类型（省内）': '高负荷预警小区'}
    # parse_kpi(select_data_by_date, g4_base_df, g4_high_load_files, g4_high_load_cols, on_1, date_col_1, 3,
    #           g4_high_load_out_path, g4_high_load_filter_map)

    # parse_kpi(select_data_by_date_range, g4_base_df, g4_traffic_files, g4_traffic_cols, on_1, date_col_1, 3,
    #           g4_traffic_out_path, {})

    # merge_res(os.path.join(work_dir, '5G'), g5_base_df,'5G')
    merge_res(os.path.join(work_dir, '4G'), g4_base_df,'4G')