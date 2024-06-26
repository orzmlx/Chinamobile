# -*- coding:utf-8 -*-
from python_calamine.pandas import pandas_monkeypatch
import pandas as pd
import numpy as np
import copy

G4_BANDS = []


def get_direction_diff(dr1, dr2):
    """
        以顺时针计算两个方位角之间的差值
    """
    if abs(dr2 - dr1) > 180:
        if dr2 >= dr1:
            return (360 - dr2) + dr1
        else:
            return (360 - dr1) + dr2
    else:
        return abs(dr2 - dr1)


def get_g5_directions(g):
    result = []

    g5_direction_df = g[g['小区频段'].isin(['2.6G', '700M', '4.9G'])]
    g4_direction_df = g[~g['小区频段'].isin(['2.6G', '700M', '4.9G'])]
    return g5_direction_df, g4_direction_df
    #
    # nrd_directions = g[g['小区频段'].str.contains('2.6G', na=False)]['方位角'].unique().tolist()
    # nr700_directions = g[g['小区频段'].str.contains('700M', na=False)]['方位角'].unique().tolist()
    # nrc_directions = g[g['小区频段'].str.contains('4.9G', na=False)]['方位角'].unique().tolist()
    # result.extend(nrd_directions)
    # result.extend(nr700_directions)
    # result.extend(nrc_directions)
    # if len(g5_directions) == 0:
    #     g5_directions = df[df['频段'].str.contains('NR-700', na=False)]['方位角'].unique().tolist()
    #     if len(g5_directions) == 0:
    #         g5_directions = df[df['频段'].str.contains('NR-C', na=False)]['方位角'].unique().tolist()
    # result.sort()
    # return result


def output_tilt_plan(df, change_value, group_cols):
    df_copy = copy.deepcopy(df)
    grouped = df_copy.groupby(group_cols)
    group_number = len(grouped)
    result = []
    index0 = 0
    prg = 0
    for s_number, g in grouped:
        number = s_number[0]
        index0 = index0 + 1
        label = s_number[1]
        c_prg = round(index0 / group_number, 3)
        # print(index0)
        if c_prg > prg:
            print("{:.2f}%".format(c_prg * 100))
            prg = c_prg
        bands = g['小区频段'].unique().tolist()
        directions = g['方位角'].unique().tolist()
        if len(directions) == 1 and directions[0] == 0.01:
            continue
            # 全是4G站点
        if len(set(['2.6G', '4.9G', '700M']) & set(bands)) == 0:
            continue
            # 全是5G站点
        if len(bands) == 3 and (set(['2.6G', '4.9G', '700M']) <= set(bands)):
            continue
        g4_sectors = g[~g['小区频段'].isin(['2.6G', '4.9G', '700M'])]
        g5_sectors = g[g['小区频段'].isin(['2.6G', '4.9G', '700M'])]
        # 元组嵌套的方法输出结果
        for label_4g, row_4g in g4_sectors.iterrows():
            cgi_4g = row_4g['CGI']
            # scene_number_4g = row_4g['方位角']#问题
            daoliu_4g = row_4g['倒流时长']
            direction_4g = row_4g['方位角']
            # problem_4g = row_4g['方位角']#问题
            if str(daoliu_4g) == 'nan' or float(daoliu_4g) < 20:
                continue
            tilt_4g = row_4g['倾角']
            for label_5g, row_5g in g5_sectors.iterrows():
                cgi_5g = row_5g['CGI']
                tilt_5g = row_5g['倾角']
                height_5g = row_5g['挂高']
                direction_5g = row_5g['方位角']
                # scene_number_5g = row_5g['方位角']#问题
                # problem_5g = row_5g['方位角']#问题
                # if problem_5g != problem_4g:
                #     continue
                if tilt_5g - tilt_4g < 3:
                    continue
                new_tilt_5g = tilt_5g - change_value
                if str(height_5g) == 'nan':
                    continue
                if height_5g < 20:
                    result.append(
                        (number, cgi_4g, direction_4g, daoliu_4g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '合格',
                         label)) if 3 <= tilt_5g <= 10 else result.append(
                        (number, direction_4g, daoliu_4g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '不合格', label))
                elif 20 <= height_5g < 30:
                    result.append(
                        (number, cgi_4g, direction_4g, daoliu_4g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '合格',
                         label)) if 5 <= tilt_5g <= 12 else result.append(
                        (number, cgi_4g, direction_4g, daoliu_4g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '不合格', label))

                elif 30 <= height_5g < 40:
                    result.append(
                        (number, cgi_4g, direction_4g, daoliu_4g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '合格',
                         label)) if 8 <= tilt_5g <= 15 else result.append(
                        (number, cgi_4g, direction_4g, daoliu_4g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '不合格', label))
                elif 40 <= height_5g < 50:
                    result.append(
                        (number, cgi_4g, direction_4g, daoliu_4g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g,
                         '合格', label)) if 12 <= tilt_5g <= 20 else result.append(
                        (number, cgi_4g, direction_4g, daoliu_4g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '不合格', label))
                elif 50 <= height_5g < 60:
                    result.append(
                        (number, cgi_4g, direction_4g, daoliu_4g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g,
                         '合格', label)) if 15 <= tilt_5g <= 30 else result.append(
                        (number, cgi_4g, direction_4g, daoliu_4g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '不合格', label))
                else:
                    result.append(
                        (number, cgi_4g, direction_4g, daoliu_4g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g,
                         '合格', label)) if 20 <= tilt_5g <= 35 else result.append(
                        (number, cgi_4g, direction_4g, daoliu_4g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '不合格', label))
                    # result.append(
                    #     (s_number, cgi_4g, direction_4g, daoliu_4g, cgi_5g, direction_5g))
    res_df = pd.DataFrame.from_records(result,
                                       columns=['物理点编号', 'CGI_4G', '方位角_4G', '倒流时长',
                                                'CGI_5G', '方位角_5G', '调整前5G倾角', '调整后5G倾角', '挂高_5G', '合规', '公扇区编号'])
    res_df.to_csv("C:\\Users\\orzmlx\\Desktop\\chinamobile\\倾角方案.csv", index=False, encoding='utf_8_sig')
    return res_df


def output_direction_plan(df, group_cols):
    df_copy = copy.deepcopy(df)
    grouped = df_copy.groupby(group_cols)
    group_number = len(grouped)
    result = []
    index0 = 0
    prg = 0
    for s_number, g in grouped:
        number = s_number[0]
        index0 = index0 + 1
        label = s_number[1]
        c_prg = round(index0 / group_number, 3)
        # print(index0)
        if c_prg > prg:
            print("{:.2f}%".format(c_prg * 100))
            prg = c_prg
        bands = g['小区频段'].unique().tolist()
        directions = g['方位角'].unique().tolist()
        if len(directions) == 1 and directions[0] == 0.01:
            continue
            # 全是4G站点
        if len({'2.6G', '4.9G', '700M'} & set(bands)) == 0:
            continue
            # 全是5G站点
        if len(bands) == 3 and ({'2.6G', '4.9G', '700M'} <= set(bands)):
            continue
        g4_sectors = g[~g['小区频段'].isin(['2.6G', '4.9G', '700M'])]
        g5_sectors = g[g['小区频段'].isin(['2.6G', '4.9G', '700M'])]
        # 元组嵌套的方法输出结果
        for label_4g, row_4g in g4_sectors.iterrows():
            cgi_4g = row_4g['CGI']
            daoliu_4g = row_4g['倒流时长']
            # scene_number_4g = row_4g['方位角']#问题
            # problem_4g = row_4g['方位角']#问题
            if str(daoliu_4g) == 'nan' or float(daoliu_4g) < 20:
                continue
            direction_4g = row_4g['方位角']
            for label_5g, row_5g in g5_sectors.iterrows():
                cgi_5g = row_5g['CGI']
                direction_5g = row_5g['方位角']
                # scene_number_5g = row_5g['方位角']#问题
                # problem_5g = row_5g['方位角']#问题
                # if problem_5g != problem_4g:
                #     continue
                if abs(direction_4g - direction_5g) >= 30:
                    result.append(
                        (number, cgi_4g, direction_4g, daoliu_4g, cgi_5g,
                         direction_5g, label)
                        # (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                        #  direction_5g, label)
                    )
    res_df = pd.DataFrame.from_records(result,
                                       columns=['物理点编号', 'CGI_4G', '方位角_4G', '倒流时长',
                                                'CGI_5G', '方位角_5G', '共扇区编号'])
    res_df.to_csv("C:\\Users\\orzmlx\\Desktop\\chinamobile\\方位角方案.csv", index=False, encoding='utf_8_sig')
    return res_df


def output_height_plan(df, group_cols):
    df_copy = copy.deepcopy(df)
    grouped = df_copy.groupby(group_cols)
    group_number = len(grouped)
    result = []
    index0 = 0
    prg = 0

    for s_number, g in grouped:
        site_number = s_number[0]
        # share_number = site_number + '-' + str(s_number[1])
        index0 = index0 + 1
        label = s_number[1]
        c_prg = round(index0 / group_number, 3)
        # print(index0)
        if c_prg > prg:
            print("{:.2f}%".format(c_prg * 100))
            prg = c_prg
        bands = g['小区频段'].unique().tolist()
        directions = g['方位角'].unique().tolist()
        if len(directions) == 1 and directions[0] == 0.01:
            continue
        # 全是4G站点
        if len({'2.6G', '4.9G', '700M'} & set(bands)) == 0:
            continue
        # 全是5G站点
        if len(bands) == 3 and ({'2.6G', '4.9G', '700M'} <= set(bands)):
            continue
        g4_sectors = g[~g['小区频段'].isin(['2.6G', '4.9G', '700M'])]
        g5_sectors = g[g['小区频段'].isin(['2.6G', '4.9G', '700M'])]
        # 元组嵌套的方法输出结果
        for label_4g, row_4g in g4_sectors.iterrows():
            cgi_4g = row_4g['CGI']
            height_4g = row_4g['挂高']
            daoliu_4g = row_4g['倒流时长']
            # is_high_backflow = row_4g['高倒流小区']
            # if is_high_backflow == '是':
            #     g5_height = g5_sectors.iloc[0]['挂高']
            #     g5_cgi = g5_sectors.iloc[0]['CGI']
            #     g5_index = g5_sectors.iloc[0].name
            #     height_diff = height_4g - g5_height

                # df.loc[g5_index, '5G小区和高倒流4G挂高差值'] = height_diff
                # if height_diff > 5:
                #     action = '降低' if g5_height > height_4g else ''
                #     advise = '4G高倒流小区' + str(cgi_4g) + '与5G小区' + str(g5_cgi) + '挂高差' + str(height_diff) \
                #              + '建议天面整改'
                #     height_diff['挂高建议优化方案'] = 1
            if str(height_4g) == 'nan' or float(height_4g) < 20:
                continue
            if str(daoliu_4g) == 'nan' or float(daoliu_4g) < 20:
                continue
            direction_4g = row_4g['方位角']

            g5_index = g5_sectors.iloc[0].name
            row_5g = g5_sectors.iloc[0]
            cgi_5g = row_5g['CGI']
            height_5g = row_5g['挂高']
            direction_5g = row_5g['方位角']
            if str(height_5g) == 'nan' or float(height_5g) < 20:
                continue
            if height_4g - height_5g >= 5:
                result.append((site_number, cgi_4g, height_4g, direction_4g, daoliu_4g, cgi_5g,
                               height_5g, direction_5g, label))
    result_col = ['物理站编号', 'CGI_4G', '挂高_4G', '方位角_4G', '倒流时长', 'CGI_5G', '挂高_5G',
                  '方位角_5G', '公扇区编号']
    # result_col.extend(group_cols)
    res_df = pd.DataFrame.from_records(result, columns=result_col)
    res_df.to_csv("C:\\Users\\orzmlx\\Desktop\\chinamobile\\挂高提升方案.csv", index=False, encoding='utf_8_sig')
    return res_df


def output_share_sector_number(df):
    # cut_bins = [-10, 0, 60, 120, 180, 240, 300, 360]
    # df = pd.read_csv(path, encoding='gbk')
    # df['方位角'].fillna(inplace=True, value=0.001)
    df.sort_values(by=['方位角'], inplace=True, ascending=True)
    df.reset_index(drop=True, inplace=True)
    df['扇区编号'] = ""
    grouped = df.groupby('物理站编号')
    group_number = len(grouped)
    index0 = 0
    prg = 0
    for s_number, g in grouped:
        # directions = []
        if s_number == 'HSX03394':
            print()
        systems = g['网络制式'].unique().tolist()
        if len(systems) == 1 and systems[0] == '4G':
            for label, row in g.iterrows():
                direction = float(row['方位角'])
                if direction < 0:
                    direction = direction + 360
                # band = row['小区频段']
                # if str(band) == 'nan':
                #     continue
                if direction < 120:
                    df.at[label, '扇区编号'] = 1
                elif 120 <= direction <= 240:
                    df.at[label, '扇区编号'] = 2
                else:
                    df.at[label, '扇区编号'] = 3
            continue
        if str(s_number) == 'nan':
            continue
        index0 = index0 + 1
        c_prg = round(index0 / group_number, 3)
        # print(index0)
        if c_prg > prg:
            print("{:.2f}%".format(c_prg * 100))
            prg = c_prg
        # g5_directions = g[g['频段'].str.contains('NR', na=False)]['方位角'].unique().tolist()
        g5_direction_df, g4_direction_df = get_g5_directions(g)
        # for index, d in enumerate(g5_directions):
        g5_sector_index = 0
        for label, row in g.iterrows():
            if g.empty:
                continue
            if row['小区频段'] in ['2.6G', '700M', '4.9G']:
                df.loc[label, '扇区编号'] = g5_sector_index + 1
                g5_sector_index = g5_sector_index + 1
        g5_direction_df, g4_direction_df = get_g5_directions(g)
        g5_directions = g5_direction_df['方位角'].tolist()
        new_g4_sector_index = 0
        for label, row in g4_direction_df.iterrows():
            if row['小区频段'] in ['2.6G', '700M', '4.9G']:
                continue
            g4_direction = row['方位角']

            able_to_allocate = False
            judge_res = []
            for index, d in enumerate(g5_directions):
                # g5_direction = g5_directions[index]
                # 如果小于最小的一个5G角度，判断是sector1还是最大扇区，主要与最大角度和最小角度做判断
                if index == 0:
                    down_d = g5_directions[0]
                    up_d = g5_directions[len(g5_directions) - 1]
                    diff, allocate_index = judge_sector(down_d, 1, up_d, len(g5_directions), g4_direction)
                elif 0 < index <= len(g5_directions) - 1:
                    down_d = g5_directions[index - 1]
                    up_d = g5_directions[index]
                    diff, allocate_index = judge_sector(down_d, index, up_d, index + 1, g4_direction)
                else:
                    raise Exception("未考虑情况")
                judge_res.append((diff, allocate_index))


            min_diff = None
            decision_index = None
            for index, res in enumerate(judge_res):
                allocate_diff = res[0]
                allocate_index = res[1]
                if allocate_diff is not None and min_diff is None:
                    min_diff = allocate_diff
                    decision_index = allocate_index
                elif min_diff is not None and  allocate_diff is not None and allocate_diff < min_diff:
                    min_diff = allocate_diff
                    decision_index = allocate_index

            if min_diff is None:
                df.loc[label, '扇区编号'] = new_g4_sector_index + 1
                new_g4_sector_index = new_g4_sector_index + 1
            else:
                df.loc[label, '扇区编号'] = decision_index
            judge_res = []
    # df.to_csv("C:\\Users\\orzmlx\\Desktop\\chinamobile\\小区共扇区编号.csv", index=False, encoding='utf_8_sig')
    return df


def judge_sector(g5_down_direction, down_sector_index, g5_up_direction, up_sector_index,
                 g4_direction):
    """
    返回扇区编号
    :param g5_down_direction:
    :param down_sector_index:
    :param g5_directions:
    :param g4_direction: 
    :return: 
    """
    if g5_up_direction < g5_down_direction:
        raise Exception("5G上下方位角错误")
    down_diff = get_direction_diff(g4_direction, g5_down_direction)
    up_diff = get_direction_diff(g5_up_direction, g4_direction)
    if down_diff <= 60 and up_diff <= 60:
        if down_diff == up_diff <= 60:
            return 60, down_sector_index
        if down_diff <= up_diff:
            return down_diff, down_sector_index
        else:
            return up_diff, up_sector_index
    elif down_diff <= 60 <= up_diff:
        return down_diff, down_sector_index
    elif down_diff >= 60 >= up_diff:
        return up_diff, up_sector_index
    # 这里重新生成一个新扇区
    elif down_diff > 60 and up_diff > 60:
        return None, None
    else:
        raise Exception("未定义的4G和5G方位角关系")


def is_high_backflow_cell(row):
    if str(row['网络制式']) != '4G':
        return np.nan
    if 'nan' == str(row['倒流时长']):
        return np.nan
    return '是' if float(row['倒流时长']) > 20 else np.nan


def direction_diff(row):
    if 'nan' != row['4G高倒流方位角'] and 'nan' != row['方位角']:
        advise = np.nan
        direction_diff = float(row['4G高倒流方位角']) - float(row['方位角'])
        if abs(direction_diff) > 30:
            advise = '4G高倒流小区' + row['CGI_4G'] + '与5G小区' + row['CGI'] + '方位角差' + str(direction_diff) \
                     + '建议调整方位角减少45G覆盖差异'
        return direction_diff, advise
    else:
        return np.nan, np.nan


if __name__ == '__main__':
    pandas_monkeypatch()
    df = pd.read_excel('C:\\Users\\orzmlx\\Desktop\\chinamobile\\优化清单-5月.xlsx', sheet_name='小区清单',
                       engine='calamine')
    g5_common_df = pd.read_csv('C:\\Users\\orzmlx\\Desktop\\chinamobile\\5G资源大表0613.csv', encoding='gbk',
                               usecols=['CGI', '总俯仰角', '方位角', 'AAU挂高', '工作频段'], dtype=str)
    g5_common_df['工作频段'] = g5_common_df['工作频段'].map({"NR-D": '2.6G', 'NR-C': '4.9G', 'NR-700': "700M"})
    g5_common_df.rename(columns={'AAU挂高': '挂高', '总俯仰角': '倾角', '工作频段': '小区频段'}, inplace=True)
    g4_common_df = pd.read_csv('C:\\Users\\orzmlx\\Desktop\\chinamobile\\LTE资源大表0613.csv', encoding='gbk',
                               usecols=['小区CGI', '天线挂高_1', '天线方向角_1', '总下倾角_1', '工作频段'], dtype=str)
    g4_common_df.rename(columns={'小区CGI': 'CGI', '天线挂高_1': '挂高', '天线方向角_1': '方位角', '总下倾角_1': '倾角', '工作频段': '小区频段'},
                        inplace=True)

    g5_sector_df = df[df['网络制式'] == '5G']
    g4_sector_df = df[df['网络制式'] == '4G']
    g5_sector_df = g5_sector_df.merge(g5_common_df, how='left', on=['CGI'])
    g4_sector_df = g4_sector_df.merge(g4_common_df, how='left', on=['CGI'])
    df = pd.concat([g5_sector_df, g4_sector_df], axis=0)
    df[['挂高', '方位角', '倾角']] = df[['挂高', '方位角', '倾角']].astype(float)
    # 填充类型必须相同
    df.loc[:, ['挂高', '方位角', '倾角']] = df.loc[:, ['挂高', '方位角', '倾角']].fillna(float(0))
    sector_df = output_share_sector_number(df)
    # sector_df.to_csv("C:\\Users\\orzmlx\\Desktop\\chinamobile\\共扇区方案.csv", index=False, encoding='utf_8_sig')

    # group_cols = ['物理站编号', '共扇区编号']
    # sector_df = pd.read_csv('C:\\Users\\orzmlx\\Desktop\\chinamobile\\共扇区方案.csv')
    # # sector_df = pd.read_excel('C:\\Users\\orzmlx\\Desktop\\chinamobile\\方案输入.xlsx', sheet_name='小区清单',
    # #                           engine='calamine')
    # # sector_df['高倒流小区'] = sector_df[['网络制式', '倒流时长']].apply(is_high_backflow_cell, axis=1)
    #
    # height_plan_df = output_height_plan(sector_df, group_cols)
    # direction_plan_df = output_direction_plan(sector_df, group_cols)
    # tilt_plan_df = output_tilt_plan(sector_df, 3, group_cols)


    # sector_df['高倒流小区'] = np.nan

    # direction_plan_df.rename(columns={'CGI_5G': 'CGI'}, inplace=True)
    # sector_df = sector_df.merge(direction_plan_df[['CGI', '方位角_4G', 'CGI_4G']], on='CGI', how='left')
    # sector_df.rename(columns=dict(方位角_4G='4G高倒流方位角'), inplace=True)
    # sector_df[['方位角差值', '方位角建议优化方案']] = sector_df.apply(direction_diff, axis=1, result_type='expand')
    # sector_df.drop('CGI_4G', axis=1, inplace=True)
    print()
