# -*- coding:utf-8 -*-

import pandas as pd
import numpy as np

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


def get_g5_directions(df):
    result = []
    nrd_directions = df[df['频段'].str.contains('NR-D', na=False)]['方位角'].unique().tolist()
    nr700_directions = df[df['频段'].str.contains('NR-700', na=False)]['方位角'].unique().tolist()
    nrc_directions = df[df['频段'].str.contains('NR-C', na=False)]['方位角'].unique().tolist()
    result.extend(nrd_directions)
    result.extend(nr700_directions)
    result.extend(nrc_directions)
    # if len(g5_directions) == 0:
    #     g5_directions = df[df['频段'].str.contains('NR-700', na=False)]['方位角'].unique().tolist()
    #     if len(g5_directions) == 0:
    #         g5_directions = df[df['频段'].str.contains('NR-C', na=False)]['方位角'].unique().tolist()
    result.sort()
    return result


def output_tilt_plan(df, change_value):
    grouped = df.groupby(['问题点物理点编号', 'label'])
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
        bands = g['频段'].unique().tolist()
        directions = g['方位角'].unique().tolist()
        if len(directions) == 1 and directions[0] == 0.01:
            continue
            # 全是4G站点
        if len(set(['NR-700', 'NR-D', 'NR-C']) & set(bands)) == 0:
            continue
            # 全是5G站点
        if len(bands) == 3 and (set(['NR-700', 'NR-D', 'NR-C']) <= set(bands)):
            continue
        g4_sectors = g[~g['频段'].isin(['NR-700', 'NR-D', 'NR-C'])]
        g5_sectors = g[g['频段'].isin(['NR-700', 'NR-D', 'NR-C'])]
        # 元组嵌套的方法输出结果
        for label_4g, row_4g in g4_sectors.iterrows():
            cgi_4g = row_4g['CGI']
            scene_number_4g = row_4g['场景编号']
            daoliu_4g = row_4g['倒流时长(H)']
            direction_4g = row_4g['方位角']
            problem_4g = row_4g['问题来源']
            if str(daoliu_4g) == 'nan' or float(daoliu_4g) < 20:
                continue
            tilt_4g = row_4g['倾角']
            for label_5g, row_5g in g5_sectors.iterrows():
                cgi_5g = row_5g['CGI']
                tilt_5g = row_5g['倾角']
                height_5g = row_5g['挂高']
                direction_5g = row_5g['方位角']
                scene_number_5g = row_5g['场景编号']
                problem_5g = row_5g['问题来源']
                if problem_5g != problem_4g:
                    continue
                if tilt_5g - tilt_4g < 3:
                    continue
                new_tilt_5g = tilt_5g - change_value
                if str(height_5g) == 'nan':
                    continue
                if height_5g < 20:
                    result.append(
                        (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '合格',
                         label)) if 3 <= tilt_5g <= 10 else result.append(
                        (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '不合格', label))
                elif 20 <= height_5g < 30:
                    result.append(
                        (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '合格',
                         label)) if 5 <= tilt_5g <= 12 else result.append(
                        (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '不合格', label))

                elif 30 <= height_5g < 40:
                    result.append(
                        (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '合格',
                         label)) if 8 <= tilt_5g <= 15 else result.append(
                        (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '不合格', label))
                elif 40 <= height_5g < 50:
                    result.append(
                        (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g,
                         '合格', label)) if 12 <= tilt_5g <= 20 else result.append(
                        (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '不合格', label))
                elif 50 <= height_5g < 60:
                    result.append(
                        (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g,
                         '合格', label)) if 15 <= tilt_5g <= 30 else result.append(
                        (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '不合格', label))
                else:
                    result.append(
                        (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g,
                         '合格', label)) if 20 <= tilt_5g <= 35 else result.append(
                        (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         direction_5g, tilt_5g, new_tilt_5g, height_5g, '不合格', label))
                    # result.append(
                    #     (s_number, cgi_4g, direction_4g, daoliu_4g, cgi_5g, direction_5g))
    res_df = pd.DataFrame.from_records(result,
                                       columns=['共扇区标号', '场景编号_4G', 'CGI_4G', '方位角_4G', '倒流时长', '场景编号_5G',
                                                'CGI_5G', '方位角_5G', '调整前5G倾角', '调整后5G倾角', '挂高_5G', '合规', 'label'])
    res_df.to_csv("C:\\Users\\No.1\\Desktop\\teleccom\\倾角方案.csv", index=False, encoding='utf_8_sig')


def output_direction_plan(df):
    grouped = df.groupby(['问题点物理点编号', 'label'])
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
        bands = g['频段'].unique().tolist()
        directions = g['方位角'].unique().tolist()
        if len(directions) == 1 and directions[0] == 0.01:
            continue
            # 全是4G站点
        if len(set(['NR-700', 'NR-D', 'NR-C']) & set(bands)) == 0:
            continue
            # 全是5G站点
        if len(bands) == 3 and (set(['NR-700', 'NR-D', 'NR-C']) <= set(bands)):
            continue
        g4_sectors = g[~g['频段'].isin(['NR-700', 'NR-D', 'NR-C'])]
        g5_sectors = g[g['频段'].isin(['NR-700', 'NR-D', 'NR-C'])]
        # 元组嵌套的方法输出结果
        for label_4g, row_4g in g4_sectors.iterrows():
            cgi_4g = row_4g['CGI']
            daoliu_4g = row_4g['倒流时长(H)']
            scene_number_4g = row_4g['场景编号']
            problem_4g = row_4g['问题来源']
            if str(daoliu_4g) == 'nan' or float(daoliu_4g) < 20:
                continue
            direction_4g = row_4g['方位角']
            for label_5g, row_5g in g5_sectors.iterrows():
                cgi_5g = row_5g['CGI']
                direction_5g = row_5g['方位角']
                scene_number_5g = row_5g['场景编号']
                problem_5g = row_5g['问题来源']
                if problem_5g != problem_4g:
                    continue
                if abs(direction_4g - direction_5g) >= 30:
                    result.append(
                        (number, scene_number_4g, cgi_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         direction_5g, label))
    res_df = pd.DataFrame.from_records(result,
                                       columns=['共扇区标号', '场景编号_4G', 'CGI_4G', '方位角_4G', '倒流时长', '场景编号_5G',
                                                'CGI_5G', '方位角_5G', 'label'])
    res_df.to_csv("C:\\Users\\No.1\\Desktop\\teleccom\\方位角方案.csv", index=False, encoding='utf_8_sig')


def output_height_plan(df):
    grouped = df.groupby(['问题点物理点编号', 'label'])
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
        bands = g['频段'].unique().tolist()
        directions = g['方位角'].unique().tolist()
        if len(directions) == 1 and directions[0] == 0.01:
            continue
        # 全是4G站点
        if len(set(['NR-700', 'NR-D', 'NR-C']) & set(bands)) == 0:
            continue
        # 全是5G站点
        if len(bands) == 3 and (set(['NR-700', 'NR-D', 'NR-C']) <= set(bands)):
            continue
        g4_sectors = g[~g['频段'].isin(['NR-700', 'NR-D', 'NR-C'])]
        g5_sectors = g[g['频段'].isin(['NR-700', 'NR-D', 'NR-C'])]
        # 元组嵌套的方法输出结果
        for label_4g, row_4g in g4_sectors.iterrows():
            cgi_4g = row_4g['CGI']
            height_4g = row_4g['挂高']
            daoliu_4g = row_4g['倒流时长(H)']
            scene_number_4g = row_4g['场景编号']
            problem_4g = row_4g['问题来源']
            if str(height_4g) == 'nan' or float(height_4g) < 20:
                continue
            if str(daoliu_4g) == 'nan' or float(daoliu_4g) < 20:
                continue
            direction_4g = row_4g['方位角']
            for label_5g, row_5g in g5_sectors.iterrows():
                cgi_5g = row_5g['CGI']
                height_5g = row_5g['挂高']
                direction_5g = row_5g['方位角']
                problem_5g = row_5g['问题来源']
                if str(height_5g) == 'nan' or float(height_5g) < 20:
                    continue
                scene_number_5g = row_5g['场景编号']
                if problem_5g != problem_4g:
                    continue
                if height_4g - height_5g >= 5:
                    result.append(
                        (number, scene_number_4g, cgi_4g, height_4g, direction_4g, daoliu_4g, scene_number_5g, cgi_5g,
                         height_5g, direction_5g, label))
    res_df = pd.DataFrame.from_records(result,
                                       columns=['共扇区标号', '场景编号_4G', 'CGI_4G', '挂高_4G', '方位角_4G', '倒流时长', '场景编号_5G',
                                                'CGI_5G', '挂高_5G', '方位角_5G', 'label'])
    res_df.to_csv("C:\\Users\\No.1\\Desktop\\teleccom\\挂高提升方案.csv", index=False, encoding='utf_8_sig')


def output_share_sector_number(df):
    # cut_bins = [-10, 0, 60, 120, 180, 240, 300, 360]
    # df = pd.read_csv(path, encoding='gbk')
    df['方位角'].fillna(inplace=True, value=0.001)
    df.sort_values(by=['方位角'], inplace=True, ascending=True)
    df.reset_index(drop=True, inplace=True)
    df['label'] = ""
    grouped = df.groupby('问题点物理点编号')
    group_number = len(grouped)
    index0 = 0
    prg = 0
    for s_number, g in grouped:
        if s_number == '居民区POI联调HHU00300':
            print()
        systems = g['制式'].unique().tolist()
        if len(systems) == 1 and systems[0] == '4G':
            for label, row in g.iterrows():
                direction = float(row['方位角'])
                if direction < 0:
                    direction = direction + 360
                band = row['频段']
                if str(band) == 'nan':
                    continue
                if direction < 120:
                    df.at[label, 'label'] = 1
                elif 120 <= direction <= 240:
                    df.at[label, 'label'] = 2
                else:
                    df.at[label, 'label'] = 3
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
        g5_directions = get_g5_directions(g)
        # for index, d in enumerate(g5_directions):
        for label, row in g.iterrows():
            # for row in g.itertuples():
            direction = row['方位角']
            band = row['频段']
            if direction == 0.001:
                continue
            if str(band) == 'nan':
                continue
            for index, d in enumerate(g5_directions):
                # 因为所有都是顺序排列
                if direction == d and band.find('NR') >= 0:
                    df.at[label, 'label'] = index + 1
                    break
                if g5_directions[len(g5_directions) - 1] < direction <= 360:
                    up_d = g5_directions[len(g5_directions) - 1]
                    down_d = g5_directions[0]
                    up_diff = get_direction_diff(up_d, direction)
                    down_diff = get_direction_diff(direction, down_d)
                    if up_diff > down_diff:
                        value = 1
                        if down_diff > 60:
                            value = value + 10
                    else:
                        value = len(g5_directions)
                        # df.at[label, 'label'] = len(g5_directions)
                        if up_diff > 60:
                            value = value + 10
                    df.at[label, 'label'] = value
                    break
                if direction <= d:
                    if index == 0:
                        up_d = g5_directions[len(g5_directions) - 1]
                    else:
                        up_d = g5_directions[index - 1]
                    down_d = d
                    up_diff = get_direction_diff(up_d, direction)
                    down_diff = get_direction_diff(direction, down_d)
                    if up_diff > down_diff:
                        if index == 0:
                            value = 1
                        else:
                            value = index + 1
                        if down_diff > 60:
                            value = value + 10
                        df.at[label, 'label'] = value
                    else:
                        if index == 0:
                            value = len(g5_directions)
                        else:
                            value = index
                        if up_diff > 60:
                            value = value + 10
                        df.at[label, 'label'] = value
                    break
    df.to_csv("C:\\Users\\No.1\\Desktop\\teleccom\\小区共扇区编号.csv", index=False, encoding='utf_8_sig')


if __name__ == '__main__':
    # df = pd.read_csv('C:\\Users\\No.1\\Desktop\\teleccom\\方案.csv')
    # output_share_sector_number(df)
    df = pd.read_csv('C:\\Users\\No.1\\Desktop\\teleccom\\小区共扇区编号.csv')
    output_height_plan(df)
    output_direction_plan(df)
    output_tilt_plan(df, 3)
