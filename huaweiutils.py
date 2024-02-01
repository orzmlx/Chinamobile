import pandas as pd
import logging
import pkgutil
import os
from copy import deepcopy
from difflib import SequenceMatcher
import numpy as np
import pathlib
replace_char = ['秒', 'dB']


def find_file(directory, file_extension):
    files = []
    for item in pathlib.Path(directory).rglob('*'):
        if str(item).endswith(file_extension):
            files.append(item)
    return files


def output_csv(df, file_name, out_path, is_format):
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    if is_format:
        file_name = remove_digit(file_name, ['=', ":"])
    file_out_path = os.path.join(out_path, file_name)
    if file_name in os.listdir(out_path):
        df.to_csv(file_out_path, index=False, mode='a', header=False)
    else:
        df.to_csv(file_out_path, index=False, mode='a', header=True)


def only_has_digtal_diff(str1, str2):
    """
        判断两个字符串不相同的部分
    """
    matcher = SequenceMatcher(None, str1, str2)
    match_codes = matcher.get_opcodes()
    replace_num = 0
    for tag, i, j, m, n in match_codes:
        if tag == 'delete' or tag == 'insert':
            return False
        if tag == 'replace':
            replace_num = replace_num + 1
            if not str1[i].isdigit() or not str2[m].isdigit():
                return False
    if replace_num > 1:
        return False
    return True

def add_4g_cgi(cell_df, enode_df):
    cell_df = pd.merge(cell_df, enode_df[['网元', 'eNodeB标识']], on='网元')
    return cell_df

def add_5g_cgi(ducell_df, gnodeb_df):
    ducell_df = pd.merge(ducell_df, gnodeb_df[['网元', 'gNodeB标识']], on='网元')
    ducell_df.rename(columns={'NRDU小区标识': 'NR小区标识'}, inplace=True)

    print(ducell_df.shape)
    return ducell_df


def remove_digit(str, add_characters):
    """
        去除数字和其它
    """
    result = ""
    if add_characters is None:
        add_characters = []
    for c in str:
        if c.isdigit() or c in add_characters:
            continue
        result += c
    return result


def remove_name(x):
    try:
        if str(x).find(":") < 0:
            return x
        values = str(x).split(':')
        name = values[1]
        return name
    except Exception as e:
        logging.error(e)
        return x


def flatten_features(df, c):
    # 取所有数据的第一行，看是否有子列
    # cols = df.columns.tolist()
    df1 = deepcopy(df)
    values = str(df1[c].iloc[0])
    if values.find("&") >= 0 or (values.find(":") >= 0 and values.find("开关") >= 0):
        edit_df = df1[c]
        df1.drop(c, axis=1, inplace=True)
        if values.find("&") >= 0:
            edit_df = edit_df.astype('str').str.split('&', expand=True)
            current_cols = edit_df.columns.tolist()
        else:
            edit_df = pd.DataFrame(edit_df)
            edit_df.rename(columns={c: 0}, inplace=True)
            current_cols = [0]
        first_row = edit_df.reset_index(drop=True).iloc[0]
        new_cols = []
        start_index = 0
        for col in current_cols:
            cell = first_row[col]
            # 如果有部分为空,那么需要不停往下检查，直到找到非空
            if pd.isna(cell):
                all_values = edit_df[col].tolist()
                for v in all_values:
                    if v is not None:
                        cell = v
                        break
            if cell.find(":") < 0:
                continue
            col_name = cell.split(":")[0]
            new_cols.append(col_name)
            edit_df[col] = edit_df[col].apply(remove_name)
        edit_df.reset_index(drop=True, inplace=True)
        edit_df.columns = new_cols
        df1 = pd.concat([df1, edit_df], axis=1)
    return df1


def parse_strategy_file(path):
    g5_data_strategy = pd.read_excel(path, sheet_name="自身推荐")
    g45_data_strategy = pd.read_excel(path, sheet_name="45G数据")
    g5_data_strategy = g5_data_strategy[g5_data_strategy['事件'] == 'A5']
    g5_data_strategy.drop('事件', axis=1, inplace=True)
    cols = g5_data_strategy.columns.tolist()
    cols0 = g45_data_strategy.columns.tolist()
    keep_name_list = ['频带', '厂家', '覆盖类型']
    for c in cols:
        if c in keep_name_list:
            continue
        g5_data_strategy.rename(columns={c: c + "#合规"}, inplace=True)
    for c in cols0:
        if c in keep_name_list:
            continue
        g45_data_strategy.rename(columns={c: c + "#合规"}, inplace=True)
    return g5_data_strategy, g45_data_strategy


def add_strategy_info(g5_data_strategy_df, g45_data_strategy_df, report_df):
    g5_cols = g5_data_strategy_df.columns.tolist()
    g45_cols = g45_data_strategy_df.columns.tolist()
    sum_cols = g5_cols + g45_cols
    unused_cols = ['厂家', '覆盖类型', '频带']
    feature_cols = [x.strip() for x in sum_cols if x not in unused_cols]
    original_feature_cols = [x.split("#")[0].strip() for x in feature_cols]
    report_cols = report_df.columns.tolist()
    report = report_df.merge(g5_data_strategy_df, how='left', on=['厂家', '覆盖类型', '频带'])
    report = report.merge(g45_data_strategy_df, how='left', on=['厂家', '频带'])
    # 对列重排序
    base_cols = ['网元', 'NR小区标识', 'NRDU小区名称', 'CGI', '频带', '双工模式', '覆盖类型', '覆盖场景', '厂家']
    diff_cols = list(set(report_cols).difference(set(original_feature_cols)))
    rest_cols = set(diff_cols) - set(base_cols)
    base_cols.extend(rest_cols)
    for index, c in enumerate(feature_cols):
        original_name = c.split("#")[0]
        # if report_cols.find(original_name) >= 0:
        if original_name in report_cols:
            new_col = '是否合规' + "_" + str(index)
            report[new_col] = report.apply(add_judgement, axis=1, args=(original_name, c))
            diff_cols.append(original_name)
            diff_cols.append(c)
            diff_cols.append(new_col)
    ##重新对列排序
    return report[diff_cols]


def freq_judge(df, param):
    judge_res = []
    df.rename(columns={"推荐值": param + "#推荐值"}, inplace=True)
    for recommand, value in zip(df[param + "#推荐值"], df[param]):
        value = str(value)
        recommand = str(recommand)
        if value == 'nan':
            judge_res.append("没有找到参考值")
            continue
        if recommand.find('[') >= 0 and recommand.find(']') >= 0:
            judge_res.append(range_judge(value, recommand))
        elif recommand.find(',') >= 0:
            judge_res.append(list_judge(value, recommand))
        else:
            judge_res.append(single_value_judge(value, recommand))
    return judge_res


def mapToBand(x, band_dict):
    for key, item in band_dict.items():
        if str(x) in list(item):  # 证明该频段是4G频段
            return key
    return '其他频段'






def generate_4g_frequency_band_dict(df):

    # df.dropna(axis=0, inplace=True, how='any')
    band_list=[]
    g4_freq_band_dict = {}
    for band, offset, SSB in zip(df['工作频段'], df['频率偏置'], df['中心载频信道号']):
        band = str(band)
        if pd.isna(band):
            continue
        band = band.replace('频段', '')
        if band.find('FDD') >= 0:
            band = str(offset)
            # 如果不是FDD-1800或者FDD-900,那么直接去掉数字
            if str(offset).find('FDD') < 0:
                band = remove_digit(band, [])
        band_list.append(band)
        value_set = g4_freq_band_dict.get(band, set())
        value_set.add(str(SSB))
        g4_freq_band_dict[band] = value_set
    return g4_freq_band_dict,band_list


def merge_dfs(list, on):
    init_df = list[0]
    for i in range(len(list)):
        if i == 0:
            continue
        init_df = init_df.merge(list[i], how='left', on=on)
    return init_df


def single_value_judge(x, standard):
    try:
        for c in replace_char:
            x = str(x).replace(c, "")
        return str(x) == standard
    except:
        print()


def list_judge(x, standard):
    for c in replace_char:
        x = str(x).replace(c, "")
    splits = standard.split(',')
    return True if str(x) in splits else  False


def range_judge(x, standard):
    for c in replace_char:
        x = str(x).replace(c, "")
    standard = standard.replace("[", "").replace("]", "")
    splits = standard.split(",")
    if len(splits) != 2:
        raise Exception("范围推荐值:【" + standard + "】格式不符合要求")
    else:
        try:
            min = int(splits[0])
            max = int(splits[1])
            if min > max:
                temp = max
                max = min
                min = temp
            if min <= int(x) <= max:
                return True
            else:
                return False
        except:
            raise Exception("范围推荐值:【" + standard + "】无法转换为数字")


def add_judgement(x, original_name, c):
    judgement_value = str(x[c])
    original_value = x[original_name]
    if len(judgement_value.strip()) == 0:
        return ""
    if judgement_value.find("~") >= 0:
        splits = judgement_value.split("~")
        value_0 = int(splits[0])
        value_1 = int(splits[1])
        temp = value_1
        if value_0 > value_1:
            value_1 = value_0
            value_0 = temp
        if value_0 < original_value < value_1:
            return True
        elif value_0 == judgement_value or value_1 == judgement_value:
            return judgement_value, True
        else:
            return False
    elif judgement_value == 'nan':
        return True
    elif judgement_value.find("[") >= 0 and judgement_value.find("]") >= 0:
        judgement_value.replace("[", "").replace("]", "")
    else:

        return True if int(float(judgement_value)) == int(
            float(original_value)) else  False


if __name__ == "__main__":
    # report_path = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\all_result.csv"
    # report_df = pd.read_csv(report_path)
    # report_df['频带'] = report_df['频带'].map({"n41": "2.6G", "n28": "700M", "n78": "4.9G", "n79": "4.9G"})
    # strategy_path = "C:\\Users\\No.1\\Desktop\\teleccom\\互操作参数策略.xlsx"
    # g5_data_strategy, g45_data_strategy = parse_strategy_file(strategy_path)
    # report = add_strategy_info(g5_data_strategy, g45_data_strategy, report_df)
    # report_cols = report.columns.tolist()
    # report.to_csv("C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\report_strategy.csv",
    #               index=False, encoding='utf_8_sig')
    str1 = 'LST NRCELLHOEUTRANMEAGRP:INTERRHOTOEUTRANMEASGRPID=2'
    # str2 = 'LST NRCELLINTRAFHOMEAGRPINTRAFREQHOMEASGROUPID=2.csv'
    str2 = 'LST NRCELLQCIBEARER:QCI=5'
    # print(only_has_digtal_diff(str1, str2))
    print(remove_digit(str1, ['=']))


