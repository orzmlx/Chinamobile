import pandas as pd
import os
import openpyxl
import huaweiutils
import copy
import zteutils
from timer import Timer
import logging
import sys
from tqdm import tqdm
import xlrd
import numpy as np

os.environ["MODIN_ENGINE"] = "dask"
import modin.pandas as md
import polars as pl

# import modin.pandas as pd
logging.basicConfig(format='%(asctime)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S', level=logging.INFO)


class ZteRawDataReader:
    def __init__(self, raw_file, zte_raw_data_path, zte_config_file_path, system, manufacturer):
        self.zte_config_file_path = zte_config_file_path
        self.zte_raw_data_path = zte_raw_data_path
        self.system = system
        self.raw_file = raw_file
        self.manufacturer = manufacturer
        # raw_output_path = os.path.join(zte_raw_data_path, system, f_name)
        self.temp_path = os.path.join(zte_raw_data_path, system, raw_file.name.split('.')[0], "temp")
        if not os.path.exists(self.temp_path):
            os.makedirs(self.temp_path)

    def depart_sheet(self, zte_config_file_path, zte_raw_data_path, system):
        zte_demand_param = pd.read_excel(zte_config_file_path, engine='openpyxl', sheet_name='需求参数')
        zte_demand_param = zte_demand_param[zte_demand_param['厂家'] == self.manufacturer]
        logging.info("开始分离原始文件:" + os.path.split(self.raw_file)[1])
        f_name = os.path.basename(self.raw_file).split('.')[0]
        # 获取上一级目录,将原始数据的读取结果存放在上一级目录
        raw_output_path = os.path.join(zte_raw_data_path, system, f_name)
        for j in tqdm(range(len(zte_demand_param)), desc="分离sheet表进度"):
            timer = Timer()
            row = zte_demand_param.iloc[j]
            sheet_name = row['CSV名'].strip()
            # 读取每一个sheet
            sheet_df = self.read_raw_data(row, self.raw_file)
            sheet_df.to_pickle(os.path.join(raw_output_path, sheet_name))
            timer.stop()
            logging.info(sheet_name + f':{timer.stop():.2f} sec')

    def output_format_data(self):
        self.depart_sheet(self.zte_config_file_path, self.zte_raw_data_path, self.system)
        self.clean_data(self.zte_config_file_path, self.zte_raw_data_path, self.system)
        zte_param_match = pd.read_excel(self.zte_config_file_path, engine='openpyxl', sheet_name='数据匹配')
        zte_param_match = zte_param_match[zte_param_match['厂家'] == self.manufacturer]
        self.match_zte_data(zte_param_match)
        zte_param_gather = pd.read_excel(self.zte_config_file_path, engine='openpyxl', sheet_name='数据汇聚')
        zte_param_gather = zte_param_gather[zte_param_gather['厂家'] == self.manufacturer]
        self.gather_files(zte_param_gather)

    def clean_data(self, zte_config_file_path, zte_raw_data_path, system):
        zte_demand_param = pd.read_excel(zte_config_file_path, engine='openpyxl', sheet_name='需求参数')
        zte_demand_param = zte_demand_param[zte_demand_param['厂家'] == self.manufacturer]
        zte_param_process = pd.read_excel(zte_config_file_path, engine='openpyxl', sheet_name='数据清理')
        zte_param_process = zte_param_process[zte_param_process['厂家'] == self.manufacturer]
        # 根据配置中需要的文件名,来读取中兴原始文件中的数据
        demand_params = zte_demand_param['CSV名'].unique().tolist()
        f = self.raw_file
        f_name = os.path.basename(f).split('.')[0]
        # 获取上一级目录,将原始数据的读取结果存放在上一级目录
        raw_output_path = os.path.join(zte_raw_data_path, system, f_name)
        for j in tqdm(range(len(demand_params)), desc="原始文件:" + f_name + "标清理进度"):
            # logging.info("\n")
            demand_param = demand_params[j]
            sheet_name = demand_params[j].strip()
            # 读取每一个sheet
            sheet_df = pd.read_pickle(os.path.join(raw_output_path, sheet_name))
            # sheet_df = self.read_raw_data(row, f)
            # 在处理流程中寻找如何处理该数据
            temp_file_dir = os.path.join(raw_output_path, 'temp')
            if not os.path.exists(temp_file_dir):
                os.makedirs(temp_file_dir)
            self.process_data_by_sheet(sheet_df, zte_param_process, sheet_name, temp_file_dir)

    def process_data(self, demand_param, raw_output_path, zte_param_process):

        # logging.info("\n")
        sheet_name = demand_param.strip()
        # 读取每一个sheet
        sheet_df = pd.read_pickle(os.path.join(raw_output_path, sheet_name))
        # sheet_df = self.read_raw_data(row, f)
        # 在处理流程中寻找如何处理该数据
        self.process_data_by_sheet(sheet_df, zte_param_process, sheet_name, raw_output_path)

    def gather_files(self, gather_df):
        grouped = gather_df.groupby('另存文件名')
        for new_file_name, g in grouped:
            relative_index = 0
            merge_result = pd.DataFrame()
            gather_out_dir = os.path.join(os.path.split(self.temp_path)[0], 'raw_result')
            if not os.path.exists(gather_out_dir):
                os.makedirs(gather_out_dir)
            for index, row in g.iterrows():
                left_file_name = row['CSV名']
                left_file_cols = row['合并字段']
                on = row['匹配字段']
                renames = row['字段更名']
                rename_dict = {}
                left_file_cols = left_file_cols.split('|') if left_file_cols.find('|') >= 0 else [left_file_cols]
                if renames is not None:
                    renames = renames.split('|') if renames.find('|') >= 0 else [renames]
                    assert len(left_file_cols) == len(renames), "数据汇聚中列名数量和改名不对应【数量不相等】"
                    rename_dict = dict(zip(left_file_cols, renames))
                on = on.split('|') if on.find('|') >= 0 else [on]

                if not '行合并' in on:
                    left_file_cols.extend(on)
                left_file_cols = list(set([x.strip() for x in left_file_cols if x]))
                left_file_path = os.path.join(self.temp_path, left_file_name + '.csv')
                left_file_df = pd.read_csv(left_file_path, usecols=left_file_cols)
                if relative_index == 0:
                    merge_result = left_file_df
                else:
                    if '行合并' in on:
                        merge_result = pd.concat([merge_result, left_file_df], axis=0)
                    else:
                        merge_result = merge_result.merge(left_file_df, on=on, how='outer')
                relative_index = relative_index + 1
                if len(rename_dict) > 0:
                    merge_result.rename(columns=rename_dict, inplace=True)
            # self.mapToband(merge_result)
            gather_out_path = os.path.join(self.temp_path, new_file_name + '.csv')
            merge_result.to_csv(os.path.join(gather_out_dir, new_file_name + '.csv'), index=False, encoding='utf_8_sig')
            merge_result.to_csv(gather_out_path, index=False, encoding='utf_8_sig')

    def process_data_by_sheet(self, sheet_df, zte_param_process, sheet_name, raw_output_path, process_split_char=','):
        sheet_process = zte_param_process[zte_param_process['CSV名'] == sheet_name]
        if sheet_process.empty:
            logging.info(sheet_name + '不需要进行数据清洗')
            return
        copy_df = sheet_df.copy(deep=True)
        for row in sheet_process.itertuples():
            index = row.Index + 2
            action_name = row[3].strip() if str(row[3]) != 'nan' else row[3]
            new_file_name = row[5].strip() if str(row[5]) != 'nan' else row[5]
            process = row[2].strip() if str(row[2]) != 'nan' else row[2]
            new_col_name = row[4].strip() if str(row[4]) != 'nan' else row[4]
            copy_df = self.process_action(copy_df, action_name, process, new_col_name, index, process_split_char)
            # if process is None and action_name is None and new_file_name is not None:
            #     temp_file_path = os.path.join(raw_output_path, new_file_name + '.csv')
            #     copy_df.to_csv(temp_file_path.strip(), index=False, encoding='utf_8_sig')
            if str(new_file_name) != 'nan' and raw_output_path is not None:
                temp_file_path = os.path.join(raw_output_path, new_file_name + '.csv')
                if str(process) == 'nan' and str(action_name) == 'nan':
                    copy_df.to_csv(temp_file_path.strip(), index=False, encoding='utf_8_sig')
                else:
                    copy_df.to_csv(temp_file_path.strip(), index=False, encoding='utf_8_sig')
                    # 如果需要产生新的文件,那么接下来算是新的循环，使用新的文件
                    copy_df = sheet_df.copy(deep=True)

    def process_action(self, sheet_df, action_name, process, new_col_name, index, split_char=','):
        if str(process) == 'nan':
            logging.info('没有发现任何处理,行号:' + str(index))
            return sheet_df
        row_processes = process.split(split_char) if process.find(split_char) >= 0 else [process]
        action_tuple_list = []
        for p in row_processes:
            exist_operators = zteutils.find_operators(p)
            # 合并的时候没有配置新的列名,抛出异常
            if str(new_col_name) == 'nan' and action_name == '合并':
                raise Exception("参数处理设置错误,一个列处理中出现多个算子或者没有发现算子:" + str(exist_operators) + ",出错行数:" + str(index))
            elif len(exist_operators) == 0:
                action_tuple = [p, None, None]
            else:
                operator = exist_operators[0].strip()
                param_process = p.split(operator)
                col_name = param_process[0].strip()
                action_range = param_process[1].strip()
                action_tuple = [col_name, action_range, operator]
            action_tuple_list.append(action_tuple)
        union_action_df = zteutils.union_action(sheet_df, action_tuple_list, action_name, new_col_name, index)
        return union_action_df

    def read_raw_data(self, row, f):
        sheet_name = str(row['CSV名']).strip()
        header_row = row['参数行']
        data_start_row = row['数据首行'] - header_row - 1
        demand_cols = row['所需字段'].split('|') if row['所需字段'].find('|') >= 0 else row[4]
        demand_cols = list(filter(lambda x: x is not None and x != '', demand_cols))
        # 有可能会有超过100万行的sheet,导致改表在excel中分表
        concat_result = pd.DataFrame()
        for sheet_index in range(1, sys.maxsize):
            timer = Timer()
            try:
                # 使用Modin pandas加快读取excel速度
                # csv_df = md.read_excel(f,  engine='openpyxl',sheet_name=sheet_name,
                #                        usecols=demand_cols, header=header_row - 1,
                #                        dtype='str')
                csv_df = pl.read_excel(f, sheet_name=sheet_name,
                                       xlsx2csv_options={"skip_empty_lines": True},
                                       read_csv_options={"has_header": True,
                                                         "skip_rows": int(header_row) - 1,
                                                         "columns": demand_cols,
                                                         'ignore_errors': True,
                                                         "batch_size": 500})
                csv_df = csv_df.to_pandas()
                if sheet_index >= 2:
                    logging.info("sheet:【" + sheet_name + "】:>>>存在超过100万行分表情况!!!<<<")
            except Exception as e:
                logging.info("sheet:【" + sheet_name + "】:不存超过100万行分表情况!")
                break
            # finally:
            timer.stop()
            logging.info(sheet_name + f':{timer.stop():.2f} sec')
            del_row_numbers = list(range(0, data_start_row, 1))
            csv_df.drop(del_row_numbers, axis=0, inplace=True)
            concat_result = csv_df if concat_result.empty else pd.concat([concat_result, csv_df], axis=0)
            sheet_name = sheet_name + " (" + str(sheet_index) + ")"
        concat_result.reset_index(inplace=True, drop=True)
        return concat_result

    def valid_and_process_configuration(self, row, right_file_df, left_file_df, index):
        right_file_cols = right_file_df.columns.tolist()
        left_file_cols = left_file_df.columns.tolist()
        left_on = row['参数名1'].strip()
        right_on = row['参数名2'].strip()
        right_file = row['匹配文件'].strip()
        left_file = row['主文件'].strip()
        # right_file_demand = row['所需字段'].strip()
        right_file_demand = row['所需字段'].strip() if str(row['所需字段']) != 'nan' else '|'.join(
            right_file_df.columns.tolist())

        right_demand_rename = row['字段更名'].strip() if str(row['字段更名']) != 'nan' else '|'.join(
            right_file_df.columns.tolist())

        right_demand_names = right_file_demand.split('|') if right_file_demand.find('|') >= 0 else [right_file_demand]
        right_demand_renames = right_demand_rename.split('|') if right_demand_rename.find('|') >= 0 else [
            right_demand_rename]
        if len(right_demand_names) != len(right_demand_renames):
            raise Exception("所需字段和字段更名不相对应,出错行数:" + str(index))
        # 右表重命名
        for id, name in enumerate(right_demand_names):
            if name not in right_file_cols:
                raise Exception("列名:【" + name + "】不在【" + right_file + "】文件中,出错行号:" + str(index))

            right_file_df.rename(columns={name: right_demand_renames[id]}, inplace=True)
        right_ons = right_on.split('|') if right_on.find('|') >= 0 else [right_on]
        left_ons = left_on.split('|') if left_on.find('|') >= 0 else [left_on]
        assert len(right_ons) == len(left_ons), '数据匹配中两张表的匹配列长度不同'
        for on in left_ons:
            if on not in left_file_cols:
                raise Exception("列名:【" + on + "】不在【" + left_file + "】文件中,出错行号:" + str(index))
        for index, on in enumerate(right_ons):
            # 如果on不在左表中,直接改名
            if on not in right_file_cols:
                raise Exception("列名:【" + on + "】不在【" + right_file + "】文件中,出错行号:" + str(index))
            if on != left_ons[index]:
                logging.info("列名:【" + on + "】与左表对应列名【" + left_ons[index] + "】不相同,这里将改名与坐标保持一致,问题行号:" + str(index))
                right_file_df.rename(columns={on: left_ons[index]}, inplace=True)

    def match_zte_data(self, zte_param_match):
        grouped = zte_param_match.groupby('主文件', sort=False)
        for left_file, g in grouped:
            logging.info("开始匹配文件:" + left_file)
            left_file_path = os.path.join(self.temp_path, left_file.strip() + '.csv')
            try:
                left_file_df = pd.read_csv(left_file_path, dtype='str', encoding='gbk')
            except Exception as e:
                left_file_df = pd.read_csv(left_file_path, dtype='str')
            for index, row in g.iterrows():
                index = index + 2
                left_on = row['参数名1'].strip()
                right_on = row['参数名2'].strip()
                right_on = right_on.split('|') if right_on.find('|') >= 0 else [right_on]
                left_on = left_on.split('|') if left_on.find('|') >= 0 else [left_on]
                right_file = row['匹配文件'].strip()
                if right_file == '地市':
                    city_df = pd.read_excel(self.zte_config_file_path, sheet_name='地市关系', engine='openpyxl',
                                            dtype='str')
                    city_df = city_df[city_df['制式'] == self.system][['地市', '子网ID']]
                    left_file_df = left_file_df.merge(city_df, how='left', on=right_on)
                    left_file_df.to_csv(left_file_path, index=False, encoding='utf_8_sig')
                    continue
                elif right_file == '5G频点关系':
                    right_file_df = pd.read_excel(self.zte_config_file_path, engine='openpyxl', sheet_name='5G频点关系')
                    band_bins = self.get_band_bin(right_file_df, right_on[0])
                    band_col = left_on[0]
                    left_file_df[band_col] = left_file_df[band_col].apply(float)
                    left_file_df[band_col] = pd.cut(x=left_file_df[band_col], bins=band_bins)
                    left_file_df[band_col] = left_file_df[band_col].apply(str)
                elif right_file == '4G频点关系':
                    right_file_df = pd.read_excel(self.zte_config_file_path, engine='openpyxl', sheet_name='4G频点关系',
                                                  dtype=str)
                    # band_col = '中心载频'
                    # band_bins = self.get_band_bin(right_file_df, band_col)
                    # left_file_df[band_col] = left_file_df[band_col].apply(float)
                    # left_file_df[band_col] = pd.cut(x=left_file_df[band_col], bins=band_bins)
                    # left_file_df[band_col] = left_file_df[band_col].apply(str)
                else:
                    right_file = os.path.join(self.temp_path, right_file + '.csv')
                    right_file_df = pd.read_csv(right_file, dtype='str')
                # 这里因为valid_and_process后会改名，所以这里直接筛选改名后的列名
                right_file_renames = row['字段更名'].strip() if str(row['字段更名']) != 'nan' else '|'.join(
                    right_file_df.columns.tolist())
                # right_file_renames = row['字段更名'].strip()
                self.valid_and_process_configuration(row, right_file_df, left_file_df, index)
                right_demand_names = right_file_renames.split('|') if right_file_renames.find('|') >= 0 else [
                    right_file_renames]
                right_demand_names.extend(left_on)
                right_file_df = right_file_df[right_demand_names]

                if right_file == '4G频点关系':
                    left_file_df = self.map_4g_band(left_file_df, right_file_df)
                else:
                    left_file_df = left_file_df.merge(right_file_df, on=left_on, how='left')
                # match_path = os.path.join(outpath, left_file + '.csv')
                left_file_df.to_csv(left_file_path, index=False, encoding='utf_8_sig')

    def get_band_bin(self, df, band_col_name):
        band_range_list = df[band_col_name].unique().tolist()
        range_bins = []
        for band_range in band_range_list:
            band_range = band_range.replace('(', '').replace(']', '')
            band_range = band_range.split(',')
            range_bins.extend(band_range)
        range_bins = [int(item) for item in range_bins]
        return sorted(range_bins)

    def map_4g_band(self, left_file_df, right_file_df):
        left_file_df['频段'] = np.nan
        index = 0
        for band_identity, center_band in zip(left_file_df['频段指示'], left_file_df['中心载频']):
            center_band = float(center_band)
            for band_identity0, center_band0, band in zip(right_file_df['频段指示'], right_file_df['中心载频'],
                                                          right_file_df['频段']):
                band0 = center_band0.replace('(', '').replace(']', '').split(',')
                min_band = float(band0[0])
                max_band = float(band0[1])
                if center_band > min_band and center_band <= max_band and band_identity0 == band_identity:
                    left_file_df['频段'].iloc[index] = band
                    break
            index = index + 1
        return left_file_df
