import pandas as pd
import os
import openpyxl
import huaweiutils
import copy
import zteutils


class ZteRawDataReader:
    def __init__(self, zte_raw_data_path, zte_config_file_path, system):
        self.zte_config_file_path = zte_config_file_path
        self.zte_raw_data_path = zte_raw_data_path
        self.system = system
        self.temp_path = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\zte\\20240121\\5G\\zte_raw_data_test\\temp"


    def output_format_data(self):
        # self.read_zte_data(zte_config_file_path, zte_raw_data_path, system)
        # zte_param_match = pd.read_excel(zte_config_file_path, engine='openpyxl', sheet_name='数据匹配')
        # self.match_zte_data(zte_param_match)
        zte_param_gather = pd.read_excel(self.zte_config_file_path, engine='openpyxl', sheet_name='数据汇聚')
        self.gather_files(zte_param_gather)

    def read_zte_data(self, zte_config_file_path, zte_raw_data_path, system):
        zte_demand_param = pd.read_excel(zte_config_file_path, engine='openpyxl', sheet_name='需求参数')
        zte_param_process = pd.read_excel(zte_config_file_path, engine='openpyxl', sheet_name='数据清理')
        # 根据配置中需要的文件名,来读取中兴原始文件中的数据
        demand_params = zte_demand_param['CVS名'].unique().tolist()
        zte_raw_data_dir = os.path.join(zte_raw_data_path, system, 'raw_data')
        raw_files = huaweiutils.find_file(zte_raw_data_dir, '.xlsx')
        for f in raw_files:
            f_name = os.path.basename(f).split('.')[0]
            # 获取上一级目录,将原始数据的读取结果存放在上一级目录
            raw_output_path = os.path.join(zte_raw_data_path, system, f_name)
            if not os.path.exists(raw_output_path):
                os.makedirs(raw_output_path)
            for row in zte_demand_param.itertuples():
                # 读取每一个sheet
                sheet_df = self.read_raw_data(row, f)
                # 在处理流程中寻找如何处理该数据
                sheet_name = row[1]
                # copy_sheet_df = copy.deepcopy(sheet_df)
                self.process_sheet(sheet_df, zte_param_process, sheet_name, raw_output_path)

    def gather_files(self, gather_df):
        grouped = gather_df.groupby('另存文件名')
        for new_file_name, g in grouped:
            relative_index = 0
            merge_result = pd.DataFrame()
            gather_out_path = os.path.join(os.path.split(self.temp_path)[0], new_file_name + '.csv')
            for index, row in g.iterrows():
                left_file_name = row['CSV名']
                left_file_cols = row['合并字段']
                on = row['匹配字段']
                left_file_cols = left_file_cols.split('|') if left_file_cols.find('|') >= 0 else [left_file_cols]
                on = on.split('|') if on.find('|') >= 0 else [on]
                left_file_path = os.path.join(self.temp_path, left_file_name + '.csv')
                left_file_df = pd.read_csv(left_file_path, usecols=left_file_cols)
                if relative_index == 0:
                    merge_result = left_file_df
                else:
                    merge_result = merge_result.merge(left_file_df, on=on, how='outer')
                relative_index = relative_index + 1
            merge_result.to_csv(gather_out_path, index=False, encoding='utf_8_sig')

    def process_sheet(self, sheet_df, zte_param_process, sheet_name, raw_output_path):
        sheet_process = zte_param_process[zte_param_process['CVS名'] == sheet_name]
        copy_df = copy.deepcopy(sheet_df)
        for row in sheet_process.itertuples():
            index = row.Index + 2
            action_name = row[3]
            new_file_name = row[5]
            process = row[2]
            new_col_name = row[4]
            copy_df = self.process_action(copy_df, action_name, process, new_col_name, index)
            if str(new_file_name) != 'nan':
                self.temp_path = os.path.join(raw_output_path, 'temp', new_file_name + '.csv')
                copy_df.to_csv(self.temp_path, index=False, encoding='utf_8_sig')
                # 如果需要产生新的文件,那么接下来算是新的循环，使用新的文件
                copy_df = copy.deepcopy(sheet_df)

    def process_action(self, sheet_df, action_name, process, new_col_name, index):
        row_processes = process.split(',') if process.find(',') >= 0 else [process]
        action_tuple_list = []
        for p in row_processes:
            exist_operators = zteutils.find_operators(p)
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
        sheet_name = row[1]
        header_row = row[2]
        data_start_row = row[3] - header_row - 1
        demand_cols = row[4].split('|') if row[4].find('|') >= 0 else row[4]
        demand_cols = list(filter(lambda x: x is not None and x != '', demand_cols))
        csv_df = pd.read_excel(f, engine='openpyxl', sheet_name=sheet_name,
                               usecols=demand_cols, header=header_row - 1, dtype='str')
        del_row_numbers = list(range(0, data_start_row, 1))
        csv_df.drop(del_row_numbers, axis=0, inplace=True)
        csv_df.reset_index(inplace=True, drop=True)
        return csv_df

    def valid_and_process_configuration(self, row, right_file_df, left_file_df, index):
        right_file_cols = right_file_df.columns.tolist()
        left_file_cols = left_file_df.columns.tolist()
        left_on = row['参数名1']
        right_on = row['参数名2']
        right_file = row['匹配文件']
        left_file = row['主文件']
        right_file_demand = row['所需字段']
        right_demand_rename = row['字段更名']
        right_demand_names = right_file_demand.split('|') if right_file_demand.find('|') >= 0 else [right_file_demand]
        right_demand_renames = right_demand_rename.split('|') if right_demand_rename.find('|') >= 0 else [
            right_demand_rename]
        if len(right_demand_names) != len(right_demand_renames):
            raise Exception("所需字段和字段更名不相对应,出错行数:" + str(index))
        # 右表重命名
        for index, name in enumerate(right_demand_names):
            if name not in right_file_cols:
                raise Exception("列名:" + name + ",不在" + right_file + "文件中,出错行号:" + str(index))
            # if left_on != right_on:
            #     right_file_df.rename(columns={right_on: left_on}, inplace=True)
            right_file_df.rename(columns={name: right_demand_renames[index]}, inplace=True)
        right_ons = right_on.split('|') if right_on.find('|') >= 0 else [right_on]
        left_ons = left_on.split('|') if left_on.find('|') >= 0 else [left_on]
        for on in right_ons:
            if on not in right_file_cols:
                raise Exception("列名:" + on + ",不在" + right_file + "文件中,出错行号:" + str(index))
        for on in left_ons:
            if on not in left_file_cols:
                raise Exception("列名:" + on + ",不在" + left_file + "文件中,出错行号:" + str(index))

    def match_zte_data(self, zte_param_match):
        grouped = zte_param_match.groupby('主文件', sort=False)
        for left_file, g in grouped:
            left_file_path = os.path.join(self.temp_path, left_file + '.csv')
            try:
                left_file_df = pd.read_csv(left_file_path, dtype='str', encoding='gbk')
            except Exception as e:
                left_file_df = pd.read_csv(left_file_path, dtype='str')
            for index, row in g.iterrows():
                index = index + 2
                left_on = row['参数名1']
                right_on = row['参数名2']
                right_file = row['匹配文件']
                right_file_demand = row['字段更名']
                if right_file == '地市' and left_file == 'NRCellCU-1':
                    city_df = pd.read_excel(self.zte_config_file_path, sheet_name='地市关系', engine='openpyxl',
                                            dtype='str')
                    city_df = city_df[city_df['制式'] == self.system][['地市', '子网ID']]
                    left_file_df = left_file_df.merge(city_df, how='left', on=right_on)
                    left_file_df.to_csv(left_file_path, index=False, encoding='utf_8_sig')
                    continue
                right_file = os.path.join(self.temp_path, right_file + '.csv')
                right_file_df = pd.read_csv(right_file)
                self.valid_and_process_configuration(row, right_file_df, left_file_df, index)
                right_demand_names = right_file_demand.split('|') if right_file_demand.find('|') >= 0 else [
                    right_file_demand]
                right_demand_names.append(left_on)
                right_file_df = right_file_df[right_demand_names]
                left_file_df = left_file_df.merge(right_file_df, on=left_on, how='left')
                # match_path = os.path.join(outpath, left_file + '.csv')
                left_file_df.to_csv(left_file_path, index=False, encoding='utf_8_sig')


