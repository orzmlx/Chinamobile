from zte_rawdata_reader import ZteRawDataReader
import os
import huaweiutils
import pandas as pd
from tqdm import tqdm
import re


class EricssonDataReader(ZteRawDataReader):

    def __init__(self, raw_file, eri_raw_data_path, eri_config_file_path, system, manufacturer):
        self.eri_config_file_path = eri_config_file_path
        self.eri_raw_data_path = eri_raw_data_path
        self.system = system
        self.raw_file_dir = raw_file
        raw_files = huaweiutils.find_file(raw_file, '.csv')
        pattern = r'_\d+'
        for f in raw_files:
            prefix = os.path.split(f)[0]
            origin_name = os.path.basename(f)
            new_name = os.path.join(prefix, re.sub(pattern, '', origin_name))
            os.rename(f, new_name)
            # 将匹配到的部分替换为空字符串
        self.manufacturer = manufacturer
        # raw_output_path = os.path.join(zte_raw_data_path, system, f_name)
        # self.temp_path = os.path.join(eri_raw_data_path, system, raw_file.name.split('.')[0], "temp")
        # if not os.path.exists(self.temp_path):
        #     os.makedirs(self.temp_path)

    def clean_data(self, eri_config_file_path, eri_raw_data_path, system):
        eri_demand_param = pd.read_excel(eri_config_file_path, engine='openpyxl', sheet_name='需求参数')
        eri_demand_param = eri_demand_param[eri_demand_param['厂家'] == self.manufacturer]
        eri_param_process = pd.read_excel(eri_config_file_path, engine='openpyxl', sheet_name='数据清理')
        eri_param_process = eri_param_process[eri_param_process['厂家'] == self.manufacturer]
        # 根据配置中需要的文件名,来读取中兴原始文件中的数据
        demand_params = eri_demand_param['CSV名'].unique().tolist()
        f = self.raw_file_dir
        f_name = os.path.basename(f).split('.')[0]
        # 获取上一级目录,将原始数据的读取结果存放在上一级目录
        # raw_output_path = os.path.join(self.raw_file_dir, f_name)
        for j in tqdm(range(len(demand_params)), desc="原始文件:" + f_name + "标清理进度"):
            # logging.info("\n")
            sheet_name = demand_params[j].strip()
            used_cols = eri_demand_param[eri_demand_param['CSV名'] == sheet_name]['所需字段'].iloc[0]
            used_cols = used_cols.split('|') if used_cols.find('|') >= 0 else [used_cols]
            # 读取每一个sheet
            file_path = os.path.join(self.raw_file_dir, sheet_name + '.csv')
            sheet_df = pd.read_csv(file_path, usecols=used_cols)
            # sheet_df = self.read_raw_data(row, f)
            # 在处理流程中寻找如何处理该数据
            out_path_dir = os.path.join(eri_raw_data_path, self.system, f_name)
            if not os.path.exists(out_path_dir):
                os.makedirs(out_path_dir)
            self.process_data_by_sheet(sheet_df, eri_param_process, sheet_name, out_path_dir, ';')

    def output_format_data(self):
        self.clean_data(self.eri_config_file_path, self.eri_raw_data_path, self.system)
