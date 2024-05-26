# -*- coding:utf-8 -*-
import pathlib
import shutil
from abc import ABC
from pathlib import Path
import logging
import pandas as pd
import copy

from configuration import zte_configuration
from model.evaluate import Evaluation
from processor.processor import Processor
from model.data_watcher import DataWatcher
from reader.ericsson_rawdata_reader import EricssonDataReader
from utils import huaweiutils
import os
import openpyxl
from python_calamine.pandas import pandas_monkeypatch, get_sheet_data, get_sheet_names


def before_parse_5g_raw_data(raw_directory, dirs):
    for file_path in dirs.glob('**/*'):
        if not file_path.is_file() and file_path.name == 'csvfiles':
            all_raw_datas = huaweiutils.find_file(file_path, '.csv')
            parent_name = file_path.parent.name
            dest_dir = os.path.join(raw_directory, parent_name, 'raw_result')
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)
            for csv in all_raw_datas:
                shutil.copy2(csv, dest_dir)


class ZteProcessor(Processor, ABC):

    def evaluate(self, dataWatcher):
        # 罗列出所有的raw_result文件夹
        # raw_files_dir = os.listdir(
        #     os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date, dataWatcher.system))
        dirs = huaweiutils.find_file(os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                                                  dataWatcher.system), 'raw_result')
        cell_class_dict = {}
        freq_class_dict = {}
        # yyds
        pandas_monkeypatch()
        cell_config_df = pd.read_excel(dataWatcher.config_path, sheet_name="小区级别核查配置", dtype=str, engine='calamine')
        freq_config_df = pd.read_excel(dataWatcher.config_path, sheet_name="频点级别核查配置", dtype=str, engine='calamine')
        for index, f in enumerate(dirs):
            base_cols = dataWatcher.get_base_cols()
            f_name = f.parent.name
            logging.info('==============开始处理文件:' + f_name + '==============')
            if not dataWatcher.is_ready_for_check():
                raise Exception(-1, "请检查输入数据是否齐全")
            try:
                evaluate = Evaluation(os.path.dirname(f), dataWatcher, used_commands=[], cell_config_df=cell_config_df,
                                      freq_config_df=freq_config_df)
                copy_base_cols = copy.deepcopy(base_cols)
                cell_class_dict, freq_class_dict = evaluate.generate_report('freq', copy_base_cols)
            except Exception as e:
                logging.error(e)
                raise Exception(e)

        return cell_class_dict, freq_class_dict

    def parse_raw_data(self, dataWatcher: DataWatcher) -> None:
        """
        中兴需要与爱立信一样分出语音和数据部分
        """
        eri_config = dataWatcher.config_path
        output_path = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                                   dataWatcher.system)
        items = huaweiutils.find_file(output_path, 'raw_result')
        for item in items:
            csv_files = huaweiutils.find_file(item, '.csv')
            reader = EricssonDataReader(str(item), str(item), eri_config, dataWatcher)
            for csv_f in csv_files:
                # 对于不同的数据,读取方法有少许差别,中兴数据第一行不读，但是后续输出需要保留
                # sheet_df = pd.read_csv(str(csv_f))
                # #保留第一行，否则会影响evaluation的读取过程
                # sheet_df = sheet_df.loc[:, ~sheet_df.columns.str.contains('Unnamed')]
                # skiprows = sheet_df.iloc[0]
                # os.path.join(csv_f.parent, system, 'kget', 'raw_result')
                reader.setRawFile(str(csv_f))
                reader.output_format_data()

    def merge_zte_4g_raw_header(self, file, config_path, copy_dest_dir):
        pandas_monkeypatch()
        # 获取所有用到的表
        cell_commands = pd.read_excel(config_path, usecols=['厂家', '制式', '主命令'], sheet_name='小区级别核查配置',
                                      engine='calamine')
        cell_commands = cell_commands[(cell_commands['厂家'] == '中兴') & (cell_commands['制式'] == '4G')][
            '主命令'].unique().tolist()
        freq_commands = pd.read_excel(config_path, usecols=['厂家', '制式', '主命令'], sheet_name='频点级别核查配置',
                                      engine='calamine')
        freq_commands = freq_commands[(freq_commands['厂家'] == '中兴') & (freq_commands['制式'] == '4G')][
            '主命令'].unique().tolist()
        commands = cell_commands + freq_commands
        sheet_names = get_sheet_names(str(file))
        for sheet in sheet_names:
            if not sheet in commands:
                continue
            raw_df = pd.read_excel(file, sheet_name=sheet, dtype=str, engine='calamine')
            # for index, row in raw_df.iterrows():
            # 去掉空格
            header_series = raw_df.iloc[0] + '|' + raw_df.iloc[1] + '|' + raw_df.iloc[2] + '|' + raw_df.iloc[3]
            header_list = list(header_series.values)
            header_list = [i.replace(' ', '') for i in header_list]
            raw_df.drop([0, 1, 2, 3], inplace=True, axis=0)
            raw_df.columns = header_list
            zte_configuration.add_cgi(raw_df, sheet)
            zte_configuration.depart_params(raw_df)
            raw_df = raw_df.astype(str)
            raw_df.to_csv(os.path.join(copy_dest_dir, sheet + '.csv'), index=False, encoding='utf_8_sig')

    def before_parse_4g_raw_data(self, raw_directory, dirs, config_path) -> None:
        copy_dest_dir = os.path.join(raw_directory, 'kget', 'raw_result')
        if not os.path.exists(copy_dest_dir):
            os.makedirs(copy_dest_dir)
        for file_path in dirs.glob('**/*'):
            if file_path.name.endswith('.xlsx'):
                self.merge_zte_4g_raw_header(file_path, config_path, copy_dest_dir)
                # shutil.copy2(file_path, copy_dest_dir)

    def before_parse_raw_data(self, dataWatcher: DataWatcher) -> None:
        """
            准备好解析文件，对于中兴的文件，按照爱立信流程分解成语音和数据部分
            之后直接复制到/5G/下
        """
        raw_directory = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                                     dataWatcher.system)
        config_path = dataWatcher.config_path
        if not os.path.exists(raw_directory):
            os.makedirs(raw_directory)
        os.chmod(raw_directory, 7)
        raw_data_dir = dataWatcher.raw_data_dir
        all_dir = Path(raw_data_dir)
        if dataWatcher.system == '5G':
            before_parse_5g_raw_data(raw_directory, all_dir)
        elif dataWatcher.system == '4G':
            self.before_parse_4g_raw_data(raw_directory, all_dir, config_path)


if __name__ == "__main__":
    path = 'C:\\Users\\No.1\\Desktop\\界面测试\\中兴\\数据'
    dest_dir = 'C:\\Users\\No.1\\Desktop\\界面测试\\中兴\\20240515\\5G\\raw_data'
    huaweiutils.unzip_all_files(path, dest_path=dest_dir, zipped_file=[])
