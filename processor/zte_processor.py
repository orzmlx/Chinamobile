# -*- coding:utf-8 -*-
import copy
import logging
import os
import shutil
from abc import ABC
from pathlib import Path

import pandas as pd
from python_calamine.pandas import pandas_monkeypatch, get_sheet_names

from configuration import zte_configuration
from model.data_watcher import DataWatcher
from model.evaluate import Evaluation
from processor.processor import Processor
from reader.ericsson_rawdata_reader import EricssonDataReader
from utils import common_utils


class ZteProcessor(Processor, ABC):

    def before_parse_5g_raw_data(self, raw_directory, dirs):
        logging.info('==============开始预处理5G原始文件' + '==============')
        common_utils.unzip_all_files(dirs)
        res = []
        for file_path in dirs.glob('**/*'):
            if not file_path.is_file() and file_path.name == 'csvfiles':
                all_raw_datas = common_utils.find_file(file_path, '.csv')
                parent_name = file_path.parent.name
                dest_dir = os.path.join(raw_directory, parent_name, 'raw_result')
                if not os.path.exists(dest_dir):
                    os.makedirs(dest_dir)
                for csv in all_raw_datas:
                    shutil.copy2(csv, dest_dir)
                    # csv = os.path.join(dest_dir, os.path.basename(csv))
                    dest_file = os.path.join(dest_dir, os.path.basename(csv))
                    zte_configuration.zte_extra_manage(dest_file)
                    res.append(dest_dir)
        return list(set(res))

    def evaluate(self, watcher, file, cell_config_df, freq_config_df):

        base_cols = watcher.get_base_cols()
        logging.info('==============开始处理文件:' + file + '==============')
        if not watcher.is_ready_for_check():
            raise Exception(-1, "请检查输入数据是否齐全")

        evaluate = Evaluation(file, watcher, used_commands=[], cell_config_df=cell_config_df,
                              freq_config_df=freq_config_df)
        copy_base_cols = copy.deepcopy(base_cols)
        cell_class_dict, freq_class_dict = evaluate.generate_report('all', copy_base_cols)
        return cell_class_dict, freq_class_dict

    def parse_raw_data(self, item, dataWatcher: DataWatcher) -> None:
        """
        中兴需要与爱立信一样分出语音和数据部分,中兴4G数据预处理代码中已经写死，不需要读取配置文件进行数据清晰
        """
        if dataWatcher.system == '4G':
            return

        zte_config = dataWatcher.config_path
        # output_path = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
        #                            dataWatcher.system)
        # items = huaweiutils.find_file(output_path, 'raw_result')
        # for item in items:
        csv_files = common_utils.find_file(item, '.csv')
        # 这里复用爱立信的流程
        reader = EricssonDataReader(str(item), str(item), zte_config, dataWatcher)
        for csv_f in csv_files:
            # 对于不同的数据,读取方法有少许差别,中兴数据第一行不读，但是后续输出需要保留
            # sheet_df = pd.read_csv(str(csv_f))
            # #保留第一行，否则会影响evaluation的读取过程
            # sheet_df = sheet_df.loc[:, ~sheet_df.columns.str.contains('Unnamed')]
            # skiprows = sheet_df.iloc[0]
            # os.path.join(csv_f.parent, system, 'kget', 'raw_result')
            reader.setRawFile(str(csv_f))
            reader.output_format_data()

    def merge_zte_4g_raw_header(self, file, config_path, copy_dest_dir, raw_directory) -> []:
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
        res_raw_files = []
        for sheet in sheet_names:
            logging.info("开始处理表:" + sheet)
            if not sheet in commands:
                continue
            raw_df = pd.read_excel(file, sheet_name=sheet, dtype=str, engine='calamine')
            # for index, row in raw_df.iterrows():
            # 去掉空格
            header_series = raw_df.iloc[0].str.strip().replace("\n", "") + '&' \
                            + raw_df.iloc[1].str.strip().replace("\n", "") + '&' \
                            + raw_df.iloc[2].str.strip().replace("\n", "") + '&' \
                            + raw_df.iloc[3].str.strip().replace("\n", "")
            header_list = list(header_series.values)
            header_list = [i.replace(' ', '').replace("\r", "").replace("\n", "") for i in header_list]
            raw_df.drop([0, 1, 2, 3], inplace=True, axis=0)
            raw_df.columns = header_list
            zte_configuration.depart_params(raw_df)
            # 读取CGI补充数据，部分CGI中间的标识号为1，需要补充数据
            supplementary_cgi_df = self.get_supplementary_cgi(raw_directory)
            zte_configuration.add_cgi(raw_df, sheet, supplementary_cgi_df)
            raw_df = raw_df.astype(str)
            out_path = os.path.join(copy_dest_dir, sheet + '.csv')
            res_raw_files.append(out_path)
            raw_df.to_csv(out_path, index=False, encoding='utf_8_sig')
        return res_raw_files

    def get_supplementary_cgi(self, raw_directory):
        raw_directory = os.path.dirname(os.path.dirname(raw_directory))
        supplementary_cgi_files = common_utils.find_file(raw_directory, 'ENBCUCPFunction.xlsx')
        if len(supplementary_cgi_files) != 1:
            raise Exception("找到多个或者没有找打中兴CGI补充文件")
        supplementary_cgi_file = supplementary_cgi_files[0]
        supplementary_cgi_df = pd.read_excel(supplementary_cgi_file, engine='calamine', header=1)
        supplementary_cgi_df.drop(index=[0, 1, 2], inplace=True)
        return supplementary_cgi_df[['网元ID', 'eNodeB标识']]

    def before_parse_4g_raw_data(self, raw_directory, dirs, config_path) -> []:
        copy_dest_dir = os.path.join(raw_directory, 'kget', 'raw_result')
        if not os.path.exists(copy_dest_dir):
            os.makedirs(copy_dest_dir)
        # 如果有zip文件需要解压
        common_utils.unzip_all_files(dirs)
        raw_files = []
        for file_path in dirs.glob('**/*'):
            if file_path.name.endswith('.xlsx'):
                logging.info('==============开始预处理文件:' + file_path.name + '==============')
                res_raw_files = self.merge_zte_4g_raw_header(file_path, config_path, copy_dest_dir, raw_directory)
                raw_files.extend(res_raw_files)
        return raw_files

    def before_parse_raw_data(self, dataWatcher: DataWatcher) -> []:
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
            return self.before_parse_5g_raw_data(raw_directory, all_dir)
        elif dataWatcher.system == '4G':
            return self.before_parse_4g_raw_data(raw_directory, all_dir, config_path)


if __name__ == "__main__":
    path = 'C:\\Users\\No.1\\Desktop\\界面测试\\中兴\\数据'
    dest_dir = 'C:\\Users\\No.1\\Desktop\\界面测试\\中兴\\20240515\\5G\\raw_data'
    common_utils.unzip_all_files(path, dest_path=dest_dir, zipped_file=[])
