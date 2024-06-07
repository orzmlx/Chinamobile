# -*- coding:utf-8 -*-
import copy
import logging
import os
import shutil
from abc import ABC

import pandas as pd

from model.data_watcher import DataWatcher
from model.evaluate import Evaluation, Reader
from processor.processor import Processor
from reader.huawei_raw_datareader import HuaweiRawDataFile
from utils import huaweiutils


class HuaweiProcessor(Processor, ABC):

    def parse_raw_data(self, item, dataWatcher: DataWatcher) -> Reader:
        """
        返回解析的原始文件数量
        :param item:
        :param dataWatcher:
        :return:
        """
        manufacturer = dataWatcher.manufacturer
        work_dir = dataWatcher.work_dir
        date = dataWatcher.date
        system = dataWatcher.system
        f_name = os.path.basename(str(item)).replace('.txt', '')
        outputPath = os.path.join(work_dir, manufacturer, date)
        if '$' in f_name:
            print("文件名:" + f_name + "是临时文件")
            return
        reader = HuaweiRawDataFile(dataWatcher.huawei_command_path, outputPath, system)
        reader.setRawFile(item)
        reader.read_huawei_txt()
        reader.output_format_data()
        return reader

    def before_parse_raw_data(self, dataWatcher: DataWatcher) -> []:
        """
            返回所有的原始文件的路径
        :param dataWatcher:
        :return:
        """
        raw_log_paths = []
        raw_directory = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                                     dataWatcher.system)
        raw_path = dataWatcher.raw_data_dir
        if not os.path.exists(raw_directory):
            os.makedirs(raw_directory)
        os.chmod(raw_directory, 7)
        raw_data_dir = os.path.join(raw_directory, 'raw_data')
        if raw_path is None or not os.path.exists(raw_path):
            raise Exception("原始Log路径不存在")
        # 解压原始Log文件
        huaweiutils.unzip_all_files(raw_path)
        if dataWatcher.manufacturer is None:
            raise Exception("请先设置厂商")
        raws = huaweiutils.find_file(raw_path, '.txt')
        if not os.path.exists(raw_data_dir):
            os.makedirs(raw_data_dir)
        for txt in raws:
            shutil.copy2(str(txt), raw_data_dir)
            raw_log_paths.append(os.path.join(raw_data_dir, txt.name))
        raw_logs = huaweiutils.find_file(raw_data_dir, '.txt')
        return raw_logs

    def evaluate(self, watcher: DataWatcher, file, cell_config_df, freq_config_df):
        # raw_files = os.listdir(
        #     os.path.join(watcher.work_dir, watcher.manufacturer, watcher.date, watcher.system, 'raw_data'))
        used_command = []
        cell_class_dict = {}
        freq_class_dict = {}
        # 对于华为数据,每个原始数据都是一个网管数据，包含全套参数
        # for index, f in enumerate(raw_files):
        base_cols = watcher.get_base_cols()
        f_name = os.path.split(file)[1].split('.')[0]
        logging.info('==============开始处理文件:' + f_name + '==============')
        raw_file_dir = os.path.join(watcher.work_dir, watcher.manufacturer, watcher.date,
                                    watcher.system, f_name)
        if len(used_command) == 0:
            raw_fs = huaweiutils.find_file(os.path.join(raw_file_dir, 'raw_result'), '.csv')
            for f in raw_fs:
                try:
                    df = pd.read_csv(f, nrows=10, encoding='gb2312')
                except Exception as e:
                    df = pd.read_csv(f, nrows=10, encoding='utf8')
                if not df.empty:
                    command = os.path.split(f)[1].split('.')[0]
                    used_command.append(command)
        if not watcher.is_ready_for_check():
            raise Exception("请检查输入数据是否齐全")
        evaluate = Evaluation(raw_file_dir, watcher, used_commands=used_command,
                              cell_config_df=cell_config_df, freq_config_df=freq_config_df)
        copy_base_cols = copy.deepcopy(base_cols)
        return evaluate.generate_report('cell', copy_base_cols)
        # return cell_class_dict, freq_class_dict
