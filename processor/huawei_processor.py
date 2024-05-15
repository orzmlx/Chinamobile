# -*- coding:utf-8 -*-

import os
import shutil

from processor.processor import Processor
from model.data_watcher import DataWatcher
from reader.huawei_raw_datareader import HuaweiRawDataFile
from utils import huaweiutils
from abc import ABC, abstractmethod


class HuaweiProcessor(Processor, ABC):

    def parse(self, item, dataWatcher: DataWatcher):
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

    def before_parse_raw_data(self, dataWatcher: DataWatcher):
        raw_directory = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                                     dataWatcher.system)
        raw_path = dataWatcher.raw_data_dir
        if not os.path.exists(raw_directory):
            os.makedirs(raw_directory)
        os.chmod(raw_directory, 7)
        raw_data_dir = os.path.join(raw_directory, 'raw_data')
        if raw_path is None or not os.path.exists(raw_path):
            raise Exception("原始Log路径不存在")
        huaweiutils.unzip_all_files(raw_path)
        if dataWatcher.manufacturer is None:
            raise Exception("请先设置厂商")
        else:
            suffix = '.txt'
        raws = huaweiutils.find_file(raw_path, suffix)
        # dest = os.path.join(self.work_dir, self.manufacturer, self.date, self.system, 'raw_data')
        if not os.path.exists(raw_data_dir):
            os.makedirs(raw_data_dir)
        for txt in raws:
            shutil.copy2(str(txt), raw_data_dir)
