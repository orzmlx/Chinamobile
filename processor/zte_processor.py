# -*- coding:utf-8 -*-
import shutil
from abc import ABC
from pathlib import Path
import logging

from processor.processor import Processor
from model.data_watcher import DataWatcher
from utils import huaweiutils
import os


class ZteProcessor(Processor, ABC):

    def parse_raw_data(self, dataWatcher: DataWatcher):
        pass

    def before_parse_raw_data(self, dataWatcher: DataWatcher):
        """
            准备好解析文件，对于中兴的文件，属于已经解析好的文件，可以直接复制到/5G/下
        """
        raw_directory = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                                     dataWatcher.system)
        if not os.path.exists(raw_directory):
            os.makedirs(raw_directory)
        os.chmod(raw_directory, 7)
        raw_data_dir = dataWatcher.raw_data_dir

        all_dir = Path(raw_data_dir)
        for file_path in all_dir.glob('**/*'):
            if not file_path.is_file() and file_path.name == 'csvfiles':
                all_raw_datas = huaweiutils.find_file(file_path, '.csv')
                parent_name = file_path.parent.name
                dest_dir = os.path.join(raw_directory, parent_name, 'raw_result')
                if not os.path.exists(dest_dir):
                    os.makedirs(dest_dir)
                for csv in all_raw_datas:
                    shutil.copy2(csv, dest_dir)


if __name__ == "__main__":
    path = 'C:\\Users\\No.1\\Desktop\\界面测试\\中兴\\数据'
    dest_dir = 'C:\\Users\\No.1\\Desktop\\界面测试\\中兴\\20240515\\5G\\raw_data'
    huaweiutils.unzip_all_files(path, dest_path=dest_dir, zipped_file=[])
