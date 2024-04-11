# -*- coding:utf-8 -*-

import os
import shutil
from datetime import datetime

from PyQt5.QtCore import QThread, pyqtSignal

from utils import huaweiutils
from reader.huawei_raw_datareader import HuaweiRawDataFile
from model.data_watcher import DataWatcher
from model.signal_message import message
from reader.zte_rawdata_reader import ZteRawDataReader


class ParseRawThread(QThread):
    handle = -1
    valueChanged = pyqtSignal(int)
    finished = pyqtSignal(message)

    def __init__(self,
                 work_path: str = None,
                 raw_path: str = None,
                 watcher: DataWatcher = None,
                 manufacturer: str = None,
                 system: str = '5G',
                 config_path: str = None,
                 *args, **kwargs):
        super(ParseRawThread, self).__init__(*args, **kwargs)
        self.work_dir = work_path  #
        self.raw_path = raw_path
        self.system = system
        self.manufacturer = manufacturer
        self.dataWatcher = watcher
        self.config_path = config_path
        now = datetime.now()
        self.date = now.strftime('%Y%m%d')

    def setWorkDir(self, work_dir):
        self.work_dir = work_dir

    def setRawPath(self, raw_dir):
        self.raw_path = raw_dir

    def setConfigPath(self, config_path):
        self.config_path = config_path



    def run(self):
        try:
            if self.dataWatcher.work_dir is None or len(self.dataWatcher.work_dir) == 0:
                self.finished.emit(message(-1, '没有设置工作路径'))
                return
            # if self.dataWatcher.manufacturer is None or len(self.dataWatcher.manufacturer) == 0:
            #     self.finished.emit(message(1, "当前没有设置制式"))
            #     return
            # 解压所有的文件,然后提取所有的txt文件，复制到工作文件夹
            self.raw_path = self.dataWatcher.raw_data_dir
            self.work_dir = self.dataWatcher.work_dir
            self.manufacturer = self.dataWatcher.manufacturer
            raw_directory = os.path.join(self.dataWatcher.work_dir, self.dataWatcher.manufacturer, self.date,
                                         self.system)
            if not os.path.exists(raw_directory):
                os.makedirs(raw_directory)
            os.chmod(raw_directory, 7)
            raw_data_dir = os.path.join(raw_directory, 'raw_data')
            huaweiutils.unzip_all_files(self.raw_path)
            huawei_txts = huaweiutils.find_file(self.raw_path, '.txt')
            # dest = os.path.join(self.work_dir, self.manufacturer, self.date, self.system, 'raw_data')
            if not os.path.exists(raw_data_dir):
                os.makedirs(raw_data_dir)
            for txt in huawei_txts:
                shutil.copy2(str(txt), raw_data_dir)
            # if not os.path.exists(raw_data_dir):
            #     shutil.copytree(self.raw_path, raw_data_dir)
            items = huaweiutils.find_file(raw_data_dir, '.txt')

            if len(items) == 0 or self.dataWatcher.huawei_command_path is None:
                self.finished.emit(message(-1, "没有可导入的原始文件或者华为命令"))
                return
            if self.manufacturer is None or self.system is None:
                self.finished.emit(message(-1, "没有设置厂商名或者制式"))
                return
            if self.manufacturer == '华为':
                index = 1
                for item in items:
                    f_name = os.path.basename(str(item)).replace('.xlsx', '')
                    outputPath = os.path.join(self.work_dir, self.manufacturer, self.date)
                    if '$' in f_name:
                        print("文件名:" + f_name + "是临时文件")
                        continue
                    reader = HuaweiRawDataFile(str(item), self.dataWatcher.huawei_command_path, outputPath, self.system)
                    reader.read_huawei_txt()
                    reader.output_format_data()
                    self.valueChanged.emit(index)
                    index = index + 1
                # 删除raw_data文件夹中的txt文件
                shutil.rmtree(raw_data_dir)
            elif self.manufacturer == '中兴':
                index = 1
                zte_config = self.config_path
                for item in items:
                    f_name = os.path.basename(str(item)).replace('.xlsx', '')
                    outputPath = os.path.join(self.work_dir, self.manufacturer, self.date, self.system, f_name)
                    if '$' in f_name:
                        print("文件名:" + f_name + "是临时文件")
                        continue
                    self.valueChanged.emit(index)
                    reader = ZteRawDataReader(item, outputPath, zte_config, self.system, '中兴')
                    reader.depart_sheet(zte_config_file_path=zte_config,
                                        zte_raw_data_path=self.raw_path,
                                        system=self.manufacturer)
                    index = index + 1
            self.finished.emit(message(2, "成功"))
        except Exception as e:

            self.finished.emit(message(-1, str(e)))
