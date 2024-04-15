# -*- coding:utf-8 -*-
import itertools
import os
import shutil
from datetime import datetime

from PyQt5.QtCore import QThread, pyqtSignal

from configuration import huawei_configuration
from exception.read_raw_exception import ReadRawException
from model.data_watcher import DataWatcher
from model.signal_message import message
from reader.huawei_raw_datareader import HuaweiRawDataFile
from utils import huaweiutils


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

    # def setWorkDir(self, work_dir):
    #     self.work_dir = work_dir
    #
    # def setRawPath(self, raw_dir):
    #     self.raw_path = raw_dir
    #
    # def setConfigPath(self, config_path):
    #     self.config_path = config_path

    def run(self):
        try:
            if self.dataWatcher.work_dir is None or len(self.dataWatcher.work_dir) == 0:
                self.finished.emit(message(-1, '没有设置工作路径'))
                return
            # if self.dataWatcher.manufacturer is None or len(self.dataWatcher.manufacturer) == 0:
            #     self.finished.emit(message(1, "当前没有设置制式"))
            #     return
            # 解压所有的文件,然后提取所有的txt文件，复制到工作文件夹
            raw_path = self.dataWatcher.raw_data_dir
            work_dir = self.dataWatcher.work_dir
            manufacturer = self.dataWatcher.manufacturer
            system = self.dataWatcher.system
            raw_directory = os.path.join(self.dataWatcher.work_dir, self.dataWatcher.manufacturer, self.date,
                                         self.dataWatcher.system)
            if not os.path.exists(raw_directory):
                os.makedirs(raw_directory)
            os.chmod(raw_directory, 7)
            raw_data_dir = os.path.join(raw_directory, 'raw_data')
            if raw_path is None or not os.path.exists(raw_path):
                self.finished.emit(message(-1, "原始Log路径不存在"))
                return
            huaweiutils.unzip_all_files(raw_path)
            huawei_txts = huaweiutils.find_file(raw_path, '.txt')
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
            if manufacturer is None or system is None:
                self.finished.emit(message(-1, "没有设置厂商名或者制式"))
                return
            if manufacturer == '华为':
                index = 1
                correct_dicts = []
                for item in items:
                    f_name = os.path.basename(str(item)).replace('.txt', '')
                    outputPath = os.path.join(work_dir, manufacturer, self.date)
                    if '$' in f_name:
                        print("文件名:" + f_name + "是临时文件")
                        continue
                    reader = HuaweiRawDataFile(str(item), self.dataWatcher.huawei_command_path, outputPath, system)
                    reader.read_huawei_txt()
                    reader.output_format_data()
                    if len(reader.command_to_be_corrected) > 0:
                        # 字典列表,每个文件出现列名不一致的情况
                        correct_dicts.append(reader.command_to_be_corrected)
                        # self.correct_columns(reader.command_to_be_corrected)
                        # self.finished.emit(message(-2, "列名不一致,需要重新启动"))
                    else:
                        self.valueChanged.emit(index)
                        index = index + 1
                # 删除raw_data文件夹中的txt文件
                # shutil.rmtree(raw_data_dir)
                if len(correct_dicts) > 0:
                    self.correct_columns(correct_dicts)
                    self.finished.emit(message(-2, "华为列名与配置不一致,自动修正后重新解析原始Log"))
                    return
            elif self.manufacturer == '中兴':
                self.finished.emit(message(-1, "暂不支持"))
            self.finished.emit(message(2, "成功"))
        except Exception as e:

            self.finished.emit(message(-1, str(e)))

    def correct_columns(self, correct_dicts):
        """
        以第一个文件的修改为基准，合并后面文件的修改列名
        :param correct_dicts:
        :return:
        """
        base_dict = correct_dicts[0]
        iter_dicts = correct_dicts[1:]
        for dt in iter_dicts:
            for key, value in dt.items():
                base_value = base_dict[key]
                # 如果不是空，那么去重合并字典
                if base_value is not None:
                    base_dict[key] = list(set(base_value.extend(value)))
                # 如果是空,那么加入到 base_dict
                else:
                    base_dict[key] = value
        # 更正华为列的配置文件
        huawei_col_list = huawei_configuration.G5_COMMAND_COLS_LIST if self.dataWatcher.system == '5G' else \
            huawei_configuration.G4_COMMAND_COLS_LIST
        huawei_col_names = huawei_configuration.G5_COMMAND_NAME_LIST if self.dataWatcher.system == '5G' else \
            huawei_configuration.G4_COMMAND_NAME_LIST
        for col_name in base_dict:
            #修正配置中的列名
            index = huawei_col_names.index(col_name)
            huawei_col_list[index] = base_dict[col_name]