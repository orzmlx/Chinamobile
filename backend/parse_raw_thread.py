# -*- coding:utf-8 -*-
import itertools
import logging
import traceback
from datetime import datetime

from PyQt5.QtCore import QThread, pyqtSignal

from configuration import huawei_configuration
from model.data_watcher import DataWatcher
from model.signal_message import message
from processor.process_util import ProcessUtils
from utils import common_utils

logging.basicConfig(format='%(asctime)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S', level=logging.INFO)


class ParseRawThread(QThread):
    handle = -1
    valueChanged = pyqtSignal(int)
    finished = pyqtSignal(message)
    total_file_number = pyqtSignal(int)

    def __init__(self,
                 work_path: str = None,
                 raw_path: str = None,
                 watcher: DataWatcher = None,
                 system: str = '5G',
                 config_path: str = None,
                 *args, **kwargs):
        super(ParseRawThread, self).__init__(*args, **kwargs)
        self.work_dir = work_path
        self.raw_path = raw_path
        self.system = system
        self.dataWatcher = watcher
        self.config_path = config_path
        now = datetime.now()
        self.date = now.strftime('%Y%m%d')

    def run(self):
        try:
            if self.dataWatcher.work_dir is None or len(self.dataWatcher.work_dir) == 0:
                raise Exception("没有设置工作路径")
            if self.dataWatcher.manufacturer is None or self.dataWatcher.system is None:
                raise Exception("请先设置厂商")
            if self.dataWatcher.huawei_command_path is None and self.dataWatcher.manufacturer == '华为':
                raise Exception("没有导入华为命令")
            # 解压所有的文件,然后提取所有的txt文件，复制到工作文件夹
            processor = ProcessUtils.get_processor(watcher=self.dataWatcher)
            raw_logs = processor.before_parse_raw_data(self.dataWatcher)
            # 向前端更新总的文件数量
            self.total_file_number.emit(len(raw_logs))
            for index, log in enumerate(raw_logs):
                processor.parse_raw_data(log, dataWatcher=self.dataWatcher)
                self.valueChanged.emit(index + 1)
            self.finished.emit(message(2, "成功"))
            logging.info('==============解析原始Log文件完成' + '==============')
        except Exception as e:
            traceback.print_exc()  # 打印错误栈
            logging.error(e)
            self.finished.emit(message(-1, str(e)))

    def correct_columns(self, correct_dicts):
        """
        以第一个文件的修改为基准，合并后面文件的修改列名
        :param correct_dicts:
        :return:
        """
        base_dict = correct_dicts[0]
        # 如果出现 QCI =1 或者QCI =9这种命令的列不同,那么会导致后面行合并出现问题
        # 在这里先对base_dict中这种命令进行合并
        base_pairs = list(itertools.combinations(base_dict.keys(), 2))
        for pr in base_pairs:
            if common_utils.only_has_digtal_diff(pr[0], pr[1]) and len(pr[0]) == len(pr[1]):
                value0 = base_dict[pr[0]]
                value1 = base_dict[pr[1]]
                value0.extend(value1)
                merge_value = list(set(value0))
                base_dict[pr[0]] = merge_value
                base_dict[pr[1]] = merge_value
        iter_dicts = correct_dicts[1:]
        for dt in iter_dicts:
            for key, value in dt.items():
                base_value = base_dict[key]
                # 如果不是空，那么去重合并字典
                if base_value is not None:
                    base_value.extend(value)
                    base_dict[key] = list(set(base_value))
                # 如果是空,那么加入到 base_dict
                else:
                    base_dict[key] = value
        # 更正华为列的配置文件
        huawei_col_list = huawei_configuration.G5_COMMAND_COLS_LIST if self.dataWatcher.system == '5G' else \
            huawei_configuration.G4_COMMAND_COLS_LIST
        huawei_col_names = huawei_configuration.G5_COMMAND_NAME_LIST if self.dataWatcher.system == '5G' else \
            huawei_configuration.G4_COMMAND_NAME_LIST
        for col_name in base_dict:
            # 修正配置中的列名
            index = huawei_col_names.index(col_name)
            huawei_col_list[index] = base_dict[col_name]
