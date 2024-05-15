# -*- coding:utf-8 -*-
import itertools
import os
import shutil
import traceback
from datetime import datetime
from PyQt5.QtCore import QThread, pyqtSignal

# from PySide6.QtCore import QThread, Signal
from processor.huawei_processor import HuaweiAdapter
from processor.processor import Processor
from processor.zte_processor import ZteAdapter
from configuration import huawei_configuration
from exception.read_raw_exception import ReadRawException
from model.data_watcher import DataWatcher
from model.signal_message import message
from reader.ericsson_rawdata_reader import EricssonDataReader
from reader.huawei_raw_datareader import HuaweiRawDataFile
from utils import huaweiutils
import logging

logging.basicConfig(format='%(asctime)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S', level=logging.INFO)


class ParseRawThread(QThread):
    handle = -1
    valueChanged = pyqtSignal(int)
    finished = pyqtSignal(message)

    def __init__(self,
                 work_path: str = None,
                 raw_path: str = None,
                 watcher: DataWatcher = None,
                 processor: Processor = None,
                 system: str = '5G',
                 config_path: str = None,
                 *args, **kwargs):
        super(ParseRawThread, self).__init__(*args, **kwargs)
        self.processor = processor
        self.work_dir = work_path  #
        self.raw_path = raw_path
        self.system = system
        self.dataWatcher = watcher
        self.config_path = config_path
        now = datetime.now()
        self.date = now.strftime('%Y%m%d')
        self.processor = processor

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
            self.processor.before_parse_raw_data(self.dataWatcher)
            raw_directory = os.path.join(self.dataWatcher.work_dir, self.dataWatcher.manufacturer, self.date,
                                         self.dataWatcher.system)
            # if not os.path.exists(raw_directory):
            #     os.makedirs(raw_directory)
            # os.chmod(raw_directory, 7)
            raw_data_dir = os.path.join(raw_directory, 'raw_data')
            # if raw_path is None or not os.path.exists(raw_path):
            #     self.finished.emit(message(-1, "原始Log路径不存在"))
            #     return
            # huaweiutils.unzip_all_files(raw_path)
            # if self.dataWatcher.manufacturer is None:
            #     self.finished.emit(message(-1, "请先设置厂商"))
            #     return
            # else:
            #     suffix = '.txt' if self.dataWatcher.manufacturer == '华为' else '.csv'
            # raws = huaweiutils.find_file(raw_path, suffix)
            # # dest = os.path.join(self.work_dir, self.manufacturer, self.date, self.system, 'raw_data')
            # if not os.path.exists(raw_data_dir):
            #     os.makedirs(raw_data_dir)
            # for txt in raws:
            #     shutil.copy2(str(txt), raw_data_dir)
            if self.dataWatcher.manufacturer is None:
                raise Exception("请先设置厂商")
            suffix = '.txt' if self.dataWatcher.manufacturer == '华为' else '.csv'
            items = huaweiutils.find_file(raw_data_dir, suffix)

            if len(items) == 0:
                self.finished.emit(message(-1, "没有可导入的原始文件"))
                return
            if self.dataWatcher.huawei_command_path is None and manufacturer == '华为':
                self.finished.emit(message(-1, "没有导入华为命令"))
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
                    reader = HuaweiRawDataFile(self.dataWatcher.huawei_command_path, outputPath, system)
                    reader.setRawFile(item)
                    reader.read_huawei_txt()
                    reader.output_format_data()

                    if len(reader.command_to_be_corrected) > 0:
                        # 字典列表,每个文件出现列名不一致的情况
                        correct_dicts.append(reader.command_to_be_corrected)
                    else:
                        self.valueChanged.emit(index)
                        index = index + 1
                # 删除raw_data文件夹中的txt文件
                # shutil.rmtree(raw_data_dir)
                if len(correct_dicts) > 0:
                    self.correct_columns(correct_dicts)
                    self.finished.emit(message(-2, "华为列名与配置不一致,自动修正后重新解析原始Log"))
                    return
            elif manufacturer == '中兴':
                self.finished.emit(message(-1, "暂不支持中兴"))
            elif manufacturer == '爱立信':
                index = 1
                outputPath = os.path.join(work_dir, manufacturer, self.date)
                eri_config = self.dataWatcher.config_path
                reader = EricssonDataReader(outputPath, eri_config, system)
                reader.auto_check_ref()
                for item in items:
                    reader.setRawFile(item)
                    f_name = os.path.basename(str(item)).replace('.txt', '')
                    if '$' in f_name:
                        print("文件名:" + f_name + "是临时文件")
                        continue
                    if eri_config is None:
                        self.finished.emit(message(-1, "请导入配置文件"))
                        return
                    reader.output_format_data()
                    self.valueChanged.emit(index)
                    index = index + 1
            self.finished.emit(message(2, "成功"))
        except Exception as e:
            logging.error(e)
            traceback.print_exc()
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
            if huaweiutils.only_has_digtal_diff(pr[0], pr[1]) and len(pr[0]) == len(pr[1]):
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
