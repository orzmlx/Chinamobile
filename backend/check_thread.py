# -*- coding:utf-8 -*-
import logging
import os
import traceback
from datetime import datetime
import sys
import pandas as pd

from PyQt5.QtCore import QThread, pyqtSignal

# from PySide6.QtCore import QThread, Signal
import copy

from python_calamine.pandas import pandas_monkeypatch

from configuration import huawei_configuration, ericsson_configuration, zte_configuration
from model import validator
from processor.huawei_processor import HuaweiProcessor
from processor.process_util import ProcessUtils
from processor.processor import Processor
from processor.zte_processor import ZteProcessor
from utils import huaweiutils
from model.data_watcher import DataWatcher
from model.evaluate import Evaluation
from model.signal_message import message
from offline_evaluate import combine_evaluation


# g5_base_cols = ['地市', '网元', 'NRDU小区名称', 'NR小区标识', 'CGI', '频段', '工作频段', '厂家', '共址类型', '覆盖类型', '覆盖场景', '区域类别']
# g4_base_cols = ['地市', '网元', '小区名称', '本地小区标识', 'CGI', '频段', '厂家', '共址类型', '覆盖类型', '覆盖场景', '区域类别']


class CheckThread(QThread):
    handle = -1
    valueChanged = pyqtSignal(int)
    finished = pyqtSignal(message)
    total_file_number = pyqtSignal(int)

    def __init__(self, watcher: DataWatcher = None,
                 processor: Processor = None):
        super().__init__()
        self.watcher = watcher
        # self.check_4g_test_preparation()
        self.check_5g_test_preparation()

    # def evaluate_eri(self):
    #     base_cols = self.watcher.get_base_cols()
    #     raw_files_dir = os.path.join(self.watcher.work_dir, self.watcher.manufacturer, self.watcher.date,
    #                                  self.watcher.system, 'kget')
    #     # raw_files = os.listdir(raw_files_dir)
    #     # 对于爱立信数据，相当于只有一个网管数据
    #     evaluate = Evaluation(raw_files_dir, self.watcher, used_commands=[])
    #     copy_base_cols = copy.deepcopy(base_cols)
    #     cell_class_dict, freq_class_dict = evaluate.generate_report('all', copy_base_cols)
    #     # self.valueChanged.emit(index + 1)
    #     return cell_class_dict, freq_class_dict

    def evaluate(self):
        raw_files = os.listdir(
            os.path.join(self.watcher.work_dir, self.watcher.manufacturer, self.watcher.date, self.watcher.system,
                         'raw_data'))
        used_command = []
        cell_class_dict = {}
        freq_class_dict = {}
        # 对于华为数据,每个原始数据都是一个网管数据，包含全套参数
        for index, f in enumerate(raw_files):
            base_cols = self.watcher.get_base_cols()
            f_name = os.path.split(f)[1].split('.')[0]
            logging.info('==============开始处理文件:' + f_name + '==============')
            raw_file_dir = os.path.join(self.watcher.work_dir, self.watcher.manufacturer, self.watcher.date,
                                        self.watcher.system, f_name)
            if len(used_command) == 0:
                raw_fs = huaweiutils.find_file(os.path.join(raw_file_dir, 'raw_result'), '.csv')
                for f in raw_fs:
                    try:
                        df = pd.read_csv(f, nrows=10, encoding='gb2312')
                    except:
                        df = pd.read_csv(f, nrows=10, encoding='utf8')
                    if not df.empty:
                        command = os.path.split(f)[1].split('.')[0]
                        used_command.append(command)
            if not self.watcher.is_ready_for_check():
                self.finished.emit(-1, "请检查输入数据是否齐全")
                return
            evaluate = Evaluation(raw_file_dir, self.watcher, used_commands=used_command)
            copy_base_cols = copy.deepcopy(base_cols)
            cell_class_dict, freq_class_dict = evaluate.generate_report('cell', copy_base_cols)
            self.valueChanged.emit(index + 1)

        return cell_class_dict, freq_class_dict

    def check_5g_test_preparation(self):
        self.watcher.setConfigPath('C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\ericsson\\参数核查规则0429.xlsx')
        self.watcher.setManufacturer('华为')
        self.watcher.setSystem('5G')
        self.watcher.setWorkDir('C:\\Users\\No.1\\Desktop\\界面测试')
        self.watcher.set_files_number(1)
        # self.watcher.setRawDataDir('C:\\Users\\No.1\\Desktop\\界面测试\\华为5G参数20240326')
        self.watcher.setRawDataDir('C:\\Users\\No.1\\Desktop\\界面测试\\华为\\数据')
        self.watcher.setDate('20240515')
        self.watcher.set_huawei_command_path(
            'C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\华为45G互操作固定通报参数20231225.txt')

    def check_4g_test_preparation(self):
        self.watcher.setConfigPath('C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\ericsson\\参数核查规则0429.xlsx')
        self.watcher.setManufacturer('中兴')
        self.watcher.setSystem('4G')
        self.watcher.setWorkDir('C:\\Users\\No.1\\Desktop\\界面测试')
        self.watcher.set_files_number(1)
        self.watcher.setDate('20240515')
        self.watcher.setRawDataDir('C:\\Users\\No.1\\Desktop\\界面测试\\中兴\\数据\\4G')

    def run(self) -> None:
        try:
            check_result_name = "all_cell_check_result.csv"
            cell_check_result_name = "param_check_cell.csv"
            target_directory = os.path.join(self.watcher.work_dir, self.watcher.manufacturer, self.watcher.date,
                                            self.watcher.system)
            all_cell_check_result_path = os.path.join(target_directory, check_result_name)
            report_path = os.path.join(target_directory, self.watcher.manufacturer, self.watcher.date, '互操作参数核查结果.xlsx')
            processor = ProcessUtils.get_processor(self.watcher)
            # raw_files = os.listdir(
            #     os.path.join(self.watcher.work_dir, self.watcher.manufacturer, self.watcher.date, self.watcher.system,
            #                  'raw_data'))
            raw_files = self.watcher.get_raw_result_files()
            pandas_monkeypatch()
            cell_config_df = pd.read_excel(self.watcher.config_path, sheet_name="小区级别核查配置", dtype=str, engine='calamine')
            freq_config_df = pd.read_excel(self.watcher.config_path, sheet_name="频点级别核查配置", dtype=str, engine='calamine')
            for index, f in enumerate(raw_files):
                cell_header_class_dict, freq_header_class_dict = processor.evaluate(watcher=self.watcher, file=f,
                                                                                    cell_config_df=cell_config_df,
                                                                                    freq_config_df=freq_config_df)

                self.valueChanged.emit(index + 1)
            combine_evaluation(target_directory, all_cell_check_result_path, cell_check_result_name)
            self.finished.emit(message(2, '成功'))
        except Exception as e:
            traceback.print_exc()
            self.finished.emit(message(-1, str(sys.exc_info())))