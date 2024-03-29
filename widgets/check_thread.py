# -*- coding:utf-8 -*-
import traceback
import logging
from datetime import datetime

from PyQt5.QtCore import QThread, pyqtSignal

import huaweiutils
from model.data_watcher import DataWatcher
from model.evaluate import Evaluation
from model.signal_message import message
import os
import pandas as pd

from offline_processor import combine_evaluation
from param_selector import param_selector


class CheckThread(QThread):
    handle = -1
    valueChanged = pyqtSignal(int)
    finished = pyqtSignal(message)

    def __init__(self, watcher: DataWatcher = None):
        super().__init__()
        self.watcher = watcher
        now = datetime.now()
        self.date = now.strftime('%Y%m%d')
        self.check_test_preparation()

    def evaluate(self, base_cols):
        raw_files = os.listdir(
            os.path.join(self.watcher.work_dir, self.watcher.manufacturer, self.date, self.watcher.system,
                         'raw_data'))
        used_command = []
        cell_class_dict = {}
        freq_class_dict = {}
        for index, f in enumerate(raw_files):
            f_name = os.path.split(f)[1].split('.')[0]
            logging.info('==============开始处理文件:' + f_name + '==============')
            raw_file_dir = os.path.join(self.watcher.work_dir, self.watcher.manufacturer, self.date,
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

            selector = Evaluation(raw_file_dir,self.watcher, used_commands=used_command)
            cell_class_dict, freq_class_dict = selector.generate_report('cell', base_cols)
            self.valueChanged.emit(index + 1)

        return cell_class_dict, freq_class_dict

    def check_test_preparation(self):
        self.watcher.setConfigPath('C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\地市规则\\参数核查规则0223.xlsx')
        self.watcher.setManufacturer('华为')
        self.watcher.setSystem('5G')
        self.watcher.setWorkDir('C:\\Users\\No.1\\Desktop\\界面测试')
        self.watcher.set_files_number(2)
        self.watcher.setRawDataDir('C:\\Users\\No.1\\Desktop\\界面测试\\华为5G参数20240326')


    def run(self) -> None:
        try:
            check_result_name = "all_cell_check_result.csv"
            cell_check_result_name = "param_check_cell.csv"
            target_directory = os.path.join(self.watcher.work_dir, self.watcher.manufacturer, self.date,
                                            self.watcher.system)
            all_cell_check_result_path = os.path.join(target_directory, check_result_name)
            report_path = os.path.join(target_directory, self.watcher.manufacturer, self.date, '互操作参数核查结果.xlsx')
            base_cols = ['地市', '网元', 'NRDU小区名称', 'NR小区标识', 'CGI', '频段', '工作频段',
                         '厂家', '共址类型', '覆盖类型', '覆盖场景', '区域类别']
            cell_header_class_dict, freq_header_class_dict = self.evaluate(base_cols)
            combine_evaluation(target_directory, all_cell_check_result_path, cell_check_result_name, cell_header_class_dict)
            self.finished.emit(message(2, '成功'))
        except Exception as e:
            traceback.print_exc()
            self.finished.emit(message(-1, '失败'))
