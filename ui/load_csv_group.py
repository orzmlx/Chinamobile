# -*- coding:utf-8 -*-
import ctypes
import os

import pandas as pd
from PyQt5.Qt import QToolButton, QProgressBar, QFileDialog, QLabel
from PyQt5.QtCore import QTimer
from PyQt5.QtWidgets import QPushButton, QErrorMessage

from handler.loading_thread import LoadingThread
from handler.parse_raw_thread import ParseRawThread
from utils import huaweiutils
from model.data_watcher import DataWatcher
from model.signal_message import message



class LoadCsvGroup(QToolButton, QLabel):
    def __init__(self,
                 load_btn: QToolButton,
                 prgbar: QProgressBar,
                 # path_label: QLabel,
                 start_btn: QPushButton,
                 stop_btn: QPushButton,
                 msg_label: QLabel,
                 watcher: DataWatcher,
                 # manufacturer: str,
                 # loadingThread: loading_thread,
                 *args, **kwargs):
        super(LoadCsvGroup, self).__init__(*args, **kwargs)
        self.em = QErrorMessage.qtHandler()
        self.em.setWindowTitle("错误提示")
        self.prgbar = prgbar
        # self.path = path_label
        self.msg_label = msg_label
        self.load_btn = load_btn
        self.start_btn = start_btn
        self.stop_btn = stop_btn
        self._percent = 0
        self._timer = QTimer()
        self.watcher = watcher
        self.data_path = None
        # self.manufacturer = manufacturer
        self.setup_ui()

        # self.parseThread = ParseRawThread(watcher=watcher)
        self.loadingThread = LoadingThread(self.data_path,
                                           self.watcher,
                                           self.load_btn.objectName()
                                           ) if self.load_btn.objectName() != 'load_raw_data_btn' else ParseRawThread(
            watcher=watcher)
        self.loadingThread.finished.connect(self.finished)
        if self.prgbar is not None:
            self.loadingThread.valueChanged.connect(self.prgbar.setValue)

    def get_df_total(self, path):
        try:
            INPUT_FILENAME = path
            LINES_TO_READ_FOR_ESTIMATION = 20
            CHUNK_SIZE_PER_ITERATION = 10 ** 5
            try:
                temp = pd.read_csv(path, nrows=LINES_TO_READ_FOR_ESTIMATION, encoding='gbk')
            except:
                temp = pd.read_csv(path, nrows=LINES_TO_READ_FOR_ESTIMATION)
            N = len(temp.to_csv(index=False))

            return int(
                os.path.getsize(INPUT_FILENAME) / N * LINES_TO_READ_FOR_ESTIMATION / CHUNK_SIZE_PER_ITERATION) + 1
        except Exception as e:
            self.msg_label.setText('数据读取失败')

    def selectFile(self):
        fd = QFileDialog(
            self, "选择一个文件", "./", "csv文件(*.csv)"
        )
        if self.load_btn.objectName() == 'load_check_rule_btn':
            self.data_path = fd.getOpenFileName(self, "Select EXCEL file", "", "EXCEL files (*.xlsx)")[0]
        else:
            self.data_path = fd.getOpenFileName(self, "Select CSV file", "", "CSV files (*.csv)")[0]
        if self.data_path is not None and len(self.data_path) > 0:
            # if self.load_btn.objectName() != 'load_check_rule_btn':
            self.start_btn.setEnabled(True)
            prgbar_total = self.get_df_total(self.data_path)
            self.prgbar.setMaximum(prgbar_total)
        self.loadingThread.setLoadFilePath(self.data_path)


    def selectDir(self):
        fd = QFileDialog(
            self, "选择文件夹", "./", "ALL(*, *);;Images(*.png *.jpg);;Python文件(*.py)"
        )
        self.data_path = fd.getExistingDirectory(self, "选择文件夹")
        if self.data_path is not None:
            # self.start_btn.setEnabled(True)
            if self.load_btn.objectName() == 'out_put_dir_btn':
                self.watcher.setWorkDir(self.data_path)
                self.msg_label.setText("成功")
            elif self.load_btn.objectName() == 'load_raw_data_btn':
                self.watcher.setRawDataDir(self.data_path)
                self.start_btn.setEnabled(True)
                self.msg_label.setText("解压中...")
                huaweiutils.unzip_all_files(self.data_path)
                huawei_txts = huaweiutils.find_file(self.data_path, '.txt')
                self.prgbar.setMaximum(len(huawei_txts))
                self.watcher.set_files_number(len(huawei_txts))
                self.msg_label.setText("解压完成")

    def setup_ui(self):
        if self.load_btn.objectName() == 'out_put_dir_btn':
            # self.msg_label.setText("成功")
            self.load_btn.clicked.connect(self.selectDir)
        elif self.load_btn.objectName() == 'load_raw_data_btn':
            self.load_btn.clicked.connect(self.selectDir)

        else:
            self.load_btn.clicked.connect(self.selectFile)
        if self.start_btn is not None:
            self.start_btn.clicked.connect(self.onStart)

    def timeout_handler(self):
        self.msg_label.setText("读取文件超时")

    def stop(self):
        try:
            if hasattr(self, "loadingThread"):
                if self.loadingThread.isRunning():
                    print("有其他文件正在导入")
                else:
                    # self.loadingThread.requestInterruption()
                    self.loadingThread.quit()
                    self.loadingThread.wait(2000)
                    # self.loadingThread.finishsignal.emit("stop")
                    del self.loadingThread
        except RuntimeError:
            pass
        # try:
        #     self._timer.stop()
        #     self.setText(self.msg_label.setText("失败"))
        # except RuntimeError:
        #     pass

    def onStopThread(self):

        ret = ctypes.windll.kernel32.TerminateThread(  # @UndefinedVariable
            self.loadingThread.handle, 0)
        print('终止线程', self.loadingThread.handle, ret)
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)

    def onStart(self):
        self.msg_label.setText("加载中....")
        self.prgbar.setValue(0)
        self.stop_btn.setEnabled(True)
        self.start_btn.setEnabled(False)
        if self.prgbar is not None:
            self.load_btn.setEnabled(False)

        self.loadingThread.start()

    def finished(self, msg: message):
        if msg.code == 2:
            self.prgbar.setValue(self.prgbar.maximum())
            if self.prgbar.maximum() == 0:
                self.prgbar.setMaximum(1)
                self.prgbar.setValue(1)
            if self.msg_label is not None:
                self.msg_label.setText('成功')
        elif msg.code == -2:
            self.em.showMessage(msg.signal_message)
            self.msg_label.setText("重新解析中..")
            self.loadingThread.started.disconnect()
            self.loadingThread.finished.connect(self.finished)
            self.prgbar.setValue(0)
            self.loadingThread.start()
        else:
            self.msg_label.setText("失败")
            self.em.showMessage(msg.signal_message)

        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.load_btn.setEnabled(True)
