# -*- coding:utf-8 -*-
from model.data_watcher import DataWatcher
from widgets import loading_thread

try:
    from PyQt5.QtCore import QTimer, QThread, pyqtSignal, qWarning
    from PyQt5.QtGui import QPainter, QColor, QPen
    from PyQt5.QtWidgets import QPushButton, QApplication, QWidget, QVBoxLayout
    from PyQt5.Qt import QToolButton, QProgressBar, QFileDialog, QLabel
except ImportError:
    from PySide2.QtCore import QTimer, QThread, Signal as pyqtSignal
    from PySide2.QtGui import QPainter, QColor, QPen
    from PySide2.QtWidgets import QPushButton, QApplication, QWidget, QVBoxLayout
import os
import pandas as pd
import sys
import time
import ctypes
from widgets.loading_thread import LoadingThread
from win32process import SuspendThread, ResumeThread


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
                 loadingThread: loading_thread,
                 *args, **kwargs):
        super(LoadCsvGroup, self).__init__(*args, **kwargs)

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
        # loadingThread.setMsgLabel(self.msg_label)
        # loadingThread.setWatcher(self.watcher)
        # loadingThread.setPrgBar(self.prgbar)
        self.loadingThread = loadingThread
        # self.loadingThread = LoadingThread(self.data_path,
        #                                    self.msg_label,
        #                                    self.prgbar,
        #                                    self.watcher)
        self.loadingThread = loadingThread
        self.loadingThread.finished.connect(self.finished)
        if self.prgbar is not None:
            self.loadingThread.valueChanged.connect(self.prgbar.setValue)

    def get_total(self, path):
        try:
            INPUT_FILENAME = path
            LINES_TO_READ_FOR_ESTIMATION = 20
            CHUNK_SIZE_PER_ITERATION = 10 ** 5
            temp = pd.read_csv(path, nrows=LINES_TO_READ_FOR_ESTIMATION, encoding='gbk')
            N = len(temp.to_csv(index=False))

            return int(
                os.path.getsize(INPUT_FILENAME) / N * LINES_TO_READ_FOR_ESTIMATION / CHUNK_SIZE_PER_ITERATION) + 1
        except Exception as e:
            self.msg_label.setText('数据读取失败')

    def setup_ui(self):
        def selectDir():
            fd = QFileDialog(
                self, "选择一个文件", "./", "ALL(*, *);;Images(*.png *.jpg);;Python文件(*.py)"
            )
            self.data_path = fd.getExistingDirectory(self, "选择文件夹")
            if self.data_path is not None:
                self.start_btn.setEnabled(True)
            # self.start(folder_path)

        def selectFile():
            fd = QFileDialog(
                self, "选择一个文件", "./", "Excel文件(*.xlsx);;文本文档(*.txt);;csv文件(*.csv)"
            )
            self.data_path = fd.getOpenFileName(self, '选择文件')[0]
            if self.data_path is not None:
                self.start_btn.setEnabled(True)
            prgbar_total = self.get_total(self.data_path)
            self.prgbar.setMaximum(prgbar_total)
            self.loadingThread.setPath(self.data_path)

        if self.load_btn.objectName() == 'out_put_dir_btn':
            # self.msg_label.setText("成功")
            self.load_btn.clicked.connect(selectDir)
        elif self.load_btn.objectName() == 'load_raw_data_btn':
            self.load_btn.clicked.connect(selectDir)
        else:
            self.load_btn.clicked.connect(selectFile)
        # self._timer.timeout.connect(self.timeout_handler)
        if self.start_btn is not None:
            self.start_btn.clicked.connect(self.onStart)

    def timeout_handler(self):
        self.msg_label.setText("读取文件超时")

    # def setPath(self, path):
    #     self.path.setText(str(path).strip())

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
        self.loadingThread.setMsgLabel(self.msg_label)
        self.loadingThread.setWatcher(self.watcher)
        self.loadingThread.setPrgBar(self.prgbar)
        # if self.data_path is None:
        #     qWarning("没有输入路径")
        # if hasattr(self, "loadingThread"):
        #     self.stop()
        self.stop_btn.setEnabled(True)
        self.start_btn.setEnabled(False)
        if self.load_btn.objectName() == 'out_put_dir_btn':
            self.watcher.setOutputPath(self.data_path)
        elif self.load_btn.objectName() == 'load_raw_data_btn':
            self.watcher.setRawDataDir(self.data_path)
        # file_name = os.path.split(self.data_path)[1]
        # self.setPath(file_name)
        if self.prgbar is not None:
            self.load_btn.setEnabled(False)
            # self.loadingThread.finishsignal.connect(self.notice_loading_finished)
            # self._timer.start(100)  # 100ms
            self.loadingThread.start()

    def finished(self, msg):
        if msg == 1:
            self.start_btn.setEnabled(True)
            self.stop_btn.setEnabled(False)
            self.load_btn.setEnabled(True)
