from random import randint

# -*- coding:utf-8 -*-
from model.data_watcher import DataWatcher

try:
    from PyQt5.QtCore import QTimer, QThread, pyqtSignal
    from PyQt5.QtGui import QPainter, QColor, QPen
    from PyQt5.QtWidgets import QPushButton, QApplication, QWidget, QVBoxLayout, QRadioButton
    from PyQt5.Qt import QToolButton, QProgressBar, QFileDialog, QLabel

except ImportError:
    from PySide2.QtCore import QTimer, QThread, Signal as pyqtSignal
    from PySide2.QtGui import QPainter, QColor, QPen
    from PySide2.QtWidgets import QPushButton, QApplication, QWidget, QVBoxLayout
import os
import pandas as pd
import ctypes
import win32con

StyleSheet = '''
PushButtonLine {
    color: white;
    border: none;
    min-height: 48px;
    background-color: #90caf9;
}
'''


class LoadingThread(QThread):
    handle = -1
    valueChanged = pyqtSignal(int)
    finished = pyqtSignal(int)

    def __init__(self, path: str = None,
                 msg_label: QLabel = None,
                 prgbar: QProgressBar = None,
                 watcher: DataWatcher = None,
                 *args, **kwargs):
        super(LoadingThread, self).__init__(*args, **kwargs)
        # self.totalValue = randint(100, 200)  # 模拟最大
        self.path = path
        self.watcher = watcher
        self.msg_label = msg_label
        self.prgbar = prgbar
        # self.manufacturer = manufacturer

    def setPath(self, path):
        self.path = path

    def setWatcher(self, watcher):
        self.watcher = watcher

    def setPrgBar(self, prgbar):
        self.prgbar = prgbar

    def setMsgLabel(self, msgLabel):
        self.msg_label = msgLabel

    def run(self):
        self.msg_label.setText("加载中....")
        self.prgbar.setValue(0)
        if os.path.isdir(self.path):
            self.msg_label.setText("成功")
            return
        try:
            self.handle = ctypes.windll.kernel32.OpenThread(  # @UndefinedVariable
                win32con.PROCESS_ALL_ACCESS, False, int(QThread.currentThreadId()))
            # INPUT_FILENAME = self.path
            LINES_TO_READ_FOR_ESTIMATION = 20
            CHUNK_SIZE_PER_ITERATION = 10 ** 5
            temp = pd.read_csv(self.path, nrows=LINES_TO_READ_FOR_ESTIMATION, encoding='gbk')
            # N = len(temp.to_csv(index=False))
            df = [temp[:0]]
            # total = int(
            #     os.path.getsize(INPUT_FILENAME) / N * LINES_TO_READ_FOR_ESTIMATION / CHUNK_SIZE_PER_ITERATION) + 1
            # # self._timer.start(2)
            # self.prgbar.setMaximum(total)
            for i, chunk in enumerate(
                    pd.read_csv(self.path, chunksize=CHUNK_SIZE_PER_ITERATION, low_memory=False, encoding='gbk')):
                df.append(chunk)
                # self.prgbar.setValue(self.prgbar.value() + 1)
                self.valueChanged.emit(i)
            data = temp[:0].append(df)
            if self.watcher.update(self.prgbar.objectName(), data):
                self.msg_label.setText("成功")
            else:
                self.msg_label.setText("数据验证失败")
                self.prgbar.setValue(0)
            self.finished.emit(1)
        except Exception as e:
            self.msg_label.setText("加载失败")
            self.prgbar.setValue(0)
            self.finished.emit(1)

        # return data
