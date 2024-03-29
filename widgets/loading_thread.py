from random import randint

# -*- coding:utf-8 -*-
from model.data_watcher import DataWatcher
from model.signal_message import message

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
    finished = pyqtSignal(message)

    def __init__(self, path: str = None,
                 watcher: DataWatcher = None,
                 name: str = None,
                 *args, **kwargs):
        super(LoadingThread, self).__init__(*args, **kwargs)
        # self.totalValue = randint(100, 200)  # 模拟最大
        self.path = path
        self.watcher = watcher
        self.name = name
        # self.manufacturer = manufacturer

    def setName(self, name):
        self.name = name

    def setWorkDir(self, path):
        self.path = path

    def setWatcher(self, watcher):
        self.watcher = watcher

    def run(self):
        try:
            self.handle = ctypes.windll.kernel32.OpenThread(  # @UndefinedVariable
                win32con.PROCESS_ALL_ACCESS, False, int(QThread.currentThreadId()))
            # INPUT_FILENAME = self.path
            LINES_TO_READ_FOR_ESTIMATION = 20
            CHUNK_SIZE_PER_ITERATION = 10 ** 5
            data = pd.DataFrame()
            try:
                dfs = pd.read_csv(self.path, chunksize=CHUNK_SIZE_PER_ITERATION, low_memory=False, encoding='utf8')
            except:
                dfs = pd.read_csv(self.path, chunksize=CHUNK_SIZE_PER_ITERATION, low_memory=False, encoding='gbk')
            for i, chunk in enumerate(dfs):
                data = chunk if data.empty else pd.concat([data, chunk], axis=0)
                self.valueChanged.emit(i)
            if self.watcher.update_csv(self.name, data):
                self.finished.emit(message(2, '成功'))
            else:
                self.finished.emit(message(-1, "数据验证失败"))

        except Exception as e:
            # self.prgbar.setValue(0)
            self.finished.emit(message(-1, str(e)))