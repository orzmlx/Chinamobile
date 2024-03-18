# -*- coding:utf-8 -*-
try:
    from PyQt5.QtCore import QTimer, QThread, pyqtSignal
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


class LoadCsvButton(QToolButton, QLabel):
    def __init__(self,
                 btn: QToolButton,
                 prgbar: QProgressBar,
                 path_label: QLabel,
                 msg_label: QLabel, *args, **kwargs):
        super(LoadCsvButton, self).__init__(*args, **kwargs)
        self.prgbar = prgbar
        self.path = path_label
        self.msg_label = msg_label
        self.btn = btn
        self._percent = 0
        self._timer = QTimer(prgbar)
        self.setup_ui()

    def setup_ui(self):
        def selectFile():
            fd = QFileDialog(
                self, "选择一个文件", "./", "ALL(*, *);;Images(*.png *.jpg);;Python文件(*.py)"
            )
            fd.fileSelected.connect(lambda val: print("多个文件被选中-列表[字符串]", val))
            fd.setFileMode(QFileDialog.ExistingFiles)
            fd.open()
            fd.fileSelected.connect(lambda file: self.load_csv(file))
            # fd.filesSelected.connect(self.msg_label.setText("只能选择一个文件"))

        self.btn.clicked.connect(selectFile)
        self._timer.timeout.connect(self.timeout_handler)

    def timeout_handler(self):
        self.msg_label.setText("读取文件超时")

    def setPath(self, path):
        self.path.setText(str(path).strip())

    def load_csv(self, path):
        file_name = os.path.split(path)[1]
        self.setPath(file_name)
        INPUT_FILENAME = path
        LINES_TO_READ_FOR_ESTIMATION = 20
        CHUNK_SIZE_PER_ITERATION = 10 ** 5
        temp = pd.read_csv(INPUT_FILENAME,
                           nrows=LINES_TO_READ_FOR_ESTIMATION, encoding='gbk')
        N = len(temp.to_csv(index=False))
        df = [temp[:0]]
        total = int(os.path.getsize(INPUT_FILENAME) / N * LINES_TO_READ_FOR_ESTIMATION / CHUNK_SIZE_PER_ITERATION) + 1

        self._timer.start(total)

        for i, chunk in enumerate(
                pd.read_csv(INPUT_FILENAME, chunksize=CHUNK_SIZE_PER_ITERATION, low_memory=False, encoding='gbk')):
            df.append(chunk)
            self.prgbar.setValue(self.prgbar.value() + 1)
            print(i)
        data = temp[:0].append(df)
        self._timer.stop()
        del df
        return data
