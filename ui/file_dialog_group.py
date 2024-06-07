# -*- coding:utf-8 -*-

try:
    from PyQt5.QtWidgets import QLabel, QToolButton, QFileDialog
except ImportError:
    from PySide6.QtWidgets import QLabel, QToolButton, QFileDialog

# from PySide6.QtWidgets import QLabel, QToolButton, QFileDialog

from model.data_watcher import DataWatcher
import os


class FileDialogGroup(QToolButton, QLabel):

    def __init__(self, btn: QToolButton, path_label: QLabel, watcher: DataWatcher):
        super().__init__()
        self.file_btn = btn
        self.watcher = watcher
        self.path_label = path_label
        # self.path_label.setStyleSheet("background-color: yellow")
        self.path_label.setStyleSheet("QLabel"
                                      "{"
                                      "border : 0.5px solid green;"
                                      "background : lightgreen;"
                                      "}")
        self.setUp()

    def setUp(self):
        if self.file_btn.objectName() == 'load_check_rule_btn':
            self.file_btn.clicked.connect(self.selectExcelFile)
        elif self.file_btn.objectName() == 'set_work_dir_btn':
            self.file_btn.clicked.connect(self.selectDir)
        elif self.file_btn.objectName() == 'huawei_command_btn':
            self.file_btn.clicked.connect(self.selectTxtFile)

    def selectTxtFile(self):
        fd = QFileDialog()
        self.data_path = fd.getOpenFileName(self, "Select TXT file", "", "TXT files (*.txt)")[0]
        self.path_label.setText(os.path.basename(self.data_path))
        if self.file_btn.objectName() == 'huawei_command_btn':
            self.watcher.tset_huawei_command_path(self.data_path)

    def selectExcelFile(self):
        fd = QFileDialog()
        self.data_path = fd.getOpenFileName(self, "Select EXCEL file", "", "EXCEL files (*.xlsx)")[0]
        self.path_label.setText(os.path.basename(self.data_path))
        if self.file_btn.objectName() == 'load_check_rule_btn':
            self.watcher.setConfigPath(self.data_path)

    def selectDir(self):
        fd = QFileDialog(
            self, "选择文件夹", "./", "ALL(*, *);;Images(*.png *.jpg);;Python文件(*.py)"
        )
        self.data_path = fd.getExistingDirectory(self, "选择文件夹")
        if self.data_path is not None:
            # self.start_btn.setEnabled(True)
            self.path_label.setText(os.path.basename(self.data_path))
            if self.file_btn.objectName() == 'set_work_dir_btn':
                self.watcher.setWorkDir(self.data_path)
