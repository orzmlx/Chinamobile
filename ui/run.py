# -*- coding:utf-8 -*-
from ui import huaweistartup, enhanced_startup
import sys
from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QApplication

from ui.enhanced_startup import EnhancedStartUp

if __name__ == '__main__':
    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling)
    QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps)
    # 获取UIC窗口操作权限
    app = QtWidgets.QApplication(sys.argv)
    MainWindow = QtWidgets.QMainWindow()
    # 调自定义的界面（即刚转换的.py对象）
    # Ui = huaweistartup.Ui_MainWindow()  # 这里也引用了一次helloworld.py文件的名字注意
    # Ui.setupUi(MainWindow
    ui = EnhancedStartUp()
    ui.setupUi(MainWindow)
    # 显示窗口并释放资源
    MainWindow.show()
    sys.exit(app.exec_())