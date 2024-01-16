# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'huawei.ui'
#
# Created by: PyQt5 UI code generator 5.10.1
#
# WARNING! All changes made in this file will be lost!

from PyQt5 import QtCore, QtGui, QtWidgets

class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(807, 600)
        self.Widge = QtWidgets.QWidget(MainWindow)
        self.Widge.setObjectName("Widge")
        self.textEdit = QtWidgets.QPlainTextEdit(self.Widge)
        self.textEdit.setGeometry(QtCore.QRect(20, 10, 391, 221))
        self.textEdit.setObjectName("textEdit")
        self.pushButton = QtWidgets.QPushButton(self.Widge)
        self.pushButton.setGeometry(QtCore.QRect(1100, 550, 140, 141))
        #self.pushButton.setGeometry(QtCore.QRect(350, 280, 75, 23))
        self.pushButton.setObjectName("pushButton")
        MainWindow.setCentralWidget(self.Widge)
        self.menubar = QtWidgets.QMenuBar(MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 807, 22))
        self.menubar.setObjectName("menubar")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "统计薪资"))
        self.textEdit.setPlaceholderText(_translate("MainWindow", "请输入薪资信息"))
        self.pushButton.setText(_translate("MainWindow", "统计"))

