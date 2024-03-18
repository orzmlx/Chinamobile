from random import randint

try:
    from PyQt5.QtCore import QTimer, QThread, pyqtSignal
    from PyQt5.QtGui import QPainter, QColor, QPen
    from PyQt5.QtWidgets import QPushButton, QApplication, QWidget, QVBoxLayout
except ImportError:
    from PySide2.QtCore import QTimer, QThread, Signal as pyqtSignal
    from PySide2.QtGui import QPainter, QColor, QPen
    from PySide2.QtWidgets import QPushButton, QApplication, QWidget, QVBoxLayout

StyleSheet = '''
PushButtonLine {
    color: white;
    border: none;
    min-height: 48px;
    background-color: #90caf9;
}
'''


class LoadingThread(QThread):
    valueChanged = pyqtSignal(float)  # 当前值/最大值

    def __init__(self, *args, **kwargs):
        super(LoadingThread, self).__init__(*args, **kwargs)
        self.totalValue = randint(100, 200)  # 模拟最大

    def run(self):
        for i in range(self.totalValue + 1):
            if self.isInterruptionRequested():
                break
            self.valueChanged.emit(i / self.totalValue)
            QThread.msleep(randint(50, 100))
