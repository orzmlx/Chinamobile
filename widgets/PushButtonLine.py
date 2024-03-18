
# -*- coding:utf-8 -*-

try:
    from PyQt5.QtCore import QTimer, QThread, pyqtSignal
    from PyQt5.QtGui import QPainter, QColor, QPen
    from PyQt5.QtWidgets import QPushButton, QApplication, QWidget, QVBoxLayout
except ImportError:
    from PySide2.QtCore import QTimer, QThread, Signal as pyqtSignal
    from PySide2.QtGui import QPainter, QColor, QPen
    from PySide2.QtWidgets import QPushButton, QApplication, QWidget, QVBoxLayout


class PushButtonLine(QPushButton):
    lineColor = QColor(0, 150, 136)

    def __init__(self, *args, **kwargs):
        self._waitText = kwargs.pop("waitText", "5G工参加载中....")
        super(PushButtonLine, self).__init__(*args, **kwargs)
        self._text = self.text()
        self._percent = 0
        self._timer = QTimer(self, timeout=self.update)
        self.clicked.connect(self.start)

    def __del__(self):
        self.stop()



    def paintEvent(self, event):
        super(PushButtonLine, self).paintEvent(event)
        if not self._timer.isActive():
            return
        # ??????
        painter = QPainter(self)
        pen = QPen(self.lineColor)
        pen.setWidth(4)
        painter.setPen(pen)
        painter.drawLine(0, self.height(), self.width()
                         * self._percent, self.height())

    def start(self):
        if hasattr(self, "loadingThread"):
            return self.stop()
        from widgets.LoadingThread import LoadingThread
        self.loadingThread = LoadingThread(self)
        self.loadingThread.valueChanged.connect(self.setPercent)
        self._timer.start(100)  # 100ms
        self.loadingThread.start()
        self.setText(self._waitText)

    def stop(self):
        try:
            if hasattr(self, "loadingThread"):
                if self.loadingThread.isRunning():
                    self.loadingThread.requestInterruption()
                    self.loadingThread.quit()
                    self.loadingThread.wait(2000)
                del self.loadingThread
        except RuntimeError:
            pass
        try:
            self._percent = 0
            self._timer.stop()
            self.setText(self._text)
        except RuntimeError:
            pass

    def setPercent(self, v):
        self._percent = v
        if v == 1:
            self.stop()
            self.update()

    def setLineColor(self, color):
        self.lineColor = QColor(color)
        return self
