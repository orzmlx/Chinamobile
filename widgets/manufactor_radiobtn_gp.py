from PyQt5.QtWidgets import QWidget

from model.data_watcher import DataWatcher
from PyQt5.QtWidgets import QPushButton, QApplication, QWidget, QVBoxLayout, QRadioButton


class RadioButtonGp(QWidget):

    def __init__(self, btn_list, watcher: DataWatcher):
        super().__init__()
        self.radioBtns = btn_list
        self.watcher = watcher
        self.setup_ui()

    def setup_ui(self):
        for btn in self.radioBtns:
            btn.toggled.connect(lambda: self.on_radio_button_toggled(btn))

    def on_radio_button_toggled(self, btn):
        text = btn.text()
        self.watcher.setManufacturer(text)
