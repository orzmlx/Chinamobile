from PyQt5.QtWidgets import QWidget

from model.data_watcher import DataWatcher
from PyQt5.QtWidgets import QPushButton, QApplication, QWidget, QVBoxLayout, QRadioButton


class RadioButtonGp(QWidget):

    def __init__(self, btn_list: [], name: str, watcher: DataWatcher):
        super().__init__()
        self.name = name
        self.radioBtns = btn_list
        self.watcher = watcher
        self.setup_ui()

    def setup_ui(self):
        init_text = self.radioBtns[0].text()
        self.radioBtns[0].setChecked(True)
        if self.name == 'manufacturers':
            self.watcher.setManufacturer(init_text)
        elif self.name == 'systems':
            self.watcher.setSystem(init_text)
        for btn in self.radioBtns:
            btn.toggled.connect(self.on_radio_button_toggled)

    def on_radio_button_toggled(self):
        for btn in self.radioBtns:
            if btn.isChecked():
                text = btn.text()
                if self.name == 'manufacturers':
                    self.watcher.setManufacturer(text)
                elif self.name == 'systems':
                    self.watcher.setSystem(text)
                break
