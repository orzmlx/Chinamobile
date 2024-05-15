
try:
    from PyQt5.QtWidgets import QWidget
except ImportError:
    from PySide6.QtWidgets import QWidget

# from PySide6.QtWidgets import QWidget

from model.data_watcher import DataWatcher


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
            self.radioBtns[0].clicked.connect(lambda: self.watcher.setManufacturer('华为'))
            self.radioBtns[1].clicked.connect(lambda: self.watcher.setManufacturer('中兴'))
            self.radioBtns[2].clicked.connect(lambda: self.watcher.setManufacturer('爱立信'))
        elif self.name == 'systems':
            self.watcher.setSystem(init_text)
            self.radioBtns[0].clicked.connect(lambda: self.watcher.setSystem('4G'))
            self.radioBtns[1].clicked.connect(lambda: self.watcher.setSystem('5G'))


    # def setManufacturer(self,manufacturer):
    #     self.watcher.setManufacturer(manufacturer)
    #
    # def setSystem(self,system):
    #     self.watcher.setSystem(system)

    # def on_radio_button_toggled(self):
    #     for btn in self.radioBtns:
    #         if btn.isChecked():
    #             text = btn.text()
    #             if self.name == 'manufacturers':
    #                 self.watcher.setManufacturer(text)
    #             elif self.name == 'systems':
    #                 self.watcher.setSystem(text)
    #             break
