# -*- coding:utf-8 -*-

try:
    from PyQt5.QtWidgets import QErrorMessage
except ImportError:
    from PySide6.QtWidgets import QErrorMessage

# from PySide6.QtWidgets import QErrorMessage

from model.data_watcher import DataWatcher
from model.signal_message import message
from backend.check_thread import CheckThread
from ui.huaweistartup import Ui_mainWindow
from ui.file_dialog_group import FileDialogGroup
from ui.load_csv_group import LoadCsvGroup
from ui.radiobtn_gp import RadioButtonGp


class EnhancedStartUp(Ui_mainWindow):

    def __int__(self, *args, **kwargs):
        super().__init__()

        # self.check_test_preparation()

    def finished(self, msg: message):
        if msg.code == 2:
            self.check_prgbar.setValue(self.check_prgbar.maximum())
            if self.check_prgbar.maximum() == 0:
                self.check_prgbar.setMaximum(1)
                self.check_prgbar.setValue(1)

        else:
            self.check_err_msg.showMessage(msg.signal_message)
            self.check_prgbar.setValue(0)

        self.check_btn.setEnabled(True)

    def check(self, watcher: DataWatcher):
        self.check_btn.setEnabled(False)
        self.check_prgbar.setValue(watcher.files_number)
        self.check_thread.start()

    def setupUi(self, MainWindow):
        super().setupUi(MainWindow)
        self.watcher = None
        self.check_err_msg = QErrorMessage.qtHandler()
        self.check_err_msg.setWindowTitle("错误提示")
        object_names = [self.load_5g_common_prgbar.objectName(), self.load_4g_common_prgbar.objectName(),
                        self.load_5g_site_info_prgbar.objectName(), self.load_4g_site_info_prgbar.objectName()
                        # self.load_raw_data_prgbar.objectName()
                        ]

        self.watcher = DataWatcher(object_names)
        self.check_thread = CheckThread(self.watcher)
        self.check_thread.finished.connect(self.finished)
        self.check_thread.valueChanged.connect(self.check_prgbar.setValue)
        manufacturer_radio_button_group = [self.huawei_radio, self.zte_radio, self.eri_radio]
        system_radios = [self.g5_radio, self.g4_radio]
        self.check_btn.clicked.connect(lambda: self.check(self.watcher))
        self.system_radio_button_group = RadioButtonGp(system_radios, 'systems', self.watcher)

        self.manufacturer_radio_button_group = RadioButtonGp(manufacturer_radio_button_group, 'manufacturers',
                                                             self.watcher)

        self.set_huawei_command = FileDialogGroup(self.huawei_command_btn,
                                                  self.huaweicommand_btn_label,
                                                  self.watcher
                                                  )

        self.set_work_dir_group = FileDialogGroup(self.set_work_dir_btn,
                                                  self.set_work_dir_label,
                                                  self.watcher
                                                  )

        self.load_check_rule_group = FileDialogGroup(self.load_check_rule_btn,
                                                     self.load_check_rule_label,
                                                     self.watcher
                                                     )
        self.load_5g_com_csv_btn = LoadCsvGroup(self.load_5g_common_btn,
                                                self.load_5g_common_prgbar,
                                                # self.load_5g_common_label,
                                                self.start_5g_common_btn,
                                                # self.stop_5g_common_btn,
                                                None,
                                                self.load_5g_common_msg,
                                                self.watcher
                                                # self.loadingThread
                                                )
        self.load_4g_com_csv_btn = LoadCsvGroup(self.load_4g_common_btn,
                                                self.load_4g_common_prgbar,
                                                # self.load_4g_common_label,
                                                self.start_4g_common_btn,

                                                None,
                                                self.load_4g_common_msg,
                                                self.watcher
                                                # self.loadingThread
                                                )
        self.load_5g_site_info_csv_btn = LoadCsvGroup(self.load_5g_site_info_btn,
                                                      self.load_5g_site_info_prgbar,
                                                      # self.load_5g_site_info_label,
                                                      self.start_5g_site_info_btn,

                                                      None,
                                                      self.load_5g_site_info_msg,
                                                      self.watcher
                                                      # self.loadingThread
                                                      )
        self.load_4g_site_info_csv_btn = LoadCsvGroup(self.load_4g_site_info_btn,
                                                      self.load_4g_site_info_prgbar,
                                                      # self.load_4g_site_info_label,
                                                      self.start_4g_site_info_btn,
                                                      # self.stop_4g_site_info_btn,
                                                      None,
                                                      self.load_4g_site_info_msg,
                                                      self.watcher
                                                      # self.loadingThread
                                                      )

        self.load_raw_data_csv_btn = LoadCsvGroup(self.load_raw_data_btn,
                                                  self.load_raw_data_prgbar,
                                                  # self.load_raw_data_label,
                                                  self.start_raw_data_btn,

                                                  None,
                                                  self.load_raw_data_msg,
                                                  self.watcher
                                                  # self.loadingThread
                                                  )
