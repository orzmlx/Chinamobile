# -*- coding:utf-8 -*-
from PyQt5.QtCore import qWarning
from model.data_watcher import DataWatcher
from ui.huaweistartup import Ui_MainWindow
from widgets.load_csv_group import LoadCsvGroup
from widgets.loading_thread import LoadingThread
from widgets.manufactor_radiobtn_gp import RadioButtonGp


class enhancer(Ui_MainWindow):


    def setupUi(self, MainWindow):
        super().setupUi(MainWindow)
        object_names = [self.load_5g_common_prgbar.objectName(), self.load_4g_common_prgbar.objectName(),
                        self.load_5g_site_info_prgbar.objectName(), self.load_4g_site_info_prgbar.objectName(),
                        self.load_check_rule_prgbar.objectName(), self.load_raw_data_prgbar.objectName()]

        button_list = [self.zte_radio, self.huawei_radio, self.zte_radio]
        watcher = DataWatcher(object_names)
        radio_btn_gp = RadioButtonGp(button_list, watcher)
        self.loadingThread = LoadingThread()
        self.load_5g_com_csv_btn = LoadCsvGroup(self.load_5g_common_btn,
                                                self.load_5g_common_prgbar,
                                                # self.load_5g_common_label,
                                                self.start_5g_common_btn,
                                                self.stop_5g_common_btn,
                                                self.load_5g_common_msg,
                                                watcher,
                                                self.loadingThread)
        self.load_4g_com_csv_btn = LoadCsvGroup(self.load_4g_common_btn,
                                                self.load_4g_common_prgbar,
                                                # self.load_4g_common_label,
                                                self.start_4g_common_btn,
                                                self.stop_4g_common_btn,
                                                self.load_4g_common_msg,
                                                watcher,
                                                self.loadingThread)
        self.load_5g_site_info_csv_btn = LoadCsvGroup(self.load_5g_site_info_btn,
                                                      self.load_5g_site_info_prgbar,
                                                      # self.load_5g_site_info_label,
                                                      self.start_5g_site_info_btn,
                                                      self.stop_5g_site_info_btn,
                                                      self.load_5g_site_info_msg,
                                                      watcher,
                                                      self.loadingThread)
        self.load_4g_site_info_csv_btn = LoadCsvGroup(self.load_4g_site_info_btn,
                                                      self.load_4g_site_info_prgbar,
                                                      # self.load_4g_site_info_label,
                                                      self.start_4g_site_info_btn,
                                                      self.stop_4g_site_info_btn,
                                                      self.load_4g_site_info_msg,
                                                      watcher,
                                                      self.loadingThread)
        self.load_check_rule_csv_btn = LoadCsvGroup(self.load_check_rule_btn,
                                                    self.load_check_rule_prgbar,
                                                    # self.load_check_rule_label,
                                                    None,
                                                    None,
                                                    self.load_check_rule_msg,
                                                    watcher,
                                                    self.loadingThread)
        self.load_raw_data_csv_btn = LoadCsvGroup(self.load_raw_data_btn,
                                                  self.load_raw_data_prgbar,
                                                  # self.load_raw_data_label,
                                                  self.start_raw_data_btn,
                                                  self.stop_raw_data_btn,
                                                  self.load_raw_data_msg,
                                                  watcher,
                                                  self.loadingThread)
        self.out_put_dir_csv_btn = LoadCsvGroup(self.out_put_dir_btn,
                                                None,
                                                # self.out_put_dir_label,
                                                None,
                                                None,
                                                self.out_put_dir_msg,
                                                watcher,
                                                self.loadingThread)
