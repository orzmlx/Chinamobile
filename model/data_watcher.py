# -*- coding:utf-8 -*-

import pandas as pd

from model import validator


class DataWatcher:
    def __init__(self, data_names):
        self.ready = False
        self.manufacturer = None
        self.data_dict = {}
        self.output_dir = None
        self.raw_data_dir = None

        for name in data_names:
            self.data_dict[name] = pd.DataFrame()

    def setManufacturer(self, manufacturer):
        self.manufacturer = manufacturer

    def setOutputPath(self, output_dir):
        self.output_dir = output_dir

    def setRawDataDir(self, raw_data_dir):
        self.raw_data_dir = raw_data_dir

    def update(self, name, data):
        """
            先验证数据,在更新
            :param name: 按钮的名字
            :param data: 传入的数据内容,DataFrame或者str
            :return: 验证是否成功
        """
        if self.valid(name, data):
            self.data_dict[name] = data
            for key in self.data_dict:
                current_data = self.data_dict[key]
                self.ready = True if not current_data.empty else False
            return True
        return False

    def valid(self, name, data):
        if name == 'load_5g_common_prgbar':
            return validator.is_5g_common_valid(data)
        elif name == 'load_4g_common_prgbar':
            return validator.is_4g_common_valid(data)
        elif name == 'load_5g_site_info_prgbar':
            return validator.is_5g_site_info_valid(data)
        elif name == 'load_4g_site_info_prgbar':
            return validator.is_4g_site_info_valid(data)
        elif name == 'load_raw_data_prgbar':
            return validator.is_raw_data_valid(data, self.manufacturer)
        return False
