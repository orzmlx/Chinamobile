# -*- coding:utf-8 -*-
import os
from datetime import datetime
import pandas as pd
from configuration import huawei_configuration, ericsson_configuration, zte_configuration
from utils import huaweiutils
from model import validator
import math


class DataWatcher:
    def __init__(self, data_names):
        self.all_ready = False
        self.parse_raw_data_ready = False
        self.manufacturer = None
        self.data_dict = {}
        self.work_dir = None
        self.raw_data_dir = None
        self.config_path = None
        self.system = None
        self.huawei_command_path = None
        self.processor = None
        self.files_number = 0
        for name in data_names:
            self.data_dict[name] = pd.DataFrame()
        now = datetime.now()
        self.date = now.strftime('%Y%m%d')

    def setDate(self, date):
        self.date = date

    def set_files_number(self, number):
        self.files_number = number

    def get_checked_raw_path(self):
        return os.path.join(self.work_dir, self.manufacturer, self.date, self.system)

    def set_huawei_command_path(self, path):
        self.huawei_command_path = path
        # self.parse_raw_data_ready = self.is_ready_for_check()

    def setSystem(self, system):
        self.system = system
        # self.parse_raw_data_ready = self.is_ready_for_check()

    def setManufacturer(self, manufacturer):
        self.manufacturer = manufacturer

        # self.parse_raw_data_ready = self.is_ready_for_check()

    def setWorkDir(self, output_dir):
        self.work_dir = output_dir
        # self.parse_raw_data_ready = self.is_ready_for_check()

    def setRawDataDir(self, raw_data_dir):
        self.raw_data_dir = raw_data_dir
        # self.parse_raw_data_ready = self.is_ready_for_check()

    def setConfigPath(self, config_path):
        self.config_path = config_path
        # self.parse_raw_data_ready = self.is_ready_for_check()

    def is_ready_for_check(self):
        return self.__is_common_input_ready() and self.__is_data_input_ready()

    def __is_data_input_ready(self):
        return not self.get_common_info().empty and not self.get_site_info().empty

    def __is_common_input_ready(self):
        return self.manufacturer is not None and self.work_dir is not None and \
               self.config_path is not None and self.raw_data_dir is not None

    def get_4g_common_df(self):
        # return self.data_dict['load_4g_common_btn']
        df = pd.read_csv('C:\\Users\\No.1\\Desktop\\teleccom\\LTE资源大表-0414.csv', encoding='gbk')
        df['工作频段'] = df[['工作频段', '频率偏置']].apply(DataWatcher.get_acc_band, axis=1)
        return df

    def get_5g_common_df(self):
        # return self.data_dict['load_5g_common_btn']
        return pd.read_csv('C:\\Users\\No.1\\Desktop\\界面测试\\5G资源大表-20240324.csv', encoding='gbk')

    def get_4g_siteifo_df(self):
        site_info = pd.read_csv('C:\\Users\\No.1\\Desktop\\界面测试\\物理站CGI_4g.csv',
                                usecols=['CGI', '4G频段', '物理站编号'])
        # site_info = self.data_dict['load_4g_site_info_btn'][['CGI', '4G频段']]
        site_info.rename(columns={'4G频段': '共址类型'}, inplace=True)
        site_info.drop_duplicates(subset=['CGI'], keep='first', inplace=True)
        return site_info

    def get_5g_siteifo_df(self):
        # site_info = self.data_dict['load_5g_site_info_btn'][['CGI', '5G频段']]
        site_info = pd.read_csv('C:\\Users\\No.1\\Desktop\\界面测试\\物理站CGI_5g.csv',
                                encoding='utf8', usecols=['CGI', '5G频段', '物理站编号'])
        # self.site_info = pd.read_csv(g5_site_info, usecols=['CGI', '5G频段'])
        # self.site_info.rename(columns={'5G频段': '共址类型'}, inplace=True)
        site_info.rename(columns={'5G频段': '共址类型'}, inplace=True)
        site_info.drop_duplicates(subset=['CGI'], keep='first', inplace=True)
        return site_info

    def get_output_path(self):
        return self.work_dir

    def get_rawdata_path(self):
        return self.raw_data_dir

    def update_csv(self, name, data):
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
                self.all_ready = True if not current_data.empty else False
            return True  # 只指示数据校验是否合格
        return False

    def valid(self, name, data):
        if name == 'load_5g_common_btn':
            return validator.is_5g_common_valid(data)
        elif name == 'load_4g_common_btn':
            return validator.is_4g_common_valid(data)
        elif name == 'load_5g_site_info_btn':
            return validator.is_5g_site_info_valid(data)
        elif name == 'load_4g_site_info_btn':
            return validator.is_4g_site_info_valid(data)
        elif name == 'load_raw_data_btn':
            return validator.is_raw_data_valid(data, self.manufacturer)
        return False

    def get_site_info(self, spec_system=None):
        if spec_system is None:
            return self.get_5g_siteifo_df() if self.system == '5G' else self.get_4g_siteifo_df()
        else:
            return self.get_5g_siteifo_df() if spec_system == '5G' else self.get_4g_siteifo_df()

    def get_common_info(self):
        return self.get_4g_common_df() if self.system == '4G' else \
            self.get_5g_common_df()

    def g4_prepare(self):
        # g4_common_table = self.get_4g_common_df()
        g4_common_df = self.get_4g_common_df()[['中心载频信道号', '工作频段', '频率偏置']]
        g4_freq_band_dict, g4_band_list = huaweiutils.generate_4g_frequency_band_dict(g4_common_df)
        return g4_freq_band_dict, g4_band_list

    def get_base_info(self, f_name):
        if self.manufacturer == '华为':
            if self.system == '4G':
                self.g4_base_info_df = self.get_huawei_4g_base_info(f_name)
                # self.all_band = huaweiutils.list_to_str(band_list)
                # self.all_band = ''
            else:
                self.g5_base_info_df = self.get_huawei_5g_base_info(f_name)
                # self.all_band = '4.9G|2.6G|700M|nan'
        elif self.manufacturer == '中兴':
            if self.system == '4G':
                self.g4_base_info_df = self.get_zte_4g_base_info()
                # self.all_band = huaweiutils.list_to_str(band_list)
            else:
                self.g5_base_info_df = self.get_zte_5g_base_info()
                # self.all_band = '4.9G|2.6G|700M|nan'
        elif self.manufacturer == '爱立信':
            if self.system == '5G':
                self.g5_base_info_df = self.get_eri_5g_base_info(f_name)
                # self.all_band = '4.9G|2.6G|700M|nan'
            else:
                self.g4_base_info_df = self.get_eri_4g_base_info()
                # self.all_band = huaweiutils.list_to_str(band_list)
        base_inf_df = self.g4_base_info_df if self.system == '4G' else self.g5_base_info_df
        return base_inf_df

    def get_eri_4g_base_info(self):
        pass

    def get_zte_4g_base_info(self):
        site_info = self.get_site_info()
        common_table = self.get_4g_common()
        base_info_df = common_table.merge(site_info, how='left', on=['CGI'])
        base_info_df['厂家'] = self.manufacturer
        return base_info_df

    def get_huawei_4g_base_info(self, f_name):
        """
            获取基本信息列
        """
        site_info = self.get_site_info()
        common_table = self.get_4g_common()
        checked_raw_path = os.path.join(self.get_checked_raw_path(), f_name, 'raw_result')
        cell_df = pd.read_csv(os.path.join(checked_raw_path, 'LST CELL.csv'))
        enode_df = pd.read_csv(os.path.join(checked_raw_path, 'LST ENODEBFUNCTION.csv'))
        cell_df = huaweiutils.add_4g_cgi(cell_df, enode_df)
        base_info_df = cell_df[['网元', huawei_configuration.G4_CELL_IDENTITY, '小区名称', 'CGI', 'NB-IoT小区指示', '下行频点']]
        base_info_df = base_info_df.merge(common_table, how='left', on=['CGI'])
        base_info_df = base_info_df.merge(site_info, how='left', on=['CGI'])
        base_info_df['厂家'] = self.manufacturer
        return base_info_df

    def get_eri_5g_base_info(self, f_name):
        site_info = self.get_site_info()
        common_table = self.get_5g_common()
        checked_raw_path = os.path.join(self.get_checked_raw_path(), 'kget', 'raw_result')
        cell_cu_df = pd.read_csv(os.path.join(checked_raw_path, 'NRCellCU.csv'))
        reserved_cols = list(filter(lambda x: x.endswith('ref'), cell_cu_df.columns.tolist()))
        # 只保留Ref列,还有MO	MeContext	city	cellName
        fixed_cols = ['MO', 'MeContext', 'cellName', 'CGI']
        fixed_cols.extend(reserved_cols)
        cell_cu_df = cell_cu_df[fixed_cols]
        # cell_cu_df.dropna(axis=1, how='all', inplace=True)
        base_info_df = cell_cu_df.merge(common_table, how='left', on=['CGI'])
        base_info_df = base_info_df.merge(site_info, how='left', on=['CGI'])
        base_info_df['频段'] = base_info_df['工作频段'].map(
            {"NR-D": "2.6G", "NR-700": "700M", "NR-C": "4.9G"})
        base_info_df['厂家'] = self.manufacturer
        return base_info_df

    def get_zte_5g_base_info(self):
        site_info = self.get_site_info()
        common_table = self.get_5g_common()
        base_info_df = common_table.merge(site_info, how='left', on=['CGI'])
        base_info_df['频段'] = base_info_df['工作频段'].map(
            {"NR-D": "2.6G", "NR-700": "700M", "NR-C": "4.9G"})
        base_info_df['厂家'] = self.manufacturer
        return base_info_df

    def get_huawei_5g_base_info(self, f_name):
        """
            获取基本信息列
        """
        site_info = self.get_site_info()
        common_table = self.get_5g_common()
        checked_raw_path = os.path.join(self.get_checked_raw_path(), f_name, 'raw_result')
        ducell_df = pd.read_csv(os.path.join(checked_raw_path, 'LST NRDUCELL.csv'), dtype=str)
        gnode_df = pd.read_csv(
            os.path.join(checked_raw_path, 'LST GNODEBFUNCTION.csv'), dtype=str)
        ducell_df = huaweiutils.add_5g_cgi(ducell_df, gnode_df)
        ducell_df['CGI'] = "460-00-" + ducell_df["gNodeB标识"].apply(str) + "-" + ducell_df[
            huawei_configuration.G5_CELL_IDENTITY].apply(str)
        base_info_df = ducell_df[['网元', 'NR小区标识', 'NRDU小区名称', 'CGI', '频带']]
        base_info_df['频带'] = base_info_df['频带'].map(
            {"n41": "2.6G", "n28": "700M", "n78": "4.9G", "n79": "4.9G"})
        base_info_df = base_info_df.rename(columns={'频带': '频段'})
        base_info_df = base_info_df.merge(common_table, how='left', on=['CGI'])
        base_info_df = base_info_df.merge(site_info, how='left', on=['CGI'])
        base_info_df['厂家'] = self.manufacturer
        return base_info_df

    @staticmethod
    def get_acc_band(row):
        band = row['工作频段']
        offset = row['频率偏置']
        band = str(band)
        if pd.isna(band):
            return math.nan
        band = band.replace('频段', '')
        if band.find('FDD') >= 0:
            band = str(offset)
            # 如果不是FDD-1800或者FDD-900,那么直接去掉数字
            if str(offset).find('FDD') < 0:
                band = huaweiutils.remove_digit(band, [])
            if str(offset).find('-') >= 0:
                band = band.replace('-', '')
        return band

    def get_4g_common(self):

        common_table = self.get_4g_common_df()[['基站覆盖类型（室内室外）', '覆盖场景', '小区CGI', '地市名称', '工作频段', '小区区域类别']]
        # common_table['工作频段'] = common_table[['工作频段', '频率偏置']].apply(DataWatcher.get_acc_band)
        common_table.rename(columns={'小区CGI': 'CGI', '基站覆盖类型（室内室外）': '覆盖类型', '地市名称': '地市',
                                     '小区区域类别': '区域类别', '工作频段': '频段'}, inplace=True)
        common_table['区域类别'].fillna(value='农村', inplace=True)
        # common_table['区域类别'] = common_table['区域类别'].apply(lambda x: str(x).replace('三类(农村)', "三类"))
        common_table['区域类别'] = common_table['区域类别'].map({"一类": "一类", "二类": "二类", "三类(农村)": "三类", '四类(农村)': '农村'})
        # 覆盖类型中，室内外和空白，归为室外
        common_table['覆盖类型'] = common_table['覆盖类型'].map({"室外": "室外", "室内外": "室外", "室内": "室内"})
        common_table['覆盖类型'].fillna("室外", inplace=True)
        na_area_common_df = common_table[common_table['区域类别'].isna()]
        # self.na_area_cgi = na_area_common_df['CGI'].unique().tolist()
        common_table['区域类别'].fillna(value='农村', inplace=True)
        return common_table

    def get_5g_common(self):
        # common_table = pd.read_csv(self.get, usecols=['覆盖类型', '覆盖场景', 'CGI', '地市', '工作频段', 'CELL区域类别'],
        #                            encoding='gbk', dtype=str)
        common_table = self.get_5g_common_df()[['覆盖类型', '覆盖场景', 'CGI', '地市', '工作频段', 'CELL区域类别']]
        common_table.rename(columns={'CELL区域类别': '区域类别'}, inplace=True)
        # common_table['工作频段'] = common_table['工作频段'].apply(lambda x: x.split('-')[1])
        # 覆盖类型中，室内外和空白，归为室外
        common_table['覆盖类型'] = common_table['覆盖类型'].map({"室外": "室外", "室内外": "室外", "室内": "室内"})
        common_table['覆盖类型'].fillna("室外", inplace=True)
        na_area_common_df = common_table[common_table['区域类别'].isna()]
        # self.na_area_cgi = na_area_common_df['CGI'].unique().tolist()
        common_table['区域类别'].fillna(value='农村', inplace=True)
        return common_table

    def get_base_cols(self):
        if self.manufacturer is None:
            raise Exception('没有设置厂商')
        if self.manufacturer == '华为':
            return huawei_configuration.g5_base_cols if self.system == '5G' else huawei_configuration.g4_base_cols
        elif self.manufacturer == '爱立信':
            return ericsson_configuration.g5_base_cols if self.system == '5G' else ericsson_configuration.g4_base_cols
        elif self.manufacturer == '中兴':
            return zte_configuration.g5_base_cols if self.system == '5G' else zte_configuration.g4_base_cols

    def get_raw_result_files(self):

        if (self.manufacturer == '中兴' and self.system == '4G') or self.manufacturer == '爱立信':
            return [os.path.join(self.work_dir, self.manufacturer, self.date, self.system, 'kget')]
        elif self.manufacturer == '中兴' and self.system == '5G':
            raw_dirs = []
            raw_files = os.listdir(os.path.join(self.work_dir, self.manufacturer, self.date, self.system))
            for f in raw_files:
                raw_dir = os.path.join(self.work_dir, self.manufacturer, self.date, self.system, os.path.basename(f))
                if not os.path.isdir(raw_dir):
                    continue
                raw_dirs.append(raw_dir)
            return raw_dirs
        else:
            raw_dirs = []
            raw_files = os.listdir(
                os.path.join(self.work_dir, self.manufacturer, self.date, self.system, 'raw_data'))
            for f in raw_files:
                raw_dir = os.path.join(self.work_dir, self.manufacturer, self.date, self.system, os.path.basename(f))
                raw_dir = raw_dir.replace('.txt', '')
                raw_dirs.append(raw_dir)
            return raw_dirs
