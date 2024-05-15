# -*- coding: utf-8 -*-
import logging
from pandas.errors import EmptyDataError
from exception.read_raw_exception import ReadRawException
from reader.zte_rawdata_reader import ZteRawDataReader
import os
from utils import huaweiutils
import pandas as pd



class EricssonDataReader(ZteRawDataReader):

    def __init__(self, eri_raw_data_path, eri_config_file_path, system):
        self.eri_config_file_path = eri_config_file_path
        self.eri_raw_data_path = eri_raw_data_path
        self.system = system
        self.raw_file = None
        self.temp_path = None
        self.on_dict = {}
        # self.raw_file_dir = raw_file
        # raw_files = huaweiutils.find_file(raw_file, '.csv')
        # pattern = r'_\d+'
        # for f in raw_files:
        #     prefix = os.path.split(f)[0]
        #     origin_name = os.path.basename(f)
        #     new_name = os.path.join(prefix, re.sub(pattern, '', origin_name))
        #     os.rename(f, new_name)
        # 将匹配到的部分替换为空字符串
        self.manufacturer = '爱立信'
        # raw_output_path = os.path.join(zte_raw_data_path, system, f_name)

    def setRawFile(self, filePath):
        # raw_files = huaweiutils.find_file(filePath, '.csv')
        # pattern = r'_\d+'
        # for f in raw_files:
        #     prefix = os.path.split(f)[0]
        #     origin_name = os.path.basename(f)
        #     new_name = os.path.join(prefix, re.sub(pattern, '', origin_name))
        #     os.rename(f, new_name)
        self.raw_file = filePath
        self.temp_path = os.path.join(self.eri_raw_data_path, self.system, 'raw_data')
        if not os.path.exists(self.temp_path):
            os.makedirs(self.temp_path)

    # def __check_preparation(self):
    #     self.__auto_check_ref()

    def auto_check_ref(self):
        """
            自动检测NRCU表中有哪些Ref,根据Ref自动清洗相应表中的Mo项
            这样可以减少配置表中的配置项
        """
        raw_dir = os.path.join(self.eri_raw_data_path, self.system, 'raw_data')
        all_raw_files = huaweiutils.find_file(raw_dir, '.csv')
        cell_cus = [f for f in all_raw_files if str(f).find('NRCellCU') >= 0]
        if len(cell_cus) >= 2:
            raise ReadRawException(manufacturer='爱立信', message='发现多个CellCU文件')
        cell_cu = cell_cus[0]
        cell_cu_df = pd.read_csv(cell_cu)
        cols = cell_cu_df.columns.tolist()
        Ref_cols = [c for c in cols if c.lower().find('ref') >= 0]
        on_dict = self.__parse_on_element(cell_cu_df, Ref_cols)
        self.__update_mo(all_raw_files, on_dict)
        for ref in Ref_cols:
            cell_cu_df.rename(columns={ref: ref.lower()}, inplace=True)
        # 去掉日期
        cell_cu = self.__remove_digtal(cell_cu.name)
        cell_cu_df.to_csv(os.path.join(raw_dir, cell_cu), index=False)

    def __remove_digtal(self, file_path):
        filter_res = []
        if file_path.find('_') >= 0:
            file_splits = file_path.split("_")
            for split in file_splits:
                if split.isdigit():
                    continue
                filter_res.append(split)
        return '_'.join(filter_res)

    def __update_mo(self, all_raw_files, on_dict):
        """
        遍历每个文件，如果在CU文件中有相应的Ref,该文件增加一列，用来直接与NRCU进行匹配
        """
        for raw_f in all_raw_files:
            f_name = os.path.split(raw_f)[1]
            ref_name, checked_elemets = self.__get_check_elemet(f_name, on_dict)
            if len(checked_elemets) > 0:
                try:
                    df = pd.read_csv(str(raw_f))
                except EmptyDataError:
                    logging.info(raw_f.name + "是空文件")
                    continue
                except:
                    df = pd.read_csv(str(raw_f), encoding='gbk')
                if df.empty:
                    continue
                self.on_dict[str(raw_f)] = ref_name.lower() + 'ref'
                df[ref_name.lower() + 'ref'] = df.apply(EricssonDataReader.__add_ref_column, args=(checked_elemets,),
                                                        axis=1)
                df.to_csv(os.path.join(os.path.dirname(raw_f), f_name), index=False)
            # 如果没有找到对应Ref，那么是站级别的配置(与华为不同，爱立信部分参数为站级别设置)

    @staticmethod
    def __add_ref_column(row, checked_elemets):
        mo = row['MO']
        need_elements = []
        if 'nan' != mo:
            elements = mo.split(',')
            for e in elements:
                ele_name = e.split('=')[0]
                if ele_name in checked_elemets:
                    need_elements.append(e)
            return ','.join(need_elements)
        else:
            return ''

    def __get_check_elemet(self, file_name, e_dict):
        for key in e_dict.keys():
            if file_name.lower().find(key.lower()) >= 0:
                return key, e_dict[key]
        return None, []

    def __parse_on_element(self, df, cols):
        res = {}
        for col in cols:
            if df[col].isna().all():
                continue
            for i in range(500):
                on = df[col].iloc[i]
                if 'nan' == str(on):
                    continue
                elements = []
                feature_elements = on.split(',')
                for f in feature_elements:
                    ele = f.split('=')[0]
                    elements.append(ele)
                    rep_col = col.replace("Ref", "")
                    res[rep_col] = elements
        return res

    def clean_data(self, eri_config_file_path, eri_raw_data_path, system):
        eri_config_df = pd.read_excel(eri_config_file_path, sheet_name='爱立信数据清洗')
        f_name = os.path.basename(self.raw_file).replace('.csv', '')
        pickle_name = str(self.raw_file).replace(".csv", "")
        sheet_df = pd.DataFrame()
        if eri_config_df.empty:
            return
        try:
            sheet_df = pd.read_csv(self.raw_file)
        except Exception as e:
            logging.info('pickle文件名:' + pickle_name + '没有在' + os.path.dirname(self.raw_file) + '下找到')
            logging.error(e)
        if sheet_df.empty:
            logging.info(f_name + "没有数据")
            return
        f_name_with_digital = self.__remove_digtal(f_name)
        out_path = os.path.join(self.eri_raw_data_path, system, 'kget', 'raw_result')
        if not os.path.exists(out_path):
            os.makedirs(out_path)
        self.process_data_by_sheet(sheet_df, eri_config_df, f_name_with_digital, out_path, ';')

    def output_format_data(self):
        # self.__auto_check_ref()
        self.clean_data(self.eri_config_file_path, self.eri_raw_data_path, self.system)
        # zte_param_gather = pd.read_excel(self.zte_config_file_path, engine='openpyxl', sheet_name='数据汇聚')
        # zte_param_gather = zte_param_gather[zte_param_gather['厂家'] == self.manufacturer]
        # self.gather_files(zte_param_gather)

