import pandas as pd
import os
from huaweirawdatareader import *
import huaweirawdatareader
import huaweiutils
import copy
import math
import numpy as np
import itertools


class HuaweiIntermediateGen:

    def __init__(self, data_path, standard_path,
                 g4_common_table, g5_common_table,
                 g4_site_info, g5_site_info, system):
        """
            一个是文件名对应的列名字典
            一个是文件名中的key的字典
        """
        self.system = system
        if self.system == '5G':
            self.site_info = pd.read_csv(g5_site_info, usecols=['CGI', '5G频段'])
            self.site_info.rename(columns={'5G频段': '共址类型'}, inplace=True)
            self.cell_identity = huaweiconfiguration.G5_CELL_IDENTITY
        else:
            self.site_info = pd.read_csv(g4_site_info, usecols=['CGI', '4G频段'])
            self.site_info.rename(columns={'4G频段': '共址类型'}, inplace=True)
            self.cell_identity = huaweiconfiguration.G4_CELL_IDENTITY

        self.g5_common_table = g5_common_table
        self.g4_common_table = g4_common_table
        self.file_path = data_path
        self.params_files_cols_dict = {}
        self.standard_path = standard_path
        self.standard_alone_df = pd.DataFrame()
        self.cal_rule = pd.read_excel(self.standard_path, sheet_name="参数计算方法", dtype=str)
        g4_common_df = pd.read_csv(g4_common_table, usecols=['中心载频信道号', '工作频段', '频率偏置'], encoding='gbk', dtype='str')
        self.g4_freq_band_dict, band_list = huaweiutils.generate_4g_frequency_band_dict(g4_common_df)
        self.all_area_classes = ""  # 所有可能小区类别
        self.all_cover_classes = ""  # 所有覆盖类型
        self.all_band = ""  # 所有的频带
        self.all_co_location = ""  # 所有的共址类型
        if self.system == '4G':

            self.g4_base_info_df = self.get_huawei_4g_base_info(band_list)
            self.all_band = huaweiutils.list_to_str(band_list)
            self.all_area_classes = huaweiutils.list_to_str(self.g4_base_info_df['区域类别'].unique().tolist())
            self.all_cover_classes = huaweiutils.list_to_str(self.g4_base_info_df['覆盖类型'].unique().tolist())
            self.all_co_location = huaweiutils.list_to_str(self.g4_base_info_df['共址类型'].unique().tolist())
        else:
            self.g5_base_info_df = self.get_huawei_5g_base_info()
            self.all_band = '4.9G|2.6G|700M'
            self.all_area_classes = huaweiutils.list_to_str(self.g5_base_info_df['区域类别'].unique().tolist())
            self.all_cover_classes = huaweiutils.list_to_str(self.g5_base_info_df['覆盖类型'].unique().tolist())
            self.all_co_location = huaweiutils.list_to_str(self.g5_base_info_df['共址类型'].unique().tolist())
        self.end_band = 'FDD-900|FDD-1800|F|A|D|E|4.9G|2.6G|700M'
        self.base_info_df = self.g4_base_info_df if system == '4G' else self.g5_base_info_df
        self.cell_config_df = pd.read_excel(self.standard_path, sheet_name="小区级别核查配置", true_values=["是"],
                                            false_values=["否"], dtype=str)
        self.freq_config_df = pd.read_excel(self.standard_path, sheet_name="频点级别核查配置", true_values=["是"],
                                            false_values=["否"], dtype=str)
        self.freq_config_df = self.freq_config_df[
            (self.freq_config_df['制式'] == self.system) & (self.freq_config_df['厂家'] == '华为')]
        self.cell_config_df = self.cell_config_df[
            (self.cell_config_df['制式'] == self.system) & (self.cell_config_df['厂家'] == '华为')]
        self.cell_config_df['参数组标识'] = self.cell_config_df['参数组标识'].astype(str)
        self.cell_config_df['QCI'] = self.cell_config_df['QCI'].astype(str)
        self.freq_config_df['参数组标识'] = self.freq_config_df['参数组标识'].astype(str)
        self.freq_config_df['QCI'] = self.freq_config_df['QCI'].astype(str)
        self.qci_file_name = 'LST NRCELLQCIBEARERQCI.csv' if self.system == '5G' else 'LST CELLQCIPARAQCI.csv'
        self.freq_config_df = self.sort_config(self.freq_config_df)
        self.cell_config_df = self.sort_config(self.cell_config_df)
        self.cell_df = pd.DataFrame()
        self.freq_df = []

    def sort_config(self, config):
        config = config.merge(self.cal_rule, how='left', on=['原始参数名称', '主命令'])
        config.sort_values(by=['伴随参数命令'], inplace=True)
        return config

    def get_huawei_4g_base_info(self, band_list):
        """
            获取基本信息列
        """
        cell_df = pd.read_csv(os.path.join(self.file_path, 'raw_result', 'LST CELL.csv'))
        enode_df = pd.read_csv(os.path.join(self.file_path, 'raw_result', 'LST ENODEBFUNCTION.csv'))
        cell_df = huaweiutils.add_4g_cgi(cell_df, enode_df)
        common_table = pd.read_csv(self.g4_common_table,
                                   usecols=['基站覆盖类型（室内室外）', '覆盖场景', '小区CGI', '地市名称', '工作频段', '小区区域类别'],
                                   encoding='gbk', dtype=str)

        common_table['工作频段'] = band_list
        common_table.rename(columns={'小区CGI': 'CGI', '基站覆盖类型（室内室外）': '覆盖类型', '地市名称': '地市',
                                     '小区区域类别': '区域类别', '工作频段': '频段'},
                            inplace=True)
        common_table['区域类别'] = common_table['区域类别'].apply(lambda x: str(x).replace('(农村)', ""))
        # 覆盖类型中，室内外和空白，归为室外
        common_table['覆盖类型'] = common_table['覆盖类型'].map({"室外": "室外", "室内外": "室外", "室内": "室内"})
        common_table['覆盖类型'].fillna("室外", inplace=True)
        # huaweiutils.output_csv(common_table, "common.csv", self.out_path)
        cell_df['CGI'] = "460-00-" + cell_df["eNodeB标识"].apply(str) + "-" + cell_df[
            huaweiconfiguration.G4_CELL_IDENTITY].apply(str)
        base_info_df = cell_df[['网元', huaweiconfiguration.G4_CELL_IDENTITY, '小区名称', 'CGI']]
        # base_info_df['频带'] = base_info_df['频带'].map({"n41": "2.6G", "n28": "700M", "n78": "4.9G", "n79": "4.9G"})
        # base_info_df = base_info_df.rename(columns={'频带': '频段'}, inplace=True)
        base_info_df = base_info_df.merge(common_table, how='left', on=['CGI'])
        base_info_df = base_info_df.merge(self.site_info, how='left', on=['CGI'])
        base_info_df['厂家'] = '华为'
        return base_info_df

    def get_huawei_5g_base_info(self):
        """
            获取基本信息列
        """
        ducell_df = pd.read_csv(os.path.join(self.file_path, 'raw_result', 'LST NRDUCELL.csv'))
        gnode_df = pd.read_csv(
            os.path.join(self.file_path, 'raw_result', 'LST GNODEBFUNCTION.csv'))
        ducell_df = huaweiutils.add_5g_cgi(ducell_df, gnode_df)
        common_table = pd.read_csv(self.g5_common_table, usecols=['覆盖类型', '覆盖场景', 'CGI', '地市', '工作频段', 'CELL区域类别'],
                                   encoding='gbk', dtype=str)
        common_table.rename(columns={'CELL区域类别': '区域类别'}, inplace=True)
        # common_table['工作频段'] = common_table['工作频段'].apply(lambda x: x.split('-')[1])
        # 覆盖类型中，室内外和空白，归为室外
        common_table['覆盖类型'] = common_table['覆盖类型'].map({"室外": "室外", "室内外": "室外", "室内": "室内"})
        common_table['覆盖类型'].fillna("室外", inplace=True)
        # huaweiutils.output_csv(common_table, "common.csv", self.out_path)
        ducell_df['CGI'] = "460-00-" + ducell_df["gNodeB标识"].apply(str) + "-" + ducell_df[
            huaweiconfiguration.G5_CELL_IDENTITY].apply(str)
        base_info_df = ducell_df[['网元', 'NR小区标识', 'NRDU小区名称', 'CGI', '频带', '双工模式']]
        base_info_df['频带'] = base_info_df['频带'].map(
            {"n41": "2.6G", "n28": "700M", "n78": "4.9G", "n79": "4.9G"})
        base_info_df = base_info_df.rename(columns={'频带': '频段'})
        base_info_df = base_info_df.merge(common_table, how='left', on=['CGI'])
        base_info_df = base_info_df.merge(self.site_info, how='left', on=['CGI'])
        base_info_df['厂家'] = '华为'
        return base_info_df

    def find_switch_cols(self, file_name, switch_params):
        find_params = {}
        base_cols = ['网元']
        df = pd.read_csv(file_name, nrows=1)
        cols = df.columns.tolist()
        if self.cell_identity in cols:
            base_cols.append(self.cell_identity)
        if '服务质量等级' in cols:
            base_cols.append('服务质量等级')
        if len(switch_params) == 0:
            logging.info("不需要在" + file_name + "中寻找开关参数")
            return find_params, base_cols
        if df.empty:
            logging.info(file_name + "中数据为空")
            return find_params, base_cols
        first_row = df.iloc[0]
        for param in switch_params:
            find = False
            for c in cols:
                cell = first_row[c]
                find_result = str(cell).find(param)
                if find_result < 0:
                    continue
                else:
                    find_params[param] = c
                    find = True
            if find is False:
                raise Exception("在【" + file_name + "】没有找到【" + param + "】,请检查参数名称")
        return find_params, base_cols

    def before_add_judgement(self, df, g):
        """
         在添加Judgement前先将数据中的4G频点变成FDD1800/FDD900/A/E/F对应
         并修改列名使其与配置中的列名相同
        """
        cols = g.columns.tolist()
        if '频点标识' in cols:
            frequencies = g['频点标识'].unique().tolist()
            if len(frequencies) > 0:
                frequency_param = frequencies[0]
                # 单独处理,将频带等于n1的去掉
                if frequency_param == '频带':
                    df = df[df[frequency_param] != 'n1']
                    df[frequency_param] = df[frequency_param].map(
                        {"n41": "2.6G", "n28": "700M", "n78": "4.9G", "n79": "4.9G"})
                all_freq = df[frequency_param].unique().tolist()
                if self.is_4g_freq(all_freq):
                    # df[frequency_param] = 'LTE'
                    df[frequency_param] = df[frequency_param].apply(huaweiutils.mapToBand,
                                                                    args=(self.g4_freq_band_dict,))
                    # 去掉对端非移动频点的行
                    df = df[df[frequency_param] != '其他频段']
                df.rename(columns={frequency_param: '对端频带'}, inplace=True)

        df = df.merge(self.base_info_df, how='left', on=['网元', self.cell_identity])
        # df.rename(columns={'频带': '频段'}, inplace=True)
        return df

    def extra_handler(self, df, param):
        """
            专门用来处'基于负载的异频RSRP触发门限(dBm)','基于业务的异频切换RSRP门限偏置(dB)'
            这个两个参数, 这两个参数是跨表,这两张表不属于同一种类型
        """
        if param == '基于业务的异频切换RSRP门限偏置(dB)':
            df = df.merge(self.cell_df[['网元', self.cell_identity, '基于负载的异频RSRP触发门限(dBm)']], how='left',
                          on=['网元', self.cell_identity])
            df[param] = df[param] + df['基于负载的异频RSRP触发门限(dBm)']
            df.drop('基于负载的异频RSRP触发门限(dBm)', inplace=True, axis=1)
            return True
        return False

    def processing_param_value(self, df, param, command):
        param_rule = self.cal_rule[(self.cal_rule['原始参数名称'] == param) & (self.cal_rule['主命令'] == command)]
        if param_rule.empty:
            return
        if self.extra_handler(df, param):
            return
        premise_param = str(param_rule['伴随参数'].iloc[0])
        cal_param = str(param_rule['计算方法'].iloc[0])
        # 如果伴随参数为空，但是计算方式里面需要伴随参数，那么设置有问题
        if premise_param == 'nan' and cal_param.find('.') >= 0:
            raise Exception("计算方式设置错误,没有伴随参数,但是计算方式是:" + cal_param)
        current_cols = df.columns.tolist()
        if premise_param in current_cols:
            if cal_param.find(',') >= 0:
                splits = cal_param.split(',')
                multiple0 = int(splits[0])
                multuple1 = int(splits[1])
                df[param] = df[param] * multiple0 + df[premise_param] * multuple1
            else:
                df[param] = df[param] * int(cal_param)
        # 如果前置参数在小区级别参数中,这种一定是频点级别merge小区级别
        elif not self.cell_df.empty and premise_param in self.cell_df.columns.tolist():
            df0 = df.merge(self.cell_df[['网元', self.cell_identity, premise_param]], how='left',
                           on=['网元', self.cell_identity])

    def get_freq_table(self, config_df):
        if config_df.empty:
            return []
        self.check_params(config_df)
        command_grouped = config_df.groupby(['主命令'])
        read_result_list = []
        for command, g in command_grouped:
            params = g['原始参数名称'].unique().tolist()
            command = huaweiutils.remove_digit(command, [",", ":"])
            file_name = os.path.join(self.file_path, 'raw_result', command + '.csv')
            read_res, base_cols = self.read_data_by_command(file_name, params, g)
            if read_res.empty:
                #有可能没有数据，因为没有加这个命令
                continue
            read_res = self.before_add_judgement(read_res, g)
            for p in params:
                self.processing_param_value(read_res, p, command)
                read_res = self.judge_compliance(read_res, p, command, g)
            read_result_list.append((read_res, command))
        # 只要主命令标识不为空，说明要读QCI进行过滤
        return read_result_list

    def check_params(self, config_df):
        # 如果某些参数的计算前提是必须有其他参数存在，则需要检查前置参数是否存在
        res_df = config_df[['原始参数名称', '主命令']].merge(self.cal_rule, how='left', on=['原始参数名称', '主命令'])
        # 有多少的伴随参数，检查这些伴随参数是否在检查参数中,否则报错
        primise_params_tuple = list(res_df[['伴随参数', '伴随参数命令']].apply(tuple).values)
        params_tuple = list(res_df[['原始参数名称', '主命令']].apply(tuple).values)
        for p in primise_params_tuple:
            if str(p[0]) == 'nan' and str(p[1]) == 'nan':
                continue
            find = False
            for q in params_tuple:
                if np.all(p == q):
                    find = True
                    break
            if not find:
                if not self.cell_df.empty and p[0] in self.cell_df.columns.tolist():
                    logging.info("伴随参数:" + p + "在小区级别参数中")
                else:
                    cal_param = \
                        self.cal_rule[(self.cal_rule['伴随参数'] == p[0]) & (self.cal_rule['伴随参数命令'] == p[1])][
                            '原始参数名称'].iloc[0]
                    raise Exception(str(p) + '是为了计算【' + cal_param + '】必须存在的参数,请在参数配置中加入该参数')

    def get_cell_table(self, config_df):
        all_param = list(config_df[['原始参数名称', '主命令']].apply(tuple).values)
        all_param = list(set(tuple(t) for t in all_param))
        self.check_params(config_df)
        command_identities = config_df['参数组标识'].tolist()
        command_identities = list(filter(lambda x: not 'nan' == x, command_identities))
        command_grouped = config_df.groupby(['主命令', 'QCI'])
        read_result_list = []
        qci_params = ['网元', self.cell_identity, '服务质量等级']
        read_qci = False
        qci_df = pd.DataFrame()
        qci_path = os.path.join(self.file_path, 'raw_result', self.qci_file_name)
        for p, g in command_grouped:
            command = p[0]
            qci = p[1]
            params = g['原始参数名称'].unique().tolist()
            command = huaweiutils.remove_digit(command, [",", ":"])
            file_name = os.path.join(self.file_path, 'raw_result', command + '.csv')
            read_res, base_cols = self.read_data_by_command(file_name, params, g)
            if qci.isdigit():
                read_qci = True
                qci = int(qci)
            read_result_list.append((read_res, qci))
        # 只要主命令标识不为空，说明要读QCI进行过滤
        if read_qci or len(command_identities) > 0:
            qci_params.extend(command_identities)
            qci_df = pd.read_csv(qci_path, usecols=qci_params)
            qci_df['服务质量等级'] = qci_df['服务质量等级'].astype(int)
        merge_qci_result = self.merge_with_qci_data(qci_df, read_result_list, qci_params)
        for p in all_param:
            param = p[0]
            command = p[1]
            self.processing_param_value(merge_qci_result, param, command)
            merge_qci_result = self.judge_compliance(merge_qci_result, param, command, config_df)
        return merge_qci_result

    def merge_with_qci_data(self, qci_df, read_result_list, qci_params):
        # 获取不需要输出的列，例如同频参数标识列
        qci_params.remove('网元')
        qci_params.remove(self.cell_identity)
        """
            将需要检查的数据合并成一张表
            这里的qci_df是全量
        """
        qci_res_list = []
        non_qci_res = self.base_info_df
        for r in read_result_list:
            cols = r[0].columns.tolist()
            qci = r[1]
            if not 'nan' == qci:
                res = qci_df[qci_df['服务质量等级'] == qci]
                on = list(set(cols) & set(res.columns.tolist()))
                qci_res = res[on].merge(r[0], how='left', on=on)
                qci_res_cols = qci_res.columns.tolist()
                last_cols = list(set(qci_res_cols) - set(qci_params))
                qci_res_list.append(qci_res[last_cols])
            else:
                on = ['网元', self.cell_identity]
                # if 'NR小区标识' in cols:
                #     on.append()
                non_qci_res = non_qci_res.merge(r[0], how='left', on=on)
        merge_qci_df = huaweiutils.merge_dfs(qci_res_list, on=['网元', self.cell_identity])
        res = merge_qci_df.merge(non_qci_res, how='left', on=['网元', self.cell_identity])
        return res

    def read_data_by_command(self, file_name, params, g):
        # 区分开关和非开关参数,剩下的params中都是非开关类参数
        check_params = copy.deepcopy(params)
        switch_params = []
        for i in range(len(check_params) - 1, -1, -1):
            param = check_params[i]
            is_switch = g[g['原始参数名称'] == param]['开关'].iloc[0]
            if not pd.isna(is_switch) and is_switch == '是':
                switch_params.append(param)
                check_params.remove(param)
        # 首先只读取一行数据，来找出switch开关在哪一行
        switch_dict, base_cols = self.find_switch_cols(file_name, switch_params)
        g['参数组标识'] = g['参数组标识'].astype('str')
        identity = [item for item in g['参数组标识'].unique().tolist() if
                    item.strip() != '' and item.strip() != 'nan' and item.strip() is not None]
        if len(identity) > 0 and not 'nan' == identity[0]:
            base_cols.append(identity[0])
        if '频点标识' in g.columns.tolist():
            frequency = g['频点标识'].unique().tolist()
            if len(frequency) > 0:
                base_cols.append(frequency[0])
        base_df = pd.read_csv(file_name, usecols=base_cols)
        # 如果没有开关参数，那么赋值为base_info
        switch_df = pd.DataFrame()
        # 查看主命令是否存在标识,主命令对应标识应该一样
        if len(switch_dict) > 0:
            switch_cols = (list(set(switch_dict.values())))
            switch_cols.extend(base_cols)
            df = pd.read_csv(file_name, usecols=switch_cols)
            switch_df = self.read_switch_data(df, base_df, switch_dict)
        # 读取非开关参数
        non_switch_df = pd.DataFrame()
        if len(check_params) > 0:
            non_switch_col = copy.deepcopy(base_cols)
            non_switch_col.extend(check_params)
            logging.debug("读取文件:【" + file_name + "】读取的列:【" + str(non_switch_col) + "】")
            non_switch_df = pd.read_csv(file_name, usecols=non_switch_col)
        # result_df = switch_df.merge(non_switch_df, how='left', on=base_cols)
        if not non_switch_df.empty and not switch_df.empty:
            result_df = pd.merge(switch_df, non_switch_df, how='left', on=base_cols)
        elif non_switch_df.empty and switch_df.empty:
            logging.warn("开关参数和非开关参数读取结果均为空,请检查参数名称或者原始数据结果是否包含该参数,当前检查参数【" + str(params) + "】")
            # raise Exception("开关参数和非开关参数读取结果均为空,请检查参数名称或者原始数据结果是否包含该参数,当前检查参数【" + str(params) + "】")
            return pd.DataFrame(), base_cols
        else:
            result_df = switch_df if not switch_df.empty else non_switch_df
        return result_df, base_cols

    def read_switch_data(self, df, base_df, switch_params):
        """
            读所有的开关参数并将其合并
        """
        result_df = pd.DataFrame()
        for param, c in switch_params.items():
            df_c = df[[c]]
            flatten_df = huaweiutils.flatten_features(df_c, c)[[param]]
            if result_df.empty:
                result_df = pd.concat([base_df, flatten_df], axis=1)
                continue
            # 由于是按命令分组,因此可以合并
            result_df = pd.concat([result_df, flatten_df], axis=1)
        return result_df

    def judge_compliance(self, df, param, command, g):
        g_c = g[(g['原始参数名称'] == param) & (g['主命令'] == command)]
        if g_c.empty:
            raise Exception('参数【' + param + '】没有找到配置信息')
        if '对端频带' not in g.columns.tolist():  # 表明是cell级别
            standard = g[g['原始参数名称'] == param]
            df = self.add_cell_judgement(df, standard, param)
        else:  # 频点级别
            # 判断是4G频点还是5G频点
            standard = g[(g['原始参数名称'] == param)]
            df = self.add_freq_judgement(df, standard, param)
        return df

    def add_freq_judgement(self, df, standard, param):
        standard_on = ['对端频带', '频段', '覆盖类型', '区域类别', '共址类型']
        deep_on = copy.deepcopy(standard_on)
        deep_on.append('推荐值')
        standard = self.expand_standard(standard, standard_on)
        df = df.merge(standard[deep_on], how='left',
                      on=standard_on)
        df.rename(columns={"推荐值": param + "#推荐值"}, inplace=True)
        df[param + "#合规"] = huaweiutils.freq_judge(df, param)
        return df

    def fill_col_na(self, standard, cols):
        for c in cols:
            if c == '区域类别':
                standard[c].fillna(self.all_area_classes, inplace=True)
            elif c == '覆盖类型':
                standard[c].fillna(self.all_cover_classes, inplace=True)
            elif c == '频段':
                standard[c].fillna(self.all_band, inplace=True)
            elif c == '共址类型':
                standard[c].fillna(self.all_co_location, inplace=True)
            elif c == '对端频带':
                standard[c].fillna(self.end_band, inplace=True)
            else:
                raise Exception(c + ",该字段没有定义过滤")

    def expand_standard(self, standard, cols):
        self.fill_col_na(standard, cols)
        for index, row in standard.iterrows():
            possible_row_list = []
            add_rows = []
            for c in cols:
                value = str(row[c])
                value = value.split('|') if value.find('|') >= 0 else [value]
                possible_row_list.append(value)
            possible_tuple = tuple(possible_row_list)
            result = list(itertools.product(*possible_tuple))
            # 二维列表去重，先转成tuple,然后去重
            result = list(set(tuple(t) for t in result))
            for r in result:
                new_row = copy.deepcopy(row)
                for idx, e in enumerate(list(r)):
                    new_row[cols[idx]] = e
                add_rows.append(new_row)
            standard = standard.drop(index=index)
            standard = standard.append(add_rows)
        return standard

    def add_cell_judgement(self, df, standard, param):
        standard = self.expand_standard(standard, ['区域类别', '覆盖类型', '频段', '共址类型'])
        df = df.merge(standard[['区域类别', '覆盖类型', '频段', '推荐值', '共址类型']], how='left', on=['频段', '覆盖类型', '区域类别', '共址类型'])
        df.rename(columns={"推荐值": param + "#推荐值"}, inplace=True)
        df[param + "#合规"] = huaweiutils.freq_judge(df, param)
        return df

    def is_4g_freq(self, all_freqs):
        for f in all_freqs:
            for key, item in self.g4_freq_band_dict.items():
                if str(f) in item:  # 证明该频段是4G频段
                    return True
        return False

    def sort_result(self, cols):
        base_cols = self.base_info_df.columns.tolist()
        diff = set(cols) - set(base_cols)
        if "对端频带" in diff:
            diff.remove("对端频带")
        diff1 = copy.deepcopy(diff)
        content_cols = []
        for m in diff:
            if m.find('#') < 0:
                content_cols.append(m)
                for n in diff1:
                    if m == n:
                        continue
                    if n.find(m) >= 0 and n.find('#') >= 0:
                        content_cols.append(n)
        for index, k in enumerate(content_cols):
            if k.find('#') >= 0:
                splits = k.split('#')
                suffix = splits[1]
                pre_element = content_cols[index - 1]
                if pre_element.find('#') >= 0:
                    pre_splits = pre_element.split('#')
                    pre_suffix = pre_splits[1]
                    if pre_suffix == '合规' and suffix == '推荐值':
                        content_cols[index - 1] = k
                        content_cols[index] = pre_element

        return list(set(cols) - set(content_cols)) + content_cols

    def generate_report(self):
        """

        :param df: 输入的推荐值和需要核查的参数
        :return:
        """
        cell_df = self.get_cell_table(self.cell_config_df)
        cell_df = cell_df[self.sort_result(cell_df.columns.tolist())]
        self.cell_df = cell_df
        out = os.path.join(self.file_path, 'check_result', 'cell')
        if not os.path.exists(out):
            os.makedirs(out)
        cell_df.to_csv(os.path.join(out, 'param_check_cell.csv'), index=False, encoding='utf_8_sig')
        freq_df_list = self.get_freq_table(self.freq_config_df)
        for df in freq_df_list:
            d = df[0]
            sorted_cols = self.sort_result(d.columns.tolist())
            d = d[sorted_cols]
            out = os.path.join(self.file_path, 'check_result', 'freq')
            if not os.path.exists(out):
                os.makedirs(out)
            d.to_csv(os.path.join(out, df[1] + '_freq.csv'),
                     index=False, encoding='utf_8_sig')


if __name__ == "__main__":
    filepaths = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\4G"
    standard_path = "C:\\Users\\No.1\\Desktop\\teleccom\\互操作参数核查结果.xlsx"
    g5_common_table = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\地市规则\\5G资源大表-20231227.csv"
    g4_common_table = "C:\\Users\\No.1\\Desktop\\teleccom\\LTE资源大表-0121\\LTE资源大表-0121.csv"
    g4_site_info = "C:\\Users\\No.1\\Desktop\\teleccom\\物理站CGI_4g.csv"
    g5_site_info = "C:\\Users\\No.1\\Desktop\\teleccom\\物理站CGI_5g.csv"
    raw_data_path = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\raw_data"

    raw_files = os.listdir(os.path.join(filepaths, 'raw_data'))
    raw_file_name_list = []
    for f in raw_files:
        f_name = os.path.split(f)[1]
        raw_file_name = os.path.join(filepaths, f_name.split('.')[0])
        report = HuaweiIntermediateGen(raw_file_name, standard_path, g4_common_table, g5_common_table,
                                       g4_site_info, g5_site_info, '4G')
        report.generate_report()
