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

    def __init__(self, data_path, standard_path, g4_common_table, g5_common_table, system):
        """
            一个是文件名对应的列名字典
            一个是文件名中的key的字典
        """
        self.system = system
        if self.system == '5G':
            self.site_info = pd.read_csv(g5_site_info, usecols=['CGI', '5G频段'])
            self.cell_identity = huaweiconfiguration.G5_CELL_IDENTITY
        else:
            self.site_info = pd.read_csv(g4_site_info, usecols=['CGI', '4G频段'])
            self.cell_identity = huaweiconfiguration.G4_CELL_IDENTITY

        self.g5_common_table = g5_common_table
        self.g4_common_table = g4_common_table
        self.file_path = data_path
        self.params_files_cols_dict = {}
        self.standard_path = standard_path
        self.standard_alone_df = pd.DataFrame()
        g4_common_df = pd.read_csv(g4_common_table, usecols=['中心载频信道号', '工作频段', '频率偏置'], encoding='gbk', dtype='str')
        self.g4_freq_band_dict, band_list = huaweiutils.generate_4g_frequency_band_dict(g4_common_df)
        self.g4_base_info_df = self.get_huawei_4g_base_info(band_list)
        self.g5_base_info_df = self.get_huawei_5g_base_info()

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


    def get_huawei_4g_base_info(self, band_list):
        """
            获取基本信息列
        """
        cell_df = pd.read_csv(os.path.join(self.file_path, 'LST CELL.csv'))
        enode_df = pd.read_csv(os.path.join(self.file_path, 'LST ENODEBFUNCTION.csv'))
        cell_df = huaweiutils.add_4g_cgi(cell_df, enode_df)
        common_table = pd.read_csv(self.g4_common_table,
                                   usecols=['基站覆盖类型（室内室外）', '覆盖场景', '小区CGI', '地市名称', '工作频段', '小区区域类别'],
                                   encoding='gbk',dtype=str)

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
        ducell_df = pd.read_csv(os.path.join(self.file_path, 'LST NRDUCELL.csv'))
        gnode_df = pd.read_csv(os.path.join(self.file_path, 'LST GNODEBFUNCTION.csv'))
        ducell_df = huaweiutils.add_5g_cgi(ducell_df, gnode_df)
        common_table = pd.read_csv(self.g5_common_table, usecols=['覆盖类型', '覆盖场景', 'CGI', '地市', '工作频段', 'CELL区域类别'],
                                   encoding='gbk')
        common_table.rename(columns={'CELL区域类别': '区域类别'}, inplace=True)
        # common_table['工作频段'] = common_table['工作频段'].apply(lambda x: x.split('-')[1])
        # 覆盖类型中，室内外和空白，归为室外
        common_table['覆盖类型'] = common_table['覆盖类型'].map({"室外": "室外", "室内外": "室外", "室内": "室内"})
        common_table['覆盖类型'].fillna("室外", inplace=True)
        # huaweiutils.output_csv(common_table, "common.csv", self.out_path)
        ducell_df['CGI'] = "460-00-" + ducell_df["gNodeB标识"].apply(str) + "-" + ducell_df[
            huaweiconfiguration.G5_CELL_IDENTITY].apply(str)
        base_info_df = ducell_df[['网元', 'NR小区标识', 'NRDU小区名称', 'CGI', '频带', '双工模式']]
        base_info_df['频带'] = base_info_df['频带'].map({"n41": "2.6G", "n28": "700M", "n78": "4.9G", "n79": "4.9G"})
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
                all_freq = df[frequency_param].unique().tolist()
                if self.is_4g_freq(all_freq):
                    # df[frequency_param] = 'LTE'
                    df[frequency_param] = df[frequency_param].apply(huaweiutils.mapToBand,
                                                                    args=(self.g4_freq_band_dict,))
                df.rename(columns={frequency_param: '对端频带'}, inplace=True)

        df = df.merge(self.base_info_df, how='left', on=['网元', self.cell_identity])
        # df.rename(columns={'频带': '频段'}, inplace=True)
        return df

    def get_freq_table(self, config_df):
        command_grouped = config_df.groupby(['主命令'])
        read_result_list = []
        for command, g in command_grouped:
            params = g['原始参数名称'].unique().tolist()
            command = huaweiutils.remove_digit(command, [",", ":"])
            file_name = os.path.join(self.file_path, command + '.csv')
            read_res, base_cols = self.read_data_by_command(file_name, params, g)
            read_res = self.before_add_judgement(read_res, g)
            for p in params:
                read_res = self.judge_compliance(read_res, p, g)
            read_result_list.append((read_res, command))
        # 只要主命令标识不为空，说明要读QCI进行过滤
        return read_result_list

    def get_cell_table(self, config_df):
        all_param = config_df['原始参数名称'].unique().tolist()
        command_identities = config_df['参数组标识'].tolist()
        command_identities = list(filter(lambda x: not 'nan' == x, command_identities))
        command_grouped = config_df.groupby(['主命令', 'QCI'])
        read_result_list = []
        qci_params = ['网元', self.cell_identity, '服务质量等级']
        read_qci = False
        qci_df = pd.DataFrame()
        qci_path = os.path.join(self.file_path, 'LST NRCELLQCIBEARERQCI.csv')
        for p, g in command_grouped:
            command = p[0]
            qci = p[1]
            params = g['原始参数名称'].unique().tolist()
            command = huaweiutils.remove_digit(command, [",", ":"])
            file_name = os.path.join(self.file_path, command + '.csv')
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
            merge_qci_result = self.judge_compliance(merge_qci_result, p, config_df)
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
        # 查看主命令是否存在标识,主命令对应标识应该一样
        if len(switch_dict) > 0:
            switch_cols = (list(set(switch_dict.values())))
            switch_cols.extend(base_cols)
            df = pd.read_csv(file_name, usecols=switch_cols)
            switch_df = self.read_switch_data(df, base_df, switch_dict)
        else:
            switch_df = pd.read_csv(file_name, usecols=base_cols)
        # 读取非开关参数
        non_switch_df = pd.DataFrame()
        if len(check_params) > 0:
            non_switch_col = copy.deepcopy(base_cols)
            non_switch_col.extend(check_params)
            logging.debug("读取文件:【" + file_name + "】读取的列:【" + str(non_switch_col) + "】")
            non_switch_df = pd.read_csv(file_name, usecols=non_switch_col)
        # result_df = switch_df.merge(non_switch_df, how='left', on=base_cols)
        if not non_switch_df.empty:
            result_df = pd.merge(switch_df, non_switch_df, how='left', on=base_cols)
        else:
            result_df = switch_df
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

    def judge_compliance(self, df, param, g):
        g_c = g[g['原始参数名称'] == param]
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
        standard = self.expand_standard(standard, ['对端频带', '频段', '覆盖类型', '区域类别'])
        # if self.system == '4G':
        #     df['对端频带'] = 'LTE'
        # else:
        if self.system == '5G':
            df = df.merge(standard[['对端频带', '频段', '推荐值', '覆盖类型', '区域类别']], how='left',
                          on=['对端频带', '频段', '覆盖类型', '区域类别'])
        df.rename(columns={"推荐值": param + "#推荐值"}, inplace=True)
        df[param + "#合规"] = huaweiutils.freq_judge(df, param)
        return df

    def expand_standard(self, standard, cols):
        for index, row in standard.iterrows():
            possible_row_list = []
            add_rows = []
            for c in cols:
                value = str(row[c])
                value = value.split('/') if value.find('/') >= 0 else [value]
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
        standard = self.expand_standard(standard, ['区域类别', '覆盖类型', '频段'])
        df = df.merge(standard[['区域类别', '覆盖类型', '频段', '推荐值']], how='left', on=['频段', '覆盖类型', '区域类别'])
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
        df = self.get_cell_table(self.cell_config_df)
        # df_list = self.get_freq_table(self.freq_config_df)
        # for df in df_list:
        #     d = df[0]
        #     sorted_cols = self.sort_result(d.columns.tolist())
        #     d = d[sorted_cols]
        #     d.to_csv(os.path.join(self.file_path, df[1] + '_check.csv'),
        #              index=False, encoding='utf_8_sig')
        df = df[self.sort_result(df.columns.tolist())]
        df.to_csv("C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\check_result1.csv",
                  index=False, encoding='utf_8_sig')


if __name__ == "__main__":
    filepaths = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\4G"
    standard_path = "C:\\Users\\No.1\\Desktop\\teleccom\\互操作参数核查结果.xlsx"
    g5_common_table = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\地市规则\\5G资源大表-20231227.csv"
    g4_common_table = "C:\\Users\\No.1\\Desktop\\teleccom\\LTE资源大表-0121\\LTE资源大表-0121.csv"
    g4_site_info = "C:\\Users\\No.1\\Desktop\\teleccom\\物理站CGI_4g.csv"
    g5_site_info = "C:\\Users\\No.1\\Desktop\\teleccom\\物理站CGI_5g.csv"
    report = HuaweiIntermediateGen(filepaths, standard_path, g4_common_table, g5_common_table, '4G')
    report.generate_report()
