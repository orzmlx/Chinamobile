import pandas as pd
import os
from huaweirawdatareader import *
import huaweirawdatareader
import huaweiutils


class HuaweiReporter:

    def __init__(self, file_path, to_be_checked_params, params_files_cols_dict, params_file_keys_dict):
        """
            一个是文件名对应的列名字典
            一个是文件名中的key的字典
        """
        self.file_path = file_path
        self.params_files_cols_dict = {}
        self.to_be_checked_params = [i.replace(" ", "") for i in to_be_checked_params]
        self.params_files_dict = params_files_cols_dict
        for file_name in self.params_files_dict:
            cols = params_files_cols_dict[file_name]
            file_keys = params_file_keys_dict[file_name]
            inter_cols = set(cols) & set(self.to_be_checked_params)
            if len(inter_cols) > 0:
                self.params_files_cols_dict[file_name] = inter_cols + file_keys

    def flatten_children_column(self, to_be_checked_params):

        dfs = []
        for param in self.to_be_checked_params:
            if param.find("开关") >= 0:
                # 如果发现开关属性的参数,所有文件加载第一行,看这个开关属性在哪个文件夹中
                files = os.listdir(self.file_path)
                for f in files:
                    abs_path = os.path.join(self.file_path, f)
                    df = pd.read_csv(abs_path, nrows=1, encoding='utf8')
                    first_row = df.loc[0]
                    cols = df.columns.tolist()
                    for c in cols:
                        cell = first_row[c]
                        if str(cell).find(param) >= 0:
                            df = pd.read_csv(abs_path)
                            df = huaweiutils.flatten_features(df, c)
                            df = df[['网元', 'NR小区标识', param]]
                            dfs.append(df)
        result = pd.DataFrame()
        if len(dfs) > 0:
            result = dfs[0]
        for index, d in enumerate(dfs):
            if index == 0:
                continue
            result = result.merge(d, how='left', on=['网元', 'NR小区标识'])
        return result

    def generate_report(self):
        ducell_df = pd.read_csv("C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\LST NRDUCELL.csv")
        gnode_df = pd.read_csv("C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\LST GNODEBFUNCTION.csv")
        common_table = pd.read_csv("C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\地市规则\\5G资源大表-20231227.csv",
                                   usecols=['覆盖类型', '覆盖场景', 'CGI'], encoding='gbk')
        ducell_df = huaweiutils.add_cgi(ducell_df, gnode_df)
        switch_df = self.flatten_children_column()
        ducell_df['CGI'] = "460-00-" + ducell_df["gNodeB标识"].apply(str) + "-" + ducell_df["NRDU小区标识"].apply(str)
        base_info_df = ducell_df[['网元', 'NR小区标识', 'NRDU小区名称', 'CGI', '频带', '双工模式']]
        base_info_df = base_info_df.rename(columns={'NRDU小区标识': 'NR小区标识'})
        base_info_df = base_info_df.merge(common_table, how='left', on=['CGI'])
        base_info_df['厂家'] = '华为'

        qci = pd.read_csv("C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\LST NRCELLQCIBEARERQCI=1.csv",
                          usecols=['网元', 'NR小区标识', '服务质量等级', '异系统切换测量参数组标识', '异系统切换至E-UTRAN测量参数组标识', '同频切换测量参数组标识',
                                   '异频切换测量参数组标识'])
        # 异频切换参数
        inter_freq_handover = pd.read_csv(
            'C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\LST NRCELLINTERFHOMEAGRPINTERFREQHOMEASGROUPID=0.csv')
        # 以系统切换
        inter_sys_handover = pd.read_csv(
            'C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\LST NRCELLINTERRHOMEAGRPINTERRATHOMEASGROUPID=0.csv')

        eutran_handover = pd.read_csv(
            'C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\LST NRCELLHOEUTRANMEAGRPINTERRHOTOEUTRANMEASGRPID=0.csv')

        inter_sys_handover_param = pd.read_csv(
            r'C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\LST NRINTERRATHOPARAM.csv',
            usecols=['网元', 'NR小区标识', '异系统切换触发事件类型'])
        srs_meas = pd.read_csv('C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\LST NRDUCELLSRSMEAS.csv',
                               usecols=['网元', 'NRDU小区标识', 'NR迁移到E-UTRAN的上行SINR低门限(0.1dB)'])
        srs_meas = srs_meas.rename(columns={'NRDU小区标识': 'NR小区标识'})
        qci = qci[qci['服务质量等级'] == 9]
        result = base_info_df.merge(qci, how='left', on=['网元', 'NR小区标识'])
        result = result.merge(switch_df, how='left', on=['网元', 'NR小区标识'])
        result = result.merge(inter_freq_handover, how='left', on=['网元', 'NR小区标识', '异频切换测量参数组标识'])
        result = result.merge(inter_sys_handover, how='left', on=['网元', 'NR小区标识', '异系统切换测量参数组标识'])
        result = result.merge(eutran_handover, how='left', on=['网元', 'NR小区标识', '异系统切换至E-UTRAN测量参数组标识'])
        result = result.merge(inter_sys_handover_param, how='left', on=['网元', 'NR小区标识'])
        result = result.merge(srs_meas, how='left', on=['网元', 'NR小区标识'])
        result = result[self.to_be_checked_params]

        # result['站点类型'].map({"n41": "2.6G", "n28": "700M", "n78": "4.9G", "n79": "4.9G"})
        result.to_csv("C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\report.csv",
                      index=False, encoding='utf_8_sig')


if __name__ == "__main__":
    need_param = [
        '网元',
        'NR小区标识',
        'NRDU小区名称',
        'CGI',
        '频带',
        '双工模式',
        '覆盖类型',
        '覆盖场景',
        '厂家',
        '基于覆盖的异频A1 RSRP触发门限(dBm)',
        '基于覆盖的异频A2 RSRP触发门限(dBm)',
        '基于覆盖的异频A5 RSRP触发门限1(dBm)',
        '基于覆盖的异频A5 RSRP触发门限2(dBm)',
        '异系统切换A1 RSRP门限(dBm)',
        '异系统切换A2 RSRP门限(dBm)',
        '异系统切换触发事件类型',
        '基于覆盖的切换至E-UTRAN B2 RSRP门限1(dBm)',
        '基于覆盖的切换B1 RSRP门限(dBm)',
        '基于频率优先级的异频切换开关',
        '基于频率优先级的异频切换A1 RSRP门限(dBm)',
        '基于频率优先级的异频切换A4 RSRP门限(dBm)',
        '移动至E-UTRAN开关',
        '基于上行SINR移动至EUTRAN开关',
        'NR迁移到E-UTRAN的上行SINR低门限(0.1dB)',
        '基于上行SINR的切换至E-UTRAN B2 RSRP门限1(dBm)',
        '网络架构优选的RSRP触发门限(dBm)'
    ]
    filepaths = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result"

    report = HuaweiReporter(filepaths, need_param, [], [])
    report.generate_report()
