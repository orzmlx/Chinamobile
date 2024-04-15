from utils import huaweiutils
import logging


class HuaweiRawDataFile(object):
    def __init__(self, raw_data_inpath, command_file_path, out_path, system):
        self.to_be_continue = False
        self.system = system
        self.out_put_dict = {}
        self.checked_unit_number = 0
        self.to_be_correct_command = dict()
        self.command_col_dict = {}
        self.__word__method = {"成功条数": "get_success_number",
                               "失败条数": "get_fail_number",
                               huawei_configuration.COMMAND: "get_command",
                               "网元": "get_network_element",
                               "报文": "get_message",
                               huawei_configuration.RETURN_CODE: "get_return_code",
                               huawei_configuration.QUERY: "get_command_meaning"}
        self.__current_phase = "成功条数"
        self.fail_phases = deque(["失败条数", huawei_configuration.COMMAND, "网元", "报文"])
        self.success_phases = deque(
            [huawei_configuration.COMMAND, "网元", "报文", huawei_configuration.RETURN_CODE, huawei_configuration.QUERY])
        self.phases_dict = {huawei_configuration.COMMAND: [], "网元": [], "报文": [],
                            huawei_configuration.RETURN_CODE: [], huawei_configuration.QUERY: []}
        self.raw_data_inpath = raw_data_inpath
        self.out_path = os.path.join(out_path, system)
        self.fail_command_number = 0
        self.success_command_number = 0
        self.command_content_dict = {}
        self.content_dict = {}
        self.command_path = command_file_path
        self.command_list = self.read_command_list()
        if system == '5G':
            # assert len(self.command_list) == len(huaweiconfiguration.G5_COMMAND_COLS_LIST), "配置和输入命令长度不相等"
            self.cell_identity = huawei_configuration.G5_CELL_IDENTITY
            self.init_table(huawei_configuration.G5_COMMAND_COLS_LIST, huawei_configuration.G5_COMMAND_NAME_LIST)
        else:
            # assert len(self.command_list) == len(huaweiconfiguration.G4_COMMAND_COLS_LIST), "配置和输入命令长度不相等"
            self.cell_identity = huawei_configuration.G4_CELL_IDENTITY
            self.init_table(huawei_configuration.G4_COMMAND_COLS_LIST, huawei_configuration.G4_COMMAND_NAME_LIST)

        self.current_command = ""
        self.exist_cols = []
        self.current_network_element = ""
        self.files_cols_dict = {}

    def parse_param(self, params):
        result = []
        merge_params = {}
        for p in params:
            p = p.strip().replace(" ", "")
            if p.find("&") >= 0 and p.find(":") >= 0:
                splits = p.split(":")
                col_name = splits[0]
                params = splits[1].split("&")
                result.extend(params)
                merge_params[col_name] = params
            else:
                result.append(p)
        return result, merge_params

    @staticmethod
    def init_dict(new_list, new_dict):
        for e in new_list:
            e = e.strip()
            if e not in new_dict:
                new_dict[e] = []

    def init_table(self, col_list, command_name_list):
        """
            初始化基本信息
        """
        for i in range(len(col_list)):
            new_dict = {}
            e = col_list[i]
            # 去掉列名中的空格
            e = [s.replace(' ', '') for s in e]
            self.init_dict(e, new_dict)
            key = command_name_list[i]
            self.command_content_dict[key] = new_dict
            self.command_col_dict[key] = e

    def read_huawei_txt(self):
        return_code = -1
        phases = self.fail_phases
        with open(self.raw_data_inpath, 'r') as f:
            line = f.readline()
            while line:
                print(line)
                if line.find(self.__current_phase) == 0:
                    if self.__current_phase == "报文" and return_code == -1:
                        self.phases_dict[huawei_configuration.RETURN_CODE].append(return_code)
                        self.phases_dict[huawei_configuration.QUERY].append(huawei_configuration.EMPTY_VALUE)
                    method_name = self.__word__method[self.__current_phase]
                    func = getattr(self, method_name)
                    if not callable(func):
                        logging.error(method_name + "不可用")
                        return
                    func(line)
                    ##如果是查询命令阶段,那么继续解读命令
                    if self.__current_phase == huawei_configuration.QUERY:
                        line = f.readline()
                        if len(line) > 0 and line.find("-") == 0:
                            line = f.readline()
                            self.parse_message_content(line, f)
                    self.__current_phase = phases.popleft()
                    if self.__current_phase != "成功条数" and self.__current_phase != "失败条数":
                        phases.append(self.__current_phase)
                line = f.readline()
                if self.to_be_continue is True:
                    self.__current_phase = huawei_configuration.RETURN_CODE
                    phases = deque([huawei_configuration.QUERY, huawei_configuration.COMMAND,
                                    "网元", "报文", huawei_configuration.RETURN_CODE])
                    self.to_be_continue = False
                if line.find("没有查到相应的结果") >= 0:
                    self.phases_dict[huawei_configuration.QUERY].append("没有查到相应的结果")
                    self.__current_phase = phases.popleft()
                    phases.append(self.__current_phase)
                if line.find("成功命令") >= 0:
                    phases = self.success_phases
                    self.__current_phase = phases.popleft()
                    phases.append(self.__current_phase)
                    return_code = 0

    def get_message(self, line):
        self.phases_dict["报文"].append(line.split(":")[1].replace("\n", ""))

    def get_command_meaning(self, line):
        self.phases_dict[huawei_configuration.QUERY].append(line.split(huawei_configuration.QUERY)[1].replace("\n", ""))

    def get_return_code(self, line):
        pattern = r'\d+'
        self.phases_dict[huawei_configuration.RETURN_CODE].append(re.findall(pattern, line)[0])

    def read_command_list(self):
        command_list = []
        with open(self.command_path, 'r') as f:
            line = f.readline()
            while line:
                line = line.replace("\n", "")
                command_list.append(line)
                line = f.readline()
        return list(filter(lambda x: x.strip() != "", command_list))

    def get_command(self, line):
        command = line.split("-----")[1].replace("\n", "")
        self.phases_dict["命令"].append(command)
        self.current_command = command

    def get_network_element(self, line):
        current_network_element = line.split(":")[1].replace("\n", "")
        self.phases_dict["网元"].append(line.split(":")[1].replace("\n", ""))
        self.current_network_element = current_network_element

    def parse_message_content(self, line, f):
        """
            此时的Line是第一行数据
            如果结果中含有等号,那么查询结果只有一行
        """
        if line.find("=") >= 0:
            unit_number = self.__read_one_unit_message(f, line)
        # 说明结果是多行
        else:
            unit_number = self.__read_multi_unit_message(f, line)
        if unit_number > 0:
            self.__check_exist_cols(unit_number)

    def __check_exist_cols(self, add_number):
        expect_cols = self.command_col_dict[self.current_command]
        if len(expect_cols) != len(self.exist_cols):
            # 缺少的列
            different_cols = set(expect_cols).difference(set(self.exist_cols))
            both_different_cols = set(expect_cols) ^ set(self.exist_cols)
            if len(different_cols) != len(both_different_cols):
                logging.info("列名待补充")
            if len(different_cols) > 0:
                command_dict = self.command_content_dict[self.current_command]
                for col in different_cols:
                    for i in range(add_number):
                        self.__init_content_dict(col, huawei_configuration.EMPTY_VALUE, command_dict)
        self.exist_cols = []

    def get_success_number(self, line):
        self.success_command_number = int(line.split(":")[1].replace("\n", ""))

    def get_fail_number(self, line):
        self.fail_command_number = int(line.split(":")[1].replace("\n", ""))

    def __read_one_unit_message(self, f, line):
        """
            在华为原始文件中读取单行结果
        """
        command_dict = self.command_content_dict[self.current_command]
        latest_col = ""
        latest_value = ""
        self.exist_cols.append("网元")
        self.__init_content_dict("网元", self.current_network_element, command_dict)
        while line:
            line = line.replace("\n", "")
            if len(line) == 0:
                line = f.readline()
                continue
            if line.find(huawei_configuration.END) >= 0:
                self.__init_content_dict(latest_col, latest_value, command_dict)
                return 1
            if line.find("结果个数") >= 0:
                line = f.readline()
                continue
            if line.find("共有") >= 0:
                line = f.readline()
                continue
            if line.find("仍有后续报告输出") >= 0:
                self.to_be_continue = True
                line = f.readline()
                continue
            splits = line.split("=")
            # 存在部分列名数据不一致，比如同样的列名，有的后面加了%,有的后面没有，在这将%干掉
            col_name = splits[0].strip().replace(" ", "").replace("(%)", "").replace("%", "")
            col_name = col_name.replace("dBm", "dBm").replace("0.1分贝", '0.1dB').replace("0.5分贝", '0.5dB')

            value = splits[1].strip()
            # 如果col列有值,value也有值
            if len(col_name) > 0 and len(value) > 0:
                if col_name != latest_col:
                    """
                        如果当前的colname不等于上一行的colname,并且上一行的colname和值都不为空，说明这是新的一列，将上一列
                        的col和value存起来
                     """
                    if latest_col != "" and latest_value != "":
                        self.__init_content_dict(latest_col, latest_value, command_dict)
                    latest_col = col_name
                    latest_value = value
                self.exist_cols.append(col_name)
            elif len(col_name) == 0 and len(value) > 0:
                latest_value = latest_value + "&" + value
            else:
                logging.error(line + ":无法解析")
            line = f.readline()
        return 1

    @staticmethod
    def __init_content_dict(key, value, new_dict):
        key = key.strip().replace(" ", "")
        # if key.find('毫瓦分贝') >= 0:
        key = key.replace('毫瓦分贝', 'dBm').replace("0.1分贝", '0.1dB').replace("0.5分贝", '0.5dB')
        value = value.strip()
        if key in new_dict:
            new_dict[key].append(value.strip())
        else:
            new_dict[key] = []
            if len(value) > 0:
                new_dict[key].append(value)

    def __merge_same_command_data(self):
        """
            将相同命令但是后面参数不同的数据合并在一起
            例如LST NRCELLQCIBEARER:QCI=1和LST NRCELLQCIBEARER:QCI=2命令
            下的数据合并在一起
        """
        out_put_dict = {}
        for d in self.command_content_dict:
            logging.info(d)
            # file_name = d.replace(":", "").replace(";", "")
            file_name = d.replace(";", "")
            try:
                df = pd.DataFrame(self.command_content_dict[d])
            except:
                raise Exception(
                    "请检查【" + d + "】命令下的参数设置是否齐全,当前的列的数量:【" + str(list(self.command_content_dict[d].keys())) + '】')
            # NRDU小区改成NR小区
            if ~df.empty:
                df.reset_index(inplace=True, drop=True)
                cols = df.columns.tolist()
                if "NRDU小区标识" in cols:
                    df.rename(columns={"NRDU小区标识": self.cell_identity}, inplace=True)
                out_name = file_name + ".csv"
                if len(out_put_dict) == 0:
                    # out_name = huaweiutils.remove_digit(out_name, ['='])
                    out_put_dict[out_name] = df
                    continue
                is_add = False
                for f, pre_df in out_put_dict.items():
                    res = ''.join(set(out_name) ^ set(f))
                    if len(res) == 0:
                        continue
                    if huaweiutils.only_has_digtal_diff(out_name, f) and len(out_name) == len(f):
                        pre_df = pd.concat([pre_df, df], axis=0)
                        # f = huaweiutils.remove_digit(f,['='])
                        out_put_dict[f] = pre_df
                        is_add = True
                        break
                if is_add is False:
                    out_put_dict[out_name] = df

        self.out_put_dict = out_put_dict

    def output_format_data(self):
        """
            华为按命令行导出数据
        """
        raw_file_name: str = os.path.split(self.raw_data_inpath)[1].split('.')[0]
        self.__merge_same_command_data()
        for f, df in self.out_put_dict.items():
            huaweiutils.output_csv(df, f, os.path.join(self.out_path, raw_file_name, 'raw_result'), True)
            self.files_cols_dict[f] = df.columns.tolist()

    def add_resource_frequency(self, file_name, df, standard_df, baseinfo):
        commands = standard_df['主命令'].unique().tolist()
        file_name = huaweiutils.remove_digit(file_name, ['=', ':'])
        file_name = file_name.replace(".csv", "")
        if file_name in commands:
            df = df.merge(baseinfo, how='left', on=['网元', self.cell_identity])
        return df

    def __read_multi_unit_message(self, f, line):
        command_dict = self.command_content_dict[self.current_command]
        is_first_line = True
        cols = []
        result_number = 0
        fact_number = 0
        while line:
            line = line.replace("\n", "")
            if len(line) == 0:
                line = f.readline()
                continue
            if line.find(huawei_configuration.END) >= 0:
                if int(result_number) != fact_number:
                    logging.warn('实际读取行数和日志行数不一致,请检查数据')
                self.exist_cols = cols
                return fact_number
            if line.find("结果个数") >= 0:
                pattern = r'\d+'
                result_number = re.findall(pattern, line)[0]
                line = f.readline()
                continue
            if line.find("共有") >= 0:
                line = f.readline()
                continue
            if line.find("仍有后续报告输出") >= 0:
                self.to_be_continue = True
                line = f.readline()
                continue
            # 第一行是列名，并且第一列加上网元列
            # 将后面带%或者(%)的全部去掉
            if is_first_line:
                cols = line.replace("\n", "").split("  ")
                cols = list(filter(lambda x: x.strip() != "", cols))
                # 去掉里面列名中的空格
                cols = [s.replace(' ', '').replace("(%)", "").replace("0.1分贝", '0.1dB').replace("0.5分贝", '0.5dB')
                            .replace("%", "").replace('dBm', 'dBm') for s in cols]
                cols.insert(0, '网元')
                is_first_line = False
            else:
                fact_number = fact_number + 1
                row = line.split("  ")
                new_row = [self.current_network_element]
                # 去掉列表中的空格和空列名
                for r in row:
                    r = r.strip()
                    if len(r) == 0:
                        continue
                    new_row.append(r)
                # 每一行都添加网元信息,作为Index,必须是最后一个
                assert len(new_row) == len(cols), "列名长度和内容长度不一致,请检查数据"
                for index, row in enumerate(new_row):
                    col_name = cols[index]
                    value = new_row[index]
                    self.__init_content_dict(col_name, value, command_dict)
            line = f.readline()
        return fact_number


if __name__ == "__main__":
    # rawDataPath = 'C:\\Users\\No.1\\Desktop\\teleccom\\MML任务结果_nb_20240126_093224.txt'
    # rawDataPath = 'C:\\Users\\No.1\\Desktop\\teleccom\\MML任务结果_zs_20240118_150344.txt'
    # rawDataPath = 'C:\\Users\\No.1\\Desktop\\teleccom\\MML任务结果_zs1_20240125_104929.txt'
    # rawDataPath = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\QCI\\华为5G-QCI-159\\MML任务结果_gt_20231226_112108.txt"
    rawDataPath = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\4G\\raw_data\\MML任务结果_zs_20240201_103223.txt"
    # commandPath = "C:\\Users\\No.1\\Desktop\\teleccom\\华为5G异频异系统切换重选语音数据-全量.txt"
    commandPath = "C:\\Users\\No.1\\Desktop\\teleccom\\华为4G异频异系统切换重选语音数据-全量.txt"
    outputPath = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\"
    # 工参表路径
    # common_table = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\地市规则\\5G资源大表-20231227.csv"
    # standard_path = "C:\\Users\\No.1\\Desktop\\teleccom\\互操作参数核查结果.xlsx"
    rawFile = HuaweiRawDataFile(rawDataPath, commandPath, outputPath, '4G')
    rawFile.read_huawei_txt()
    # rawFile.output_handover_result(qci=9)
    rawFile.output_format_data()
    # rawFile.output_handover_result(9)
from collections import deque
import pandas as pd
import logging
import os
import re
from configuration import huawei_configuration


class HuaweiRawDataFile(object):
    def __init__(self, raw_data_inpath, command_file_path, out_path, system):
        self.to_be_continue = False
        self.system = system
        self.out_put_dict = {}
        self.checked_unit_number = 0
        self.command_col_dict = {}
        self.command_to_be_corrected = {}
        self.__word__method = {"成功条数": "get_success_number",
                               "失败条数": "get_fail_number",
                               huawei_configuration.COMMAND: "get_command",
                               "网元": "get_network_element",
                               "报文": "get_message",
                               huawei_configuration.RETURN_CODE: "get_return_code",
                               huawei_configuration.QUERY: "get_command_meaning"}
        self.__current_phase = "成功条数"
        self.fail_phases = deque(["失败条数", huawei_configuration.COMMAND, "网元", "报文"])
        self.success_phases = deque(
            [huawei_configuration.COMMAND, "网元", "报文", huawei_configuration.RETURN_CODE, huawei_configuration.QUERY])
        self.phases_dict = {huawei_configuration.COMMAND: [], "网元": [], "报文": [],
                            huawei_configuration.RETURN_CODE: [], huawei_configuration.QUERY: []}
        self.raw_data_inpath = raw_data_inpath
        self.out_path = os.path.join(out_path, system)
        self.fail_command_number = 0
        self.success_command_number = 0
        self.command_content_dict = {}
        self.content_dict = {}
        self.command_path = command_file_path
        self.command_list = self.read_command_list()
        if system == '5G':
            # assert len(self.command_list) == len(huaweiconfiguration.G5_COMMAND_COLS_LIST), "配置和输入命令长度不相等"
            self.cell_identity = huawei_configuration.G5_CELL_IDENTITY
            self.init_table(huawei_configuration.G5_COMMAND_COLS_LIST, huawei_configuration.G5_COMMAND_NAME_LIST)
        else:
            # assert len(self.command_list) == len(huaweiconfiguration.G4_COMMAND_COLS_LIST), "配置和输入命令长度不相等"
            self.cell_identity = huawei_configuration.G4_CELL_IDENTITY
            self.init_table(huawei_configuration.G4_COMMAND_COLS_LIST, huawei_configuration.G4_COMMAND_NAME_LIST)

        self.current_command = ""
        self.exist_cols = []
        self.current_network_element = ""
        self.files_cols_dict = {}

    def parse_param(self, params):
        result = []
        merge_params = {}
        for p in params:
            p = p.strip().replace(" ", "")
            if p.find("&") >= 0 and p.find(":") >= 0:
                splits = p.split(":")
                col_name = splits[0]
                params = splits[1].split("&")
                result.extend(params)
                merge_params[col_name] = params
            else:
                result.append(p)
        return result, merge_params

    @staticmethod
    def init_dict(new_list, new_dict):
        for e in new_list:
            e = e.strip()
            if e not in new_dict:
                new_dict[e] = []

    def init_table(self, col_list, command_name_list):
        """
            初始化基本信息
        """
        for i in range(len(col_list)):
            new_dict = {}
            e = col_list[i]
            e = [s.replace(' ', '') for s in e]
            self.init_dict(e, new_dict)
            key = command_name_list[i]
            self.command_content_dict[key] = new_dict
            self.command_col_dict[key] = e

    def read_huawei_txt(self):
        return_code = -1
        phases = self.fail_phases
        with open(self.raw_data_inpath, 'r') as f:
            line = f.readline()
            while line:
                print(line)
                if line.find(self.__current_phase) == 0:
                    if self.__current_phase == "报文" and return_code == -1:
                        self.phases_dict[huawei_configuration.RETURN_CODE].append(return_code)
                        self.phases_dict[huawei_configuration.QUERY].append(huawei_configuration.EMPTY_VALUE)
                    method_name = self.__word__method[self.__current_phase]
                    func = getattr(self, method_name)
                    if not callable(func):
                        logging.error(method_name + "不可用")
                        return
                    func(line)
                    ##如果是查询命令阶段,那么继续解读命令
                    if self.__current_phase == huawei_configuration.QUERY:
                        line = f.readline()
                        if len(line) > 0 and line.find("-") == 0:
                            line = f.readline()
                            self.parse_message_content(line, f)
                    self.__current_phase = phases.popleft()
                    if self.__current_phase != "成功条数" and self.__current_phase != "失败条数":
                        phases.append(self.__current_phase)
                line = f.readline()
                if self.to_be_continue is True:
                    self.__current_phase = huawei_configuration.RETURN_CODE
                    phases = deque([huawei_configuration.QUERY, huawei_configuration.COMMAND,
                                    "网元", "报文", huawei_configuration.RETURN_CODE])
                    self.to_be_continue = False
                if line.find("没有查到相应的结果") >= 0:
                    self.phases_dict[huawei_configuration.QUERY].append("没有查到相应的结果")
                    self.__current_phase = phases.popleft()
                    phases.append(self.__current_phase)
                if line.find("成功命令") >= 0:
                    phases = self.success_phases
                    self.__current_phase = phases.popleft()
                    phases.append(self.__current_phase)
                    return_code = 0

    def get_message(self, line):
        self.phases_dict["报文"].append(line.split(":")[1].replace("\n", ""))

    def get_command_meaning(self, line):
        self.phases_dict[huawei_configuration.QUERY].append(line.split(huawei_configuration.QUERY)[1].replace("\n", ""))

    def get_return_code(self, line):
        pattern = r'\d+'
        self.phases_dict[huawei_configuration.RETURN_CODE].append(re.findall(pattern, line)[0])

    def read_command_list(self):
        command_list = []
        with open(self.command_path, 'r') as f:
            line = f.readline()
            while line:
                line = line.replace("\n", "")
                command_list.append(line)
                line = f.readline()
        return list(filter(lambda x: x.strip() != "", command_list))

    def get_command(self, line):
        command = line.split("-----")[1].replace("\n", "")
        self.phases_dict["命令"].append(command)
        self.current_command = command

    def get_network_element(self, line):
        current_network_element = line.split(":")[1].replace("\n", "")
        self.phases_dict["网元"].append(line.split(":")[1].replace("\n", ""))
        self.current_network_element = current_network_element

    def parse_message_content(self, line, f):
        """
            此时的Line是第一行数据
            如果结果中含有等号,那么查询结果只有一行
        """
        if line.find("=") >= 0:
            unit_number = self.__read_one_unit_message(f, line)
        # 说明结果是多行
        else:
            unit_number = self.__read_multi_unit_message(f, line)
        if unit_number > 0:
            self.__check_exist_cols(unit_number)

    def __check_exist_cols(self, add_number):
        expect_cols = self.command_col_dict[self.current_command]
        if len(expect_cols) != len(self.exist_cols):
            # 缺少的列
            different_cols = set(expect_cols).difference(set(self.exist_cols))
            both_different_cols = set(expect_cols) ^ set(self.exist_cols)
            if len(different_cols) != len(both_different_cols):
                logging.info("列名待补充")
            if len(different_cols) > 0:
                command_dict = self.command_content_dict[self.current_command]
                for col in different_cols:
                    for i in range(add_number):
                        self.__init_content_dict(col, huawei_configuration.EMPTY_VALUE, command_dict)
        self.exist_cols = []

    def get_success_number(self, line):
        self.success_command_number = int(line.split(":")[1].replace("\n", ""))

    def get_fail_number(self, line):
        self.fail_command_number = int(line.split(":")[1].replace("\n", ""))

    def __read_one_unit_message(self, f, line):
        """
            在华为原始文件中读取单行结果
        """
        command_dict = self.command_content_dict[self.current_command]
        latest_col = ""
        latest_value = ""
        self.exist_cols.append("网元")
        self.__init_content_dict("网元", self.current_network_element, command_dict)
        while line:
            line = line.replace("\n", "")
            if len(line) == 0:
                line = f.readline()
                continue
            if line.find(huawei_configuration.END) >= 0:
                self.__init_content_dict(latest_col, latest_value, command_dict)
                return 1
            if line.find("结果个数") >= 0:
                line = f.readline()
                continue
            if line.find("共有") >= 0:
                line = f.readline()
                continue
            if line.find("仍有后续报告输出") >= 0:
                self.to_be_continue = True
                line = f.readline()
                continue
            splits = line.split("=")
            # 存在部分列名数据不一致，比如同样的列名，有的后面加了%,有的后面没有，在这将%干掉
            col_name = splits[0].strip().replace(" ", "").replace("(%)", "") \
                .replace("%", "").replace("毫瓦分贝", "dBm").replace("分贝", "dB")
            value = splits[1].strip()
            # 如果col列有值,value也有值
            if len(col_name) > 0 and len(value) > 0:
                if col_name != latest_col:
                    """
                        如果当前的colname不等于上一行的colname,并且上一行的colname和值都不为空，说明这是新的一列，将上一列
                        的col和value存起来
                     """
                    if latest_col != "" and latest_value != "":
                        self.__init_content_dict(latest_col, latest_value, command_dict)
                    latest_col = col_name
                    latest_value = value
                self.exist_cols.append(col_name)
            elif len(col_name) == 0 and len(value) > 0:
                latest_value = latest_value + "&" + value
            else:
                logging.error(line + ":无法解析")
            line = f.readline()
        return 1

    @staticmethod
    def __init_content_dict(key, value, new_dict):
        key = key.strip().replace(" ", "")
        if key.find('毫瓦分贝') >= 0:
            key = key.replace('毫瓦分贝', 'dBm').replace('分贝', 'dB')
        value = value.strip()
        if key in new_dict:
            new_dict[key].append(value.strip())
        else:
            new_dict[key] = []
            if len(value) > 0:
                new_dict[key].append(value)
            # length = huaweiutils.is_lists_of_same_length(new_dict)
            # if length > 1:
            #     if len(value) > 0:
            #         fill_empty_list = [''] * (length-1)
            #         fill_empty_list.append(value)
            #     else:
            #         fill_empty_list = [''] * length
            #     new_dict[key] = fill_empty_list
            # elif length == 1 or length == 0:
            #     new_dict[key] = []
            #     if len(value) > 0:
            #         new_dict[key].append(value)
            # elif length == -1:
            #     print()

    def __merge_same_command_data(self):
        """
            将相同命令但是后面参数不同的数据合并在一起
            例如LST NRCELLQCIBEARER:QCI=1和LST NRCELLQCIBEARER:QCI=2命令
            下的数据合并在一起
        """
        out_put_dict = {}
        for d in self.command_content_dict:
            logging.info(d)
            # file_name = d.replace(":", "").replace(";", "")
            file_name = d.replace(";", "")
            try:
                df = pd.DataFrame(self.command_content_dict[d])
            except Exception as e:
                logging.info("命令:" + d + "列表长度不一致")
                if str(e) == 'All arrays must be of the same length':
                    # n_d = d.replace(" ", "_").replace(":", "_").replace("=","_")
                    self.command_to_be_corrected[d] = list(self.command_content_dict[d].keys())
                    continue
                # raise ReadRawException(message="检查【" + d + "】命令下的参数设置是否齐全",
                #                        raw_message=str(e),
                #                        system=self.system,
                #                        manufacturer='huawei',
                #                        model=(d, list(self.command_content_dict[d].keys())),
                #                        code=-1)

            # NRDU小区改成NR小区
            if ~df.empty:
                df.reset_index(inplace=True, drop=True)
                cols = df.columns.tolist()
                if "NRDU小区标识" in cols:
                    df.rename(columns={"NRDU小区标识": self.cell_identity}, inplace=True)
                out_name = file_name + ".csv"
                if len(out_put_dict) == 0:
                    # out_name = huaweiutils.remove_digit(out_name, ['='])
                    out_put_dict[out_name] = df
                    continue
                is_add = False
                for f, pre_df in out_put_dict.items():
                    res = ''.join(set(out_name) ^ set(f))
                    if len(res) == 0:
                        continue
                    if huaweiutils.only_has_digtal_diff(out_name, f) and len(out_name) == len(f):
                        pre_df = pd.concat([pre_df, df], axis=0)
                        # f = huaweiutils.remove_digit(f,['='])
                        out_put_dict[f] = pre_df
                        is_add = True
                        break
                if is_add is False:
                    out_put_dict[out_name] = df

        self.out_put_dict = out_put_dict

    def output_format_data(self):
        """
            华为按命令行导出数据
        """
        raw_file_name: str = os.path.split(self.raw_data_inpath)[1].split('.')[0]
        self.__merge_same_command_data()
        # 如果有需要增加字段的,后面不需要跑，直接跳过重新来
        if len(self.command_to_be_corrected) > 0:
            logging.info("有不一致列名,后续自动修后重新启动")
            return
        for f, df in self.out_put_dict.items():
            huaweiutils.output_csv(df, f, os.path.join(self.out_path, raw_file_name, 'raw_result'), True)
            self.files_cols_dict[f] = df.columns.tolist()

    def add_resource_frequency(self, file_name, df, standard_df, baseinfo):
        commands = standard_df['主命令'].unique().tolist()
        file_name = huaweiutils.remove_digit(file_name, ['=', ':'])
        file_name = file_name.replace(".csv", "")
        if file_name in commands:
            df = df.merge(baseinfo, how='left', on=['网元', self.cell_identity])
        return df

    def __read_multi_unit_message(self, f, line):
        command_dict = self.command_content_dict[self.current_command]
        is_first_line = True
        cols = []
        result_number = 0
        fact_number = 0
        while line:
            line = line.replace("\n", "")
            if len(line) == 0:
                line = f.readline()
                continue
            if line.find(huawei_configuration.END) >= 0:
                if int(result_number) != fact_number:
                    logging.warn('实际读取行数和日志行数不一致,请检查数据')
                self.exist_cols = cols
                return fact_number
            if line.find("结果个数") >= 0:
                pattern = r'\d+'
                result_number = re.findall(pattern, line)[0]
                line = f.readline()
                continue
            if line.find("共有") >= 0:
                line = f.readline()
                continue
            if line.find("仍有后续报告输出") >= 0:
                self.to_be_continue = True
                line = f.readline()
                continue
            # 第一行是列名，并且第一列加上网元列
            # 将后面带%或者(%)的全部去掉
            if is_first_line:
                cols = line.replace("\n", "").split("  ")
                cols = list(filter(lambda x: x.strip() != "", cols))
                # 去掉里面列名中的空格
                cols = [s.replace(' ', '').replace("(%)", "").replace("%", "")
                            .replace('毫瓦分贝', 'dBm').replace('分贝', 'dB') for s in cols]
                cols.insert(0, '网元')
                is_first_line = False
            else:
                fact_number = fact_number + 1
                row = line.split("  ")
                new_row = [self.current_network_element]
                # 去掉列表中的空格和空列名
                for r in row:
                    r = r.strip()
                    if len(r) == 0:
                        continue
                    new_row.append(r)
                # 每一行都添加网元信息,作为Index,必须是最后一个
                assert len(new_row) == len(cols), "列名长度和内容长度不一致,请检查数据"
                for index, row in enumerate(new_row):
                    col_name = cols[index]
                    value = new_row[index]
                    self.__init_content_dict(col_name, value, command_dict)
            line = f.readline()
        return fact_number


if __name__ == "__main__":
    # rawDataPath = 'C:\\Users\\No.1\\Desktop\\teleccom\\MML任务结果_nb_20240126_093224.txt'
    # rawDataPath = 'C:\\Users\\No.1\\Desktop\\teleccom\\MML任务结果_zs_20240118_150344.txt'
    # rawDataPath = 'C:\\Users\\No.1\\Desktop\\teleccom\\MML任务结果_zs1_20240125_104929.txt'
    # rawDataPath = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\QCI\\华为5G-QCI-159\\MML任务结果_gt_20231226_112108.txt"
    rawDataPath = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\4G\\raw_data\\MML任务结果_zs_20240201_103223.txt"
    # commandPath = "C:\\Users\\No.1\\Desktop\\teleccom\\华为5G异频异系统切换重选语音数据-全量.txt"
    commandPath = "C:\\Users\\No.1\\Desktop\\teleccom\\华为4G异频异系统切换重选语音数据-全量.txt"
    outputPath = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\"
    # 工参表路径
    # common_table = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\地市规则\\5G资源大表-20231227.csv"
    # standard_path = "C:\\Users\\No.1\\Desktop\\teleccom\\互操作参数核查结果.xlsx"
    rawFile = HuaweiRawDataFile(rawDataPath, commandPath, outputPath, '4G')
    rawFile.read_huawei_txt()
    # rawFile.output_handover_result(qci=9)
    rawFile.output_format_data()
    # rawFile.output_handover_result(9)
