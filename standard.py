import pandas as pd


class Standard:
    def __init__(self, standard_path, sheet_names):
        self.standard_path = standard_path
        self.sheet_names = sheet_names

    def import_standard_file(self):
        point_standard = pd.read_excel(self.standard_path, sheet_name="点对点推荐值")
        cell_standard = pd.read_excel(self.standard_path, sheet_name="自身推荐")

    def parse_cell_level_strategy_file(self, check_params,manufactor):
        cell_standard = pd.read_excel(self.standard_path, sheet_name="自身推荐")
        #过滤需要检查的参数
        cell_standard = cell_standard[cell_standard['原始参数名称'].isin(check_params) & cell_standard['厂家'] == manufactor]
        cell_standard = cell_standard[["厂家","频带","参数名称","原始参数名称","推荐值"]]
        cols = g5_data_strategy.columns.tolist()
        cols0 = g45_data_strategy.columns.tolist()
        keep_name_list = ['频带', '厂家', '覆盖类型']
        for c in cols:
            if c in keep_name_list:
                continue
            g5_data_strategy.rename(columns={c: c + "#合规"}, inplace=True)
        for c in cols0:
            if c in keep_name_list:
                continue
            g45_data_strategy.rename(columns={c: c + "#合规"}, inplace=True)
        return g5_data_strategy, g45_data_strategy
    # def add_judgement(self, df):
    #     feature_cols = list(df.columns.tolist())
    #     for index, c in enumerate(feature_cols):
    #         original_name = c['原始参数名称']
    #         # if report_cols.find(original_name) >= 0:
    #         if original_name in report_cols:
    #             new_col = '是否合规' + "_" + str(index)
    #             report[new_col] = report.apply(add_judgement, axis=1, args=(original_name, c))
    #             diff_cols.append(original_name)
    #             diff_cols.append(c)
    #             diff_cols.append(new_col)

    def add_huawei_cell_judgement(x, original_name, c):
        """
            判断小区级别是否合标
        """
        judgement_value = str(x[c])
        original_value = x[original_name]
        if len(judgement_value.strip()) == 0:
            return ""
        if judgement_value.find("~") >= 0:
            splits = judgement_value.split("~")
            value_0 = int(splits[0])
            value_1 = int(splits[1])
            temp = value_1
            if value_0 > value_1:
                value_1 = value_0
                value_0 = temp
            if value_0 < original_value < value_1:
                return "是"
            elif value_0 == judgement_value or value_1 == judgement_value:
                return "是"
            else:
                return "否"
        elif judgement_value == 'nan':
            return '是'
        elif judgement_value.find("[") >= 0 and judgement_value.find("]") >= 0:
            judgement_value.replace("[", "").replace("]", "")
        else:

            return "是" if int(float(judgement_value)) == int(float(original_value)) else "否"
