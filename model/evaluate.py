# -*- coding:utf-8 -*-
# import itertools
# from pandas import DataFrame
# import logging
#
# from configuration import zte_configuration, ericsson_configuration
# from model.data_watcher import DataWatcher
# from reader.huawei_raw_datareader import *
# import math
#
# logging.basicConfig(format='%(asctime)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S', level=logging.INFO)
#
#
# class Evaluation:
#
#     def __init__(self, data_path,
#                  watcher: DataWatcher,
#                  # f_name: str,
#                  used_commands,
#                  cell_config_df=None,
#                  freq_config_df=None,
#                  cities=['湖州', '杭州', '金华', '嘉兴', '丽水', '宁波', '衢州', '绍兴', '台州', '温州', '舟山'],
#                  countries=huawei_configuration.COUNTRIES_DICT
#                  ):
#         """
#             一个是文件名对应的列名字典
#             一个是文件名中的key的字典
#         """
#         self.cities = cities
#         self.countries = countries
#         self.na_area_cgi = []
#         self.used_commands = used_commands
#         self.system = watcher.system
#         self.manufacturer = watcher.manufacturer
#         self.site_info = watcher.get_site_info()[['CGI', '共址类型']]
#         self.site_info.drop_duplicates(subset=['CGI'], keep='last', inplace=True)
#         self.cell_identity = self.get_cell_identity()
#         self.file_path = data_path
#         f_name = os.path.split(data_path)[1]
#         self.params_files_cols_dict = {}
#         self.standard_path = watcher.config_path
#         self.standard_alone_df = pd.DataFrame()
#         self.cal_rule = pd.read_excel(self.standard_path, sheet_name="参数计算方法", dtype=str)
#         # g4_common_df = pd.read_csv(g4_common_table, usecols=['中心载频信道号', '工作频段', '频率偏置'], encoding='gbk', dtype='str')
#         # self.band_list = []
#         self.g4_freq_band_dict = {}
#         # if self.system == '4G':
#         g4_common_df = watcher.get_4g_common_df()[['中心载频信道号', '工作频段', '频率偏置']]
#         # self.g4_freq_band_dict, self.band_list = huaweiutils.generate_4g_frequency_band_dict()
#         # prepare_res = watcher.g4_prepare() if self.system == '4G' else {}, []
#         # self.g4_freq_band_dict = prepare_res[0]
#         self.all_area_classes = ""  # 所有可能小区类别
#         self.all_cover_classes = ""  # 所有覆盖类型
#         self.all_band = ""  # 所有的频带
#         self.all_co_location = [math.nan]  # 所有的共址.类型
#         self.base_info_df = watcher.get_base_info(f_name)
#         if self.manufacturer == '华为':
#             self._inference_city(self.base_info_df)
#         # self.base_info_df = self.get_base_info(band_list)
#         self.end_band = 'FDD900|FDD1800|F|A|D|E|4.9G|2.6G|700M'
#         # self.base_info_df = self.g4_base_info_df if system == '4G' else self.g5_base_info_df
#         self.all_area_classes = huaweiutils.list_to_str(self.base_info_df['区域类别'].unique().tolist())
#         self.all_cover_classes = huaweiutils.list_to_str(self.base_info_df['覆盖类型'].unique().tolist())
#         self.all_co_location = huaweiutils.list_to_str(self.base_info_df['共址类型'].unique().tolist())
#         # if self.standard_path.endswith('xlsx'):
#         #     logging.info("开始读取配置表")
#         #     self.cell_config_df = pd.read_excel(self.standard_path, sheet_name="小区级别核查配置", dtype=str) \
#         #         if cell_config_df is None else cell_config_df
#         #     self.freq_config_df = pd.read_excel(self.standard_path, sheet_name="频点级别核查配置", dtype=str) \
#         #         if freq_config_df is None else freq_config_df
#         # elif self.standard_path.endswith('csv'):
#         #     self.cell_config_df = pd.read_csv(self.standard_path, sheet_name="小区级别核查配置", true_values=["是"],
#         #                                         false_values=["否"], dtype=str)
#         #     self.freq_config_df = pd.read_csv(self.standard_path, sheet_name="频点级别核查配置", true_values=["是"],
#         #                                         false_values=["否"], dtype=str)
#         self.freq_config_df = self.freq_config_df[
#             (self.freq_config_df['制式'] == self.system) & (self.freq_config_df['厂家'] == self.manufacturer)]
#         self.cell_config_df = self.cell_config_df[
#             (self.cell_config_df['制式'] == self.system) & (self.cell_config_df['厂家'] == self.manufacturer)]
#         self.cell_config_df['参数组标识'] = self.cell_config_df['参数组标识'].astype(str)
#         self.cell_config_df['QCI'] = self.cell_config_df['QCI'].astype(str)
#         self.freq_config_df['参数组标识'] = self.freq_config_df['参数组标识'].astype(str)
#         self.freq_config_df['QCI'] = self.freq_config_df['QCI'].astype(str)
#         self.qci_file_name = 'LST NRCELLQCIBEARERQCI.csv' if self.system == '5G' else 'LST CELLQCIPARAQCI.csv'
#         self.freq_config_df = self.precheck_config(self.freq_config_df)
#         self.cell_config_df = self.precheck_config(self.cell_config_df)
#         self.config = pd.DataFrame()
#         self.cell_df = pd.DataFrame()
#         self.freq_df = pd.DataFrame()
#         self.pre_params = []
#         self.cover_filter_list = ['高铁']
#         self.pre_param_dict = {}
#         self.key_col = self.get_key_col()
#
#     def _inference_city(self, df):
#         na_city_df = df[df['地市'].isna()]
#         countries = self.countries.keys()
#         city_file_keys = huawei_configuration.FILE_CITY_DICT.keys()
#         for index, row in na_city_df.iterrows():
#             net_element = row[self.key_col]
#             find = False
#             if self.manufacturer == '华为':
#                 for key in city_file_keys:
#                     if os.path.basename(self.file_path).find(key) >= 0:
#                         df.at[index, '地市'] = huawei_configuration.FILE_CITY_DICT[key]
#                         find = True
#                         break
#             if not find:
#                 for c in self.cities:
#                     if net_element.find(c) >= 0:
#                         df.at[index, '地市'] = c
#                         find = True
#                         break
#             if not find:
#                 for c in countries:
#                     if net_element.find(c) >= 0:
#                         df.at[index, '地市'] = self.countries[c]
#                         find = True
#
#     def get_key_col(self):
#         if self.manufacturer == '中兴':
#             return 'CGI'
#         elif self.manufacturer == '华为':
#             return '网元'
#         elif self.manufacturer == '爱立信':
#             return 'MeContext'
#
#     def get_cell_identity(self):
#         if self.system == '4G' and self.manufacturer == '中兴':
#             return zte_configuration.G4_CELL_IDENTITY
#         elif self.system == '5G' and self.manufacturer == '中兴':
#             return zte_configuration.G5_CELL_IDENTITY
#         elif self.system == '5G' and self.manufacturer == '华为':
#             return huawei_configuration.G5_CELL_IDENTITY
#         elif self.system == '4G' and self.manufacturer == '华为':
#             return huawei_configuration.G4_CELL_IDENTITY
#         elif self.system == '5G' and self.manufacturer == '爱立信':
#             return ericsson_configuration.G5_CELL_IDENTITY
#         elif self.system == '4G' and self.manufacturer == '爱立信':
#             return ericsson_configuration.G4_CELL_IDENTITY
#
#     def precheck_config(self, config):
#         """
#             按qci重新命名参数
#         """
#         if config.empty:
#             logging.info('===============配置表为空===============')
#             return
#         merged_config = config.merge(self.cal_rule, how='left', on=['原始参数名称', '主命令'])
#         # config0.sort_values(by=['伴随参数命令'], inplace=True)
#         merged_config = merged_config[~merged_config['推荐值'].isna()]
#         merged_config[['原始参数名称', '参数名称']] = merged_config.apply(lambda x: Evaluation.generate_unique_name(x),
#                                                                 axis=1).apply(pd.Series)
#         merged_config = merged_config[merged_config['推荐值'] != '/']
#         return merged_config
#
#     @staticmethod
#     def generate_unique_name(row):
#         """
#             由于不同的厂家，参数名存在不同情况的重复，因此参数由以下组合形成
#             row['原始参数名称'] + "_" + row['参数名称'] + "_" + row['二级表头']可以唯一确定一个参数
#         """
#         qci = str(row['QCI'])
#         unique_name = row['原始参数名称'] + "|" + row['参数名称'] + "|" + row['二级表头']
#         if qci != 'nan':
#             original_name = row['原始参数名称'] + "|" + qci
#
#             return original_name, unique_name
#         else:
#             return row['原始参数名称'], unique_name
#
#     def get_base_cols(self):
#         return [self.get_key_col()]
#
#     def find_huawei_switch_cols(self, file_name, switch_params):
#
#         find_params = {}
#         try:
#             df = pd.read_csv(file_name, nrows=1)
#         except:
#             df = pd.read_csv(file_name, nrows=1, encoding='gbk')
#         cols = df.columns.tolist()
#         # if self.cell_identity in cols:
#         #     base_cols.append(self.cell_identity)
#         # if '服务质量等级' in cols:
#         #     base_cols.append('服务质量等级')
#         if len(switch_params) == 0:
#             logging.info("不需要在" + file_name + "中寻找开关参数")
#             return find_params
#         if df.empty:
#             logging.info(file_name + "中数据为空")
#             return find_params
#         first_row = df.iloc[0]
#         for param in switch_params:
#             find = False
#             for c in cols:
#                 cell = first_row[c]
#                 find_result = str(cell).find(param)
#                 if find_result < 0:
#                     continue
#                 else:
#                     find_params[param] = c
#                     find = True
#             if find is False:
#                 raise Exception("在【" + file_name + "】没有找到【" + param + "】,请检查参数名称")
#         return find_params
#
#     def mapToFreqPtToBand(self, config_df: DataFrame, df: DataFrame, command: str) -> DataFrame:
#         cols = config_df.columns.tolist()
#         frequencies = config_df['频点标识'].unique().tolist()
#         if '频点标识' in cols and len(frequencies) > 0:
#             frequency_param = frequencies[0]
#             df = self.freq_param_map(df, self.manufacturer, self.system, frequency_param,
#                                      self.g4_freq_band_dict)
#
#         return df
#
#     def freq_param_map(self, df, manufacturer, system, frequency_param, g4_freq_band_dict) -> DataFrame:
#         if manufacturer == '华为':
#             df = huawei_configuration.map_huawei_freq_pt(df, frequency_param, g4_freq_band_dict)
#         elif manufacturer == '中兴':
#             df = zte_configuration.map_zte_freq_pt(df, frequency_param, g4_freq_band_dict)
#         elif manufacturer == '爱立信':
#             df = ericsson_configuration.map_eri_freq_pt(df, system, frequency_param)
#         df.rename(columns={frequency_param: '对端频带'}, inplace=True)
#
#         return df
#
#     def before_add_judgement(self, df, g, command) -> DataFrame:
#         """
#          在添加Judgement前先将数据中的4G频点变成FDD1800/FDD900/A/E/F对应
#          并修改列名使其与配置中的列名相同
#         """
#         df = self.mapToFreqPtToBand(g, df, command)
#         on = [self.key_col]
#         if self.cell_identity in df.columns.tolist():
#             on.append(self.cell_identity)
#             on = list(set(on))
#             df[self.cell_identity] = df[self.cell_identity].apply(str)
#             self.base_info_df[self.cell_identity] = self.base_info_df[self.cell_identity].apply(str)
#         # if self.manufacturer == 'zte':
#         #     on = ['CGI']
#         # df = df.merge(self.base_info_df, how='left', on=on)
#         df_new = self.base_info_df.merge(df, how='left', on=on)
#         # df.rename(columns={'频带': '频段'}, inplace=True)
#         # df_new.dropna(inplace=True, )
#         if self.manufacturer == '华为':
#             self._inference_city(df_new)
#         return df_new
#
#     def extra_handler(self, df, param):
#         if self.manufacturer == '中兴':
#             return zte_configuration.zte_extra_handler(df, param)
#         """
#             专门用来处'基于负载的异频RSRP触发门限(dBm)','基于业务的异频切换RSRP门限偏置(dB)'
#             这个两个参数, 这两个参数是跨表,这两张表不属于同一种类型
#         """
#         if param == '基于业务的异频切换RSRP门限偏置(dB)':
#             df = df.merge(self.cell_df[['网元', self.cell_identity, '基于负载的异频RSRP触发门限(dBm)']], how='left',
#                           on=['网元', self.cell_identity])
#             df[param] = df[param] + df['基于负载的异频RSRP触发门限(dBm)']
#             df.drop('基于负载的异频RSRP触发门限(dBm)', inplace=True, axis=1)
#             return ['基于业务的异频切换RSRP门限偏置(dB)']
#         return []
#
#     def processing_param_value(self, df, param, command):
#         all_checked_param = []
#         param0 = copy.deepcopy(param)
#         if param0.find('|') > 0:
#             param0 = param.split('|')[0]
#         if param0 in all_checked_param:
#             logging.info(param + "已经对数值进行过处理,不在需要重复处理")
#             return
#         param_rule = self.cal_rule[(self.cal_rule['原始参数名称'] == param0) & (self.cal_rule['主命令'] == command)]
#         if param_rule.empty:
#             return
#         # 需要特殊处理的参数，例如中兴的ifA=0 elseB=C规则
#         # checked_params = self.extra_handler(df, param0)
#         # if len(checked_params) > 0:
#         #     return
#         # all_checked_param.extend(checked_params)
#         premise_param = str(param_rule['伴随参数'].iloc[0])
#         premise_command = str(param_rule['伴随参数命令'].iloc[0])
#         cal_param = str(param_rule['计算方法'].iloc[0])
#         if 'nan' == premise_param and 'nan' == premise_command:
#             return
#         # 如果伴随参数为空，但是计算方式里面需要伴随参数，那么设置有问题
#         if premise_param == 'nan' and cal_param.find(',') >= 0:
#             raise Exception("计算方式设置错误,没有伴随参数,但是计算方式是:" + cal_param)
#         if premise_command in self.pre_param_dict.keys():
#             premise_command_df = self.pre_param_dict[premise_command]
#             if premise_param in premise_command_df.columns.tolist():
#                 on = [self.key_col]
#                 if self.cell_identity in df.columns.tolist() and self.cell_identity in premise_command_df:
#                     # merge出现空置，一定是数据类型不对
#                     premise_command_df[self.cell_identity] = premise_command_df[self.cell_identity].astype(str)
#                     on.append(self.cell_identity)
#                 cal_df = pd.merge(left=df, right=premise_command_df, how='left', on=on)
#             else:
#                 raise Exception("缓存中找不到计算参数:" + premise_command)
#             cal_df[param0].fillna(value=0, inplace=True)
#             cal_df[param0] = cal_df[param0].apply(int)
#             cal_df[premise_param].fillna(value=0, inplace=True)
#             cal_df[premise_param] = cal_df[premise_param].apply(int)
#             if cal_param.find(',') >= 0:
#                 splits = cal_param.split(',')
#                 multiple0 = int(splits[0])
#                 multuple1 = int(splits[1])
#                 cal_df[param0] = cal_df[param0] * multiple0 + cal_df[premise_param] * multuple1
#             else:
#                 cal_df[param0] = cal_df[param0] * int(cal_param)
#             df[param] = cal_df[param0]
#         if premise_param == 'nan' and cal_param != 'nan':
#             df[param].fillna(value="99999", inplace=True)
#             df[param] = df[param].apply(int) * float(cal_param)
#             df.loc[df[param] > 9000, param] = math.nan
#
#         # 如果前置参数在小区级别参数中,这种一定是频点级别merge小区级别
#         # elif not self.cell_df.empty and premise_param in self.cell_df.columns.tolist():
#         #     df0 = df.merge(self.cell_df[['网元', self.cell_identity, premise_param]], how='left',
#         #                    on=['网元', self.cell_identity])
#
#     def evaluate_freq_params(self, config_df):
#         if config_df.empty:
#             return []
#         checked_config = self.check_params(config_df)
#         command_grouped = checked_config.groupby(['主命令'])
#         read_result_list = []
#         for command, g in command_grouped:
#             params = g['参数名称'].unique().tolist()
#             # 华为参数需要去掉数字,中兴不需要
#             if self.manufacturer == '华为':
#                 command = huaweiutils.remove_digit(command, [",", ":"])
#             file_name = os.path.join(self.file_path, 'raw_result', command + '.csv')
#             base_cols = self.get_base_cols()
#             read_res = self.read_data_by_command(file_name, params, g, base_cols)
#             if read_res.empty:
#                 # 有可能没有数据，因为没有加这个命令
#                 continue
#             read_res = self.before_add_judgement(read_res, g, command)
#             read_res.dropna(inplace=True, axis=0, subset=params, how='all')
#             for p in params:
#                 if p in self.pre_params:
#                     logging.info(p + '是计算依赖参数,不需要输出')
#                     continue
#                 self.processing_param_value(read_res, p, command)
#                 read_res = self.judge_compliance(read_res, p, g)
#             read_result_list.append((read_res, command))
#         # 只要主命令标识不为空，说明要读QCI进行过滤
#         return read_result_list
#
#     def check_params(self, config_df):
#         copy_config_df = config_df.copy(deep=True)
#         # 如果某些参数的计算前提是必须有其他参数存在，则需要检查前置参数是否存在
#         res_df = copy_config_df[['原始参数名称', '主命令']].merge(self.cal_rule, how='left', on=['原始参数名称', '主命令'])
#         # 有多少的伴随参数，检查这些伴随参数是否在检查参数中,否则报错
#         primise_params_tuple = list(res_df[['伴随参数', '伴随参数命令']].apply(tuple).values)
#         for p in primise_params_tuple:
#             """
#                 将所有需要的计算的参数放入缓存
#             """
#             pre_param_name = str(p[0])
#             pre_param_command = str(p[1])
#             if pre_param_name == 'nan' and pre_param_command == 'nan':
#                 continue
#             pre_param_file = os.path.join(self.file_path, 'raw_result', pre_param_command + '.csv')
#             pre_param_df = pd.read_csv(pre_param_file)
#             on_cols = [pre_param_name]
#             pre_param_df_cols = pre_param_df.columns.tolist()
#             if self.cell_identity in pre_param_df_cols:
#                 on_cols.append(self.cell_identity)
#             if self.key_col in pre_param_df_cols:
#                 on_cols.append(self.key_col)
#             pre_param_df = pre_param_df[on_cols]
#             self.pre_param_dict[pre_param_command] = pre_param_df
#         return copy_config_df
#
#     def evaluate_cell_params(self, config_df):
#         # all_param = list(config_df[['原始参数名称', '主命令', 'QCI']].apply(tuple).values)
#         # all_param = list(set(tuple(t) for t in all_param))
#         checked_config = self.check_params(config_df, False)
#         command_identities = config_df['参数组标识'].tolist()
#         command_identities = list(filter(lambda x: not 'nan' == x, command_identities))
#         command_grouped = checked_config.groupby(['主命令', 'QCI'])
#         read_result_list = []
#         qci_params = [self.key_col, self.cell_identity, '服务质量等级']
#         read_qci = False
#         qci_df = pd.DataFrame()
#         qci_path = os.path.join(self.file_path, 'raw_result', self.qci_file_name)
#         for p, g in command_grouped:
#             command = p[0]
#             qci = p[1]
#             params = g['参数名称'].unique().tolist()
#             if self.manufacturer == '华为':
#                 command = huaweiutils.remove_digit(command, [",", ":"])
#             # if not command in self.used_commands or qci == '1' or qci == '5':
#             # 华为的数据首先过滤以下命令,没有使用到的命令直接跳过
#             if not command in self.used_commands and self.manufacturer == '华为':
#                 logging.info("该命令没有在此次参数核查中使用:" + command + 'QCI:' + str(qci))
#                 continue
#             file_name = os.path.join(self.file_path, 'raw_result', command.strip() + '.csv')
#             logging.info("开始读取参数:" + str(params))
#             base_cols = self.get_base_cols()
#             read_res = self.read_data_by_command(file_name, params, g, base_cols)
#             if qci.isdigit() and self.manufacturer == '华为':
#                 # 目前是有华为匹配QCI是下面代码的逻辑,爱立信中兴则不需要QCI
#                 read_qci = True
#                 qci = int(qci)
#             read_result_list.append((read_res, qci))
#         # 只要主命令标识不为空，说明要读QCI进行过滤
#         if read_qci or len(command_identities) > 0:
#             qci_params.extend(command_identities)
#             qci_df = pd.read_csv(qci_path, usecols=qci_params, dtype=str)
#             qci_df['服务质量等级'].fillna(value=-1, inplace=True)
#             qci_df['服务质量等级'] = qci_df['服务质量等级'].astype(int)
#         logging.info("开始合并所有参数")
#         merge_qci_result = self.merge_qci_data(qci_df, read_result_list, qci_params)
#         modified_cols = [i for i in merge_qci_result.columns.tolist() if i.find('|') >= 0]
#         for p in modified_cols:
#             logging.info("开始核查参数:" + p)
#             if p in self.pre_params:
#                 logging.info(p + '是计算依赖参数,不需要输出')
#                 continue
#             origin_param = p.split('|')[0]
#             display_param = p.split('|')[1]
#             sec_class = p.split('|')[2]
#             standard = self.cell_config_df[self.cell_config_df['参数名称'] == p]
#             commands = standard['主命令'].unique().tolist()
#             if len(commands) > 1 or len(commands) == 0:
#                 raise Exception("规则配置表错误,参数名:【" + display_param + '】,二级表头:【' + sec_class + '】出现重复或者没有找到相应配置')
#             command = commands[0]
#             if not huaweiutils.remove_digit(command, [",", ":"]) in self.used_commands and self.manufacturer == '华为':
#                 # or qci == '1' or qci == '5':
#                 continue
#             self.processing_param_value(merge_qci_result, origin_param, command)
#             merge_qci_result = self.judge_compliance(merge_qci_result, p, standard)
#         return merge_qci_result
#
#     def merge_qci_data(self, qci_df, read_result_list, qci_params):
#
#         """
#             华为数据专有逻辑
#             将需要检查的数据合并成一张表
#             这里的qci_df是全量
#         """
#         qci_params.remove(self.key_col)
#         qci_params.remove(self.cell_identity)
#         qci_res_list = []
#         non_qci_res = self.base_info_df
#         for r in read_result_list:
#             # 这里删除所有行都是nan的行,中兴数据以CGI连接，这里是全量
#             cols = r[0].columns.tolist()
#             qci = r[1]
#             if not 'nan' == qci and self.manufacturer == '华为':
#                 # 同样下面的逻辑只使用与华为
#                 if qci == 1 or qci == 5:
#                     suffix = '语音'
#                 elif qci == 9:
#                     suffix = '数据'
#                 else:
#                     raise Exception("华为数据未知的QCI值:" + qci)
#                 # 为了防止不同QCI的相同参数产生错误
#                 for c in cols:
#                     if c != self.key_col and c != self.cell_identity and c.find('参数组') < 0:
#                         r[0].rename(columns={c: c + "_" + suffix + str(qci)}, inplace=True)
#                 res = qci_df[qci_df['服务质量等级'] == qci]
#                 on = list(set(cols) & set(res.columns.tolist()))
#                 left_on = copy.deepcopy(on)
#                 if self.cell_identity not in on:
#                     left_on.append(self.cell_identity)
#                 qci_res = res[left_on].merge(r[0], how='left', on=on)
#                 qci_res_cols = qci_res.columns.tolist()
#                 last_cols = list(set(qci_res_cols) - set(qci_params))
#                 qci_res = qci_res[last_cols]
#                 # 如果on中没有cell.identity,那么会有很多重复
#                 if not self.cell_identity in qci_res_cols:
#                     qci_res.drop_duplicates(subset=[self.key_col], keep='last', inplace=True, ignore_index=False)
#                 qci_res_list.append(qci_res[last_cols])
#             else:
#                 on = [self.key_col]
#                 if self.cell_identity in r[0] and self.cell_identity in non_qci_res:
#                     on.append(self.cell_identity)
#                     non_qci_res[self.cell_identity] = non_qci_res[self.cell_identity].astype(str)
#                 if self.manufacturer == '爱立信':
#                     # 如果是nrcell这张表就不用再次merge，因为该表已经在base_info_df中
#                     # 如果列里面有ref,那么连接列只需要ref就够了
#                     # 但是如果此时两边都有cellName列, 只按照ref合并就会造成重复,
#                     # 因此，在只以Ref为合并列的情况下,如果两边都有cellName,将右表的cellName删除,因为左表是base_info_df
#                     # 如果ref大于5(经验值),当做是nrcucell表,直接跳过(废除)
#                     refCols = list(filter(lambda x: x.endswith('ref'), r[0].columns.tolist()))
#                     if len(refCols) > 0:
#                         on = refCols
#                         on.append(self.key_col)
#                         # r[0].dropna(axis=1, inplace=True, how='all')
#                         # 两边都有cellName, 删除右表的cellName,以为mecontext列都是一致的,但是cellName会有错误
#                         if self.cell_identity in r[
#                             0].columns.tolist() and self.cell_identity in non_qci_res.columns.tolist():
#                             r[0].drop(columns=self.cell_identity, inplace=True)
#                 # non_qci_res = non_qci_res.merge(r[0], how='left', on=on)
#                 # 华为这里因为base_info_df这里已经过滤了一遍CGI,所以base_info_df在左表，但是中兴没有过滤
#                 non_qci_res = pd.merge(non_qci_res, r[0], how='left', on=on)
#                 cols.remove('CGI')
#                 non_qci_res.dropna(axis=0, subset=cols, how='all', inplace=True)
#         merge_qci_df = huaweiutils.merge_dfs(qci_res_list, on=[self.key_col], cell_identity=self.cell_identity)
#         res = non_qci_res.merge(merge_qci_df, how='left',
#                                 on=[self.key_col, self.cell_identity]) if not merge_qci_df.empty else non_qci_res
#         return res
#
#     def read_data_by_command(self, file_name, params, g, base_cols) -> DataFrame:
#         # 区分开关和非开关参数,剩下的params中都是非开关类参数
#         # 其中爱立信参数需要保留带Ref的参数, 方便与Base_info_df合并,这个参数最准确
#         check_params = copy.deepcopy(params)
#         switch_params = []
#         for i in range(len(check_params) - 1, -1, -1):
#             param = check_params[i]
#             # origin_param = param.split('_')[0]
#             is_switch = g[g['参数名称'] == param]['开关'].iloc[0]
#             if not pd.isna(is_switch) and is_switch == '是':
#                 switch_params.append(param)
#                 check_params.remove(param)
#         # 首先只读取一行数据，来找出switch开关在哪一行
#
#         switch_dict = {}
#         if self.manufacturer == '华为':
#             switch_dict = self.find_huawei_switch_cols(file_name, switch_params)
#
#         g['参数组标识'] = g['参数组标识'].astype('str')
#         identity = [item for item in g['参数组标识'].unique().tolist() if
#                     item.strip() != '' and item.strip() != 'nan' and item.strip() is not None]
#         if len(identity) > 0 and not 'nan' == identity[0]:
#             base_cols.append(identity[0])
#         if '频点标识' in g.columns.tolist():
#             frequency = g['频点标识'].unique().tolist()
#             if len(frequency) > 0:
#                 base_cols.append(frequency[0])
#         base_df = huaweiutils.read_csv(file_name, None, str)
#         if self.cell_identity in base_df.columns:
#             base_cols.append(self.cell_identity)
#         if '服务质量等级' in base_df.columns:
#             base_cols.append('服务质量等级')
#         # 如果没有开关参数，那么赋值为base_info
#         switch_df = pd.DataFrame()
#         # 查看主命令是否存在标识,主命令对应标识应该一样
#         # 目前开关参数只针对华为有特殊的匹配方法
#         if len(switch_dict) > 0:
#             switch_cols = (list(set(switch_dict.values())))
#             switch_cols.extend(base_cols)
#             df = pd.read_csv(file_name, usecols=switch_cols, dtype=str)
#             switch_df = self.read_switch_data(df, base_df[base_cols], switch_dict)
#             switch_df.drop_duplicates(how='all', inplace=True, keep='first')
#             # if self.manufacturer == '爱立信':
#             # self.rename_eri_param(switch_df, switch_cols, file_name)
#         # 读取非开关参数
#         non_switch_df = pd.DataFrame()
#         if len(check_params) > 0:
#             non_switch_col = copy.deepcopy(base_cols)
#             # 之前为了区分相同参数下的不同qci,参数后加上了_QCI,在这里去掉，否则无法查到原始数据
#             for i in range(len(check_params)):
#                 if check_params[i].find('|') >= 0:
#                     o_check_param = check_params[i].split('|')[0]
#                     non_switch_col.append(o_check_param)
#             logging.debug("读取文件:【" + file_name + "】读取的列:【" + str(non_switch_col) + "】")
#             # 读取开关参数和非开关参数均读取Ref,防止多次读取Ref列，用set过滤
#             if self.manufacturer == '爱立信':
#                 refCols = list(filter(lambda x: x.endswith('ref'), base_df.columns.tolist()))
#                 non_switch_col.extend(refCols)
#             non_switch_df = huaweiutils.read_csv(file_name, list(set(non_switch_col)), str, self.manufacturer)
#             non_switch_df.drop_duplicates(inplace=True, keep='first')
#             # if self.manufacturer == '爱立信':
#             #     self.rename_eri_param(non_switch_df, check_params, file_name)
#         if not non_switch_df.empty and not switch_df.empty:
#             result_df = pd.merge(switch_df, non_switch_df, how='left', on=base_cols)
#         elif non_switch_df.empty and switch_df.empty:
#             logging.warn("开关参数和非开关参数读取结果均为空,请检查参数名称或者原始数据结果是否包含该参数,当前检查参数【" + str(params) + "】")
#             # raise Exception("开关参数和非开关参数读取结果均为空,请检查参数名称或者原始数据结果是否包含该参数,当前检查参数【" + str(params) + "】")
#             return pd.DataFrame()
#         else:
#             result_df = switch_df if not switch_df.empty else non_switch_df
#         # 针对不同列名的重复情况，需要扩展参数名,相同的参数名，可能对应多个参数
#         remove_col = set()
#         for p in params:
#             o_param = p.split('|')[0]
#             result_df[p] = result_df[o_param]
#             remove_col.add(o_param)
#         # 扩展完成之后删除
#         result_df.drop(labels=remove_col, inplace=True, axis=1)
#         return result_df
#
#     def read_switch_data(self, df, base_df, switch_params):
#         """
#             读所有的开关参数并将其合并
#         """
#         result_df = pd.DataFrame()
#         for param, c in switch_params.items():
#             df_c = df[[c]]
#             flatten_df = huaweiutils.flatten_features(df_c, c)[[param]]
#             if result_df.empty:
#                 result_df = pd.concat([base_df, flatten_df], axis=1)
#                 continue
#             # 由于是按命令分组,因此可以合并
#             result_df = pd.concat([result_df, flatten_df], axis=1)
#         return result_df
#
#     def judge_compliance(self, df, param, standard):
#         if standard.empty:
#             raise Exception('参数【' + param + '】没有找到配置信息')
#         if '对端频带' not in standard.columns.tolist():  # 表明是cell级别
#             # standard = g[g['原始参数名称'] == param]
#             df = self.add_cell_judgement(df, standard, param)
#         else:  # 频点级别
#             # 判断是4G频点还是5G频点
#             standard = standard[(standard['参数名称'] == param)]
#             df = self.add_freq_judgement(df, standard, param)
#         return df
#
#     def add_freq_judgement(self, df, standard, param):
#         standard_on = ['对端频带', '频段', '覆盖类型', '区域类别', '共址类型']
#         df['对端频带'] = df['对端频带'].apply(str)
#         deep_on = copy.deepcopy(standard_on)
#         deep_on.append('推荐值')
#         standard = self.expand_standard(standard, standard_on)
#         df = df.merge(standard[deep_on], how='left',
#                       on=standard_on)
#         df.rename(columns={"推荐值": param + "#推荐值"}, inplace=True)
#         df[param + "#合规"] = huaweiutils.judge(df, param)
#         return df
#
#     def fill_col_na(self, standard, cols):
#         for c in cols:
#             if c == '区域类别':
#                 standard[c].fillna(self.all_area_classes + "|" + 'nan', inplace=True)
#             elif c == '覆盖类型':
#                 standard[c].fillna(self.all_cover_classes + "|" + 'nan', inplace=True)
#             elif c == '频段':
#                 standard[c].fillna(self.all_band + "|" + 'nan', inplace=True)
#             elif c == '共址类型':
#                 standard[c].fillna(self.all_co_location + "|" + 'nan', inplace=True)
#             elif c == '对端频带':
#                 standard[c].fillna(self.end_band + "|" + 'nan', inplace=True)
#             else:
#                 raise Exception(c + ",该字段没有定义过滤")
#
#     def expand_standard(self, standard, cols):
#         self.fill_col_na(standard, cols)
#         for index, row in standard.iterrows():
#             possible_row_list = []
#             add_rows = []
#             for c in cols:
#                 value = str(row[c])
#                 value = value.split('|') if value.find('|') >= 0 else [value]
#                 possible_row_list.append(value)
#             possible_tuple = tuple(possible_row_list)
#             result = list(itertools.product(*possible_tuple))
#             # 二维列表去重，先转成tuple,然后去重
#             result = list(set(tuple(t) for t in result))
#             for r in result:
#                 new_row = copy.deepcopy(row)
#                 for idx, e in enumerate(list(r)):
#                     if e == 'nan':
#                         new_row[cols[idx]] = math.nan
#                     else:
#                         new_row[cols[idx]] = e
#                 add_rows.append(new_row)
#             standard = standard.drop(index=index)
#             standard = standard.append(add_rows)
#         return standard
#
#     def add_cell_judgement(self, df, standard, param):
#         standard = self.expand_standard(standard, ['区域类别', '覆盖类型', '频段', '共址类型']).reset_index(drop=True)
#         #  df = df.merge(standard[['区域类别', '覆盖类型', '频段', '推荐值', '共址类型']],how='left', on=['频段', '覆盖类型', '区域类别', '共址类型'])
#         modified_standard = copy.deepcopy(standard)
#         df = pd.merge(df, modified_standard[['区域类别', '覆盖类型', '频段', '推荐值', '共址类型']], how='left',
#                       on=['频段', '覆盖类型', '区域类别', '共址类型'])
#         df.rename(columns={"推荐值": param + "#推荐值"}, inplace=True)
#         df[param + "#合规"] = huaweiutils.judge(df, param)
#         return df
#
#     def is_4g_freq(self, all_freqs):
#         for f in all_freqs:
#             for key, item in self.g4_freq_band_dict.items():
#                 if str(f) in item:  # 证明该频段是4G频段
#                     return True
#         return False
#
#     def get_content_col(self, cols) -> []:
#         """
#             检出除了base_cols外的参数检查项
#         """
#         base_cols = self.base_info_df.columns.tolist()
#         diff = set(cols) - set(base_cols)
#         if "对端频带" in diff:
#             diff.remove("对端频带")
#         copy_diff = copy.deepcopy(diff)
#         content_cols = []
#         for m in diff:
#             if m.find('#') < 0:
#                 content_cols.append(m)
#                 for n in copy_diff:
#                     if m == n:
#                         continue
#                     if n.find(m) >= 0 and n.find('#') >= 0:
#                         content_cols.append(n)
#         return content_cols
#
#     def sort_result(self, cols, config, base_cols):
#         ordered_params = config['参数名称'].unique().tolist()
#         content_cols = self.get_content_col(cols)
#         order_content_cols = []
#         for param in ordered_params:
#             param = param.strip()
#             if param in content_cols:
#                 order_content_cols.extend([param, param + '#推荐值', param + '#合规'])
#         order_content, class_dict = self.order_content_cols(config, order_content_cols)
#         # base_cols = ['地市', '网元', 'NRDU小区名称', 'NR小区标识', 'CGI', '频段', '工作频段',
#         #              '双工模式', '厂家', '共址类型', '覆盖类型', '覆盖场景', '区域类别']
#         return base_cols + order_content, class_dict
#
#     def generate_cell_report(self, check_type, base_cols) -> {}:
#         first_class_dict = {}
#         if check_type == 'cell' or check_type == 'all':
#             logging.info("开始核查小区级别参数")
#             cell_df = self.evaluate_cell_params(self.cell_config_df)
#             order_cols, first_class_dict = self.sort_result(cell_df.columns.tolist(), self.cell_config_df, base_cols)
#             cell_df = cell_df[order_cols]
#             # 将区域类型为空的之前填补成农村区域的站点还原
#             na_index = cell_df[cell_df['CGI'].isin(self.na_area_cgi)].index.tolist()
#             # 不在过滤高铁站
#             # cell_df = cell_df[~cell_df['覆盖场景'].isin(self.cover_filter_list)]
#             if len(na_index) > 0:
#                 select_col_index = cell_df.columns.get_loc('区域类别')
#                 cell_df.iloc[na_index, select_col_index] = math.nan
#             self.cell_df = cell_df
#             out = os.path.join(self.file_path, 'check_result', 'cell')
#             if not os.path.exists(out):
#                 os.makedirs(out)
#             cell_df.dropna(subset=['CGI'], how='any', inplace=True)
#             cell_df.drop_duplicates(keep="first", inplace=True)
#             # 不根据地市删除小区
#             # cell_df.dropna(subset=['地市'], how='any', inplace=True)
#             # cell_df = self.format_result(cell_df, self.cell_config_df, base_cols)
#             cell_df.to_csv(os.path.join(out, 'param_check_cell.csv'), index=False, encoding='utf_8_sig')
#         return first_class_dict
#
#     def generate_freq_report(self, check_type, base_cols) -> {}:
#         freq_class_dict = {}
#         if check_type == 'freq' or check_type == 'all':
#             logging.info("开始核查频点级别参数")
#             base_cols.extend(["对端频带"])
#             freq_df_list = self.evaluate_freq_params(self.freq_config_df)
#             for df in freq_df_list:
#                 d = df[0]
#                 sorted_cols, freq_class_dict = self.sort_result(d.columns.tolist(), self.freq_config_df, base_cols)
#                 if '频点' in d.columns.tolist():
#                     index = sorted_cols.index('对端频带')
#                     sorted_cols.insert(index + 1, '频点')
#                 d = d[sorted_cols]
#                 # 将区域类型为空的之前填补成农村区域的站点还原
#                 d[d['CGI'].isin(self.na_area_cgi)]['CGI'] = math.nan
#                 out = os.path.join(self.file_path, 'check_result', 'freq')
#                 if not os.path.exists(out):
#                     os.makedirs(out)
#                 d.dropna(subset=['地市'], how='any', inplace=True)
#                 d.drop_duplicates(keep="first", inplace=True)
#                 d.to_csv(os.path.join(out, df[1] + '_freq.csv'),
#                          index=False, encoding='utf_8_sig')
#         return freq_class_dict
#
#     def generate_report(self, check_type, base_cols):
#         """
#             输入的推荐值和需要核查的参数
#         """
#         first_class_dict = self.generate_cell_report(check_type, base_cols)
#         freq_class_dict = self.generate_freq_report(check_type, base_cols)
#         return first_class_dict, freq_class_dict
#
#     def order_content_cols(self, config, content_cols):
#         clzz = config['类别'].unique().tolist()
#         clzz = [x for x in clzz if not x == 'nan']
#         order_content = []
#         first_class_dict = {}
#         for i in range(len(clzz)):
#             # 该层级下有那些参数
#             c = clzz[i]
#             # 一级class列表是一个字典列表，每个字典中的key是二级表头
#             first_class_content = []
#             class_config = copy.deepcopy(config[config['类别'] == c])
#             # if self.manufacturer == '爱立信':
#             #     class_config['原始参数名称'] = class_config['原始参数名称'] + "_" + class_config['主命令']
#             param_names = class_config['参数名称'].unique().tolist()
#             param_names = [i.strip() for i in param_names]
#             used_cols = list(set(param_names) & set(content_cols))
#             if len(used_cols) == 0:
#                 continue
#             second_class_dict = {}
#             for u in used_cols:
#                 # 一个参数只可能对应一个二级表头，下面的代码的结果取第一个
#                 # if self.manufacturer == '爱立信':
#                 # u = u.split('|')[0]
#                 second_class = class_config[class_config['参数名称'].str.strip() == u]['二级表头'].unique().tolist()[0]
#                 second_cols = second_class_dict.get(second_class, [])
#                 second_cols.extend([u, u + '#推荐值', u + '#合规'])
#                 second_class_dict[second_class] = second_cols
#                 order_content.extend([u, u + '#推荐值', u + '#合规'])
#                 first_class_content.extend([u, u + '#推荐值', u + '#合规'])
#             copy_second_dict = copy.deepcopy(second_class_dict)
#             first_class_dict[c] = copy_second_dict
#         return order_content, first_class_dict


# -*- coding:utf-8 -*-

import copy
import itertools
from pandas import DataFrame
import math
from configuration import zte_configuration, ericsson_configuration, common_configuration
from reader.huawei_raw_datareader import *
from model.data_watcher import DataWatcher

logging.basicConfig(format='%(asctime)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S', level=logging.INFO)


class Evaluation:

    def __init__(self, data_path,
                 watcher: DataWatcher,
                 # f_name: str,
                 used_commands,
                 cell_config_df=None,
                 freq_config_df=None,
                 cities=['湖州', '杭州', '金华', '嘉兴', '丽水', '宁波', '衢州', '绍兴', '台州', '温州', '舟山'],
                 countries=huawei_configuration.COUNTRIES_DICT
                 ):
        """
            一个是文件名对应的列名字典
            一个是文件名中的key的字典
        """
        self.freq_config_df = freq_config_df
        self.cell_config_df = cell_config_df
        self.cities = cities
        self.countries = countries
        self.na_area_cgi = []
        self.used_commands = used_commands
        self.system = watcher.system
        self.manufacturer = watcher.manufacturer
        self.key_col = self.get_key_col()
        self.site_info = watcher.get_site_info()[['CGI', '共址类型']]
        self.site_info.drop_duplicates(subset=['CGI'], keep='last', inplace=True)
        self.cell_identity = self.get_cell_identity()
        self.file_path = data_path
        f_name = os.path.split(data_path)[1]
        self.params_files_cols_dict = {}
        self.standard_path = watcher.config_path
        self.standard_alone_df = pd.DataFrame()
        self.cal_rule = pd.read_excel(self.standard_path, sheet_name="参数计算方法", dtype=str)
        # g4_common_df = pd.read_csv(g4_common_table, usecols=['中心载频信道号', '工作频段', '频率偏置'], encoding='gbk', dtype='str')
        self.band_list = []
        self.g4_freq_band_dict = {}
        # if self.system == '4G':
        # g4_common_df = watcher.get_4g_common_df()[['中心载频信道号', '工作频段', '频率偏置']]
        # self.g4_freq_band_dict, self.band_list = huaweiutils.generate_4g_frequency_band_dict()
        self.g4_freq_band_dict = common_configuration.g4_band_dict
        # prepare_res = watcher.g4_prepare() if self.system == '4G' else {}, []
        # self.g4_freq_band_dict = prepare_res[0]
        self.all_area_classes = ""  # 所有可能小区类别
        self.all_cover_classes = ""  # 所有覆盖类型
        self.all_band = ""  # 所有的频带
        self.all_co_location = [math.nan]  # 所有的共址.类型
        self.base_info_df = watcher.get_base_info(f_name)
        if self.manufacturer == '华为':
            self._inference_city(self.base_info_df)
        # self.base_info_df = self.get_base_info(band_list)
        self.end_band = 'FDD900|FDD1800|F|A|D|E|4.9G|2.6G|700M'
        # self.base_info_df = self.g4_base_info_df if system == '4G' else self.g5_base_info_df
        self.all_area_classes = huaweiutils.list_to_str(self.base_info_df['区域类别'].unique().tolist())
        self.all_cover_classes = huaweiutils.list_to_str(self.base_info_df['覆盖类型'].unique().tolist())
        self.all_co_location = huaweiutils.list_to_str(self.base_info_df['共址类型'].unique().tolist())
        # if self.standard_path.endswith('xlsx'):
        #     logging.info("开始读取配置表")
        #     self.cell_config_df = pd.read_excel(self.standard_path, sheet_name="小区级别核查配置", dtype=str) \
        #         if cell_config_df is None else cell_config_df
        #     self.freq_config_df = pd.read_excel(self.standard_path, sheet_name="频点级别核查配置", dtype=str) \
        #         if freq_config_df is None else freq_config_df
        # elif self.standard_path.endswith('csv'):
        #     self.cell_config_df = pd.read_csv(self.standard_path, sheet_name="小区级别核查配置", true_values=["是"],
        #                                         false_values=["否"], dtype=str)
        #     self.freq_config_df = pd.read_csv(self.standard_path, sheet_name="频点级别核查配置", true_values=["是"],
        #                                         false_values=["否"], dtype=str)
        self.freq_config_df = self.freq_config_df[
            (self.freq_config_df['制式'] == self.system) & (self.freq_config_df['厂家'] == self.manufacturer)]
        self.cell_config_df = self.cell_config_df[
            (self.cell_config_df['制式'] == self.system) & (self.cell_config_df['厂家'] == self.manufacturer)]
        self.cell_config_df['参数组标识'] = self.cell_config_df['参数组标识'].astype(str)
        self.cell_config_df['QCI'] = self.cell_config_df['QCI'].astype(str)
        self.freq_config_df['参数组标识'] = self.freq_config_df['参数组标识'].astype(str)
        self.freq_config_df['QCI'] = self.freq_config_df['QCI'].astype(str)
        self.qci_file_name = 'LST NRCELLQCIBEARERQCI.csv' if self.system == '5G' else 'LST CELLQCIPARAQCI.csv'
        self.freq_config_df = self.precheck_config(self.freq_config_df)
        self.cell_config_df = self.precheck_config(self.cell_config_df)
        self.config = pd.DataFrame()
        self.cell_df = pd.DataFrame()
        self.freq_df = pd.DataFrame()
        self.pre_params = []
        self.cover_filter_list = ['高铁']
        self.pre_param_dict = {}

    def _inference_city(self, df):
        na_city_df = df[df['地市'].isna()]
        countries = self.countries.keys()
        city_file_keys = huawei_configuration.FILE_CITY_DICT.keys()
        for index, row in na_city_df.iterrows():
            net_element = row[self.key_col]
            find = False
            for key in city_file_keys:
                if os.path.basename(self.file_path).find(key) >= 0:
                    df.at[index, '地市'] = huawei_configuration.FILE_CITY_DICT[key]
                    find = True
                    break
            if not find:
                for c in self.cities:
                    if net_element.find(c) >= 0:
                        df.at[index, '地市'] = c
                        find = True
                        break
            if not find:
                for c in countries:
                    if net_element.find(c) >= 0:
                        df.at[index, '地市'] = self.countries[c]
                        find = True

    def get_key_col(self):
        if self.manufacturer == '中兴':
            return 'CGI'
        elif self.manufacturer == '华为':
            return '网元'
        elif self.manufacturer == '爱立信':
            return 'MeContext'

    def get_cell_identity(self):
        if self.system == '4G' and self.manufacturer == '中兴':
            return zte_configuration.G4_CELL_IDENTITY
        elif self.system == '5G' and self.manufacturer == '中兴':
            return zte_configuration.G5_CELL_IDENTITY
        elif self.system == '5G' and self.manufacturer == '华为':
            return huawei_configuration.G5_CELL_IDENTITY
        elif self.system == '4G' and self.manufacturer == '华为':
            return huawei_configuration.G4_CELL_IDENTITY
        elif self.system == '5G' and self.manufacturer == '爱立信':
            return ericsson_configuration.G5_CELL_IDENTITY
        elif self.system == '4G' and self.manufacturer == '爱立信':
            return ericsson_configuration.G4_CELL_IDENTITY

    def precheck_config(self, config):
        """
            按qci重新命名参数
        """
        if config.empty:
            logging.info('===============配置表为空===============')
            return
        merged_config = config.merge(self.cal_rule, how='left', on=['原始参数名称', '主命令'])
        # config0.sort_values(by=['伴随参数命令'], inplace=True)
        merged_config = merged_config[~merged_config['推荐值'].isna()]
        merged_config[['原始参数名称', '参数名称']] = merged_config.apply(lambda x: Evaluation.generate_unique_name(x),
                                                                axis=1).apply(
            pd.Series)
        merged_config = merged_config[merged_config['推荐值'] != '/']
        return merged_config

    @staticmethod
    def generate_unique_name(row):
        """
            由于不同的厂家，参数名存在不同情况的重复，因此参数由以下组合形成
            row['原始参数名称'] + "_" + row['参数名称'] + "_" + row['二级表头']可以唯一确定一个参数
        """
        qci = str(row['QCI'])
        unique_name = str(row['原始参数名称']) + "|" + str(row['参数名称']) + "|" + str(row['二级表头'])
        if qci != 'nan':
            original_name = row['原始参数名称'] + "|" + qci

            return original_name, unique_name
        else:
            return row['原始参数名称'], unique_name

    def get_base_cols(self):
        return [self.get_key_col()]

    def find_huawei_switch_cols(self, file_name, switch_params):

        find_params = {}
        try:
            df = pd.read_csv(file_name, nrows=1)
        except:
            df = pd.read_csv(file_name, nrows=1, encoding='gbk')
        cols = df.columns.tolist()
        # if self.cell_identity in cols:
        #     base_cols.append(self.cell_identity)
        # if '服务质量等级' in cols:
        #     base_cols.append('服务质量等级')
        if len(switch_params) == 0:
            logging.info("不需要在" + file_name + "中寻找开关参数")
            return find_params
        if df.empty:
            logging.info(file_name + "中数据为空")
            return find_params
        first_row = df.iloc[0]
        for param in switch_params:
            param = param.split('|')[0] if param.find('|') >= 0 else param
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
        return find_params

    def mapToFreqPtToBand(self, config_df: DataFrame, df: DataFrame) -> DataFrame:
        cols = config_df.columns.tolist()
        frequencies = config_df['频点标识'].unique().tolist()
        if '频点标识' in cols and len(frequencies) > 0:
            frequency_param = frequencies[0]
            df = self.freq_param_map(df, self.manufacturer, frequency_param, common_configuration.g4_band_dict)
        return df

    def freq_param_map(self, df, manufacturer, frequency_param, g4_freq_band_dict) -> DataFrame:
        if manufacturer == '华为':
            df = huawei_configuration.map_huawei_freq_pt(df, frequency_param, g4_freq_band_dict)
        elif manufacturer == '中兴':
            df = zte_configuration.map_zte_freq_pt(df, frequency_param, g4_freq_band_dict)
        elif manufacturer == '爱立信':
            df = ericsson_configuration.map_eri_freq_pt(df, frequency_param, g4_freq_band_dict)
        df.rename(columns={frequency_param: '对端频带'}, inplace=True)
        return df

    def before_add_judgement(self, df, g) -> DataFrame:
        """
         在添加Judgement前先将数据中的4G频点变成FDD1800/FDD900/A/E/F对应
         并修改列名使其与配置中的列名相同
        """
        df = self.mapToFreqPtToBand(g, df)
        on = [self.key_col]
        if self.cell_identity in df.columns.tolist():
            on.append(self.cell_identity)
            on = list(set(on))
            df[self.cell_identity] = df[self.cell_identity].apply(str)
            self.base_info_df[self.cell_identity] = self.base_info_df[self.cell_identity].apply(str)
        # if self.manufacturer == 'zte':
        #     on = ['CGI']
        # df = df.merge(self.base_info_df, how='left', on=on)
        df_new = self.base_info_df.merge(df, how='left', on=on)
        # df.rename(columns={'频带': '频段'}, inplace=True)
        # df_new.dropna(inplace=True, )
        if self.manufacturer == '华为':
            self._inference_city(df_new)
        return df_new

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
        param0 = copy.deepcopy(param)
        if param0.find('|') > 0:
            param0 = param.split('|')[0]
        param_rule = self.cal_rule[(self.cal_rule['原始参数名称'].str.strip() == param0.strip()) & (
                self.cal_rule['主命令'].str.strip() == command.strip())]
        if param_rule.empty:
            return
        if self.extra_handler(df, param0):
            return
        premise_param = str(param_rule['伴随参数'].iloc[0])
        premise_command = str(param_rule['伴随参数命令'].iloc[0])
        cal_param = str(param_rule['计算方法'].iloc[0])
        if 'nan' == premise_param and 'nan' == premise_command and 'nan' == cal_param:
            return
        # 如果伴随参数为空，但是计算方式里面需要伴随参数，那么设置有问题
        if premise_param == 'nan' and cal_param.find(',') >= 0:
            raise Exception("计算方式设置错误,没有伴随参数,但是计算方式是:" + cal_param)
        # 这里计算需要其他参数加入计算的参数
        if premise_command in self.pre_param_dict.keys():
            premise_command_df = self.pre_param_dict[premise_command]
            if premise_param in premise_command_df.columns.tolist():
                on = [self.key_col]
                if self.cell_identity in df.columns.tolist() and self.cell_identity in premise_command_df:
                    # merge出现空置，一定是数据类型不对
                    premise_command_df[self.cell_identity] = premise_command_df[self.cell_identity].astype(str)
                    on.append(self.cell_identity)
                cal_df = pd.merge(left=df, right=premise_command_df, how='left', on=list(set(on)))
            else:
                raise Exception("缓存中找不到计算参数:" + premise_command)
            cal_df[param].fillna(value=0, inplace=True)
            cal_df[param] = cal_df[param].apply(float)
            cal_df[premise_param].fillna(value=0, inplace=True)
            cal_df[premise_param] = cal_df[premise_param].apply(float)
            if cal_param.find(',') >= 0:
                splits = cal_param.split(',')
                multiple0 = int(splits[0])
                multuple1 = int(splits[1])
                cal_df[param] = cal_df[param] * multiple0 + cal_df[premise_param] * multuple1
            else:
                cal_df[param] = cal_df[param] * int(cal_param)
            df[param] = cal_df[param]
        if premise_param == 'nan' and cal_param != 'nan':
            df[param].fillna(value="99999", inplace=True)
            df[param] = df[param].apply(int) * float(cal_param)
            df.loc[df[param] > 9000, param] = math.nan

        # 如果前置参数在小区级别参数中,这种一定是频点级别merge小区级别
        # elif not self.cell_df.empty and premise_param in self.cell_df.columns.tolist():
        #     df0 = df.merge(self.cell_df[['网元', self.cell_identity, premise_param]], how='left',
        #                    on=['网元', self.cell_identity])

    def evaluate_freq_params(self, config_df):
        if config_df.empty:
            return []
        checked_config = self.check_params(config_df)
        command_grouped = checked_config.groupby(['主命令'])
        read_result_list = []
        for command, g in command_grouped:
            params = g['参数名称'].unique().tolist()
            # 华为参数需要去掉数字,中兴不需要
            if self.manufacturer == '华为':
                command = huaweiutils.remove_digit(command, [",", ":"])
            file_name = os.path.join(self.file_path, 'raw_result', command.strip() + '.csv')
            base_cols = self.get_base_cols()
            read_res = self.read_data_by_command(file_name, params, g, base_cols)
            if read_res.empty:
                # 有可能没有数据，因为没有加这个命令
                continue
            read_res = self.before_add_judgement(read_res, g)
            read_res.dropna(inplace=True, axis=0, subset=params, how='all')
            for p in params:
                if p in self.pre_params:
                    logging.info(p + '是计算依赖参数,不需要输出')
                    continue
                self.processing_param_value(read_res, p, command)
                read_res = self.judge_compliance(read_res, p, g)
            read_result_list.append((read_res, command))
        # 只要主命令标识不为空，说明要读QCI进行过滤
        return read_result_list

    def check_params(self, config_df):
        copy_config_df = config_df.copy(deep=True)
        copy_config_df['主命令'] = copy_config_df['主命令'].str.strip().tolist()
        self.cal_rule['主命令'] = self.cal_rule['主命令'].str.strip().tolist()
        # 如果某些参数的计算前提是必须有其他参数存在，则需要检查前置参数是否存在
        res_df = copy_config_df[['原始参数名称', '主命令']].merge(self.cal_rule, how='left', on=['原始参数名称', '主命令'])
        # 有多少的伴随参数，检查这些伴随参数是否在检查参数中,否则报错
        primise_params_tuple = list(res_df[['伴随参数', '伴随参数命令']].apply(tuple).values)
        for p in primise_params_tuple:
            """
                将所有需要的计算的参数放入缓存
            """
            pre_param_name = str(p[0])
            pre_param_command = str(p[1])
            if pre_param_name == 'nan' and pre_param_command == 'nan':
                continue
            pre_param_file = os.path.join(self.file_path, 'raw_result', pre_param_command.strip() + '.csv')
            pre_param_df = pd.read_csv(pre_param_file)
            on_cols = [pre_param_name]
            pre_param_df_cols = pre_param_df.columns.tolist()
            if self.cell_identity in pre_param_df_cols:
                on_cols.append(self.cell_identity)
            if self.key_col in pre_param_df_cols:
                on_cols.append(self.key_col)
            on_cols = list(set(on_cols))
            pre_param_df = pre_param_df[on_cols]
            self.pre_param_dict[pre_param_command] = pre_param_df
        return copy_config_df

    def evaluate_cell_params(self, config_df):
        # all_param = list(config_df[['原始参数名称', '主命令', 'QCI']].apply(tuple).values)
        # all_param = list(set(tuple(t) for t in all_param))
        checked_config = self.check_params(config_df)
        command_identities = config_df['参数组标识'].tolist()
        command_identities = list(filter(lambda x: not 'nan' == x, command_identities))
        command_grouped = checked_config.groupby(['主命令', 'QCI'])
        read_result_list = []
        qci_params = [self.key_col, self.cell_identity, '服务质量等级']
        read_qci = False
        qci_df = pd.DataFrame()
        qci_path = os.path.join(self.file_path, 'raw_result', self.qci_file_name)
        for p, g in command_grouped:
            command = p[0]
            qci = p[1]
            params = g['参数名称'].unique().tolist()
            if self.manufacturer == '华为':
                command = huaweiutils.remove_digit(command, [",", ":"])
            # if not command in self.used_commands or qci == '1' or qci == '5':
            # 华为的数据首先过滤以下命令,没有使用到的命令直接跳过
            if not command in self.used_commands and self.manufacturer == '华为':
                logging.info("该命令没有在此次参数核查中使用:" + command + 'QCI:' + str(qci))
                continue
            file_name = os.path.join(self.file_path, 'raw_result', command.strip() + '.csv')
            logging.info("开始读取参数:" + str(params))
            base_cols = self.get_base_cols()
            read_res = self.read_data_by_command(file_name, params, g, base_cols)
            if qci.isdigit() and self.manufacturer == '华为':
                # 目前是有华为匹配QCI是下面代码的逻辑,爱立信中兴则不需要QCI
                read_qci = True
                qci = int(qci)
            read_result_list.append((read_res, qci))
        # 只要主命令标识不为空，说明要读QCI进行过滤
        if read_qci or len(command_identities) > 0:
            qci_params.extend(command_identities)
            qci_df = pd.read_csv(qci_path, usecols=qci_params, dtype=str)
            qci_df['服务质量等级'].fillna(value=-1, inplace=True)
            qci_df['服务质量等级'] = qci_df['服务质量等级'].astype(int)
        logging.info("开始合并所有参数")
        merge_qci_result = self.merge_qci_data(qci_df, read_result_list, qci_params)
        modified_cols = [i for i in merge_qci_result.columns.tolist() if i.find('|') >= 0]
        for p in modified_cols:
            logging.info("开始核查参数:" + p)
            if p in self.pre_params:
                logging.info(p + '是计算依赖参数,不需要输出')
                continue
            origin_param = p.split('|')[0]
            display_param = p.split('|')[1]
            sec_class = p.split('|')[2]
            standard = self.cell_config_df[self.cell_config_df['参数名称'] == p]
            commands = standard['主命令'].unique().tolist()
            if len(commands) > 1 or len(commands) == 0:
                raise Exception("规则配置表错误,参数名:【" + display_param + '】,二级表头:【' + sec_class + '】出现重复或者没有找到相应配置')
            command = commands[0]
            if not huaweiutils.remove_digit(command, [",", ":"]) in self.used_commands and self.manufacturer == '华为':
                # or qci == '1' or qci == '5':
                continue
            self.processing_param_value(merge_qci_result, p, command)
            merge_qci_result = self.judge_compliance(merge_qci_result, p, standard)
        return merge_qci_result

    def merge_qci_data(self, qci_df, read_result_list, qci_params):

        """
            华为数据专有逻辑
            将需要检查的数据合并成一张表
            这里的qci_df是全量
        """
        qci_params.remove(self.key_col)
        qci_params.remove(self.cell_identity)
        qci_res_list = []
        non_qci_res = self.base_info_df
        checked_param = []
        for r in read_result_list:
            # 这里删除所有行都是nan的行,中兴数据以CGI连接，这里是全量
            cols = r[0].columns.tolist()
            qci = r[1]
            if not 'nan' == qci and self.manufacturer == '华为':
                # read_res_qci = r[0][r[0]['QCI算法'] == qci]
                read_res_qci = r[0]
                qci = str(int(float(qci)))
                # read_res_qci['服务质量等级'] = qci
                qci_df['服务质量等级'] = qci_df['服务质量等级'].apply(str)
                res = qci_df[qci_df['服务质量等级'] == qci]
                res['服务质量等级'] = qci
                if res.empty:
                    raise Exception('华为QCI合并过程中,QCI=' + qci + '过滤结果为空')
                on = list(set(cols) & set(res.columns.tolist()))
                left_on = copy.deepcopy(on)
                if self.cell_identity not in on:
                    left_on.append(self.cell_identity)
                qci_res = res[left_on].merge(read_res_qci, how='left', on=on)
                qci_res_cols = qci_res.columns.tolist()
                last_cols = list(set(qci_res_cols) - set(qci_params))
                qci_res = qci_res[last_cols]
                # 如果on中没有cell.identity,那么会有很多重复
                if not self.cell_identity in qci_res_cols:
                    qci_res.drop_duplicates(subset=[self.key_col], keep='last', inplace=True, ignore_index=False)
                qci_res_list.append(qci_res[last_cols])
            else:
                on = [self.key_col]
                if self.cell_identity in r[0] and self.cell_identity in non_qci_res:
                    on.append(self.cell_identity)
                    non_qci_res[self.cell_identity] = non_qci_res[self.cell_identity].astype(str)
                if self.manufacturer == '爱立信':
                    # 如果是nrcell这张表就不用再次merge，因为该表已经在base_info_df中
                    # 如果列里面有ref,那么连接列只需要ref就够了
                    # 但是如果此时两边都有cellName列, 只按照ref合并就会造成重复,
                    # 因此，在只以Ref为合并列的情况下,如果两边都有cellName,将右表的cellName删除,因为左表是base_info_df
                    # 如果ref大于5(经验值),当做是nrcucell表,直接跳过(废除)
                    refCols = list(filter(lambda x: x.endswith('ref'), r[0].columns.tolist()))
                    if len(refCols) > 0:
                        on = refCols
                        on.append(self.key_col)
                        # 两边都有cellName, 删除右表的cellName,以为mecontext列都是一致的,但是cellName会有错误
                        if self.cell_identity in r[
                            0].columns.tolist() and self.cell_identity in non_qci_res.columns.tolist():
                            r[0].drop(columns=self.cell_identity, inplace=True)
                # non_qci_res = non_qci_res.merge(r[0], how='left', on=on)
                # 华为这里因为base_info_df这里已经过滤了一遍CGI,所以base_info_df在左表，但是中兴没有过滤
                non_qci_res = pd.merge(non_qci_res, r[0], how='left', on=on)
                cols.remove('CGI') if 'CGI' in cols else cols
                checked_param.extend(cols)
        non_qci_res.dropna(axis=0, subset=checked_param, how='all', inplace=True)
        merge_qci_df = huaweiutils.merge_dfs(qci_res_list, on=[self.key_col], cell_identity=self.cell_identity)
        res = non_qci_res.merge(merge_qci_df, how='left',
                                on=[self.key_col, self.cell_identity]) if not merge_qci_df.empty else non_qci_res
        return res

    def read_data_by_command(self, file_name, params, g, base_cols) -> DataFrame:
        # 区分开关和非开关参数,剩下的params中都是非开关类参数
        # 其中爱立信参数需要保留带Ref的参数, 方便与Base_info_df合并,这个参数最准确
        check_params = copy.deepcopy(params)
        switch_params = []
        for i in range(len(check_params) - 1, -1, -1):
            param = check_params[i]
            # origin_param = param.split('_')[0]
            is_switch = g[g['参数名称'] == param]['开关'].iloc[0]
            if not pd.isna(is_switch) and is_switch == '是':
                switch_params.append(param)
                check_params.remove(param)
        # 首先只读取一行数据，来找出switch开关在哪一行

        switch_dict = {}
        if self.manufacturer == '华为':
            switch_dict = self.find_huawei_switch_cols(file_name, switch_params)

        g['参数组标识'] = g['参数组标识'].astype('str')
        identity = [item for item in g['参数组标识'].unique().tolist() if
                    item.strip() != '' and item.strip() != 'nan' and item.strip() is not None]
        if len(identity) > 0 and not 'nan' == identity[0]:
            base_cols.append(identity[0])
        if self.is_freq_evaluation(g):
            frequency = g['频点标识'].unique().tolist()
            if len(frequency) > 0:
                base_cols.append(frequency[0])
        base_df = huaweiutils.read_csv(file_name, None, str)
        if self.cell_identity in base_df.columns:
            base_cols.append(self.cell_identity)
        if '服务质量等级' in base_df.columns:
            base_cols.append('服务质量等级')
        # 如果没有开关参数，那么赋值为base_info
        switch_df = pd.DataFrame()
        # 查看主命令是否存在标识,主命令对应标识应该一样
        # 目前开关参数只针对华为有特殊的匹配方法
        if len(switch_dict) > 0:
            switch_cols = (list(set(switch_dict.values())))
            switch_cols.extend(base_cols)
            df = pd.read_csv(file_name, usecols=switch_cols, dtype=str)
            switch_df = self.read_switch_data(df, base_df[base_cols], switch_dict)
            switch_df.drop_duplicates(inplace=True, keep='first')
            # if self.manufacturer == '爱立信':
            # self.rename_eri_param(switch_df, switch_cols, file_name)
        # 读取非开关参数
        non_switch_df = pd.DataFrame()
        if len(check_params) > 0:
            non_switch_col = copy.deepcopy(base_cols)
            # 之前为了区分相同参数下的不同qci,参数后加上了_QCI,在这里去掉，否则无法查到原始数据
            for i in range(len(check_params)):
                if check_params[i].find('|') >= 0:
                    o_check_param = check_params[i].split('|')[0]
                    non_switch_col.append(o_check_param)
            logging.debug("读取文件:【" + file_name + "】,读取的列:【" + str(non_switch_col) + "】")
            # 读取开关参数和非开关参数均读取Ref,防止多次读取Ref列，用set过滤
            if self.manufacturer == '爱立信':
                refCols = list(filter(lambda x: x.endswith('ref'), base_df.columns.tolist()))
                non_switch_col.extend(refCols)
            non_switch_df = huaweiutils.read_csv(file_name, list(set(non_switch_col)), str, self.manufacturer)
            non_switch_df.drop_duplicates(inplace=True, keep='first')
            # if self.manufacturer == '爱立信':
            #     self.rename_eri_param(non_switch_df, check_params, file_name)
        if not non_switch_df.empty and not switch_df.empty:
            result_df = pd.merge(switch_df, non_switch_df, how='left', on=base_cols)
        elif non_switch_df.empty and switch_df.empty:
            logging.warn("开关参数和非开关参数读取结果均为空,请检查参数名称或者原始数据结果是否包含该参数,当前检查参数【" + str(params) + "】")
            # raise Exception("开关参数和非开关参数读取结果均为空,请检查参数名称或者原始数据结果是否包含该参数,当前检查参数【" + str(params) + "】")
            return pd.DataFrame()
        else:
            result_df = switch_df if not switch_df.empty else non_switch_df
        # 针对不同列名的重复情况，需要扩展参数名,相同的参数名，可能对应多个参数
        remove_col = set()
        for p in params:
            o_param = p.split('|')[0]
            result_df[p] = result_df[o_param]
            remove_col.add(o_param)
        # 扩展完成之后删除
        result_df.drop(labels=remove_col, inplace=True, axis=1)
        return result_df

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

    def is_freq_evaluation(self, df):
        return '对端频带' in df.columns.tolist()

    def judge_compliance(self, df, param, standard):
        if standard.empty:
            raise Exception('参数【' + param + '】没有找到配置信息')
        if not self.is_freq_evaluation(standard):  # 表明是cell级别
            # standard = g[g['原始参数名称'] == param]
            df = self.add_cell_judgement(df, standard, param)
        else:  # 频点级别
            # 判断是4G频点还是5G频点
            standard = standard[(standard['参数名称'] == param)]
            df = self.add_freq_judgement(df, standard, param)
        return df

    def add_freq_judgement(self, df, standard, param):
        standard_on = ['对端频带', '频段', '覆盖类型', '区域类别', '共址类型']
        df['对端频带'] = df['对端频带'].apply(str)
        deep_on = copy.deepcopy(standard_on)
        deep_on.append('推荐值')
        standard = self.expand_standard(standard, standard_on)
        df = df.merge(standard[deep_on], how='left',
                      on=standard_on)
        df.rename(columns={"推荐值": param + "#推荐值"}, inplace=True)
        df[param + "#合规"] = huaweiutils.judge(df, param)
        return df

    def fill_col_na(self, standard, cols):
        for c in cols:
            if c == '区域类别':
                standard[c].fillna(self.all_area_classes + "|" + 'nan', inplace=True)
            elif c == '覆盖类型':
                standard[c].fillna(self.all_cover_classes + "|" + 'nan', inplace=True)
            elif c == '频段':
                standard[c].fillna(self.all_band + "|" + 'nan', inplace=True)
            elif c == '共址类型':
                standard[c].fillna(self.all_co_location + "|" + 'nan', inplace=True)
            elif c == '对端频带':
                standard[c].fillna(self.end_band + "|" + 'nan', inplace=True)
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
                    if e == 'nan':
                        new_row[cols[idx]] = math.nan
                    else:
                        new_row[cols[idx]] = e
                add_rows.append(new_row)
            standard = standard.drop(index=index)
            standard = standard.append(add_rows)
        return standard

    def add_cell_judgement(self, df, standard, param):
        standard = self.expand_standard(standard, ['区域类别', '覆盖类型', '频段', '共址类型']).reset_index(drop=True)
        #  df = df.merge(standard[['区域类别', '覆盖类型', '频段', '推荐值', '共址类型']],how='left', on=['频段', '覆盖类型', '区域类别', '共址类型'])
        modified_standard = copy.deepcopy(standard)
        df = pd.merge(df, modified_standard[['区域类别', '覆盖类型', '频段', '推荐值', '共址类型']], how='left',
                      on=['频段', '覆盖类型', '区域类别', '共址类型'])
        df.rename(columns={"推荐值": param + "#推荐值"}, inplace=True)
        df[param + "#合规"] = huaweiutils.judge(df, param)
        return df

    def is_4g_freq(self, all_freqs):
        for f in all_freqs:
            for key, item in self.g4_freq_band_dict.items():
                if str(f) in item:  # 证明该频段是4G频段
                    return True
        return False

    def get_content_col(self, cols) -> []:
        """
            检出除了base_cols外的参数检查项
        """
        base_cols = self.base_info_df.columns.tolist()
        diff = set(cols) - set(base_cols)
        if "对端频带" in diff:
            diff.remove("对端频带")
        copy_diff = copy.deepcopy(diff)
        content_cols = []
        for m in diff:
            if m.find('#') < 0:
                content_cols.append(m)
                for n in copy_diff:
                    if m == n:
                        continue
                    if n.find(m) >= 0 and n.find('#') >= 0:
                        content_cols.append(n)

        return content_cols

    def sort_result(self, cols, config, base_cols):
        ordered_params = config['参数名称'].unique().tolist()
        content_cols = self.get_content_col(cols)
        order_content_cols = []
        for param in ordered_params:
            param = param.strip()
            if param in content_cols:
                order_content_cols.extend([param, param + '#推荐值', param + '#合规'])
        order_content, class_dict = self.order_content_cols(config, order_content_cols)
        # base_cols = ['地市', '网元', 'NRDU小区名称', 'NR小区标识', 'CGI', '频段', '工作频段',
        #              '双工模式', '厂家', '共址类型', '覆盖类型', '覆盖场景', '区域类别']
        return base_cols + order_content, class_dict

    def generate_cell_report(self, check_type, base_cols) -> {}:
        first_class_dict = {}
        if check_type == 'cell' or check_type == 'all':
            logging.info("开始核查小区级别参数")
            cell_df = self.evaluate_cell_params(self.cell_config_df)
            order_cols, first_class_dict = self.sort_result(cell_df.columns.tolist(), self.cell_config_df, base_cols)
            cell_df = cell_df[order_cols]
            # 将区域类型为空的之前填补成农村区域的站点还原
            na_index = cell_df[cell_df['CGI'].isin(self.na_area_cgi)].index.tolist()
            # 不在过滤高铁站
            # cell_df = cell_df[~cell_df['覆盖场景'].isin(self.cover_filter_list)]
            if len(na_index) > 0:
                select_col_index = cell_df.columns.get_loc('区域类别')
                cell_df.iloc[na_index, select_col_index] = math.nan
            self.cell_df = cell_df
            out = os.path.join(self.file_path, 'check_result', 'cell')
            if not os.path.exists(out):
                os.makedirs(out)
            cell_df.dropna(subset=['CGI'], how='any', inplace=True)
            cell_df.drop_duplicates(keep="first", inplace=True)
            # 不根据地市删除小区
            # cell_df.dropna(subset=['地市'], how='any', inplace=True)
            # cell_df = self.format_result(cell_df, self.cell_config_df, base_cols)
            cell_df.to_csv(os.path.join(out, 'param_check_cell.csv'), index=False, encoding='utf_8_sig')
        return first_class_dict

    def generate_freq_report(self, check_type, base_cols) -> {}:
        freq_class_dict = {}
        if check_type == 'freq' or check_type == 'all':
            logging.info("开始核查频点级别参数")
            base_cols.extend(["对端频带"])
            freq_df_list = self.evaluate_freq_params(self.freq_config_df)
            for df in freq_df_list:
                d = df[0]
                sorted_cols, freq_class_dict = self.sort_result(d.columns.tolist(), self.freq_config_df, base_cols)
                if '频点' in d.columns.tolist():
                    d.rename(columns={'频点': '对端频点'}, inplace=True)
                    index = sorted_cols.index('对端频带')
                    sorted_cols.insert(index + 1, '对端频点')
                d = d[sorted_cols]

                # 将区域类型为空的之前填补成农村区域的站点还原
                d[d['CGI'].isin(self.na_area_cgi)]['CGI'] = math.nan
                out = os.path.join(self.file_path, 'check_result', 'freq')
                if not os.path.exists(out):
                    os.makedirs(out)
                d.dropna(subset=['地市'], how='any', inplace=True)
                # d.drop_duplicates(keep="first", inplace=True)
                self.drop_duplicates_by_type(d, check_type)
                d.to_csv(os.path.join(out, df[1] + '_freq.csv'),
                         index=False, encoding='utf_8_sig')
        return freq_class_dict

    def drop_duplicates_by_type(self, df, check_type):
        if check_type == 'freq':
            df.drop_duplicates(subset=['CGI', '频段', '对端频带'], keep="first", inplace=True)
        else:
            df.drop_duplicates(subset=['CGI', '频段'], keep="first", inplace=True)

    def generate_report(self, check_type, base_cols):
        """
            输入的推荐值和需要核查的参数
        """
        first_class_dict = self.generate_cell_report(check_type, base_cols)
        logging.info('==============小区级别参数检查完成' + '==============')
        freq_class_dict = self.generate_freq_report(check_type, base_cols)
        logging.info('==============频点级别参数检查完成' + '==============')
        return first_class_dict, freq_class_dict

    def order_content_cols(self, config, content_cols):
        clzz = config['类别'].unique().tolist()
        clzz = [x for x in clzz if not x == 'nan']
        order_content = []
        first_class_dict = {}
        for i in range(len(clzz)):
            # 该层级下有那些参数
            c = clzz[i]
            # 一级class列表是一个字典列表，每个字典中的key是二级表头
            first_class_content = []
            class_config = copy.deepcopy(config[config['类别'] == c])
            # if self.manufacturer == '爱立信':
            #     class_config['原始参数名称'] = class_config['原始参数名称'] + "_" + class_config['主命令']
            param_names = class_config['参数名称'].unique().tolist()
            param_names = [i.strip() for i in param_names]
            used_cols = list(set(param_names) & set(content_cols))
            if len(used_cols) == 0:
                continue
            second_class_dict = {}
            for u in used_cols:
                # 一个参数只可能对应一个二级表头，下面的代码的结果取第一个
                # if self.manufacturer == '爱立信':
                # u = u.split('|')[0]
                second_class = class_config[class_config['参数名称'].str.strip() == u]['二级表头'].unique().tolist()[0]
                second_cols = second_class_dict.get(second_class, [])
                second_cols.extend([u, u + '#推荐值', u + '#合规'])
                second_class_dict[second_class] = second_cols
                order_content.extend([u, u + '#推荐值', u + '#合规'])
                first_class_content.extend([u, u + '#推荐值', u + '#合规'])
            copy_second_dict = copy.deepcopy(second_class_dict)
            first_class_dict[c] = copy_second_dict
        return order_content, first_class_dict
