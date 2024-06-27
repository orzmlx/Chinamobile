# -*- coding:utf-8 -*-
import shutil
from abc import ABC
from pathlib import Path
from model.data_watcher import DataWatcher
from model.evaluate import Evaluation, common_utils
from processor.processor import Processor
import os
import copy
import pandas as pd
from reader.ericsson_rawdata_reader import EricssonDataReader
import csv


class EricssonProcessor(Processor, ABC):
    def evaluate(self, watcher: DataWatcher, file, cell_config_df, freq_config_df):
        base_cols = watcher.get_base_cols()
        raw_files_dir = os.path.join(watcher.work_dir, watcher.manufacturer, watcher.date,
                                     watcher.system, 'kget')
        # raw_files = os.listdir(raw_files_dir)
        # 对于爱立信数据，相当于只有一个网管数据
        evaluate = Evaluation(raw_files_dir, watcher, freq_config_df=freq_config_df,
                              cell_config_df=cell_config_df, used_commands=[])
        copy_base_cols = copy.deepcopy(base_cols)
        cell_class_dict, freq_class_dict = evaluate.generate_report('freq', copy_base_cols)
        # self.valueChanged.emit(index + 1)
        return cell_class_dict, freq_class_dict

    def attach_cgi(self, dataWatcher: DataWatcher):
        # 下面tdd和fdd的位置不能变,'reportconfigsearch_qcia1a2throffsets','eutranfreqrelation_eutranfreqtoqciprofilerelation'位置不能变

        sheet_names = ['reportconfigsearch_qcia1a2throffsets', 'eutranfreqrelation_eutranfreqtoqciprofilerelation',
                       'UeMeasControl', 'EUtranFreqRelation', 'ReportConfigSearch', 'ReportConfigA5',
                       'ReportConfigB1NR', 'ReportConfigEUtraBestCell', 'ReportConfigA5Spifho',
                       'ReportConfigEUtraIFBestCell',
                       'EUtranCellFDD', 'EUtranCellTDD', 'EUtranCellFDD_systemInformationBlock3',
                       'EUtranCellTDD_systemInformationBlock3']
        g4_common_df = dataWatcher.get_eri_4g_base_info()
        # g4_common_df.rename(columns={'小区CGI': 'CGI'}, inplace=True)
        g4_common_df = g4_common_df[['CGI', '中心载频信道号', 'cellName']]
        raw_dir = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                               dataWatcher.system, 'kget', 'raw_result')
        raw_files = common_utils.find_file(raw_dir, '.csv')
        for checked_f in sheet_names:
            for f in raw_files:
                # try:
                to_checked_f = common_utils.remove_date_number(os.path.basename(f))
                to_checked_f = to_checked_f.replace('.csv', '')
                if to_checked_f.lower() != checked_f.lower():
                    continue
                df = pd.read_csv(f, on_bad_lines='skip', low_memory=False)
                df = df.merge(g4_common_df[['CGI', '中心载频信道号', 'cellName']], on=['cellName'])
                # merge完CGI后再进行其他处理
                if checked_f.lower() == 'EUtranFreqRelation'.lower():
                    self.EUtranFrqRelation(df, raw_dir)
                elif checked_f.lower() == 'ReportConfigSearch'.lower():
                    df.drop('中心载频信道号', axis=1, inplace=True)
                    self.reportConfigSearch(df, raw_dir)
                # 合并TDD和FDD
                elif checked_f.lower() == 'EUtranCellTDD'.lower():
                    df.drop('中心载频信道号', axis=1, inplace=True)
                    self.EUtranCellTDD(df, raw_dir)
                elif checked_f.lower() == 'EUtranCellTDD_systemInformationBlock3'.lower():
                    df.drop('中心载频信道号', axis=1, inplace=True)
                    self.EUtranCellTDD_systemInformationBlock3(df, raw_dir)
                elif checked_f.lower() == 'reportconfigsearch_qcia1a2throffsets'.lower():
                    df.drop('中心载频信道号', axis=1, inplace=True)
                    self.qcia1a2throffsets(df, raw_dir)
                elif checked_f.lower() == 'eutranfreqrelation_eutranfreqtoqciprofilerelation'.lower():
                    df.drop('中心载频信道号', axis=1, inplace=True)
                    self.eutranfreqrelation_eutranfreqtoqciprofilerelation(df, raw_dir)
                elif checked_f.lower() == 'ReportConfigA5'.lower():
                    df.drop('中心载频信道号', axis=1, inplace=True)
                    self.reportConfigA5(df, raw_dir)
                else:
                    df.drop('中心载频信道号', axis=1, inplace=True)
                    outpath = os.path.join(raw_dir, checked_f + '.csv')
                    df.to_csv(outpath, index=False, encoding='utf_8_sig')
            # except Exception as e:
            #     print(e)

    def eutranfreqrelation_eutranfreqtoqciprofilerelation(self, eutranfreqtoqciprofilerelation_df, raw_dir):
        eutranfreqtoqciprofilerelation_df_qci1 = eutranfreqtoqciprofilerelation_df[
            eutranfreqtoqciprofilerelation_df['qciProfileRef'].str.contains('qci1', False)]
        eutranfreqtoqciprofilerelation_df_qci9 = eutranfreqtoqciprofilerelation_df[
            eutranfreqtoqciprofilerelation_df['qciProfileRef'].str.contains('qci9', False)]
        qci_1_path = os.path.join(raw_dir, 'eutranfreqrelation_eutranfreqtoqciprofilerelation_qci1.csv')
        qci_9_path = os.path.join(raw_dir, 'eutranfreqrelation_eutranfreqtoqciprofilerelation_qci9.csv')
        eutranfreqtoqciprofilerelation_df_qci1.to_csv(qci_1_path, index=False, encoding='utf_8_sig')
        eutranfreqtoqciprofilerelation_df_qci9.to_csv(qci_9_path, index=False, encoding='utf_8_sig')

    def qcia1a2throffsets(self, qcia1a2throffsets_df, raw_dir):
        qcia1a2throffsets_qci1 = qcia1a2throffsets_df[qcia1a2throffsets_df['qciProfileRef'].str.contains('qci1', False)]
        qcia1a2throffsets_qci9 = qcia1a2throffsets_df[qcia1a2throffsets_df['qciProfileRef'].str.contains('qci9', False)]
        qci_1_path = os.path.join(raw_dir, 'reportconfigsearch_qcia1a2throffsets_qci1.csv')
        qci_9_path = os.path.join(raw_dir, 'reportconfigsearch_qcia1a2throffsets_qci9.csv')
        qcia1a2throffsets_qci1.to_csv(qci_1_path, index=False, encoding='utf_8_sig')
        qcia1a2throffsets_qci9.to_csv(qci_9_path, index=False, encoding='utf_8_sig')

    def EUtranFrqRelation(self, dataWatcher:DataWatcher):
        raw_dir = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                               dataWatcher.system, 'kget', 'raw_result')

        raw_files = common_utils.find_file(raw_dir, '.csv')
        for f in raw_files:
            to_checked_f = common_utils.remove_date_number(os.path.basename(f))
            to_checked_f = to_checked_f.replace('.csv', '')
            if to_checked_f.lower() == 'EUtranFreqRelation':
                g4_common_df = dataWatcher.get_eri_4g_base_info()
                g4_common_df = g4_common_df[['CGI', '中心载频信道号', 'cellName']]
                df = pd.read_csv(f, on_bad_lines='skip', low_memory=False)
                df = df.merge(g4_common_df[['CGI', '中心载频信道号', 'cellName']], on=['cellName'])
                self.EUtranFrqRelation(df,raw_dir)

    def EUtranFrqRelation(self, eutranFrqRelation_df, raw_dir):
        """
        分为对端和本端,这里需要利用到4G工参表
        :return:
        """
        eutranFrqRelation_self_end_df = eutranFrqRelation_df[
            eutranFrqRelation_df['中心载频信道号'] == eutranFrqRelation_df['FreqRelation']]
        eutranFrqRelation_end_df = eutranFrqRelation_df[
            eutranFrqRelation_df['中心载频信道号'] != eutranFrqRelation_df['FreqRelation']]
        eutranFrqRelation_end_df.drop('中心载频信道号', axis=1, inplace=True)
        eutranFrqRelation_self_end_df.drop('中心载频信道号', axis=1, inplace=True)

        eutranFrqRelation_self_end_path = os.path.join(str(raw_dir), 'EUtranFrqRelation_self_end.csv')
        eutranFrqRelation_end_path = os.path.join(raw_dir, 'EUtranFreqRelation_end.csv')
        eutranFrqRelation_path = os.path.join(raw_dir, 'EUtranFreqRelation.csv')
        eutranFrqRelation_self_end_df.to_csv(eutranFrqRelation_self_end_path, index=False, encoding='utf_8_sig')
        eutranFrqRelation_end_df.to_csv(eutranFrqRelation_end_path, index=False, encoding='utf_8_sig')
        eutranFrqRelation_df.to_csv(eutranFrqRelation_path, index=False, encoding='utf_8_sig')

    def NRCell(self, dataWatcher: DataWatcher):
        """
            NRCELLCU需要与NRCELLDU进行合并,获取最小接受电平
        """
        raw_dir = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                               dataWatcher.system, 'kget', 'raw_result')
        nrcellcu_path = os.path.join(raw_dir, 'nrcellcu.csv')
        nrcelldu_path = os.path.join(raw_dir, 'nrcelldu.csv')
        cudf = pd.read_csv(nrcellcu_path)
        dudf = pd.read_csv(nrcelldu_path, usecols=['CGI', 'qRxLevMin'])
        cudf = cudf.merge(dudf, how='left', on=['CGI'])
        cudf.to_csv(nrcellcu_path, index=False)

    def before_parse_raw_data(self, dataWatcher: DataWatcher):
        # common_utils.unzip_all_files(dataWatcher.raw_data_dir, zipped_file=[], suffix='tar.gz')
        res = []
        for file_path in Path(dataWatcher.raw_data_dir).glob('**/*'):
            if not file_path.is_file():
                all_raw_datas = common_utils.find_file(file_path, '.csv')
                dest_dir = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                                        dataWatcher.system, 'kget', 'raw_result')
                if not os.path.exists(dest_dir):
                    os.makedirs(dest_dir)
                for csv in all_raw_datas:
                    shutil.copy2(csv, dest_dir)
                    dest_file = os.path.join(dest_dir, os.path.basename(csv))
                    res.append(dest_dir)
        return [dataWatcher.raw_data_dir]

    def parse_raw_data(self, item, dataWatcher: DataWatcher):
        # csv_files = common_utils.find_file(item, '.csv')
        eri_config = dataWatcher.config_path
        out_path = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                                dataWatcher.system, 'kget', 'raw_result')
        reader = EricssonDataReader(str(item), out_path, eri_config, dataWatcher)
        if dataWatcher.system == '5G':
            self.process_by_config(dataWatcher.get_rawdata_path(), reader)
            self.NRCell(dataWatcher)
            self.EUtranFrqRelation(dataWatcher)
        elif dataWatcher.system == '4G':
            self.attach_cgi(dataWatcher)
            self.process_by_config(out_path, reader)

    def process_by_config(self, item, reader):
        csv_files = common_utils.find_file(item, '.csv')
        for csv_f in csv_files:
            reader.setRawFile(str(csv_f))
            reader.output_format_data()

    def reportConfigA5(self, report_config_df, raw_dir):
        """
            offset与congfigA5进行拼接，只要本端数据
        :param report_config_df:
        :param raw_dir:
        :return:
        """
        report_config_df = report_config_df[['cellName', 'a5Threshold1Rsrp', 'a5Threshold2Rsrp']]
        report_config_qcia1a2throffsets_qci1_path = os.path.join(raw_dir,
                                                                 'eutranfreqrelation_eutranfreqtoqciprofilerelation_qci1.csv')
        report_config_qcia1a2throffsets_qci9_path = os.path.join(raw_dir,
                                                                 'eutranfreqrelation_eutranfreqtoqciprofilerelation_qci9.csv')
        report_config_qcia1a2throffsets_qci1_df = pd.read_csv(report_config_qcia1a2throffsets_qci1_path)
        # report_config_qcia1a2throffsets_qci1_df = report_config_qcia1a2throffsets_qci1_df[[
        #     'cellName', 'a5Thr1RsrpFreqQciOffset']]

        report_config_qcia1a2throffsets_qci9_df = pd.read_csv(report_config_qcia1a2throffsets_qci9_path)
        # report_config_qcia1a2throffsets_qci9_df = report_config_qcia1a2throffsets_qci9_df[[
        #     'cellName', 'a5Thr1RsrpFreqQciOffset']]

        report_config_search_qci1 = report_config_qcia1a2throffsets_qci1_df.merge(report_config_df,
                                                                                  on=['cellName'], how='left')
        report_config_search_qci1_outpath = os.path.join(raw_dir, 'ReportConfigA5_qci1.csv')
        report_config_search_qci1.to_csv(report_config_search_qci1_outpath, index=False, encoding='utf_8_sig')

        report_config_search_qci9 = report_config_qcia1a2throffsets_qci9_df.merge(report_config_df,
                                                                                  on=['cellName'], how='left')
        report_config_search_qci9_outpath = os.path.join(raw_dir, 'ReportConfigA5_qci9.csv')
        report_config_search_qci9.to_csv(report_config_search_qci9_outpath, index=False, encoding='utf_8_sig')

    def reportConfigSearch(self, report_config_search_df, raw_dir):
        report_config_qcia1a2throffsets_qci1_path = os.path.join(raw_dir,
                                                                 'reportconfigsearch_qcia1a2throffsets_qci1.csv')
        report_config_qcia1a2throffsets_qci9_path = os.path.join(raw_dir,
                                                                 'reportconfigsearch_qcia1a2throffsets_qci9.csv')
        report_config_qcia1a2throffsets_qci1_df = pd.read_csv(report_config_qcia1a2throffsets_qci1_path)
        report_config_qcia1a2throffsets_qci1_df = report_config_qcia1a2throffsets_qci1_df[[
            'cellName', 'a1a2ThrRsrpQciOffset']]

        report_config_qcia1a2throffsets_qci9_df = pd.read_csv(report_config_qcia1a2throffsets_qci9_path)
        report_config_qcia1a2throffsets_qci9_df = report_config_qcia1a2throffsets_qci9_df[[
            'cellName', 'a1a2ThrRsrpQciOffset']]

        report_config_search_qci1 = report_config_search_df.merge(report_config_qcia1a2throffsets_qci1_df,
                                                                  on=['cellName'], how='left')
        report_config_search_qci1_outpath = os.path.join(raw_dir, 'ReportConfigSearch_qci1.csv')
        report_config_search_qci1.to_csv(report_config_search_qci1_outpath, index=False, encoding='utf_8_sig')

        report_config_search_qci9 = report_config_search_df.merge(report_config_qcia1a2throffsets_qci9_df,
                                                                  on=['cellName'], how='left')
        report_config_search_qci9_outpath = os.path.join(raw_dir, 'ReportConfigSearch_qci9.csv')
        report_config_search_qci9.to_csv(report_config_search_qci9_outpath, index=False, encoding='utf_8_sig')

    def EUtranCellTDD(self, EUtranCellTDD_df, raw_dir):
        EUtranCellFDD_path = os.path.join(raw_dir, 'EUtranCellFDD.csv')
        EUtranCellFDD_df = pd.read_csv(EUtranCellFDD_path)
        EUtranCell = pd.concat([EUtranCellTDD_df, EUtranCellFDD_df], axis=0)
        EUtranCell_path = os.path.join(raw_dir, 'EUtranCell.csv')
        EUtranCell.to_csv(EUtranCell_path, index=False, encoding='utf_8_sig')

    def EUtranCellTDD_systemInformationBlock3(self, EUtranCellTDD_systemInformationBlock3_df, raw_dir):
        """
        将结果与EUtranCell合并
        :param EUtranCellTDD_systemInformationBlock3_df:
        :param raw_dir:
        :return:
        """
        EUtranCellFDD_systemInformationBlock3_path = os.path.join(raw_dir, 'EUtranCellFDD_systemInformationBlock3.csv')
        EUtranCell_path = os.path.join(raw_dir, 'EUtranCell.csv')
        EUtranCell_df = pd.read_csv(EUtranCell_path)
        EUtranCellFDD_systemInformationBlock3_df = pd.read_csv(EUtranCellFDD_systemInformationBlock3_path)
        EUtranCell_systemInformationBlock3 = pd.concat(
            [EUtranCellTDD_systemInformationBlock3_df, EUtranCellFDD_systemInformationBlock3_df], axis=0)
        EUtranCell_systemInformationBlock3 = EUtranCell_systemInformationBlock3[
            ['cellName', 'qHyst', 'sNonIntraSearchP']]
        EUtranCell_df = EUtranCell_df.merge(EUtranCell_systemInformationBlock3, how='left', on=['cellName'])
        EUtranCell_path = os.path.join(raw_dir, 'EUtranCell.csv')
        EUtranCell_df.to_csv(EUtranCell_path, index=False,encoding='utf_8_sig')
