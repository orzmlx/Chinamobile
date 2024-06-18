# -*- coding:utf-8 -*-
import shutil
from abc import ABC
from pathlib import Path

from configuration import zte_configuration
from model.data_watcher import DataWatcher
from model.evaluate import Evaluation, common_utils
from processor.processor import Processor
import os
import copy
import pandas as pd
from reader.ericsson_rawdata_reader import EricssonDataReader


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

    def NRCell(self, dataWatcher: DataWatcher):
        """
            NRCELLCU需要与NRCELLDU进行合并,获取最小接受电平
        """
        nrcellcu_path = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                                     dataWatcher.system, 'kget', 'raw_result', 'nrcellcu.csv')
        nrcelldu_path = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                                     dataWatcher.system, 'kget', 'raw_result', 'nrcelldu.csv')
        cudf = pd.read_csv(nrcellcu_path)
        dudf = pd.read_csv(nrcelldu_path, usecols=['cellName', 'qRxLevMin'])
        cudf = cudf.merge(dudf, how='left', on=['cellName'])
        cudf.to_csv(nrcellcu_path, index=False)

    def before_parse_raw_data(self, dataWatcher: DataWatcher):

        common_utils.unzip_all_files(dataWatcher.raw_data_dir, zipped_file=[], suffix='tar.gz')
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
        csv_files = common_utils.find_file(item, '.csv')
        eri_config = dataWatcher.config_path
        out_path = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                                dataWatcher.system, 'kget', 'raw_result')
        if dataWatcher.system == '5G':
            reader = EricssonDataReader(str(item), out_path, eri_config, dataWatcher)
            for csv_f in csv_files:
                reader.setRawFile(str(csv_f))
                reader.output_format_data()
            self.NRCell(dataWatcher)
