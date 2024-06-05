# -*- coding:utf-8 -*-

from abc import ABC
from model.data_watcher import DataWatcher
from model.evaluate import Evaluation, huaweiutils
from processor.processor import Processor
import os
import copy

from reader.ericsson_rawdata_reader import EricssonDataReader


class EricssonProcessor(Processor, ABC):
    def evaluate(self, watcher: DataWatcher, file, cell_config_df, freq_config_df):
        base_cols = watcher.get_base_cols()
        raw_files_dir = os.path.join(watcher.work_dir, watcher.manufacturer, watcher.date,
                                     watcher.system, 'kget')
        # raw_files = os.listdir(raw_files_dir)
        # 对于爱立信数据，相当于只有一个网管数据
        evaluate = Evaluation(raw_files_dir, watcher, used_commands=[])
        copy_base_cols = copy.deepcopy(base_cols)
        cell_class_dict, freq_class_dict = evaluate.generate_report('freq', copy_base_cols)
        # self.valueChanged.emit(index + 1)
        return cell_class_dict, freq_class_dict

    def before_parse_raw_data(self, dataWatcher: DataWatcher):
        return [dataWatcher.raw_data_dir]

    def parse_raw_data(self, item, dataWatcher: DataWatcher):
        csv_files = huaweiutils.find_file(item, '.csv')

        path = dataWatcher.raw_data_dir

        # eri_config = os.path.join(path, dataWatcher.manufacturer, '参数核查规则0429.xlsx')
        eri_config = dataWatcher.config_path
        out_path = os.path.join(dataWatcher.work_dir, dataWatcher.manufacturer, dataWatcher.date,
                                dataWatcher.system, 'kget', 'raw_result')
        reader = EricssonDataReader(str(item), out_path, eri_config, dataWatcher)
        for csv_f in csv_files:
            reader.setRawFile(str(csv_f))
            reader.output_format_data()
