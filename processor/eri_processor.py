from abc import ABC
import os
from model.data_watcher import DataWatcher
from processor.processor import Processor


class EriProcssor(Processor, ABC):


    def evaluate(self, dataWatcher: DataWatcher, file, cell_config_df, freq_config_df):
        base_cols = watcher.get_base_cols()
        raw_files_dir = os.path.join(watcher.work_dir, watcher.manufacturer, watcher.date,
                                     watcher.system, 'kget')
        # raw_files = os.listdir(raw_files_dir)
        # 对于爱立信数据，相当于只有一个网管数据
        evaluate = Evaluation(raw_files_dir, self.watcher, used_commands=[])
        copy_base_cols = copy.deepcopy(base_cols)
        cell_class_dict, freq_class_dict = evaluate.generate_report('all', copy_base_cols)
        # self.valueChanged.emit(index + 1)
        return cell_class_dict, freq_class_dict


