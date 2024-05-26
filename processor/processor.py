from abc import ABC, abstractmethod

from model.data_watcher import DataWatcher


class Processor:

    @abstractmethod
    def before_parse_4g_raw_data(self, dataWatcher: DataWatcher):
        pass

    @abstractmethod
    def before_parse_raw_data(self, dataWatcher: DataWatcher):
        pass

    @abstractmethod
    def parse_raw_data(self, dataWatcher: DataWatcher):
        pass

    @abstractmethod
    def evaluate(self, dataWatcher: DataWatcher):
        pass
