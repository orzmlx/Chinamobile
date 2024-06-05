# -*- coding:utf-8 -*-

from model.data_watcher import DataWatcher
from processor.ericsson_procssor import EricssonProcessor
from processor.huawei_processor import HuaweiProcessor
from processor.zte_processor import ZteProcessor


class ProcessUtils:

    def get_processor(watcher: DataWatcher):
        if watcher.manufacturer == '华为':
            return HuaweiProcessor()
        elif watcher.manufacturer == '中兴':
            return ZteProcessor()
        elif watcher.manufacturer == '爱立信':
            return EricssonProcessor()
