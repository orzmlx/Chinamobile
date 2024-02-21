import os
import pathlib
from tqdm import tqdm
import time
import pandas as pd
from huaweiparamselector import HuaweiIntermediateGen
from huaweirawdatareader import HuaweiRawDataFile
from tqdm import trange
from reporter import reporter
from zterawdatareader import ZteRawDataReader


def read_raw_data(path, system, date, manufacturer):
    directory = os.path.join(path, manufacturer, date, system, 'raw_data')
    if not os.path.exists(directory):
        os.makedirs(directory)
    items = pathlib.Path(directory).rglob('*')
    command_file_path = g5_command_path if system == '5G' else g4_command_path
    outputPath = os.path.join(path, manufacturer, date)
    # progress_bar = tqdm(total=len(list(items)), desc='解析【'+ manufacturer + "】原始数据...", unit='item')
    for item in items:
        if manufacturer == 'huawei':
            reader = HuaweiRawDataFile(str(item), command_file_path, outputPath, system)
            reader.read_huawei_txt()
            reader.output_format_data()
        elif manufacturer == 'zte':
            zte_config = os.path.join(path, manufacturer, '中兴网管算法_修改.xlsx')
            reader = ZteRawDataReader(outputPath, zte_config, system)
        elif manufacturer == 'ericsson':
            pass
        else:
            raise Exception('未知厂商:' + manufacturer)
        del reader
        # progress_bar.update()


def combine_cell_evaluation(dir0, result_path):
    # directory = os.path.join(filepath, manufacturer, date, system)
    items = pathlib.Path(dir0).rglob('*')
    all_result = pd.DataFrame()
    for item in items:
        path = str(item)
        if path.endswith(cell_check_result_name):
            res = pd.read_csv(path)
            all_result = res if all_result.empty else pd.concat([all_result, res], axis=0)
    # all_cell_check_result_path = os.path.join(dir, check_result_name)
    all_result.to_csv(result_path, index=False, encoding='utf_8_sig')


def evaluate(filepath, system, manufacturer, date):
    raw_files = os.listdir(os.path.join(filepath, manufacturer, date, system, 'raw_data'))
    for f in raw_files:
        f_name = os.path.split(f)[1]
        raw_file_name = os.path.join(filepath, manufacturer, date, system, f_name.split('.')[0])
        report = HuaweiIntermediateGen(raw_file_name, standard_path, g4_common_table, g5_common_table,
                                       g4_site_info, g5_site_info, system)
        report.generate_report()
        del report


if __name__ == '__main__':
    check_result_name = "all_cell_check_result.csv"
    cell_check_result_name = "param_check_cell.csv"
    date = '20240121'
    path = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\"
    g5_command_path = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\20240121\\5G\\华为45G互操作固定通报参数20231225.txt"
    g4_command_path = "C:\\Users\\No.1\\Desktop\\teleccom\\华为4G异频异系统切换重选语音数据-全量.txt"
    standard_path = "C:\\Users\\No.1\\Desktop\\teleccom\\互操作参数核查结果_test.xlsx"
    g5_common_table = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\地市规则\\5G资源大表-20240131.csv"
    g4_common_table = "C:\\Users\\No.1\\Desktop\\teleccom\\LTE资源大表-0121\\LTE资源大表-0121.csv"
    g4_site_info = "C:\\Users\\No.1\\Desktop\\teleccom\\物理站CGI_4g.csv"
    g5_site_info = "C:\\Users\\No.1\\Desktop\\teleccom\\物理站CGI_5g.csv"
    raw_data_path = "C:\\Users\\No.1\\Downloads\\py torch\\pytorch\\huawei\\result\\raw_data"
    # read_raw_data(path, '5G', '20240121', 'huawei')
    read_raw_data(path, '5G', '20240121', 'zte')
    # target_directory = os.path.join(path, 'huawei', date, '5G')
    # all_cell_check_result_path = os.path.join(target_directory, check_result_name)
    # report_path = os.path.join(target_directory, 'huawei', date, '互操作参数核查结果.xlsx')
    # evaluate(path, '5G', 'huawei', '20240121')
    # combine_cell_evaluation(target_directory, all_cell_check_result_path)
    # cities = ['湖州', '杭州', '金华', '嘉兴', '丽水', '宁波', '衢州', '绍兴', '台州', '温州', '舟山', '汇总']
    # reporter = reporter(all_cell_check_result_path, path, standard_path, cities, date)
    # reporter.output_general_check_result()
