import os
import pathlib
from tqdm import tqdm
import time
import pandas as pd
from huaweiparamselector import HuaweiIntermediateGen
from huaweirawdatareader import HuaweiRawDataFile
from tqdm import trange


def __init__():
    pass


def read_raw_data(path, system, date, manufacturer):
    directory = os.path.join(path, manufacturer, date, system, 'raw_data')
    items = pathlib.Path(directory).rglob('*')
    command_file_path = g5_command_path if system == '5G' else g4_command_path
    outputPath = os.path.join(path, manufacturer, date)
    # progress_bar = tqdm(total=len(list(items)), desc='解析【'+ manufacturer + "】原始数据...", unit='item')
    for item in items:
        reader = HuaweiRawDataFile(str(item), command_file_path, outputPath, system)
        reader.read_huawei_txt()
        reader.output_format_data()
        del reader
        # progress_bar.update()


def combine_cell_evaluation(filepath, system, manufacturer, date):
    directory = os.path.join(filepath, manufacturer, date, system)
    items = pathlib.Path(directory).rglob('*')
    all_result = pd.DataFrame()
    for item in items:
        path = str(item)
        if path.endswith("param_check_cell.csv"):
            res = pd.read_csv(path)
            if all_result.empty:
                all_result = res
            else:
                all_result = pd.concat([all_result, res], axis=0)
    all_cell_check_result_path = os.path.join(directory, "all_cell_check_result.csv")
    all_result.to_csv(all_cell_check_result_path, index=False, encoding='utf_8_sig')


def evaluate(filepath, system, manufacturer, date):
    raw_files = os.listdir(os.path.join(filepath, manufacturer, date, system, 'raw_data'))
    raw_file_name_list = []
    for f in raw_files:
        f_name = os.path.split(f)[1]
        raw_file_name = os.path.join(filepath, 'huawei', date, system, f_name.split('.')[0])
        report = HuaweiIntermediateGen(raw_file_name, standard_path, g4_common_table, g5_common_table,
                                       g4_site_info, g5_site_info, system)
        report.generate_report()
        del report


if __name__ == '__main__':
    path = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\"
    g5_command_path = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\20240121\\5G\\华为45G互操作固定通报参数20231225.txt"
    g4_command_path = "C:\\Users\\No.1\\Desktop\\teleccom\\华为4G异频异系统切换重选语音数据-全量.txt"

    standard_path = "C:\\Users\\No.1\\Desktop\\teleccom\\互操作参数核查结果_test.xlsx"
    g5_common_table = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\地市规则\\5G资源大表-20231227.csv"
    g4_common_table = "C:\\Users\\No.1\\Desktop\\teleccom\\LTE资源大表-0121\\LTE资源大表-0121.csv"
    g4_site_info = "C:\\Users\\No.1\\Desktop\\teleccom\\物理站CGI_4g.csv"
    g5_site_info = "C:\\Users\\No.1\\Desktop\\teleccom\\物理站CGI_5g.csv"
    raw_data_path = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\raw_data"
    # read_raw_data(path, '5G', '20240121', 'huawei')
    # evaluate(path, '5G', 'huawei', '20240121')
    combine_cell_evaluation(path, '5G', 'huawei', '20240121')

