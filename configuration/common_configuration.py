# -*- coding:utf-8 -*-

from pandas import DataFrame

# from configuration import huawei_configuration, zte_configuration, ericsson_configuration

g4_band_dict = {
    'FDD1800': {'1300', '1250', '1350', '1301', '1425', '1400', '1375', '1275', '1475', '1450', '1506', '1825', '1650',
                '1850', '1401', '1401', '1399'},
    'FDD900': {'3600', '3684', '3591', '3589', '3641', '3590', '3615', '3637'},
    'F': {'38590', '38400', '38544', '38350', '38496', '38375', '38352', '38325', '38550', '38499', '38351', '38500',
          '38525', '38402', '38494', '38521', '38475', '38396', '38450'},
    'E': {'39324', '39298', '39296', '39292', '38950', '39148', '39123', '39150', '39300', '38952', '39294', '39273',
          '39496', '39295', '38953', '39392',
          '39346', '39250', '39450', '38752', '39151', '38750', '39448', '39325', '39544', '39050'},
    'D': {'40936', '41332', '41134', '38098', '37900', '41340', '40940', '41336', '41140', '41138', '40938', '40342',
          '40540', '38100', '40140', '39940', '37902', '40340', '40937', '41334', '40738', '40738', '40684', '40711',
          '40861', '40866', '37950', '40144'},
    'A': {'36251', '36275', '36250'}}



def map4gFrqPtToBand(x, band_dict):
    if 'nan' == str(x):
        return '其他频段'
    for key, item in band_dict.items():
        if str(x) in item:  # 证明该频段是4G频段
            return key
    return '其他频段'


def is_4g_freq(all_freq, g4_freq_band_dict):
    # 由于4G存在很多没有定义过的频点，所以会判断多个频点，直到判断出频点出现在已定义的频点之中
    judge_number = 100 if len(all_freq) >= 100 else len(all_freq)
    for index, f in enumerate(all_freq):
        for key, item in g4_freq_band_dict.items():
            if not str(f) in item and index >= judge_number - 1:  # 证明该频段是4G频段
                return False
            if str(f) in item:
                return True
        judge_number = judge_number + 1
    return False
