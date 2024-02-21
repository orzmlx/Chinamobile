# coding:utf-8
import difflib
import pandas as pd
import numpy as np


def test1(string):
    # string = "基于信道质量切换开关:关&同频条件切换开关:关&同频协同切换优化开关:关"
    splits = string.split("&")
    result = []
    for s in splits:
        col = s.split(":")[0]
        result.append(col)
    print(result)


def compare_strings(str1, str2):
    d = difflib.Differ()
    return list(d.compare(list(str1), list(str2)))


def test2(string):
    splits = string.split("  ")
    splits = list(filter(lambda x: x.strip() != "", splits))

    print(splits)
    print(len(splits))


if __name__ == "__main__":
    # str1 = "LST NRCELLHOEUTRANMEAGRPINTERRHOTOEUTRANMEASGRPID=0"
    # str2 = "LST NRCELLHOEUTRANMEAGRP:INTERRHOTOEUTRANMEASGRPID=0;"
    # # test1("E-UTRAN切换开关:开&E-UTRAN重定向开关:开&语音业务盲模式开关:开&基于覆盖的E-UTRAN最强邻区重定向开关:关&VoNR基于覆盖的切换优先开关:关&重建流程中EPS Fallback变更开关:关")
    # test2('NR DU小区标识  SIB1净荷精简策略   最低接收电平(2dBm)  最低接收信号质量(dB)  SIB可选信元指示   小区为运营商保留  RA-SDT数据量门限(Byte)  RA-SDT RSRP门限(dBm)  UE最大发射功率(dBm)')
    # res =compare_strings("LST NRCELLHOEUTRANMEAGRPINTERRHOTOEUTRANMEASGRPID=0", "LST NRCELLHOEUTRANMEAGRPINTERRHOTOEUTRANMEASGRPID=1")
    #  # dff1 = [diff_str for diff_str in str1 if diff_str not in str2]
    #  # print(dff1)
    # str = 'WZ2809_3920', 'WZ2805_3916', 'WZ2803_3918', 'WZ2808_3920', 'WZ2802_3921', 'WZ2811_3916', 'WZ2801_3918', 'WZ2803_3921', 'WZ2808_3916', 'WZ2809_3929', 'WZ2807_3919', 'WZ2804_3919', 'WZ2804_3926', 'WZ2802_3923', 'WZ2801_3926', 'WZ2809_3921', 'WZ2800_3922', 'WZ2805_3925', 'WZ2804_3915', 'WZ2802_3927', 'WZ2809_3928', 'WZ2803_3916', 'WZ2803_3926', 'WZ2806_3926', 'WZ2807_3925', 'WZ2809_3918', 'WZ2804_3924', 'WZ2806_3918', 'WZ2805_3926', 'WZ2806_3927', 'WZ2800_3925', 'WZ2808_3918', 'WZ2804_3929', 'WZ2804_3925', 'WZ2809_3927', 'WZ2807_3926', 'WZ2811_3917', 'WZ2814_3919', 'WZ2810_3920', 'WZ2807_3920', 'WZ2806_3928', 'WZ2807_3916', 'WZ2803_3928', 'WZ2812_3916', 'WZ2814_3918', 'WZ2805_3922', 'WZ2804_3920', 'WZ2806_3915', 'WZ2804_3917', 'WZ2812_3920', 'WZ2800_3923', 'WZ2805_3920', 'WZ2803_3919', 'WZ2801_3919', 'WZ2801_3925', 'WZ2805_3918', 'WZ2803_3925', 'WZ2802_3926', 'WZ2806_3919', 'WZ2807_3923', 'WZ2806_3920', 'WZ2809_3917', 'WZ2811_3919', 'WZ2802_3924', 'WZ2800_3921', 'WZ2802_3918', 'WZ2802_3917', 'WZ2807_3917', 'WZ2808_3925', 'WZ2805_3927', 'WZ2803_3920', 'WZ2810_3915', 'WZ2812_3917', 'WZ2804_3928', 'WZ2811_3920', 'WZ2810_3919', 'WZ2803_3917', 'WZ2809_3915', 'WZ2804_3918', 'WZ2808_3928', 'WZ2805_3929', 'WZ2812_3919', 'WZ2806_3916', 'WZ2805_3921', 'WZ2805_3917', 'WZ2806_3921', 'WZ2802_3925', 'WZ2811_3918', 'WZ2802_3920', 'WZ2812_3918', 'WZ2807_3928', 'WZ2801_3924', 'WZ2805_3915', 'WZ2802_3919', 'WZ2810_3918', 'WZ2806_3924', 'WZ2810_3917', 'WZ2808_3919', 'WZ2808_3926', 'WZ2804_3921', 'WZ2810_3916', 'WZ2807_3918', 'WZ2808_3921', 'WZ2802_3922', 'WZ2803_3924', 'WZ2807_3915', 'WZ2804_3927', 'WZ2806_3929', 'WZ2808_3929', 'WZ2809_3919', 'WZ2800_3924', 'WZ2807_3927', 'WZ2800_3920', 'WZ2805_3923', 'WZ2807_3922', 'WZ2808_3927', 'WZ2813_3917', 'WZ2807_3924', 'WZ2805_3919', 'WZ2801_3923', 'WZ2801_3922', 'WZ2806_3917', 'WZ2806_3923', 'WZ2803_3927', 'WZ2813_3919', 'WZ2801_3921', 'WZ2806_3925', 'WZ2808_3915', 'WZ2807_3921', 'WZ2801_3920', 'WZ2806_3922', 'WZ2803_3922', 'WZ2804_3923', 'WZ2800_3919', 'WZ2804_3922', 'WZ2803_3923', 'WZ2804_3916', 'WZ2807_3929', 'WZ2808_3917', 'WZ2805_3924', 'WZ2805_3928', 'WZ2809_3916', 'WZ2813_3918'
    # print(len(str.split(",")))
    lst1 = ['网元', 'NR小区标识', '异频切换测量参数组标识', '异频测量事件时间迟滞(毫秒)',
                                                     '异频测量事件幅度迟滞(0.5dB)',
                                                     '异频A1A2时间迟滞(毫秒)', '异频A1A2幅度迟滞(0.5dB)', '基于频率优先级的异频切换A2RSRP门限(dBm)',
                                                     '基于频率优先级的异频切换A1RSRP门限(dBm)',
                                                     '基于覆盖的异频A5RSRP触发门限1(dBm)', '基于覆盖的异频A5RSRP触发门限2(dBm)',
                                                     '基于覆盖的异频A2RSRP触发门限(dBm)',
                                                     '基于覆盖的异频A1RSRP触发门限(dBm)', '基于频率优先级的异频切换A4RSRP门限(dBm)',
                                                     '基于MLB的异频A4RSRP门限(dBm)', '运营商专用优先级异频切换A4RSRP门限(dBm)',
                                                     '基于业务的异频切换A4RSRP门限(dBm)', '基于覆盖的异频RSRQ门限(0.5dB)',
                                                     '基于覆盖的异频A2RSRQ触发门限(0.5dB)', '基于覆盖的异频A1RSRQ触发门限(0.5dB)',
                                                     '基于干扰的异频切换A3RSRP偏置(0.5dB)', '基于干扰的异频切换RSRP门限(dBm)',
                                                     '基于SSBSINR的异频切换A1门限(0.5dB)', '基于SSBSINR的异频切换A2门限(0.5dB)',
                                                     '基于A3异频切换的A2RSRP触发门限(dBm)', '基于A3异频切换的A1RSRP触发门限(dBm)',
                                                     '异频切换A3偏置(0.5dB)', '基于上行覆盖的异频A5RSRP触发门限1(dBm)', '高速用户A2门限偏置(dB)',
                                                     '基于MBS兴趣指示的异频切换A5RSRP触发门限2(dBm)', '基于覆盖的异频切换A5SINR门限2(0.5dB)',
                                                     '异频盲重定向A2SINR门限(0.5dB)', '特殊终端基于频率优先级切换的A1RSRP门限(dBm)',
                                                     '特殊终端基于频率优先级切换的A2RSRP门限(dBm)', '特殊终端基于频率优先级切换的A4RSRP门限(dBm)',
                                                     '基于干扰隔离的异频A5RSRP触发门限1(dBm)', '低速用户迁出A2RSRP门限(dBm)',
                                                     '特殊终端上行基于频率优先级切换的A1RSRP门限(dBm)', '特殊终端上行基于频率优先级切换的A2RSRP门限(dBm)',
                                                     '特殊终端上行基于频率优先级切换的A4RSRP门限(dBm)', '基于干扰的异频切换RSRP门限(dBm)',
                                                     '异频A4A5时间迟滞(毫秒)', '异频A4A5幅度迟滞(0.5dB)', '异频A1A2幅度迟滞(0.5dB)',
                                                     '基于频率优先级的异频切换A2RSRP门限(dBm)', '基于频率优先级的异频切换A1RSRP门限(dBm)',
                                                     '基于覆盖的异频A5RSRP触发门限1(dBm)', '基于覆盖的异频A5RSRP触发门限2(dBm)',
                                                     '基于覆盖的异频A2RSRP触发门限(dBm)', '基于覆盖的异频A1RSRP触发门限(dBm)',
                                                     '基于频率优先级的异频切换A4RSRP门限(dBm)', '基于MLB的异频A4RSRP门限(dBm)',
                                                     '运营商专用优先级异频切换A4RSRP门限(dBm)', '基于业务的异频切换A4RSRP门限(dBm)',
                                                     '基于覆盖的异频A5RSRQ门限2(0.5dB)', '基于覆盖的异频A2RSRQ触发门限(0.5dB)',
                                                     '基于覆盖的异频A1RSRQ触发门限(0.5dB)', '基于干扰的异频切换A3RSRP偏置(0.5dB)']
    lst2 = ['网元', 'NR小区标识', '异频切换测量参数组标识', '异频测量事件时间迟滞(毫秒)', '异频测量事件幅度迟滞(0.5dB)', '异频A1A2时间迟滞(毫秒)', '异频A1A2幅度迟滞(0.5dB)', '基于频率优先级的异频切换A2RSRP门限(dBm)', '基于频率优先级的异频切换A1RSRP门限(dBm)', '基于覆盖的异频A5RSRP触发门限1(dBm)', '基于覆盖的异频A5RSRP触发门限2(dBm)', '基于覆盖的异频A2RSRP触发门限(dBm)', '基于覆盖的异频A1RSRP触发门限(dBm)', '基于频率优先级的异频切换A4RSRP门限(dBm)', '基于MLB的异频A4RSRP门限(dBm)', '运营商专用优先级异频切换A4RSRP门限(dBm)', '基于业务的异频切换A4RSRP门限(dBm)', '基于覆盖的异频RSRQ门限(0.5dB)', '基于覆盖的异频A2RSRQ触发门限(0.5dB)', '基于覆盖的异频A1RSRQ触发门限(0.5dB)', '基于干扰的异频切换A3RSRP偏置(0.5dB)', '基于干扰的异频切换RSRP门限(dBm)', '基于SSBSINR的异频切换A1门限(0.5dB)', '基于SSBSINR的异频切换A2门限(0.5dB)', '基于A3异频切换的A2RSRP触发门限(dBm)', '基于A3异频切换的A1RSRP触发门限(dBm)', '异频切换A3偏置(0.5dB)', '基于上行覆盖的异频A5RSRP触发门限1(dBm)', '高速用户A2门限偏置(dB)', '基于MBS兴趣指示的异频切换A5RSRP触发门限2(dBm)', '基于覆盖的异频切换A5SINR门限2(0.5dB)', '异频盲重定向A2SINR门限(0.5dB)', '特殊终端基于频率优先级切换的A1RSRP门限(dBm)', '特殊终端基于频率优先级切换的A2RSRP门限(dBm)', '特殊终端基于频率优先级切换的A4RSRP门限(dBm)', '基于干扰隔离的异频A5RSRP触发门限1(dBm)', '低速用户迁出A2RSRP门限(dBm)', '特殊终端上行基于频率优先级切换的A1RSRP门限(dBm)', '特殊终端上行基于频率优先级切换的A2RSRP门限(dBm)', '特殊终端上行基于频率优先级切换的A4RSRP门限(dBm)', '异频A4A5时间迟滞(毫秒)', '异频A4A5幅度迟滞(0.5dB)', '基于覆盖的异频A5RSRQ门限2(0.5dB)', '异频A4A5幅度迟滞(0.5dB)', '异频A1A2幅度迟滞(0.5dB)', '基于覆盖的异频A5RSRQ门限2(0.5dB)', '基于覆盖的异频A2RSRQ触发门限(0.5dB)', '基于覆盖的异频A1RSRQ触发门限(0.5dB)', '基于干扰的异频切换A3RSRP偏置(0.5dB)']
    # lst1 = [s.replace(" ","") for s in lst1]
    # lst2 = [s.replace(" ","") for s in lst2]
    dff = set(lst1).difference(set(lst2))
    print(dff)
    # df1 = pd.DataFrame({'A': ['A', np.nan, np.nan], 'B': ['A', 'B', 'E']})
    # df2 = pd.DataFrame({'A': ['A', np.nan, np.nan], 'B': ['A', 'B', np.nan], 'C': ['A', 'B', np.nan]})
    # df3 = pd.merge(df1,df2,how='left',on=['A','B'])
    # print()

