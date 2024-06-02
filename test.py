# coding:utf-8
import difflib
import os
import sys
import time

import pandas as pd
from tqdm import tqdm


def read_csv():
    INPUT_FILENAME = "C:\\Users\\No.1\\Desktop\\teleccom\\物理站CGI_4g.csv"
    LINES_TO_READ_FOR_ESTIMATION = 20
    CHUNK_SIZE_PER_ITERATION = 10 ** 5
    temp = pd.read_csv(INPUT_FILENAME,
                       nrows=LINES_TO_READ_FOR_ESTIMATION)
    N = len(temp.to_csv(index=False))
    df = [temp[:0]]
    t = int(os.path.getsize(INPUT_FILENAME) / N * LINES_TO_READ_FOR_ESTIMATION / CHUNK_SIZE_PER_ITERATION) + 1
    with tqdm(total=t, file=sys.stdout) as pbar:
        for i, chunk in enumerate(pd.read_csv(INPUT_FILENAME, chunksize=CHUNK_SIZE_PER_ITERATION, low_memory=False)):
            df.append(chunk)
            pbar.set_description('Importing: %d' % (1 + i))
            pbar.update(1)
    data = temp[:0].append(df)
    del df


def read():
    return pd.read_csv("C:\\Users\\No.1\\Desktop\\teleccom\\物理站CGI_4g.csv")


def progress(thread_name):
    letters = '|\\-|/-'
    i = 0
    is_alive = True
    start = time.time()
    while is_alive:
        sys.stdout.write('Loading file.' + letters[i % 5] + '\b' * 20)
        sys.stdout.flush()
        time.sleep(0.5)
        i += 1
        is_alive = thread_name.is_alive()
    end = time.time()
    elapsed_time = '%.2f' % (end - start)
    sys.stdout.write('Loading finished. Elapsed time: ' + str(elapsed_time) + " Seconds")


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
    splits = [i.replace(" ", '') for i in splits]
    print(splits)
    print(len(splits))


def filter_device():
    try:
        df = pd.read_csv(
            'C:\\Users\\No.1\\Documents\\WeChat Files\\wxid_5zkc7x50zh3822\\FileStorage\\File\\2024-04\\5G驻留比终端统计表(天)(1).csv')
    except:
        df = pd.read_csv(
            'C:\\Users\\No.1\\Documents\\WeChat Files\\wxid_5zkc7x50zh3822\\FileStorage\\File\\2024-04\\5G驻留比终端统计表(天)(1).csv',
            encoding='gbk')

    filter_df = df[(df['用户类型'] == 'SA单模') & (df['区域类型'] == '省级-全网')]
    types = ['小米', '红米', '华为', '荣耀', '三星', 'HI NOVA', 'VIVO']
    res = pd.DataFrame()
    for t in types:
        type_df = filter_df[filter_df['终端型号'].str.contains(t)]
        if res.empty:
            res = type_df
        else:
            res = pd.concat([res, type_df], axis=0)

    res.to_csv("C:\\Users\\No.1\\Desktop\\teleccom\\5G终端过滤.csv", index=False, encoding='utf_8_sig')


if __name__ == "__main__":
    # str1 = "LST NRCELLHOEUTRANMEAGRPINTERRHOTOEUTRANMEASGRPID=0"
    # str2 = "LST NRCELLHOEUTRANMEAGRP:INTERRHOTOEUTRANMEASGRPID=0;"
    # # test1("E-UTRAN切换开关:开&E-UTRAN重定向开关:开&语音业务盲模式开关:开&基于覆盖的E-UTRAN最强邻区重定向开关:关&VoNR基于覆盖的切换优先开关:关&重建流程中EPS Fallback变更开关:关")
    test2(
        'NR DU小区标识  NSA TDM功控触发SINR幅度迟滞(0.1dB)  NSA TDM功控触发SINR门限(0.1dB)  SRS TA测量开关                                 NSA 上行回落到LTE SINR迟滞(0.1dB)  NSA 上行回落到LTE SINR门限(0.1dB)  Hyper Cell内TRP间切换的RSRP差值(0.5dB)  NSA上行路径选择SINR高门限(0.1dB)  NSA上行路径选择SINR低门限(0.1dB)  NSA分流用户上行路径选择SINR门限(0.1dB)  NSA上行路径选择SINR幅度迟滞(0.1dB)  NSA上行路径选择SINR时间迟滞(100毫秒)  NSA上行路径变更至LTE速率比  NSA上行路径变更至NR速率比  NSA上行小包返回NR后惩罚时长(秒)  NSA 上行回落到LTE SINR时间迟滞(100毫秒)  NSA上行回落到LTE SINR优化开关                                    UL ROHC的SINR门限(0.1dB)  UL ROHC的SINR幅度迟滞(0.1dB)  DMM TRP选择RSRP差值门限(dB)  上行SRS测量值使用开关  SRS干扰门限(dB)  NR迁移到E-UTRAN的上行SINR低门限(0.1dB)  E-UTRAN迁移到NR的SINR高门限(0.1dB)  SRS TA异常保护门限(Ts)  SRS均衡前信干噪比低门限(0.2dB)  SRS测量优化策略         SRS RSRP异常门限(dB)  DMRS TA的SINR判决门限(dB)  SRS和DMRS TA差值门限(Ts)  SRS测量扩展开关                 SRS测量优化开关                                                               SRS干扰下TA测量优化开关  上行语音ROHC退出SINR门限(0.1dB)')
    # res =compare_strings("LST NRCELLHOEUTRANMEAGRPINTERRHOTOEUTRANMEASGRPID=0", "LST NRCELLHOEUTRANMEAGRPINTERRHOTOEUTRANMEASGRPID=1")
    #  # dff1 = [diff_str for diff_str in str1 if diff_str not in str2]
    #  # print(dff1)
    # str = 'WZ2809_3920', 'WZ2805_3916', 'WZ2803_3918', 'WZ2808_3920', 'WZ2802_3921', 'WZ2811_3916', 'WZ2801_3918', 'WZ2803_3921', 'WZ2808_3916', 'WZ2809_3929', 'WZ2807_3919', 'WZ2804_3919', 'WZ2804_3926', 'WZ2802_3923', 'WZ2801_3926', 'WZ2809_3921', 'WZ2800_3922', 'WZ2805_3925', 'WZ2804_3915', 'WZ2802_3927', 'WZ2809_3928', 'WZ2803_3916', 'WZ2803_3926', 'WZ2806_3926', 'WZ2807_3925', 'WZ2809_3918', 'WZ2804_3924', 'WZ2806_3918', 'WZ2805_3926', 'WZ2806_3927', 'WZ2800_3925', 'WZ2808_3918', 'WZ2804_3929', 'WZ2804_3925', 'WZ2809_3927', 'WZ2807_3926', 'WZ2811_3917', 'WZ2814_3919', 'WZ2810_3920', 'WZ2807_3920', 'WZ2806_3928', 'WZ2807_3916', 'WZ2803_3928', 'WZ2812_3916', 'WZ2814_3918', 'WZ2805_3922', 'WZ2804_3920', 'WZ2806_3915', 'WZ2804_3917', 'WZ2812_3920', 'WZ2800_3923', 'WZ2805_3920', 'WZ2803_3919', 'WZ2801_3919', 'WZ2801_3925', 'WZ2805_3918', 'WZ2803_3925', 'WZ2802_3926', 'WZ2806_3919', 'WZ2807_3923', 'WZ2806_3920', 'WZ2809_3917', 'WZ2811_3919', 'WZ2802_3924', 'WZ2800_3921', 'WZ2802_3918', 'WZ2802_3917', 'WZ2807_3917', 'WZ2808_3925', 'WZ2805_3927', 'WZ2803_3920', 'WZ2810_3915', 'WZ2812_3917', 'WZ2804_3928', 'WZ2811_3920', 'WZ2810_3919', 'WZ2803_3917', 'WZ2809_3915', 'WZ2804_3918', 'WZ2808_3928', 'WZ2805_3929', 'WZ2812_3919', 'WZ2806_3916', 'WZ2805_3921', 'WZ2805_3917', 'WZ2806_3921', 'WZ2802_3925', 'WZ2811_3918', 'WZ2802_3920', 'WZ2812_3918', 'WZ2807_3928', 'WZ2801_3924', 'WZ2805_3915', 'WZ2802_3919', 'WZ2810_3918', 'WZ2806_3924', 'WZ2810_3917', 'WZ2808_3919', 'WZ2808_3926', 'WZ2804_3921', 'WZ2810_3916', 'WZ2807_3918', 'WZ2808_3921', 'WZ2802_3922', 'WZ2803_3924', 'WZ2807_3915', 'WZ2804_3927', 'WZ2806_3929', 'WZ2808_3929', 'WZ2809_3919', 'WZ2800_3924', 'WZ2807_3927', 'WZ2800_3920', 'WZ2805_3923', 'WZ2807_3922', 'WZ2808_3927', 'WZ2813_3917', 'WZ2807_3924', 'WZ2805_3919', 'WZ2801_3923', 'WZ2801_3922', 'WZ2806_3917', 'WZ2806_3923', 'WZ2803_3927', 'WZ2813_3919', 'WZ2801_3921', 'WZ2806_3925', 'WZ2808_3915', 'WZ2807_3921', 'WZ2801_3920', 'WZ2806_3922', 'WZ2803_3922', 'WZ2804_3923', 'WZ2800_3919', 'WZ2804_3922', 'WZ2803_3923', 'WZ2804_3916', 'WZ2807_3929', 'WZ2808_3917', 'WZ2805_3924', 'WZ2805_3928', 'WZ2809_3916', 'WZ2813_3918'
    # print(len(str.split(",")))

    # lst2 = [s.replace(" ","") for s in lst2]
    # dff = set(lst1).difference(set(lst2))
    # print(dff)
    # df1 = pd.DataFrame({'A': ['A', np.nan, np.nan], 'B': ['A', 'B', 'E']})
    # df2 = pd.DataFrame({'A': ['A', np.nan, np.nan], 'B': ['A', 'B', np.nan], 'C': ['A', 'B', np.nan]})
    # df3 = pd.merge(df1,df2,how='left',on=['A','B'])
    # print()

    # read_thread = Thread(target=read)
    # progress_thread = Thread(target=progress, args=(read_thread,))
    # read_thread.start()
    # progress_thread.start()
    # read_thread.join()
    # progress_thread.join()
    # filter()
