import logging
import math
import os
import threading
import time
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox

import pandas as pd
from python_calamine.pandas import pandas_monkeypatch

out_put_path = ""
last_week_data = pd.DataFrame()
latest_week_data = pd.DataFrame()
last_week = ""
latest_week = ""
result_drafts = {}
errMsg = ""
now = datetime.now()
date = now.strftime('%Y%m%d')


class LoggerBox(tk.Text):
    """
    日志框
    """

    def write(self, message):
        self.insert("end", message)


def init_dict(new_dict):
    for c in common_cols:
        new_dict[c] = []
    g4_dict['CGI'] = []
    g5_dict['CGI'] = []
    g2_dict['CGI'] = []
    g4_dict['制式'] = []
    g5_dict['制式'] = []
    g2_dict['制式'] = []
    g4_dict['频段'] = []
    g5_dict['频段'] = []
    g2_dict['频段'] = []


def update_dict(row, new_dict):
    for c in common_cols:
        new_dict[c].append(row[c])


def split_cgi(row, cgi, new_dict, system, band):
    if pd.isna(cgi):
        return
    else:
        cgis = str(cgi).split(";")
        for c in cgis:
            if len(c.strip()) == 0:
                continue
            update_dict(row, new_dict)
            new_dict['CGI'].append(c.strip())
            new_dict['制式'].append(system)
            new_dict['频段'].append(band)


# def split_cgi(row, cgi, new_dict, system):
#     if pd.isna(cgi):
#         return
#     else:
#         cgis = str(cgi).split(";")
#         for c in cgis:
#             if len(c.strip()) == 0:
#                 continue
#             update_dict(row, new_dict)
#             new_dict['CGI'].append(c.strip())
#             new_dict['制式'].append(system)


# def depart_cgi(row, g4_dict, g5_dict):
#     for col in g5_cols:
#         split_cgi(row, row[col], g4_dict, g5_dict)


def handle_by_system(site_info, g4_dict, g5_dict,g2_dict):
    row_number = site_info.shape[0]
    prg_int = 0
    for index, row in site_info.iterrows():
        progress = (index / row_number) * 100
        prg = math.ceil(progress)
        if prg > prg_int:
            prg_int = prg
            logging.info(str(prg) + "%")
        for col in g4_cols:
            band = col.replace('CGI', "").strip()
            split_cgi(row, row[col], g4_dict, '4G', band)
        for col in g5_cols:
            band = col.replace('CGI', "").replace("NR", "").strip()
            split_cgi(row, row[col], g5_dict, '5G', band)
        for col in g2_cols:
            band = col.replace('CGI', "").strip()
            split_cgi(row, row[col], g2_dict, '2G', band)


def select_files():
    # 多个文件选择
    selected_files_path = filedialog.askopenfilenames()  # askopenfilenames函数选择多个文件
    select_path.set('\n'.join(selected_files_path))  # 多个文件的路径用换行符隔开


def select_output_folder():
    global out_put_path
    out_put_path = filedialog.askdirectory()
    logging.info("输出文件夹绝对路径:" + os.path.abspath(out_put_path))
    select_path1.set(out_put_path)


# 检查文件夹中是否含有需要的文件，没有则报错
def select_file():
    # 文件夹选择
    global site_info
    input_path = filedialog.askopenfilename()  # 使用askdirectory函数选择文件夹
    logging.info("输入文件夹绝对路径:" + os.path.abspath(input_path))
    select_path.set(input_path)


def depart_cgi():
    t = threading.Thread(target=parse)
    t.setDaemon(True)
    t.start()
    check_button.config(state=tk.DISABLED, text="检查")
    in_btn.config(state=tk.DISABLED, text="统计表路径")
    out_btn.config(state=tk.DISABLED, text="输出表路径")


def parse():
    try:
        input_path = select_path.get()
        out_put_path = select_path1.get()
        use_cols = ['地市', '区县', '区域', '物理站编号', '物理站名', '基站类型', '室内外', '覆盖场景', '经度', '纬度', '全部制式', '4G频段',
                    '4G逻辑站(包含NB）',
                    '5G频段', '5G逻辑站', 'NR 2.6G CGI', 'NR 700M CGI', 'NR 4.9G CGI', 'TDD-F CGI', 'TDD-A CGI',
                    'TDD-D CGI', 'TDD-E CGI', 'FDD-1800 CGI', 'FDD-900 CGI', 'GSM-DCS1800', 'GSM-900']
        logging.info("导入文件中,请稍后...")
        start_time = time.time()
        if input_path.find("pkl") >= 0:
            site_info = pd.read_pickle(input_path)
        elif select_path.get().find("xlsx") >= 0:
            pandas_monkeypatch()
            site_info = pd.read_excel(input_path, usecols=use_cols, sheet_name='物理站信息表', engine='calamine')
        elif input_path.find("csv") >= 0:
            try:
                site_info = pd.read_csv(input_path)
            except:
                site_info = pd.read_csv(input_path, encoding='gbk')
        end_time = time.time()
        execution_time = end_time - start_time
        logging.info("文件导入完成,开始处理数据")
        logging.info(f"代码执行时间：{execution_time} 秒")
        init_dict(g2_dict)
        init_dict(g4_dict)
        init_dict(g5_dict)
        handle_by_system(site_info, g4_dict, g5_dict,g2_dict)
        print(len(g4_dict))
        df_2g = pd.DataFrame(g2_dict)
        df_4g = pd.DataFrame(g4_dict)
        df_5g = pd.DataFrame(g5_dict)
        df_4g = df_4g[
            ['地市', '区县', '区域', '物理站编号', '物理站名', '基站类型', '室内外', '覆盖场景', '经度', '纬度', '全部制式', '4G频段',
             '4G逻辑站(包含NB）',
             '5G频段', '5G逻辑站', 'CGI', '制式', '频段']]
        df_5g = df_5g[
            ['地市', '区县', '区域', '物理站编号', '物理站名', '基站类型', '室内外', '覆盖场景', '经度', '纬度', '全部制式', '4G频段',
             '4G逻辑站(包含NB）',
             '5G频段', '5G逻辑站', 'CGI', '制式', '频段']]
        df_2g = df_2g[
            ['地市', '区县', '区域', '物理站编号', '物理站名', '基站类型', '室内外', '覆盖场景', '经度', '纬度', '全部制式', '4G频段',
             '4G逻辑站(包含NB）',
             '5G频段', '5G逻辑站', 'CGI', '制式', '频段']]
        df_2g.dropna(axis=0, how="any", subset=['CGI'], inplace=True)
        df_4g.dropna(axis=0, how="any", subset=['CGI'], inplace=True)
        df_5g.dropna(axis=0, how="any", subset=['CGI'], inplace=True)
        df_2g.to_csv(os.path.join(os.path.abspath(out_put_path), "物理站CGI_2g_" + date + ".csv"), encoding='utf-8-sig',
                     index=False)
        df_4g.to_csv(os.path.join(os.path.abspath(out_put_path), "物理站CGI_4g_" + date + ".csv"), encoding='utf-8-sig',
                     index=False)
        df_5g.to_csv(os.path.join(os.path.abspath(out_put_path), "物理站CGI_5g_" + date + ".csv"), encoding='utf-8-sig',
                     index=False)
        logging.info("数据处理完成!")

    except Exception as e:
        messagebox.showwarning("警告", e)
        raise Exception(e)
    finally:
        check_button.config(state=tk.ACTIVE, text="检查")
        in_btn.config(state=tk.ACTIVE, text="统计表路径")
        out_btn.config(state=tk.ACTIVE, text="输出表路径")


if __name__ == "__main__":
    site_info = pd.DataFrame()
    g4_dict = {}
    g5_dict = {}
    g2_dict = {}
    g2_cols = ['GSM-DCS1800', 'GSM-900']
    g5_cols = ['NR 2.6G CGI', 'NR 700M CGI', 'NR 4.9G CGI']
    g4_cols = ['TDD-F CGI', 'TDD-A CGI', 'TDD-D CGI', 'TDD-E CGI', 'FDD-1800 CGI', 'FDD-900 CGI']
    common_cols = ['地市', '区县', '区域', '物理站编号', '物理站名', '基站类型', '室内外', '覆盖场景', '经度', '纬度', '全部制式', '4G频段',
                   '4G逻辑站(包含NB）', '5G频段', '5G逻辑站']
    root = tk.Tk()
    root.resizable(False, False)
    root.title("提取CGI")
    root.geometry("600x250")

    frame = tk.Frame(root)
    sbar_v = tk.Scrollbar(frame, orient='vertical')
    sbar_h = tk.Scrollbar(frame, orient='horizontal')
    streamHandlerBox = LoggerBox(frame, height=15, wrap='none', xscrollcommand=sbar_h.set, yscrollcommand=sbar_v.set
                                 , background="black", foreground="white")
    logger = logging.getLogger()
    logger.setLevel(level=logging.INFO)

    sbar_v.config(command=streamHandlerBox.yview)
    sbar_h.config(command=streamHandlerBox.xview)
    frame.pack()
    sbar_h.pack(side='bottom', fill='x')
    sbar_v.pack(side='right', fill='y')
    streamHandlerBox.pack()
    handler = logging.StreamHandler(streamHandlerBox)
    logger.addHandler(handler)

    select_path = tk.StringVar()
    select_path1 = tk.StringVar()
    in_entry = tk.Entry(root, textvariable=select_path)
    in_entry.pack(side='left', padx=10, pady=5, anchor='nw')
    in_btn = tk.Button(root, text="统计表路径", command=select_file, fg='white', bg='black', font=("楷体", 10, "roman"))
    in_btn.pack(side='left', anchor='ne', pady=5)
    out_entry = tk.Entry(root, textvariable=select_path1)
    out_entry.pack(side='left', padx=10, pady=5, anchor='nw')
    out_btn = tk.Button(root, text="输出表路径", command=select_output_folder, fg='white', bg='black',
                        font=("楷体", 10, "roman"))
    out_btn.pack(side='left', anchor='nw', pady=5)
    check_button = tk.Button(root, text="检查", command=depart_cgi, fg='white'
                             , relief='groove', bg="black", width=10, font=("楷体", 10, "roman"))
    check_button.pack(side='left', anchor='ne', padx=10, pady=5)
    root.mainloop()
