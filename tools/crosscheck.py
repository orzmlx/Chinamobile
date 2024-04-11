import tkinter as tk
from tkinter import filedialog
import os
import sys
import pandas as pd
import tkinter.messagebox
import logging

out_put_path = ""
last_week_data = pd.DataFrame()
latest_week_data = pd.DataFrame()
last_week = ""
latest_week = ""
result_drafts = {}
errMsg = ""


class LoggerBox(tkinter.Text):
    """
    日志框
    """

    def write(self, message):
        self.insert("end", message)


def select_file():
    # 单个文件选择
    selected_file_path = filedialog.askopenfilename()  # 使用askopenfilename函数选择单个文件
    select_path.set(selected_file_path)


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
def select_input_folder():
    global out_put_path
    global last_week_data
    global latest_week_data
    global last_week
    global latest_week
    global result_drafts
    # 文件夹选择
    selected_folder = filedialog.askdirectory()  # 使用askdirectory函数选择文件夹
    logging.info("输入文件夹绝对路径:" + os.path.abspath(selected_folder))
    select_path.set(selected_folder)
    statistics_files = {}
    residency_ratio_files = {}
    if os.path.isdir(selected_folder):
        dir_list = os.listdir(selected_folder)
        for f in dir_list:
            # 检查是否是终端驻留指标文件
            if is_residency_ratio_file(f):
                mobile_type = get_mobile_type(f)
                logging.info("检测到终端驻留文件:" + f + ",厂商:" + mobile_type)
                residency_ratio_files[mobile_type] = os.path.join(os.path.abspath(selected_folder), f)
            elif is_statistical_file(f):
                date_str = get_date_str(f)
                logging.info("检测到终端统计文件:" + f + ",日期:" + date_str)
                if len(date_str) > 0:
                    statistics_files[date_str] = os.path.join(os.path.abspath(selected_folder), f)
    if len(statistics_files) == 0:
        logging.error("错误:没有找到统计文件")
        select_path.set("")
        tkinter.messagebox.showinfo(title="错误", message="没有找到统计文件")
        return
    elif len(statistics_files) > 2:
        logging.error("错误:有超过两个统计文件")
        select_path.set("")
        tkinter.messagebox.showinfo(title="错误", message="有超过两个统计文件")
        return
    if len(residency_ratio_files) == 0:
        logging.error("错误:没有终端驻留指标文件")
        select_path.set("")
        tkinter.messagebox.showinfo(title="错误", message="没有终端驻留指标文件")
        return
    last_week, latest_week = is_time_valid(statistics_files.keys())
    last_week_data, latest_week_data = cal_residency_ratio(last_week, latest_week, statistics_files)
    result_drafts = pre_check_index_file(residency_ratio_files)
    if len(result_drafts) == len(residency_ratio_files) and len(result_drafts) > 0:
        logging.info("终端驻留指标解析成功!")
    else:
        logging.info("错误:没终端驻留指标解析失败")
    # cross_check(last_week_data, latest_week_data, last_week, latest_week, result_drafts)


def highlight(s):
    s = s.str.rstrip('%').astype(float) / 100
    result = []
    for v in s:
        if v > 0.01:
            result.append('background-color: #33CC00')
        elif v < -0.01:
            result.append('background-color: #CC0000')
        else:
            result.append('')
    return result


def cross_check():
    global out_put_path
    global last_week_data
    global latest_week_data
    global last_week
    global latest_week
    global result_drafts
    if len(result_drafts) == 0:
        select_path.set("")
        tkinter.messagebox.showinfo(title="错误", message="没有终端驻留指标文件")
        return
    for type, draft in result_drafts.items():
        logging.info("开始核查【" + type + "】数据")
        result = draft.merge(latest_week_data, on="终端型号", how='left')
        result = result.merge(last_week_data, on="终端型号", how='left')
        result['本周与拍照时长驻留比对比'] = result['本周时长驻留比' + latest_week] - result['时长驻留比拍照值(0901-0907)']
        result['本周与上周时长驻留比对比'] = result['本周时长驻留比' + latest_week] - result['上周时长驻留比' + last_week]
        result['本周与拍照流量驻留比对比'] = result['本周流量驻留比' + latest_week] - result['流量驻留比拍照值(0901-0907)']
        result['本周与上周流量驻留比对比'] = result['本周流量驻留比' + latest_week] - result['上周流量驻留比' + last_week]
        subset = ['本周与拍照时长驻留比对比', '本周与上周时长驻留比对比', '本周与拍照流量驻留比对比', '本周与上周流量驻留比对比']
        result = result[
            ['终端型号', '用户数', '时长驻留比拍照值(0901-0907)', '本周时长驻留比' + latest_week, '本周与拍照时长驻留比对比', '本周与上周时长驻留比对比',
             '流量驻留比拍照值(0901-0907)', '本周流量驻留比' + latest_week, '本周与拍照流量驻留比对比', '本周与上周流量驻留比对比']]
        for x in subset:
            result[x] = result[x].apply(lambda j: format(j, '.2%'))
        global out_put_path
        if len(out_put_path) == 0:
            logging.error("没有设置输出路径")
            tkinter.messagebox.showinfo(title="错误", message="没有设置输出路径")
            return
        else:
            out_path = os.path.join(os.path.abspath(out_put_path), type + "核查结果.xlsx")
            logging.info("输出文件路径:" + out_path)
            result.style.apply(highlight, subset=subset).to_excel(out_path, engine='openpyxl', index=False,
                                                                  encoding='utf_8_sig')
            logging.info(type + "数据检查完成!")
    logging.info("核查完成!")
    tkinter.messagebox.showinfo(title="消息", message="核查完成")
    select_path.set("")
    select_path1.set("")


def is_time_valid(keys):
    early_date_suffix = ""
    early_date = sys.maxsize
    later_date_suffix = ""
    later_date = -sys.maxsize
    for key in keys:
        start_date = int(key.split("-")[0])
        if start_date < early_date:
            early_date = start_date
            early_date_suffix = key.split("-")[1]
        if start_date > later_date:
            later_date = start_date
            later_date_suffix = key.split("-")[1]
    return str(early_date) + "-" + early_date_suffix, str(later_date) + "-" + later_date_suffix


def pre_check_index_file(residency_ratio_files):
    result_drafts = {}
    for key in residency_ratio_files.keys():
        f = residency_ratio_files[key]
        result_draft = pd.read_excel(f)
        result_draft = result_draft.dropna(axis=0, how='all')
        result_drafts[key] = result_draft
    return result_drafts


def read_filter_cols(input_path, date_str, prefix):
    input = pd.read_csv(input_path, encoding="gbk")
    input = input[(input['用户类型'] == 'SA单模') & (input['区域类型'] == '省级-全网')]
    input = input.groupby(['终端型号']).sum().reset_index()
    input[prefix + '时长驻留比' + date_str] = input['SA_网络业务时长(s)'] / (
            input['SA_网络业务时长(s)'] + input['4g_网络业务时长(s)'])
    input[prefix + '流量驻留比' + date_str] = input['SA流量（MB）'] / (
            input['SA流量（MB）'] + input['4G流量（MB）'])
    return input[['终端型号', prefix + '时长驻留比' + date_str, prefix + '流量驻留比' + date_str]]


def cal_residency_ratio(last_week, latest_week, statistics_files):
    last_week_input = statistics_files[last_week]
    latest_week_input = statistics_files[latest_week]
    last_week_data = read_filter_cols(last_week_input, last_week, "上周")
    latest_week_data = read_filter_cols(latest_week_input, latest_week, "本周")
    return last_week_data, latest_week_data


def is_residency_ratio_file(f):
    return f.endswith("xlsx") and f.find("~") < 0 and f.find("终端驻留指标") >= 0 and f.find("-") >= 0


def is_statistical_file(f):
    return f.endswith("csv") and f.find("~") < 0 and f.find("统计表") >= 0


def get_date_str(f):
    find_index = f.find("-")
    return f[find_index - 8:find_index + 5]


def get_mobile_type(f):
    find_index = f.find("-")
    if find_index < 0:
        return ""
    find_index0 = f.find("2")
    return f[find_index + 1:find_index0]


root = tk.Tk()
root.resizable(False, False)
root.title("驻留比核查")
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
in_btn = tk.Button(root, text="统计表路径", command=select_input_folder, fg='white', bg='black',font=("楷体",10,"roman"))
in_btn.pack(side='left', anchor='ne',pady=5)
out_entry = tk.Entry(root, textvariable=select_path1)
out_entry.pack(side='left', padx=10, pady=5, anchor='nw')
in_btn = tk.Button(root, text="输出表路径", command=select_output_folder, fg='white', bg='black',font=("楷体",10,"roman"))
in_btn.pack(side='left', anchor='nw',pady=5)
check_button = tk.Button(root, text="检查", command=cross_check
                         , relief='groove', background="green", width=10,font=("楷体",10,"roman"))
check_button.pack(side='left', anchor='ne', padx=10,pady=5)
if __name__ == "__main__":
    root.mainloop()
