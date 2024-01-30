import pandas as pd
import openpyxl
from openpyxl.chart.series import Series

from openpyxl.styles import Font, Alignment, NamedStyle, PatternFill
from openpyxl.chart import BarChart, Reference
from openpyxl import load_workbook


class reporter:

    def __init__(self, check_result, outpath, standard_path, cities):
        self.result = check_result
        self.outpath = outpath
        self.cities = cities
        self.standard_path = standard_path
        self.point_standard_df = pd.read_excel(self.standard_path, sheet_name='4G自身推荐', true_values=["是"],
                                               false_values=["否"], dtype=str)
        self.multi_standard_df = pd.read_excel(self.standard_path, sheet_name='4G点对点推荐值', true_values=["是"],
                                               false_values=["否"], dtype=str)
        # self.writer = pd.ExcelWriter(outpath, engine='xlsxwriter')  # 创建pandas.ExcelWriter实例，赋值给writer

    def create_result_header(self, wb, sheet, start_col,row):
        pt_params = self.point_standard_df[["层级", "参数名称", "原始参数名称"]]
        # 先生成层级
        clzz = pt_params['层级'].unique().tolist()
        # start_col = 4
        class_colors = ['48D1CC', '00FA9A', 'FFA500', '1E90FF', '800080']
        for i in range(len(clzz)):
            # 该层级下有那些参数
            c = clzz[i]
            j = i
            if i >= len(class_colors):
                j = 0
            color = class_colors[j]
            param_names = pt_params[pt_params['层级'] == c]['参数名称'].unique().tolist()
            self.create_business_sector(c, sheet, start_col, param_names, row, color)
            start_col = start_col + len(param_names)
        wb.save("C:\\Users\\No.1\\Desktop\\teleccom\\互操作参数核查结果1.xlsx")

    def create_business_sector(self, c, sheet, start_col, param_names, row, color):
        # 先把参数写上去
        param_start_col = start_col
        param_row = row + 1
        for p in param_names:
            cell = sheet.cell(row=param_row, column=param_start_col, value=p)
            cell.font = Font(u'微软雅黑', size=9, bold=True, color='00000000')
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            param_start_col = param_start_col + 1
        # 在写层级上去
        end_columns = start_col + len(param_names) - 1
        sheet.merge_cells(start_row=row, start_column=start_col, end_row=row, end_column=end_columns)
        cell = sheet.cell(row=row, column=start_col, value=c)
        cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        cell.font = Font(u'微软雅黑', size=10, bold=True, color='00000000')
        cell.fill = PatternFill('solid', fgColor=color)

    def generate_excel_format(self):
        wb = openpyxl.load_workbook(filename=self.standard_path)
        sheet = wb.get_sheet_by_name('工参')
        all_params = self.point_standard_df['参数名称'].unique().tolist()
        start_row =1
        start_col = 1
        headers = ['4G小区不合规数量', '4G小区核查项总数', '4G小区参数配置核查合规率', '5G小区不合规数量', '5G小区核查项总数']
        for index, header_value in enumerate(headers):
            sheet.merge_cells(start_row=start_row, start_column=start_col, end_row=start_row, end_column=4 + len(all_params))
            cell = sheet.cell(row=start_row, column=start_col, value=header_value)
            cell.font = Font(u'微软雅黑', size=10, bold=True, color='00000000')
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            cell.fill = PatternFill('solid', fgColor='FFFF00')
            self.create_common_cell(sheet, 1, start_row + 1)
            self.create_result_header(wb, sheet, 4, start_row + 1)
            city_row = start_row + 3
            for c in cities:
                cell = sheet.cell(row=city_row, column=1, value=c)
                cell.font = Font(u'微软雅黑', size=10, bold=True, color='00000000')
                cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
                city_row = city_row + 1
            start_row = start_row + 2 + len(self.cities) + 1
        wb.save("C:\\Users\\No.1\\Desktop\\teleccom\\互操作参数核查结果1.xlsx")

    def statistic(self):
        pass



    def create_common_cell(self, sheet, start_col, row):
        # colors = ['#000080', '#B22222', '#2E8B57']
        common_cell = ['地市', '核查项总数', '合规率']
        for index, c in enumerate(common_cell):
            sheet.merge_cells(start_row=row, start_column=start_col, end_row=row + 1, end_column=start_col)
            cell = sheet.cell(row=row, column=start_col, value=c)
            cell.font = Font(u'微软雅黑', size=9, bold=True, color='00000000')
            cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            start_col = start_col + 1


if __name__ == "__main__":
    check_result = 'C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result\\check_result.csv'
    outpath = 'C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\huawei\\result'
    standard_path = "C:\\Users\\No.1\\Desktop\\teleccom\\互操作参数核查结果.xlsx"
    cities = ['湖州', '杭州', '金华', '嘉兴', '丽水', '宁波', '衢州', '绍兴', '台州', '温州', '舟山', '汇总']

    reporter = reporter(check_result, outpath, standard_path, cities)
    reporter.generate_excel_format()
