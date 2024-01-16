import pandas as pd


class ZteRawDataReader:
    def __init__(self, raw_data_path):
        self.raw_data_path = raw_data_path
        pass

    def parse_cgi(self, sheet_name):
        antConfig = pd.read_excel(self.raw_data_path, sheet_name=sheet_name)
        real_col = antConfig.iloc[0]
        real_data = antConfig.iloc[4:, :]
        real_data.columns = real_col
       # real_data[""]



if __name__ == "__main__":
    raw_data_path = "C:\\Users\\No.1\\Downloads\\pytorch\\pytorch\\zte\\RANCM-自定义参数模板-chaxun-20240109104733.xlsx"
    reader = ZteRawDataReader(raw_data_path)
    reader.parse_cgi("AntConfig")
