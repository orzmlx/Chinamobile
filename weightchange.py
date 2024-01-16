import pandas as pd
import math
import gutils
from tqdm import tqdm
from shapely.geometry import Point

tqdm.pandas()


class WeightChange:
    # csv文件解析异常,直接改后缀无法解决，将xlsx另存为包含,的csv文件
    def __init__(self, common_file_path, cloud_file_path, output_path,distance):
        self.output_path = output_path
        self.match_result = {}
        self.cover_distance = distance
        self.common_file_path = common_file_path
        self.cloud_file_path = cloud_file_path
        self.common_file_df = pd.read_csv(self.common_file_path
                                          ,
                                          usecols=["CGI", "地市", "网管中网元名称", "覆盖类型", "覆盖场景", "经度", "纬度", "AAU挂高", "总俯仰角",
                                                   '是否大于20G', 'CELL区域类别', '方位角']
                                          , encoding='gbk')
        self.cloud_file_df = pd.read_csv(self.cloud_file_path,
                                         usecols=["名称", "地市名称", "经度", "纬度", "MR总数_移动",
                                                  '区域类型'
                                                  # "SSB-RSRP(dBm)_移动",
                                                  # "SSB-DLSINR(dB)_移动",,
                                                  # "下行弱覆盖MR比例(SSB)(%)_移动",
                                                  # "下行质差MR比例(SSB)(%)_移动", "重叠覆盖MR比例(SSB)(%)_移动", "RSRP低于-120的MR数_移动",
                                                  # "RSRP低于-115的MR数_移动",
                                                  # "RSRP低于-110的MR数_移动", "RSRP低于-109的MR数_移动",
                                                  # "RSRP低于-108的MR数_移动", "RSRP低于-107的MR数_移动", "RSRP低于-106的MR数_移动",
                                                  # "RSRP低于-105的MR数_移动", "RSRP低于-104的MR数_移动", "RSRP低于-103的MR数_移动",
                                                  # "RSRP低于-102的MR数_移动", "RSRP低于-101的MR数_移动", "RSRP低于-100的MR数_移动",
                                                  # "RSRP低于-99的MR数_移动", "RSRP低于-98的MR数_移动", "RSRP低于-97的MR数_移动",
                                                  # "RSRP低于-96的MR数_移动", "RSRP低于-95的MR数_移动", "RSRP低于-90的MR数_移动"
                                                  ],
                                         encoding='gbk')

        self.cloud_file_df.rename(columns={"地市名称": "地市"}, inplace=True)
        self.common_file_df.rename(columns={"CELL区域类别": "区域类型"}, inplace=True)
        self.cloud_file_df['地市'] = self.cloud_file_df['地市'].apply(lambda x: x.replace("市(移动定制)", ""))
        self.common_file_df['区域类型'] = self.common_file_df['区域类型'].map({"农村": "四类区域", "三类": "三类区域"})
        self.cloud_file_df['MR总数_移动'].fillna(value=0, inplace=True)

    @staticmethod
    def match_distance_by_geo(lat_node, lon_node, lat, lon, distance):
        """
            根据小数点位数来快速过滤站点
        """
        lat_node_decimal_part = abs(lat_node - lat)
        lon_node_decimal_part = abs(lon_node - lon)
        return False if lat_node_decimal_part > 0.1 \
                        or lon_node_decimal_part > 0.1 else True

    # 如果dataFrame中遍历的数据不多，zip()是最高效的遍历方法，速度约是iterrows的300倍
    @staticmethod
    def get_proper_point_by_node(x, cloud_file_df, cover_distance):
        # 获取每个云勘点的经纬度,与小区经纬度做计算
        lat_node, lon_node = x['纬度'], x['经度']
        direction = x["方位角"]
        city = x['地市']
        height = x['AAU挂高']
        match_node_list = []
        start_angle, end_angle = gutils.get_rad_by_direction(direction)
        p_node = Point(lon_node, lat_node)
        # index = 0
        for lat, lon, c, weight in zip(cloud_file_df['纬度'], cloud_file_df['经度'], cloud_file_df['地市'],
                                       cloud_file_df['MR总数_移动']):
            if c != city:
                continue
            if WeightChange.match_distance_by_geo(lat_node, lon_node, lat, lon, cover_distance):
                real_distance = gutils.haversine((lat_node, lon_node), (lat, lon))
                if real_distance < cover_distance:
                    angle = gutils.get_angle(lat_node, lon_node, lat, lon)
                    if angle < 0:
                        angle = 360 + angle
                    if abs(end_angle - start_angle) > 90:
                        match = angle <= start_angle or angle >= end_angle
                    else:
                        match = start_angle <= angle <= end_angle
                    # 如果在扇形区域内,那么判断一下是在法线的逆时针还是顺时针方向
                    if match:
                        cloud_point = Point(lon, lat)
                        center_angle, is_clock_wise = WeightChange.get_normal_direction(angle, start_angle, end_angle,
                                                                                        45)
                        absolute_angle = gutils.get_angle_absolute_value(center_angle, angle)
                        # 云勘点到法线的距离
                        linear_distance = gutils.point_2_vertical_line_distance(cloud_point, p_node, absolute_angle)
                        # if linear_distance < 0.1:
                        #     print()
                        match_node_list.append((weight, is_clock_wise, linear_distance))

            # index = index + 1
        clock_wise_angle, anti_clock_wise_angle, new_mid_point_distance = weightchange.calculate_proper_direction_and_tilt(
                                                         match_node_list, cover_distance)

        # 反推需要调整的方位角
        new_direction = weightchange.get_new_direction(start_angle, end_angle, anti_clock_wise_angle)
        # 反推下倾角
        new_tilt = weightchange.get_new_down_tilt(height, new_mid_point_distance)
        return new_direction,new_tilt

    @staticmethod
    def get_new_down_tilt(height, down_point_distance):
        angle_radian = down_point_distance * 1000 / height
        angle_degree = math.degrees(round(math.atan(angle_radian),2))
        return 90 - angle_degree

    @staticmethod
    def get_new_direction(start_angle, end_angle, anti_clock_wise_angle):
        center_angle, clockwise = weightchange.get_normal_direction(None, start_angle, end_angle, anti_clock_wise_angle)
        return center_angle

    @staticmethod
    def get_normal_direction(angle, start_angle, end_angle, buffer):
        """"
            判断这个点是否在法线的顺时针方向,并且给出法线角度和法线坐标
        """
        clockwise = None
        if abs(end_angle - start_angle) > 90:
            # 判断点是否在法线的顺时针方向
            if end_angle + buffer < 360:
                center_angle = end_angle + buffer
                if angle is not None:
                    clockwise = False if end_angle <= angle <= center_angle else True
            else:
                center_angle = buffer - (360 - end_angle)
                if angle is not None:
                    clockwise = True if start_angle >= angle >= center_angle else False
        else:
            center_angle = start_angle + buffer
            if angle is not None:
                clockwise = True if center_angle <= angle <= end_angle else False

        return round(center_angle, 1), clockwise

    @staticmethod
    def calculate_proper_direction_and_tilt(points, cover_distance):
        clock_wise_weight = 0
        anti_clock_wise_weight = 0
        up_mid_distance_weight = 0
        down_mid_distance_weight = 0
        for p in points:
            weight = p[0]
            is_clock_wise = p[1]
            v_distance = p[2]
            if v_distance > cover_distance / 2:
                up_mid_distance_weight = up_mid_distance_weight + weight
            else:
                down_mid_distance_weight = down_mid_distance_weight + weight
            if is_clock_wise:
                clock_wise_weight = clock_wise_weight + weight
            else:
                anti_clock_wise_weight = anti_clock_wise_weight + weight
        total_clock_wise_weight = clock_wise_weight + anti_clock_wise_weight
        clock_wise_rate = clock_wise_weight / total_clock_wise_weight
        anti_clock_wise_rate = anti_clock_wise_weight / total_clock_wise_weight
        clock_wise_angle = clock_wise_rate * 90
        anti_clock_wise_angle = anti_clock_wise_rate * 90
        total_distance_weight = down_mid_distance_weight + up_mid_distance_weight
        down_mid_distance_rate = down_mid_distance_weight / total_distance_weight
        up_mid_distance_rate = up_mid_distance_weight / total_distance_weight
        new_mid_point_distance = cover_distance * up_mid_distance_rate
        return clock_wise_angle, anti_clock_wise_angle,new_mid_point_distance

    def get_proper_points(self):
        self.common_file_df = self.common_file_df[self.common_file_df['是否大于20G'] == '是']
        self.common_file_df['new_value'] = self.common_file_df.progress_apply(WeightChange.get_proper_point_by_node,
                                                                                 axis=1, args=(self.cloud_file_df, self.cover_distance))

        self.common_file_df.to_csv( self.output_path,index=False,encoding='utf-8-sig')


if __name__ == "__main__":
    common_file_path = "C:\\Users\\No.1\\Desktop\\teleccom\\5G小区工参-温州衢州三类四类(1).csv"
    cloud_file_path = "C:\\Users\\No.1\\Desktop\\teleccom\\云勘数据-温州衢州三类四类(1).csv"
    output_path = "C:\\Users\\No.1\\Desktop\\teleccom\\result.csv"
    weightchange = WeightChange(common_file_path, cloud_file_path,output_path ,0.8)
    weightchange.get_proper_points()
    print()
