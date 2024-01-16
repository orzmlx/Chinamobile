from haversine import haversine
import time
from shapely.geometry import Point, Polygon, LineString
import numpy as np
import matplotlib.pyplot as plt
import math
import geopandas as gpd


def get_sector_normal_line(angle, lat, lon, radius):
    point = Point(lat, lon)
    # 计算法向量
    x = radius * math.cos(math.radians(angle))
    y = radius * math.sin(math.radians(angle))
    normal = (y, -x)

    # 计算中线坐标
    line = LineString([point, (lat + normal[0], lon + normal[1])])

    # 输出中线坐标
    print(line.coords)


def get_angle_absolute_value(center_angle, angle):
    if abs(angle - center_angle) > 45:
        if center_angle >= angle:
            return 360 - center_angle + angle
        else:
            return 360 - angle + center_angle
    else:
        return abs(center_angle - angle)


def point_2_vertical_line_distance(p1, p2, angle):
    """
        计算点到线的距离
    """
    # points_df = gpd.GeoDataFrame({'geometry': [p1, p2]}, crs='EPSG:4326')
    # points_df = points_df.to_crs('EPSG:5234')
    # points_df2 = points_df.shift()  # We shift the dataframe by 1 to align pnt1 with pnt2
    # d=points_df.distance(points_df2)
    d = get_distance(p1.x, p1.y, p2.x, p2.y)
    # points_df.distance(points_df2)
    return math.cos(angle * (math.pi / 180)) * d


# def create_vector(angle,):
#     deg2rad = math.pi / 180
#
#     dlon = (origin_lat - origin_lon) * deg2rad
#     angle = angle * np.pi / 180  # 转换为弧度
#     # distance = 1  # 点与原始点之间的距离
#     x = origin_lat + (distance * np.cos(angle))
#     y =  origin_lon+ (distance * np.sin(angle))
#     coordinates = [(origin_lon, origin_lat), (x, y)]
#     return LineString(coordinates)


def get_start_angle(direction):
    if direction >= 45:
        start_angle = direction - 45
    else:
        start_angle = 360 - (90 - direction)
    return start_angle


def get_end_angle(direction):
    if direction > 315:
        end_angle = 45 - (360 - direction)
    else:
        end_angle = direction + 45
    return end_angle


def get_rad_by_direction(direction):
    start_rad = get_start_angle(direction)
    end_rad = get_end_angle(direction)
    if start_rad > end_rad:
        temp = end_rad
        end_rad = start_rad
        start_rad = temp
    return start_rad, end_rad


def get_angle(center_latitude, center_longitude, target_latitude, target_longitude):
    deg2rad = math.pi / 180

    dlat = (target_latitude - center_latitude) * deg2rad

    dlon = (target_longitude - center_longitude) * deg2rad

    y = math.sin(dlon) * math.cos(target_latitude * deg2rad)

    x = math.cos(center_latitude * deg2rad) * math.sin(target_latitude * deg2rad) - math.sin(
        center_latitude * deg2rad) * math.cos(
        target_latitude * deg2rad) * math.cos(dlon)

    angle = math.atan2(y, x) * 180 / math.pi

    return angle


def is_point_in_sector(center_latitude, center_longitude, start_angle, end_angle, target_latitude, target_longitude):
    angle = get_angle(center_latitude, center_longitude, target_latitude, target_longitude)
    if angle < 0:
        angle = 360 + angle
    if abs(end_angle - start_angle) > 90:
        match = angle <= start_angle or angle >= end_angle
    else:
        match = start_angle <= angle <= end_angle
    ##angle = math.atan2(target_radian[1] - center_radian[1], target_radian[0] - center_radian[0]) % (2 * math.pi)
    return match


def is_covered_by_sector(latitude, longitude, sector):
    pass


def get_sector_polygon(latitude, longitude, radius):
    # 定义顶点坐标和扇形半径
    x = 0
    y = 0
    radius = 1.0

    # 构建扇形
    center = Point(x, y)
    circle = center.buffer(radius)
    # 构建一个矩形
    # 绘制图形
    fig, ax = plt.subplots()
    ax.set_aspect('equal')
    ax.plot(*circle.exterior.xy, color='black')
    ax.fill(*circle.exterior.xy, color='blue', alpha=0.5)

    # 显示图形
    plt.show()

    # 输出扇形的面积
    print("Area of the polygon:", circle.area)


def get_distance(lon1, lat1, lon2, lat2):
    start_time = time.time()  # 记录开始时间
    # 在这里写入要计时的代码段

    # lon1, lat1 = 120.512547, 27.779988
    #
    # lon2, lat2 = 120.5067892, 27.78458067

    distance = haversine((lat1, lon1), (lat2, lon2))
    end_time = time.time()  # 记录结束时间
    execution_time = end_time - start_time  # 计算执行时间
   # print("计算距离程序运行时间为：", execution_time)
    return distance

# if __name__ == "__main__":
#     geoutils = gutils()
#
#     geoutils.get_sector_polygon(27.94571089, 120.7590752, 800)
