import math

from haversine import haversine
from shapely.geometry import Point, LineString


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
        if int(center_angle) >= angle:
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


def get_start_angle(direction):
    direction = float(direction)
    if int(direction) >= 45:
        start_angle = direction - 45
    else:
        start_angle = 360 + (direction - 45)
    return start_angle


def get_end_angle(direction):
    direction = float(direction)
    if int(direction) > 315:
        end_angle = 45 - (360 - direction)
    else:
        end_angle = direction + 45
    return end_angle


def get_rad_by_direction(direction):
    start_rad = float(get_start_angle(direction))
    end_rad = float(get_end_angle(direction))
    if start_rad > end_rad:
        temp = end_rad
        end_rad = start_rad
        start_rad = temp
    return start_rad, end_rad


def get_angle(center_latitude, center_longitude, target_latitude, target_longitude):
    return math.atan2(
        math.sin((target_longitude - center_longitude) * (math.pi / 180)) * math.cos(target_latitude * (math.pi / 180)),
        math.cos(center_latitude * (math.pi / 180)) * math.sin(target_latitude * (math.pi / 180)) - math.sin(
            center_latitude * (math.pi / 180)) * math.cos(
            target_latitude * (math.pi / 180)) * math.cos(
            (target_longitude - center_longitude) * math.pi / 180)) * (180 / math.pi)


def is_point_in_sector(center_latitude, center_longitude, start_angle, end_angle, target_latitude, target_longitude):
    angle = get_angle(center_latitude, center_longitude, target_latitude, target_longitude)
    if angle < 0:
        angle = 360 + angle
    if abs(int(end_angle) - int(start_angle)) > 90:
        match = angle <= start_angle or angle >= end_angle
    else:
        match = start_angle <= angle <= end_angle
    ##angle = math.atan2(target_radian[1] - center_radian[1], target_radian[0] - center_radian[0]) % (2 * math.pi)
    return match




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
    # fig, ax = plt.subplots()
    # ax.set_aspect('equal')
    # ax.plot(*circle.exterior.xy, color='black')
    # ax.fill(*circle.exterior.xy, color='blue', alpha=0.5)

    # 显示图形

    # 输出扇形的面积
    print("Area of the polygon:", circle.area)


def get_distance(lon1, lat1, lon2, lat2):

    return haversine((lat1, lon1), (lat2, lon2))



def calculate_bearing(start_longitude, start_latitude, end_longitude, end_latitude):
    # 将经纬度转换成弧度
    start_rad = math.radians((90 - start_latitude) + (180 / math.pi))
    end_rad = math.radians((90 - end_latitude) + (180 / math.pi))

    delta_lon = math.radians(end_longitude - start_longitude)

    y = math.sin(delta_lon) * math.cos(end_rad)
    x = math.cos(start_rad) * math.sin(end_rad) - \
        math.sin(start_rad) * math.cos(end_rad) * math.cos(delta_lon)

    bearing = math.degrees(math.atan2(y, x)) % 360

    return bearing


if __name__ == "__main__":
    # node_lon = 120.590601
    # node_lat = 28.082305
    #
    # cloud_point_lon = 120.589599613697
    # cloud_point_lat = 28.0860557594443

    node_lon = 120.550292
    node_lat = 28.096551

    cloud_point_lon = 120.5544582
    cloud_point_lat = 28.09302628
    # geoutils = gutils()
    # print(calculate_bearing(cloud_point_lon,cloud_point_lat,node_lon,node_lat))
    print(get_angle(node_lat, node_lon, cloud_point_lat, cloud_point_lon))
    # get_angle()
