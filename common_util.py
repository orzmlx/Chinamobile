from decimal import Decimal


def count_digits(num):
    # 将小数转换为字符串形式
    num_str = str(num)
    # 查找小数点所在的索引位置
    dot_index = num_str.find('.')

    if dot_index == -1:
        return 0  # 如果没有小数部分，则返回0
    else:
        decimal_part = len(num_str[dot_index + 1:])  # 获取小数部分长度
        return decimal_part
