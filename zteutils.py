import re
import copy


def get_action_charactors():
    return [':', 'match', 'eq']


def get_action_names():
    return {'删除整行', '合并', '整体筛选', '包含筛选'}


def union_action(df, action_tuples_list, action_name, new_col_name, index):
    if action_name == '删除整行':
        action_df = action_delete(df, action_tuples_list, index)
    elif action_name == '合并':
        if new_col_name is None:
            raise Exception("列合并操作没有定义新列的名称,出错行号:" + str(index))
        action_df = action_columns_merge(df, action_tuples_list, new_col_name, index)
    elif action_name == '筛选':
        action_df = action_filter(df, action_tuples_list, index)
    else:
        raise Exception("没有定义的操作名:" + action_name, "出错行号:" + str(index))
    return action_df


def find_operators(str):
    result = []
    operators = get_action_charactors()
    for op in operators:
        if str.find(op) >= 0:
            result.append(op)
    return result


def filter(df, operator, col_name, range):
    if operator == 'match':
        filter_df = df[col_name].str.contains(range)
    elif operator == ':':
        filter_df = df[df[col_name].str.contains(range)]
    elif operator == 'eq':
        filter_df = df[df[col_name] == range]
    else:
        raise Exception("未定义的操作符:" + operator)
    return filter_df


def action_delete(df, action_tuples, index):
    """
        删除就是过滤的逻辑，删除过滤后的结果
    """
    for tuple in action_tuples:
        col_name = tuple[0].strip()
        range = tuple[1].strip()
        operator = tuple[2].strip()
        if operator is None:
            raise Exception("删除操作,但是缺少算子,出错行数:" + str(index))
        #筛选出来的df,全部在原df中通过Index删除
        filter_df = filter(df, operator, col_name, range)
        index = filter_df.index.tolist()
        df.drop(index=index, axis=0, inplace=True)
    return df


def split_column(x, pattern):
    match_res = re.search(pattern, x)
    if match_res is not None:
        return match_res.group()
    else:
        return ''


def action_columns_merge(df, action_tuples_list, new_column_name, index):
    constant = None
    df[new_column_name] = ""
    for action_tuple in action_tuples_list:
        col_name = action_tuple[0].strip() if action_tuple[0] is not None else None
        action_range = action_tuple[1].strip() if action_tuple[1] is not None else None
        operator = action_tuple[2].strip() if action_tuple[2] is not None else None
        if operator == 'match':
            try:
                df[col_name + "#"] = df[col_name].apply(split_column, args=(action_range,))
            except Exception as e:
                raise Exception("请检查正则表达式的正确性," + "出错行数:" + str(index), e)
            if constant is not None:
                df[col_name + "#"] = constant + df[col_name + "#"]
                constant = None
            # cols.append(col_name + "#")
            # # 规整列的顺序,确保新的列在最后一个
            # df = df[cols]
        elif operator == ':' and action_range == '整体':
            df[col_name + "#"] = df[col_name]
            if constant is not None:
                df[col_name + "#"] = constant + df[col_name + "#"]
                constant = None
            # 规整列的顺序,,确保新的列在最后一个
            # cols.append(col_name + "#")
            # df = df[cols]
        elif operator is None:  # 如果operator是None,那么该数据是常量，
            constant = constant + col_name if constant is not None else col_name
        else:
            raise Exception("未定义的操作符:" + operator + "出错行数:" + str(index))
    cols = df.columns.tolist()
    for c in cols:
        if c.find('#') >= 0:
            df[new_column_name] = df[new_column_name] + df[c]
            df.drop([c], axis=1, inplace=True)
    if constant is not None:
        df[new_column_name] = df[new_column_name] + constant
    return df


def action_filter(df, action_tuples, index):
    filter_df = df
    for tuple in action_tuples:
        col_name = tuple[0].strip()
        range = tuple[1].strip()
        operator = tuple[2].strip()
        if operator is None:
            raise Exception("删除操作,但是缺少算子,出错行数:" + str(index))
        filter_df = filter(filter_df, operator, col_name, range)
    return filter_df
