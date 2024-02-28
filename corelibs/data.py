import pathlib
import pandas as pd

from corelibs.config import Conf_tpl



def parse_sheet_general(file_path: pathlib.Path, conf_data: Conf_tpl, sheet=0, header=0) -> pd.DataFrame:
    """分析一般数据sheet：支持sheet中仅含单表，返回dataframe"""
    # 读取工作表内容
    df = pd.read_excel(file_path, sheet_name=sheet, header=header, skiprows=0, dtype=str)
        
    # 列数据处理
    if (_col := verify_data(df, conf_data.verify_cols)) != 0: # 执行数据检查
        raise Exception(f"验证未通过，需清洗数据列：{_col}") # todo: 临时措施
    for _k, _v in conf_data.new_cols.items(): # 执行新列赋值
        df[_k] = _v
    for _k, _v in conf_data.merge_2cols.items(): # 执行两列合并
        merge_2cols(df, _k, _v[0], _v[1])
    for _k, _v in conf_data.date_cols.items(): # 执行日期列数据转换
        df[_k] = pd.to_datetime(df[_v]).dt.date
    for _k, _v in conf_data.time_cols.items(): # 执行时间列数据转换
        df[_k] = pd.to_datetime(df[_v]).dt.time
    for _k, _v in conf_data.digi_cols.items(): # 执行数据列数据转换
        df[_k] = pd.to_numeric(df[_k])
    if conf_data.cdid: # 执行借贷列分列
        CD_to_InOut(df, conf_data.cdid)
    for _k, _v in conf_data.fill_cols.items(): # 执行列条件填充
        fill_col(df, _v[2], _k, _v[0], _v[1])

#     if 'split_col' in conf_data:
#         for _key, _val in conf_data['split_col'].items():
#             split_2col(df, _key, _val[0], _val[1], _val[2])
#     if 'copy_col' in conf_data:
#         for _key, _val in conf_data['copy_col'].items():
#             df[_val] = df[_key]
        
    # 执行修改列名
    df.rename(columns=conf_data.col_name_map, inplace=True, errors='raise')
    #执行列序重排
    df = df.reindex(columns=conf_data.cols_new_order, copy=False)
        
    return df

def verify_data(df: pd.DataFrame, cols: dict) -> str | int:
    """验证给定dataframe的相关列是否完整：存在空值返回列名，验证通过返回0"""
    for _col in cols:
        if df[_col].isnull().any():
            return _col
    return 0

def merge_2cols(df: pd.DataFrame, new_col: str, col1: str, col2: str) -> pd.DataFrame:
    """合并两个dataframe字符串列为一个新列：两列中元素不同的直接相加，元素相同的只取一个避免重复"""
    _df1 = df[col1].fillna('')
    _df2 = df[col2].fillna('') # 填充两列空值为空字符串
    _cond = _df1 == _df2 # 保存判断条件：两列内容相等的行为true
    df[new_col] = (_df1 + ' ' + _df2).str.strip() # 两列相加并保存为新列
    # df[new_col][_cond] = _df2 # 恢复两列内容相同的行
    df.loc[_cond, new_col] = _df2 # 恢复两列内容相同的行
    return df

def split_2col(df: pd.DataFrame, col: str, delimiter: str, new_col1: str, new_col2: str) -> pd.DataFrame:
    """将dataframe中一列分割为两列"""
    df[[new_col1, new_col2]] = df[col].str.split(delimiter, expand=True)
    return df

def CD_to_InOut(df: pd.DataFrame, cdid: dict) -> pd.DataFrame:
    """将借/贷方式表示的交易金额改为出账列、入账列方式表示"""
    _C_crit = df[cdid['CD_col']] == cdid['C']
    df[cdid['C_col']] = df.loc[_C_crit, cdid['trans_col']]
    df[cdid['D_col']] = df.loc[~_C_crit, cdid['trans_col']]
    return df
    
def fill_col(df: pd.DataFrame, from_col: str, to_col: str, crit_col: str, crit_val) -> pd.DataFrame:
    """根据填充标志列的取值，将原列的值填充到目标列"""
    if crit_val is None:
        _crit = df[crit_col].isnull()
    else:
        _crit = df[crit_col] == crit_val
    # df[to_col][_crit] = df[from_col][_crit]
    df.loc[_crit, to_col] = df.loc[_crit, from_col] 
    return df
