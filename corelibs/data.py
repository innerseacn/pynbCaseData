import pathlib
import pandas as pd
import numpy as np

from corelibs.config import Conf_tpl



def parse_sheet_general(file_path: pathlib.Path, conf_data: Conf_tpl, sheet=0, header=0) -> pd.DataFrame:
    """分析一般数据sheet：支持sheet中仅含单表，返回dataframe"""
    # 读取工作表内容
    df = pd.read_excel(file_path, sheet_name=sheet, header=header, skiprows=0, dtype=str)
        
    # 列数据处理
    if (_col := _verify_data(df, conf_data.verify_cols)): # 执行数据检查
        raise Exception(f"验证未通过，需清洗数据列：{_col}") # todo: 临时措施
    for _k, _v in conf_data.new_cols.items(): # 执行新列赋值
        df[_k] = _v
    for _k, _v in conf_data.from_dir.items(): # 从目录名中找到信息向对应列赋值
        df[_k] = _get_str_from_dir(file_path, _v[0], _v[1])
    for _k, _v in conf_data.from_file.items(): # 从文件名中找到信息向对应列赋值
        df[_k] = _get_str_from_file(file_path, _v[0], _v[1])
    for _k, _v in conf_data.merge_cols.items(): # 执行列合并
        _merge_N_cols(df, _k, _v)
    for _k, _v in conf_data.date_cols.items(): # 执行日期列数据转换
        _format = 'mixed' if len(_v) == 1 else _v[1]
        df[_k] = pd.to_datetime(df[_v[0]], format=_format).dt.date
    for _k, _v in conf_data.time_cols.items(): # 执行时间列数据转换
        _format = 'mixed' if len(_v) == 1 else _v[1]
        df[_k] = pd.to_datetime(df[_v[0]], format=_format).dt.time
    for _k, _v in conf_data.digi_cols.items(): # 执行数据列数据转换
        df[_k] = pd.to_numeric(df[_k])
    if conf_data.cdid: # 执行借贷列分列
        _CD_to_InOut(df, conf_data.cdid)
    for _k, _v in conf_data.fill_cols.items(): # 执行列条件填充
        _fill_col(df, _v[2], _k, _v[0], _v[1])
    # 执行修改列名
    df.rename(columns=conf_data.col_name_map, inplace=True, errors='raise')
    #执行列序重排
    df = df.reindex(columns=conf_data.cols_new_order, copy=False)

    df.drop_duplicates(inplace=True)
    return df

def _verify_data(df: pd.DataFrame, cols: dict) -> list:
    """验证给定dataframe的相关列是否完整：存在空值返回列名，验证通过返回0"""
    _err_cols = []
    for _k, _v in cols.items(): # 第一版_v恒为true，暂无作用
        if df[_k].isnull().any():
            _err_cols.append(_k)
    return _err_cols

def _merge_2_cols(df: pd.DataFrame, new_col: str, col1: str, col2: str) -> pd.DataFrame:
    """合并两个dataframe字符串列为一个新列：两列中元素不同的直接相加，元素相同的只取一个避免重复"""
    _df1 = df[col1].fillna('')
    _df2 = df[col2].fillna('') # 填充两列空值为空字符串
    _cond = _df1 == _df2 # 保存判断条件：两列内容相等的行为true
    df[new_col] = (_df1 + ' ' + _df2).str.strip() # 两列相加并保存为新列
    df.loc[_cond, new_col] = _df2 # 恢复两列内容相同的行
    df[new_col].replace('', np.nan, inplace=True) # 保持和读取文件后的内容一致(便于去重操作)
    return df

def _merge_N_cols(df: pd.DataFrame, new_col: str, cols: list) -> pd.DataFrame:
    """合并多个dataframe字符串列为一个新列，并去除重复元素"""
    if (l := len(cols)) < 2:
        raise Exception(r'merge_N_cols参数cols应该大于1')
    elif l == 2:
        _merge_2_cols(df, new_col, cols[0], cols[1])
    elif l > 2:
        _df = df[cols].fillna('')
        df[new_col] = _df.apply(lambda r: ' '.join(dict.fromkeys(r[cols])), axis=1)
        df[new_col].replace('', np.nan, inplace=True) # 保持和读取文件后的内容一致(便于去重操作)
    return df

def _split_2col(df: pd.DataFrame, col: str, delimiter: str, new_col1: str, new_col2: str) -> pd.DataFrame:
    """将dataframe中一列分割为两列"""
    df[[new_col1, new_col2]] = df[col].str.split(delimiter, expand=True)
    return df

def _CD_to_InOut(df: pd.DataFrame, cdid: dict) -> pd.DataFrame:
    """将借/贷方式表示的交易金额改为出账列、入账列方式表示"""
    _C_crit = df[cdid['CD_col']] == cdid['C']
    df[cdid['C_col']] = df.loc[_C_crit, cdid['trans_col']]
    df[cdid['D_col']] = df.loc[~_C_crit, cdid['trans_col']]
    return df
    
def _fill_col(df: pd.DataFrame, from_col: str, to_col: str, crit_col: str, crit_val) -> pd.DataFrame:
    """根据填充标志列的取值，将原列的值填充到目标列"""
    if crit_val is None:
        _crit = df[crit_col].isnull()
    else:
        _crit = df[crit_col] == crit_val
    df.loc[_crit, to_col] = df.loc[_crit, from_col] 
    return df

def _get_str_from_dir(file_path: pathlib.Path, delimiter: str, index: int) -> str:
    """从文件路径中获取字符串"""
    return file_path.parts[-2].split(delimiter)[index]

def _get_str_from_file(file_path: pathlib.Path, delimiter: str, index: int) -> str:
    """从文件名中获取字符串"""
    return file_path.stem.split(delimiter)[index]