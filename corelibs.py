import yaml
import pandas as pd
import pathlib
from typing import List, Dict, Union

# 加载全局配置文件
with open('config.yaml', 'r', encoding='utf-8') as f:
    CONF_DATA = yaml.load(f, Loader=yaml.SafeLoader)
    OUTPUT_FORMAT = CONF_DATA['output_format']

    
    
# ===========================================================
def parse_sheet_general(excel_file: pd.ExcelFile, sheet, conf_data: Dict) -> pd.ExcelFile:
    """分析一般数据sheet：支持sheet中仅含单表，返回dataframe"""
    df = excel_file.parse(sheet_name=sheet, header=0, skiprows=0, dtype=str)
    
    # 根据配置进行前处理
    if 'verify_cols' in conf_data:
        if 0 != (_col := verify_data(df, conf_data['verify_cols'])):
            raise Exception(f"验证未通过，需清洗数据列：{_col}") # todo: 临时措施
    if 'merged_2cols' in conf_data:
        for _key, _val in conf_data['merged_2cols'].items():
            merge_2cols(df, _key, _val[0], _val[1])
    if 'split_col' in conf_data:
        for _key, _val in conf_data['split_col'].items():
            split_2col(df, _key, _val[0], _val[1], _val[2])
            
    df = df.reindex(columns=conf_data['cols_new_order'])
    return df



# ===========================================================
def verify_data(df: pd.ExcelFile, cols: Dict) -> Union[str, int]:
    """验证给定dataframe的相关列是否完整：存在空值返回列名，验证通过返回0"""
    for _col in cols:
        if df[_col].isnull().any():
            return _col
    return 0

def merge_2cols(df: pd.ExcelFile, new_col: str, col1: str, col2: str) -> pd.ExcelFile:
    """合并两个dataframe字符串列为一个新列：两列中元素不同的直接相加，元素相同的只取一个避免重复"""
    _df1 = df[col1].fillna('')
    _df2 = df[col2].fillna('') # 填充两列空值为空字符串
    _cond = _df1 == _df2 # 保存判断条件：两列内容相等的行为true
    df[new_col] = _df1 + ' ' + _df2 # 两列相加并保存为新列
    df[new_col][_cond] = _df2 # 恢复两列内容相同的行
    return df

def split_2col(df: pd.ExcelFile, col: str, delimiter: str, new_col1: str, new_col2: str) -> pd.ExcelFile:
    """将dataframe中一列分割为两列"""
    df[[new_col1, new_col2]] = df[col].str.split(delimiter, expand=True)
    return df

# ===========================================================
def save_accounts(df: pd.DataFrame, output_dir: pathlib.Path, bank_name: str) -> None:
    """保存账户信息：在“0银行账户”目录中每个银行保存一个文件"""
    _account_dir = output_dir.joinpath('0银行账户') # 默认账户文件根目录
    _account_dir.mkdir(parents=True, exist_ok=True) # 创建未创建的目录
    _save_as_format(df, _account_dir.joinpath(bank_name), OUTPUT_FORMAT) # 用指定格式保存文件
    
def _save_as_format(df: pd.DataFrame, file_name:  pathlib.Path, output_form: str='csv') -> None:
    """根据配置格式保存dataframe：默认为csv文件"""
    if output_form == "excel":
        df.to_excel(file_name.with_suffix('.xlsx'), index=False)
    else:
        df.to_csv(file_name.with_suffix('.csv'), mode='a', index=False)
        
    

# 写入csv：每账户存一个csv，每个人存一个目录，文件名为姓名+银行+卡号

