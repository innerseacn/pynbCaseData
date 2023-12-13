import json
import pandas as pd
import pathlib
from typing import List, Dict


# 加载全局配置文件
with open('config.json', 'r', encoding='utf-8') as f:
    CONF_DATA = json.load(f)
    BANK_CONF = CONF_DATA['bank_conf']

# ===========================================================
def parse_account_bot(excel_file: pd.ExcelFile) -> pd.ExcelFile:
    """分析天津银行电子版流水中的账户文件，返回dataframe"""
    _local_bank_conf = BANK_CONF['天津银行账户'] #缓存本地配置
    df = excel_file.parse(sheet_name=0, header=0, skiprows=0, dtype=str, keep_default_na='')
    if 'merged_2cols' in _local_bank_conf:
        for _key, _value_list in _local_bank_conf['merged_2cols'].items():
            merge_2cols(df, _key, _value_list[0], _value_list[1])
    df = df.reindex(columns=_local_bank_conf['cols_new_order'])
    return df



# ===========================================================
def merge_2cols(df: pd.ExcelFile, new_col: str, col1: str, col2: str) -> pd.ExcelFile:
    """合并两个dataframe字符串列为一个新列：两列中元素不同的直接相加，元素相同的只取一个避免重复"""
    _cond = df[col1] == df[col2]
    df[new_col] = df[col1] + ' ' + df[col2]
    df[new_col][_cond] = df[col2]
    return df

# ===========================================================
def save_accounts(df: pd.DataFrame, output_dir: pathlib.Path, bank_name: str):
    """账户信息写入csv：在“0银行账户”目录中每个银行保存一个csv文件"""
    _account_dir = output_dir.joinpath('0银行账户')
    _account_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(_account_dir.joinpath(bank_name + '.csv'), mode='a', index=False)
    

# 写入csv：每账户存一个csv，每个人存一个目录，文件名为姓名+银行+卡号

