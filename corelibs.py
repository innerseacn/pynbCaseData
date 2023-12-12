import json
import pandas as pd
from typing import List, Dict

# 加载全局配置文件
with open('config.json', 'r', encoding='utf-8') as f:
    CONF_DATA = json.load(f)
    BANK_CONF = CONF_DATA['bank_conf']

# todo  数据验证：分析提取出的dataFrame数据，检查needed_cols数据完整性
def verify_data(df: pd.DataFrame, needed_cols: List[str]) -> bool:
    return True
    
# 通用格式1：excel文件中仅一个sheet，sheet中仅一个表，表中信息齐全，单账户, 返回dataFrame列表
def parse_1sheet_1table_1account(excel_file: pd.ExcelFile, bank_name: str, account_or_deposit: str='deposit') -> List[pd.DataFrame]:
    _local_bank_conf = BANK_CONF[bank_name][account_or_deposit] #缓存本地配置
    df = excel_file.parse(sheet_name=0, header=0, skiprows=0, dtype=str) # 读取sheet内容
    if verify_data(df, _local_bank_conf['verify_cols']):  # 验证数据完整性
        return [df] # 数据验证通过则返回原样数据
    return None #数据验证未通过返回空，由外部程序进行处理

# 通用格式2：excel文件中仅一个sheet，sheet中仅一个表，表中信息齐全，多账户混排, 返回dataFrame列表
def parse_1sheet_1table_Naccounts(excel_file: pd.ExcelFile, bank_name: str, account_or_deposit: str='deposit') -> List[pd.DataFrame]:
    _local_bank_conf = BANK_CONF[bank_name][account_or_deposit] #缓存本地配置
    df = excel_file.parse(sheet_name=0, header=0, skiprows=0, dtype=str) # 读取sheet内容
    if verify_data(df, _local_bank_conf['verify_cols']):  # 验证数据完整性
        grouped = df.groupby(df, _local_bank_conf['account_col']) #按账号将数据分组
        return [grouped.get_group(x) for x in grouped.groups] #返回分组后的列表
    return None #数据验证未通过返回空，由外部程序进行处理


# 写入csv：每账户存一个csv，每个人存一个目录，文件名为姓名+银行+卡号

