import pandas as pd
from typing import List, Union

# 通用格式：仅一个sheet，sheet中仅一个表，表中信息齐全。
def parse_trans_general(excel_file: pd.ExcelFile, account_col: str) -> List[pd.DataFrame]:
    df = excel_file.parse(sheet_name=0, header=0, skiprows=0, dtype=str)
    grouped = df.groupby(account_col)
    return [grouped.get_group(x) for x in grouped.groups]