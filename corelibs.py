import yaml
import pandas as pd
import pathlib
from tqdm import tqdm
from typing import List, Dict, Union

# 加载全局配置文件
with open('config.yaml', 'r', encoding='utf-8') as f:
    CONF_DATA = yaml.load(f, Loader=yaml.SafeLoader)
    OUTPUT_FORMAT = CONF_DATA['output_format']

    
    
# ===========================================================
def parse_sheet_general(excel_file: pd.ExcelFile, sheet, conf_data: Dict) -> pd.ExcelFile:
    """分析一般数据sheet：支持sheet中仅含单表，返回dataframe"""
    if 'amount_cols' in conf_data:
        converters = {_col:float for _col in conf_data['amount_cols']}
    else:
        converters = None
    df = excel_file.parse(sheet_name=sheet, header=0, skiprows=0, dtype=str, converters=converters)
    
    # 根据配置进行前处理
    if 'verify_cols' in conf_data:
        if 0 != (_col := verify_data(df, conf_data['verify_cols'])):
            raise Exception(f"验证未通过，需清洗数据列：{_col}") # todo: 临时措施
    if 'datetime_cols' in conf_data:
        _datetime_cols = conf_data['datetime_cols']
        df[_datetime_cols[0]] = pd.to_datetime(df[_datetime_cols[1]]).dt.date
        df[_datetime_cols[2]] = pd.to_datetime(df[_datetime_cols[3]]).dt.time
    if 'merged_2cols' in conf_data:
        for _key, _val in conf_data['merged_2cols'].items():
            merge_2cols(df, _key, _val[0], _val[1])
    if 'split_col' in conf_data:
        for _key, _val in conf_data['split_col'].items():
            split_2col(df, _key, _val[0], _val[1], _val[2])
    if 'copy_col' in conf_data:
        for _key, _val in conf_data['copy_col'].items():
            df[_val] = df[_key]
    if 'CDid' in conf_data:
        CD_to_InOut(df, conf_data['CDid'])
    if 'cols_new_order' in conf_data:
        df = df.reindex(columns=conf_data['cols_new_order'], copy=False)
    if 'col_name_map' in conf_data:
        df.rename(columns=conf_data['col_name_map'], inplace=True, errors='raise')
        
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

def CD_to_InOut(df: pd.ExcelFile, cdid: Dict) -> pd.ExcelFile:
    """将借/贷方式表示的交易金额改为出账列、入账列方式表示"""
    _C_crit = df[cdid['CD_col']] == cdid['C']
    df[cdid['C_col']] = df[cdid['trans_col']][_C_crit]
    df[cdid['D_col']] = df[cdid['trans_col']][~_C_crit]
    return df
    
    

# ===========================================================
def save_accounts(df: pd.DataFrame, output_dir: pathlib.Path, bank_name: str='默认银行') -> int:
    """保存账户数据：在“0银行账户”目录中每个银行保存一个文件，返回写入的行数"""
    _account_dir = output_dir.joinpath('0银行账户') # 默认账户文件根目录
    _account_dir.mkdir(parents=True, exist_ok=True) # 创建未创建的目录
    return  _save_as_format(df, _account_dir.joinpath(bank_name), OUTPUT_FORMAT, True)
    
def save_statements(df_list: List, output_dir: pathlib.Path, bank_name: str='默认银行', doc_No: str=None) -> int:
    """保存流水数据：每个人名设立一个目录，每个账户保存一个文件，文件名为银行+账户；可以传入文书号，这样将在单独的文书号文件中做记录，返回写入的流水条数"""
    _lines = 0
    if doc_No is not None:
        _text = bank_name + doc_No + "\n"
        with open(output_dir.joinpath('0查询文号.txt'),'a') as f:
            f.write(_text)
    for _df in tqdm(df_list):
        _acc_name = _df['姓名'].iat[0]
        _acc = _df['账号'].iat[0]
        _statement_dir = output_dir.joinpath(_acc_name) # 每个人名建立一个目录
        _statement_dir.mkdir(parents=True, exist_ok=True) # 创建未创建的目录
        _file_name = '：'.join([_make_df_brief(_df), bank_name, _acc])
        _lines += _save_as_format(_df, _statement_dir.joinpath(_file_name), OUTPUT_FORMAT, False)
    return _lines

def _save_as_format(df: pd.DataFrame, file_name:  pathlib.Path, output_form: str='excel', append=True) -> int:
    """根据配置格式保存dataframe：默认为excel文件，返回写入的行数"""
    if output_form == 'csv':
        pass
#         注意：本行未经充分测试，不可正式使用
#         if append:
#             df.to_csv(file_name.with_suffix('.csv'), mode='a', index=False)
#         else:
#             df.to_csv(file_name.with_suffix('.csv'), mode='w', index=False)
    else: # excel and more
        _name = file_name.with_suffix('.xlsx')
        if append:
            if _name.exists():
                _old_df = pd.read_excel(_name, dtype=str)
                df = pd.concat([_old_df, df], copy=False)
        else:
            while _name.exists():
                 _name = _name.with_name(_name.stem + '_').with_suffix('.xlsx')
        df.to_excel(_name, index=False)
    return len(df)
        
def _make_df_brief(df: pd.DataFrame) -> str:
    """生成流水简介：内容包括流水条数和最大数额（约到整万）"""
    _max = int(df[['出账金额','入账金额','余额']].max().max() // 10000 +1)
    return '最大' + str(_max) + '万,共' +str(len(df)) + '条'