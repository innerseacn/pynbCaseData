import pathlib
import pandas as pd
from corelibs.config import *



def save_general(df: pd.DataFrame, output_dir: pathlib.Path, bank_name: str, file_type: str) -> int:
    """保存非流水数据：在对应目录中每个银行保存一个文件，返回写入的行数"""
    _account_dir = output_dir.joinpath(get_output_dirs(file_type)) # 默认账户文件根目录
    _account_dir.mkdir(parents=True, exist_ok=True) # 创建未创建的目录
    return  _save_as_format(df, _account_dir.joinpath(bank_name), get_output_format(), True)
    
def save_statements(df_list: list[pd.DataFrame], output_dir: pathlib.Path, bank_name: str,  
                    file_type: str, doc_No: str=None) -> int:
    """保存流水数据：每个人名设立一个目录，每个账户保存一个文件，文件名为银行+账户；可以传入文书号，这样将在单独的文书号文件中做记录，返回写入的流水条数"""
    _lines = 0
    _acc_name_set = set() # 记录本次流水包含的姓名
    for _df in df_list:
        _acc_name = _df['姓名'].iat[0] # 按姓名分配目录
        _bank_dir = _df['银行'].iat[0] # 按银行分配子目录
        _acc_name_set.add(_acc_name)
        _statement_dir = output_dir.joinpath(get_output_dirs(file_type), _acc_name, _bank_dir) # 每个人名建立一个目录
        _statement_dir.mkdir(parents=True, exist_ok=True) # 创建未创建的目录
        _acc = _df['卡号'].drop_duplicates().to_list()
        _acc_str = f'尾号{",".join([x[-5:] for x in _acc])}'
        _file_name = '_'.join([_make_df_brief(_df), bank_name, _acc_str])
        _df.sort_values(['日期','时间'], inplace=True)
        _lines += _save_as_format(_df, _statement_dir.joinpath(_file_name), get_output_format(), False)
    if doc_No is not None: # 保存查询文书记录
        _text = ','.join([doc_No, bank_name, str(_acc_name_set).replace(',', '')])  + "\n"
        with open(output_dir.joinpath('0查询文号.csv'), 'a') as f:
            f.write(_text)
    return _lines

def _save_as_format(df: pd.DataFrame, file_name:  pathlib.Path, output_format: str='', append=True) -> int:
    """根据保存dataframe：excel文件和csv文件各一套，返回写入的行数"""
# todo：在单独的目录中保存一套csv文件
#         if append:
#             df.to_csv(file_name.with_suffix('.csv'), mode='a', index=False)
#         else:
#             df.to_csv(file_name.with_suffix('.csv'), mode='w', index=False)
    _name = file_name.with_suffix('.xlsx')
    if append:
        if _name.exists():
            _old_df = pd.read_excel(_name, dtype=str)
            df = pd.concat([_old_df, df], copy=False)
            df.drop_duplicates(inplace=True)
    else:
        while _name.exists():
                _name = _name.with_name(_name.stem + '_').with_suffix('.xlsx')
    df.to_excel(_name, index=False)
    return len(df)
        
def _make_df_brief(df: pd.DataFrame) -> str:
    """生成流水简介：内容包括流水条数和最大数额（约到整万）"""
    _max = int(df[['出账金额','入账金额','余额']].max().max() // 10000 +1)
    return '最大' + str(_max) + '万,共' +str(len(df)) + '条'

