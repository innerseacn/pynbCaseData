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
def parse_sheet_general(file_path: pathlib.Path, sheet, conf_data: Dict) -> pd.DataFrame:
    """分析一般数据sheet：支持sheet中仅含单表，返回dataframe"""
    _new_cols = {} # 构造需要填充统一内容的列字典，通常是新列
    _verify_cols = {} # 构造需要进行数据检查的列字典，（v1）版本只检查空值
    _col_name_map = {} # 构造需要改名的列字典
    _merge_2cols = {} # 构造需要合并的列字典
    
    #根据配置信息生成列处理逻辑
    for _key, _val in conf_data.items(): # 读取配置中的列名（key）和列配置（val）
        if isinstance(_val, str): # 如果列配置为str，则本列全部内容为该字符串，通常是全新的列
            _new_cols[_key] = _val
        elif isinstance(_val, bool) and _val: # 如果列配置为bool且为True，代表本列需要数据检查
            _verify_cols[_key] = True # （v1）版本只检查空值，此处恒为True
        elif isinstance(_val, dict): # 如果列配置为dict，则分情况讨论
            for __k, __v in _val.items(): # 首先检查所有字典值，将为True的加入数据检查中
                if __v:
                     _verify_cols[__k] = True
            if len(_val) == 1: # 当字典中只有一项时，代表本列需要改名
                _col_name_map[next(iter(_val))] = _key
            elif len(_val) > 1: # 当字典中有两项或更多时，代表本列需要合并多个原始列（目前仅支持两个）
                _merge_2cols[_key] = list(_val.keys())
            else:
                raise Exception(f"字典类型长度错误，检查配置列：{_key}") 
        elif isinstance(_val, list): # 
            if _key != 'cols_new_order':
                raise Exception(f"只有cols_new_order可以配置为list类型，检查配置列：{_key}")                 
        else:
            raise Exception(f"配置内容类型不支持，检查配置列：{_key}") 
                
    # 读取工作表内容
    df = pd.read_excel(file_path, sheet_name=sheet, header=0, skiprows=0, dtype=str)
        
    # 列数据处理
    if (_col := verify_data(df, _verify_cols)) != 0: # 执行数据检查
        raise Exception(f"验证未通过，需清洗数据列：{_col}") # todo: 临时措施
    for _k, _v in _new_cols.items(): # 执行新列赋值
        df[_k] = _v
    for _k, _v in _merge_2cols.items(): # 执行两列合并
        merge_2cols(df, _k, _v[0], _v[1])
            
            
            
#     if 'datetime_cols' in conf_data:
#         _datetime_cols = conf_data['datetime_cols']
#         df[_datetime_cols[0]] = pd.to_datetime(df[_datetime_cols[1]]).dt.date
#         df[_datetime_cols[2]] = pd.to_datetime(df[_datetime_cols[3]]).dt.time
#     if 'split_col' in conf_data:
#         for _key, _val in conf_data['split_col'].items():
#             split_2col(df, _key, _val[0], _val[1], _val[2])
#     if 'copy_col' in conf_data:
#         for _key, _val in conf_data['copy_col'].items():
#             df[_val] = df[_key]
#     if 'CDid' in conf_data:
#         CD_to_InOut(df, conf_data['CDid'])

#     if 'amount_cols' in conf_data:
#         converters = {_col:float for _col in conf_data['amount_cols']}
#     else:
#         converters = None
        
    if len(_col_name_map) > 0: # 执行修改列名
        df.rename(columns=_col_name_map, inplace=True, errors='raise')
    if 'cols_new_order' in conf_data: #执行列序重排
        df = df.reindex(columns=conf_data['cols_new_order'], copy=False)
        
    return df

def acc_or_stat(file: pathlib.Path, sheet, conf_data: Dict) -> bool:
    """判断给定文件是账户还是流水，返回bool值，True表示是账户文件，False表示是流水文件"""
    df = excel_file.parse(sheet_name=sheet, header=0, skiprows=0, dtype=str, converters=converters)



# ===========================================================
def verify_data(df: pd.DataFrame, cols: Dict) -> Union[str, int]:
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
    df[new_col][_cond] = _df2 # 恢复两列内容相同的行
    return df

def split_2col(df: pd.DataFrame, col: str, delimiter: str, new_col1: str, new_col2: str) -> pd.DataFrame:
    """将dataframe中一列分割为两列"""
    df[[new_col1, new_col2]] = df[col].str.split(delimiter, expand=True)
    return df

def CD_to_InOut(df: pd.DataFrame, cdid: Dict) -> pd.DataFrame:
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
    _acc_name_set = set()
    for _df in tqdm(df_list):
        _acc_name = _df['姓名'].iat[0]
        _acc_name_set.add(_acc_name)
        _acc = _df['账号'].iat[0]
        _statement_dir = output_dir.joinpath(_acc_name) # 每个人名建立一个目录
        _statement_dir.mkdir(parents=True, exist_ok=True) # 创建未创建的目录
        _file_name = '：'.join([_make_df_brief(_df), bank_name, _acc])
        _lines += _save_as_format(_df, _statement_dir.joinpath(_file_name), OUTPUT_FORMAT, False)
    if doc_No is not None: # 保存查询文书记录
        _text = ','.join([doc_No, bank_name, str(_acc_name_set)])  + "\n"
        with open(output_dir.joinpath('0查询文号.csv'), 'a') as f:
            f.write(_text)
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
