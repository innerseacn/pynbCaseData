import pathlib
import pandas as pd, openpyxl as op, xlrd as xl
from corelibs.config import Conf_tpl, CONF_TPL_CACHE
from tqdm.notebook import tqdm as tqnb
from itertools import islice
from collections import Counter
from hashlib import md5



    
# ===========================================================
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

def read_header(file_path: pathlib.Path, sheet=None, header: int=0) -> str:
    """读取文件表头，用于识别文件来源，目前支持xls和xlsx文件"""
    if (_suff := file_path.suffix) == '.xlsx':
        return _read_header_xlsx(file_path, sheet, header)
    elif _suff == '.xls':
        return _read_header_xls(file_path, sheet, header)
    else:
        return ''

def _read_header_xlsx(file_path: pathlib.Path, sheet=None, header: int=0) -> str:
    """读取文件表头，用于识别文件来源"""
    _work_book = op.load_workbook(file_path, read_only=True)
    if sheet is None:
        _sheet = _work_book.active
    elif type(sheet) == int:
        _sheet = _work_book[_work_book.sheetnames[sheet]]
    elif type(sheet) == str:
        _sheet = _work_book[sheet]
    else:
        raise Exception(f"sheet参数只能为int或str") 
    _sheet.calculate_dimension(force=True)
    _sheet.reset_dimensions()
    return str(next(islice(_sheet.values, header, header+1)))

def _read_header_xls(file_path: pathlib.Path, sheet=None, header: int=0) -> str:
    _work_book = xl.open_workbook(file_path, on_demand=True)
    if sheet is None:
        _sheet = _work_book.sheet_by_index(0)
    elif type(sheet) == int:
        _sheet = _work_book.sheet_by_index(sheet)
    elif type(sheet) == str:
        _sheet = _work_book.sheet_by_name(sheet)
    else:
        raise Exception(f"sheet参数只能为int或str") 
    return str(_sheet.row_values(header))

    

# ===========================================================
def verify_data(df: pd.DataFrame, cols: dict) -> str or int:
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
    
    

# ===========================================================
def save_accounts(df: pd.DataFrame, output_dir: pathlib.Path, bank_name: str='默认银行') -> int:
    """保存账户数据：在“0银行账户”目录中每个银行保存一个文件，返回写入的行数"""
    _account_dir = output_dir.joinpath('0银行账户') # 默认账户文件根目录
    _account_dir.mkdir(parents=True, exist_ok=True) # 创建未创建的目录
    return  _save_as_format(df, _account_dir.joinpath(bank_name), output_dir, True)
    
def save_statements(df_list: list, output_dir: pathlib.Path, bank_name: str='默认银行', doc_No: str=None) -> int:
    """保存流水数据：每个人名设立一个目录，每个账户保存一个文件，文件名为银行+账户；可以传入文书号，这样将在单独的文书号文件中做记录，返回写入的流水条数"""
    _lines = 0
    _acc_name_set = set()
    for _df in df_list:
        _acc_name = _df['姓名'].iat[0]
        _acc_name_set.add(_acc_name)
        _acc = _df['账号'].iat[0]
        _statement_dir = output_dir.joinpath('人员流水', _acc_name) # 每个人名建立一个目录
        _statement_dir.mkdir(parents=True, exist_ok=True) # 创建未创建的目录
        _file_name = '：'.join([_make_df_brief(_df), bank_name, _acc])
        _lines += _save_as_format(_df, _statement_dir.joinpath(_file_name), OUTPUT_FORMAT, False)
    if doc_No is not None: # 保存查询文书记录
        _text = ','.join([doc_No, bank_name, str(_acc_name_set).replace(',', '')])  + "\n"
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


# ===========================================================
def process_account_file_general(file: pathlib.Path, output_dir: pathlib.Path, bank_name: str, acc_or_stat: str) -> pd.DataFrame:
    """根据配置处理单个账户文件，并保存到指定目录"""
    _conf_obj = get_conf_obj(bank_name, acc_or_stat)
    _df = parse_sheet_general(file, _conf_obj)
    save_accounts(_df, output_dir, bank_name)
    return _df

def process_statment_file_general(file: pathlib.Path, output_dir: pathlib.Path, bank_name: str, acc_or_stat: str, doc_No: str=None) -> pd.DataFrame:
    """根据配置处理单个流水文件，并保存到指定目录"""
    _conf_obj = get_conf_obj(bank_name, acc_or_stat)
    _df = parse_sheet_general(file, _conf_obj)
    _grouped = _df.groupby('账号')
    _df_list = [x.reset_index(drop=True) for _ , x in _grouped]
    save_statements(_df_list, output_dir, bank_name, doc_No)
    return _df

def process_files_general(files_list: list, output_dir: pathlib.Path, doc_No: str=None) -> list:
    """根据配置处理多个文件，跳过出错文件，返回处理文件个数和出错文件列表"""
    _err_file_dict = {} # 保存解析出错的文件和原因
    for _file in tqnb(files_list):
        print(f'{_file.name}……', end='')
        _header = read_header(_file).encode() # 读取每个文件的表头
        _conf_name = HEADER_HASH.get(md5(_header).hexdigest()) # 根据表头md5值找到相应的配置
        if _conf_name is None:
            print(_msg := '未找到对应配置，跳过')
            _err_file_dict[_file] = _msg
            continue
        else:
            try:    
                if _conf_name[1] == '账户':
                    process_account_file_general(_file, output_dir, _conf_name[0], '账户')
                elif _conf_name[1] == '流水':
                    process_statment_file_general(_file, output_dir, _conf_name[0], '流水', doc_No)
                else:
                     raise Exception("header_hash配置有误") 
            except Exception as e:
                print( _msg := str(e))
                _err_file_dict[_file] = _msg
            else:
                print(f'{"".join(_conf_name)}done')
    return _err_file_dict



# ===========================================================
def process_dir_ccb_branch(dir_path: pathlib.Path, output_dir: pathlib.Path, doc_No: str=None) -> None:
    """分析建设银行网点结果目录"""
    # 分析是否存在因为超过9999条而分文件保存的流水文件
    if dir_path.is_dir():
        _file_names = list(dir_path.glob('[!~]*.xlsx')) # 找到目录中所有的excel文件（不含子目录）
         # 检查是否存在超过9999条的数据（文件名第二字段的数字一样）
        _d, _c = Counter(map(lambda x: x.name.split('_')[1], _file_names)).most_common(1)[0]
        if _c > 1: 
            raise Exception(f'目录中包含分开保存的同账户流水文件，需要手动合并，文件名第二字段为{_d}')
        return process_files_general(_file_names, output_dir, doc_No)
    else:
        raise Exception("传入的路径不是目录，请传入目录路径，或使用单文件分析") 

