import pathlib
from tqdm.notebook import tqdm
from corelibs.config import *
from corelibs.data import parse_sheet_general
from corelibs.header import read_header
from corelibs.storage import *
from hashlib import md5



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
    for _file in tqdm(files_list):
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

