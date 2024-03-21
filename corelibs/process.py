import pathlib
from tqdm.auto import tqdm
from corelibs.config import *
from corelibs.data import parse_sheet_general
from corelibs.header import read_header
from corelibs.storage import *
from hashlib import md5
import pandas as pd



def process_general_file(file: pathlib.Path, output_dir: pathlib.Path, bank_name: str, 
                         file_type: str, header=0) -> pd.DataFrame:
    """根据配置处理单个普通文件，并保存到特定目录"""
    _conf_obj = get_conf_obj(bank_name, file_type)
    _df = parse_sheet_general(file, _conf_obj, header=header)
    save_general(_df, output_dir, bank_name, file_type)
    return _df

def process_statment_file_general(file: pathlib.Path, output_dir: pathlib.Path, bank_name: str, 
                                  file_type: str, doc_No: str=None, df_acc: pd.DataFrame=None) -> pd.DataFrame:
    """根据配置处理单个流水文件， 如果提供账户信息则按照配置更新流水信息，并保存到指定目录"""
    _conf_obj = get_conf_obj(bank_name, file_type)
    _df_stat = parse_sheet_general(file, _conf_obj)
    if _conf_obj.acc_rel_cols and df_acc is not None:
        _df = fill_stat_cols_by_acc(_df_stat, df_acc, _conf_obj.acc_rel_cols)
    else:
        _df = _df_stat
    _grouped = _df.groupby('账号')
    _df_list = [x.reset_index(drop=True) for _ , x in _grouped]
    save_statements(_df_list, output_dir, bank_name, file_type, doc_No)
    return _df

def fill_stat_cols_by_acc(df_stat: pd.DataFrame, df_acc: pd.DataFrame, acc_rel_cols: dict) -> pd.DataFrame:
    for _k, _v in acc_rel_cols.items():
        _df_acc = df_acc[_v[:2]].drop_duplicates()
        df_stat = pd.merge(df_stat, _df_acc, left_on=_v[2], right_on=_v[0], how='left', 
                           validate='m:1', copy=False, suffixes=('', '_d'))
        _col_d = _v[1] + '_d'
        df_stat[_k] = df_stat[_col_d].combine_first(df_stat[_v[2]])
        # df_stat.drop(_col_d, axis=1, errors='ignore', inplace=True)
    return df_stat
        
def process_files_1by1(files_list: list, output_dir: pathlib.Path, doc_No: str=None) -> dict:
    """根据配置处理多个文件，跳过出错文件，返回出错文件字典。
    本函数依次处理每个文件，不能根据账户信息丰富流水数据。"""
    _err_file_dict = {} # 保存解析出错的文件和原因
    for _file in tqdm(files_list, desc='分析目录文件'):
        print(f'{_file.name}……', end='')
        _conf_name = get_file_type(_file)
        if _conf_name is None:
            print(_msg := '未找到对应配置，跳过')
            _err_file_dict[_file] = _msg
            continue
        else:
            print(f'{_conf_name[0]}', end=':')
            for x in _conf_name[1:]:
                try:
                    match x:
                        case '客户' | '账户':
                            process_general_file(_file, output_dir, _conf_name[0], x)
                        case '流水':
                            process_statment_file_general(_file, output_dir, _conf_name[0], x, doc_No)
                        case _:
                            raise Exception(f"{x}暂不支持") 
                except Exception as e:
                    print( _msg := str(e), end=':')
                    _err_file_dict[_file] = _conf_name[0] + _msg
                else:
                    print(f'{x}完成', end=':')
            print()
    print('\n'.join([f'{len(_err_file_dict)}个文件出错：'] + 
                    [f'{_f.name} => {_m}' for _f, _m in _err_file_dict.items()]))    
    return _err_file_dict
    
def process_files_accs_then_stats(files_list: list, output_dir: pathlib.Path, 
                                  doc_No: str=None, df_acc: pd.DataFrame=None) -> list:
    """根据配置处理多个文件，跳过出错文件，返回处理文件个数和出错文件列表。
    本函数先根据文件类型将文件分类，依次处理账户文件和流水文件，因此可以根据账户信息丰富流水数据。
    返回解析好的账户信息和出错文件字典组成的列表"""
    # 首先对文件列表根据表头类型进行分组，得到分组文件字典和出错文件字典
    _file_cate, _err_file_dict = classify_files_by_category(files_list)
    print(f"{len(_err_file_dict)}个文件未识别：[Y继续/非Y显示详情并退出]")
    if _err_file_dict and input().lower() != 'y':
        print('\n'.join([f'{_f.name} => {_m}' for _f, _m in _err_file_dict.items()]))
        return None
    # 对每一个银行首先处理所有账户文件，然后依次处理流水文件，并根据账户信息和配置填充流水文件相关列
    for _bank, _dict_files in _file_cate.items():
        _df_list = [] # 存储所有该银行账户文件DataFrame的列表
        _err_files_tmp = {} # 该银行下的临时错误文件字典

        # 先处理所有账户文件
        for _file in tqdm(_dict_files.pop('账户', []), desc=f'{_bank}:账户'):          
            print(f'{_file.name}……', end='')
            try:    
                _df_list.append(process_general_file(_file, output_dir, _bank, '账户'))
            except Exception as e:
                print( _msg := str(e))
                _err_files_tmp[_file] = _msg
                # raise e # debug
            else:
                print('完成')
        print(f"{len(_err_files_tmp)}个账户文件出错：[Y继续/非Y显示详情并退出]")
        if _err_files_tmp and input().lower() != 'y':
            print('\n'.join([f'{_f.name} => {_m}' for _f, _m in _err_files_tmp.items()]))
            return None
        _err_file_dict.update(_err_files_tmp)

        # 得到全部账户信息
        _df_acc = pd.concat(_df_list, ignore_index=True) if _df_list else None
        if df_acc:
            _df_acc = pd.concat([_df_acc, df_acc], ignore_index=True)

        _err_files_tmp = {} # 错误文件字典清零
        # 再依次处理流水文件
        for _file in tqdm(_dict_files.pop('流水', []), desc=f'{_bank}:流水'):          
            print(f'{_file.name}……', end='')
            try:    
                process_statment_file_general(_file, output_dir, _bank, '流水', doc_No, _df_acc)
            except Exception as e:
                print( _msg := str(e))
                _err_files_tmp[_file] = _msg
            else:
                print('完成')
        print(f"{len(_err_files_tmp)}个流水文件出错：")
        _err_file_dict.update(_err_files_tmp)

       # 最后处理所有客户文件
        for _file in tqdm(_dict_files.pop('客户', []), desc=f'{_bank}:客户'):          
            print(f'{_file.name}……', end='')
            try:    
                process_general_file(_file, output_dir, _bank, '客户')
            except Exception as e:
                print( _msg := str(e))
                _err_files_tmp[_file] = _msg
            else:
                print('完成')
        print(f"{len(_err_files_tmp)}个客户文件出错：")
        _err_file_dict.update(_err_files_tmp)
 
        if _dict_files:
            print(f'{_bank}:{[f for f in _dict_files]}暂不支持')
    print('\n'.join([f'共{len(_err_file_dict)}个文件出错：'] + [f'{_f.name} => {_m}' for _f, _m in _err_file_dict.items()]))    
    return [_df_acc, _err_file_dict]

def classify_files_by_category(files_list: list) -> tuple[dict, dict]:
    """将文件列表按照配置分组，返回分组后的字典和无法识别的文件字典"""
    _file_cate = {} # 保存识别后的文件类型
    _err_file_dict = {} # 保存解析出错的文件和原因
    for _file in tqdm(files_list, desc='识别文件类型'):
        print(f'{_file.name} => ', end='')
        _conf_name = get_file_type(_file)
        if _conf_name is None: # 如果未成功识别
            print(_msg := '未找到对应配置，跳过')
            _err_file_dict[_file] = _msg
        else:
            print(f'{":".join(_conf_name)}')
            for x in _conf_name[1:]:
                _file_cate.setdefault(_conf_name[0], {}).setdefault(x, []).append(_file)
    return _file_cate, _err_file_dict

def get_file_type(file: pathlib.Path) -> list:
    """根据文件表头找到该文件类型,亦即解析文件配置入口"""
    _header = read_header(file).encode() # 读取每个文件的表头
    return get_header_hash().get(md5(_header).hexdigest()) # 根据表头md5值找到相应的配置
