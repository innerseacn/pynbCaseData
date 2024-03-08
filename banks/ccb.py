import pathlib as pb
from collections import Counter
from corelibs.process import *
import itertools as it
from tqdm.auto import tqdm

def _validate_dir(dir_path: pb.Path) -> list:
    """校验目录有效性，返回待处理文件列表"""
      # 分析是否存在因为超过9999条而分文件保存的流水文件
    if not dir_path.is_dir():
        raise Exception("传入的路径不是目录，请传入目录路径，或使用单文件分析") 
    _file_names = list(dir_path.glob('[!~]*.xlsx')) # 找到目录中所有的excel文件（不含子目录）
     # 检查是否存在超过9999条的数据（文件名第二字段的数字一样）
    _d, _c = Counter(map(lambda x: x.name.split('_')[1], _file_names)).most_common(1)[0]
    if _c > 1: 
        raise Exception(f'目录中包含分开保存的同账户流水文件，需要手动合并，文件名第二字段为{_d}')
    return _file_names

def process_dir_ccb_branch_v1(dir_path: pb.Path, output_dir: pb.Path, doc_No: str=None) -> None:
    """分析建设银行网点结果目录第一版，不分账户流水混合处理，无法根据账户信息更新流水中的账号字段"""
    #验证目录有效性
    _files = _validate_dir(dir_path)
    return process_files_1by1(_files, output_dir, doc_No)

def process_dir_ccb_branch_v2(dir_path: pb.Path, output_dir: pb.Path, doc_No: str=None) -> None:
    """分析建设银行网点结果目录第二版，根据目录特点依次处理账hu和流水文件，并根据账户信息更新流水中的账号字段"""
    # 验证目录有效性
    _files = _validate_dir(dir_path)
    return process_files_accs_then_stats(_files, output_dir, doc_No)
    