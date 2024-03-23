import pathlib as pb
from corelibs.process import process_files_accs_then_stats

def _validate_dir(dir_path: pb.Path) -> list:
    """校验目录有效性，返回待处理文件列表"""
    if not dir_path.is_dir():
        raise Exception("传入的路径不是目录，请传入目录路径，或使用单文件分析") 
    _file_names = list(filter(lambda f: not f.stem.endswith('关联子账户信息'), dir_path.glob('*客户*.xlsx')))
    return _file_names

def process_dir_yangdi(dir_path: pb.Path, output_dir: pb.Path, doc_No: str=None) -> None:
    """分析央地协查结果目录，依次处理账户和流水文件，并根据账户信息更新流水中的账号字段"""
    # 验证目录有效性
    _files = _validate_dir(dir_path)
    return process_files_accs_then_stats(_files, output_dir, doc_No)
    