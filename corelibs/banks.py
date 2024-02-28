import pathlib
from collections import Counter
from corelibs.process import process_files_general



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

