import pathlib
import openpyxl as op, xlrd as xl
from itertools import islice


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

