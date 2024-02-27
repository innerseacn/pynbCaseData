
import yaml, pathlib
from collections import namedtuple

# 加载全局配置文件
def load_conf(conf_dir: str) -> dict:
    _dir = pathlib.Path(conf_dir)
    _result = {}
    for _file in _dir.glob('[!#]*.yaml'):
        with open(_file, 'r', encoding='utf-8') as f:
            _d = yaml.safe_load(f)
            _result.update(_d)
    return _result

CONF_DATA = load_conf('./config.yaml.d')
OUTPUT_FORMAT = CONF_DATA['output_format']
HEADER_HASH = CONF_DATA['header_hash']

Conf_tpl = namedtuple('Conf_tpl', 'bank_name new_cols verify_cols col_name_map merge_2cols date_cols time_cols digi_cols cdid fill_cols cols_new_order')

CONF_TPL_CACHE: dict[str:Conf_tpl] = {}

def get_conf_obj(bank_name: str, acc_or_stat: str) -> Conf_tpl: # type: ignore
    """先在缓存中查找操作配置，如缓存中没有则转换配置并存入缓存"""
    _key_str = bank_name + acc_or_stat
    _conf_obj = CONF_TPL_CACHE.get(_key_str, None)
    if _conf_obj is None:
        _conf_obj = creat_conf_obj(CONF_DATA[bank_name][acc_or_stat])
        CONF_TPL_CACHE[_key_str] = _conf_obj
    return _conf_obj

def creat_conf_obj(conf_data: dict) -> Conf_tpl: # type: ignore
    """将银行配置转换成操作配置"""
    _new_cols = {} # 构造需要填充统一内容的列字典，通常是新列
    _verify_cols = {} # 构造需要进行数据检查的列字典，（v1）版本只检查空值
    _col_name_map = {} # 构造需要改名的列字典
    _merge_2cols = {} # 构造需要合并的列字典
    _date_cols = {} # 构造转换日期列字典
    _time_cols = {} # 构造转换时间列字典
    _digi_cols = {} # 构造转换数值列字典
    _cdid = {} # 构造借贷分列转换字典
    _fill_cols = {} # 构造列条件填充字典
    
    #根据配置信息生成列处理逻辑
    for _key, _val in conf_data.items(): # 读取配置中的列名（key）和列配置（val）
        if _val is None: # 如果列配置为None，什么都不做
            continue
        elif type(_val) == str: # 如果列配置为str，则本列全部内容为该字符串，通常是全新的列
            _new_cols[_key] = _val
        elif type(_val) == bool and _val: # 如果列配置为bool且值为真，代表本列需要数据检查
            _verify_cols[_key] = True # （v1）版本只检查空值，此处恒为True
        elif type(_val) == int: # 如果列配置为int，代表本列需转换为数值；若值为真，代表本列需要数据检查
            if _val:
                _verify_cols[_key] = True
            _digi_cols[_key] = True # 加入转换为数值列字典
        elif type(_val) == dict: # 如果列配置为dict，则分情况讨论
            for __k, __v in _val.items(): # 首先检查所有字典值
                if __v: # 将值为真的加入数据检查中，兼容bool类型和int类型
                     _verify_cols[__k] = True
                if type(__v) == int:
                    _digi_cols[__k] = True # 加入转换为数值列字典
            if len(_val) == 1: # 当字典中只有一项时，代表本列需要改名
                _col_name_map[next(iter(_val))] = _key
            elif len(_val) > 1: # 当字典中有两项或更多时，代表本列需要合并多个原始列（目前仅支持两个）
                _merge_2cols[_key] = list(_val.keys())
            else:
                raise Exception(f"字典类型长度错误，检查配置列：{_key}") 
        elif type(_val) == list: 
            if _key != 'cols_new_order': # 进行扩展操作，目前支持date、time、C、D
                if _val[0] == 'date':
                    _date_cols[_key] = _val[1]
                elif _val[0] == 'time':
                    _time_cols[_key] = _val[1]
                elif _val[0] == 'C':
                    _cdid['C'] = _val[2]
                    _cdid['CD_col'] = _val[1]
                    _cdid['C_col'] = _key
                    _cdid['trans_col'] = _val[3]
                elif _val[0] == 'D':
                    _cdid['D'] = _val[2]
                    _cdid['CD_col'] = _val[1]
                    _cdid['D_col'] = _key
                    _cdid['trans_col'] = _val[3]
                elif _val[0] == 'fill':
                    _fill_cols[_key] = _val[1:]
                else:
                    raise Exception(f"配置为list类型目前只支持转换为date和time格式，检查配置列：{_key}")                 
        else:
            raise Exception(f"配置内容类型不支持，检查配置列：{_key}") 

    # 返回列处理逻辑对象        
    return Conf_tpl(None,
                            _new_cols,
                            _verify_cols, 
                            _col_name_map, 
                            _merge_2cols, 
                            _date_cols, 
                            _time_cols, 
                            _digi_cols,
                            _cdid, 
                            _fill_cols,
                            conf_data.get('cols_new_order')
                            )
