import pandas as pd

import os
import sys

from ruamel.yaml import YAML

from xbbg.io import files

PKG_PATH = files.abspath(__file__, 1)


def load_info(cat):
    """
    Load parameters for assets

    Args:
        cat: category

    Returns:
        dict

    Examples:
        >>> assets = load_info(cat='assets')
        >>> for cat_ in ['Equity', 'Index', 'Curncy', 'Corp']:
        ...     if cat_ not in assets: print(f'Missing cateogry: {cat_}')
        >>> os.environ['BBG_PATH'] = ''
        >>> exch = load_info(cat='exch')
        >>> pd.Series(exch['EquityUS']).allday
        [400, 2000]
        >>> test_root = f'{PKG_PATH}/tests'
        >>> os.environ['BBG_PATH'] = test_root
        >>> ovrd_exch = load_info(cat='exch')
        >>> # Somehow os.environ is not set properly in doctest environment
        >>> ovrd_exch.update(_load_yaml_(f'{test_root}/markets/exch.yml'))
        >>> pd.Series(ovrd_exch['EquityUS']).allday
        [300, 2100]
    """
    yaml_file = f'{PKG_PATH}/markets/{cat}.yml'
    root = os.environ.get('BBG_ROOT', '').replace('\\', '/')
    yaml_ovrd = f'{root}/markets/{cat}.yml' if root else ''
    if not files.exists(yaml_ovrd): yaml_ovrd = ''

    pkl_file = f'{PKG_PATH}/markets/cached/{cat}.pkl'
    ytime = files.file_modified_time(yaml_file)
    if yaml_ovrd: ytime = max(ytime, files.file_modified_time(yaml_ovrd))
    if files.exists(pkl_file) and files.file_modified_time(pkl_file) > ytime:
        return pd.read_pickle(pkl_file).to_dict()

    res = _load_yaml_(yaml_file)
    if yaml_ovrd:
        for cat, ovrd in _load_yaml_(yaml_ovrd).items():
            if isinstance(ovrd, dict):
                if cat in res: res[cat].update(ovrd)
                else: res[cat] = ovrd
            if isinstance(ovrd, list) and isinstance(res[cat], list):
                res[cat] += ovrd

    if not hasattr(sys, 'pytest_call'):
        files.create_folder(pkl_file, is_file=True)
        pd.Series(res).to_pickle(pkl_file)

    return res


def _load_yaml_(file_name):
    """
    Load assets infomation from file

    Args:
        file_name: file name

    Returns:
        dict
    """
    if not os.path.exists(file_name): return dict()

    with open(file_name, 'r', encoding='utf-8') as fp:
        return YAML().load(stream=fp)


def to_hour(num) -> str:
    """
    Convert YAML input to hours

    Args:
        num: number in YMAL file, e.g., 900, 1700, etc.

    Returns:
        str

    Examples:
        >>> to_hour(900)
        '09:00'
        >>> to_hour(1700)
        '17:00'
    """
    to_str = str(int(num))
    return pd.Timestamp(f'{to_str[:-2]}:{to_str[-2:]}').strftime('%H:%M')
