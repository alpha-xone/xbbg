import yaml
import os

PKG_PATH = '/'.join(os.path.abspath(__file__).replace('\\', '/').split('/')[:-2])


def load_info(cat):
    """
    Load parameters for assets

    Args:
        cat: category

    Returns:
        dict

    Examples:
        >>> import pandas as pd
        >>>
        >>> assets = load_info(cat='assets')
        >>> all(cat in assets for cat in ['Equity', 'Index', 'Curncy', 'Corp'])
        True
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
    res = _load_yaml_(f'{PKG_PATH}/markets/{cat}.yml')
    root = os.environ.get('BBG_ROOT', '')
    if root: res.update(_load_yaml_(f'{root}/markets/{cat}.yml'))
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
        return yaml.load(fp)
