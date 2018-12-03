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
        >>> info = load_info(cat='assets')
        >>> all(cat in info for cat in ['Equity', 'Index', 'Curncy', 'Corp'])
        True
        >>> pd.DataFrame(info)['EquityUS'].allday
        [400, 2000]
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
