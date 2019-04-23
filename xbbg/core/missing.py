import os

from xbbg.io import files
from xbbg.core import utils
from xbbg.core.assist import BBG_ROOT


def missing_info(**kwargs) -> str:
    """
    Full infomation for missing query
    """
    func = kwargs.pop('func', 'unknown')
    if 'ticker' in kwargs: kwargs['ticker'] = kwargs['ticker'].replace('/', '_')
    info = utils.to_str(kwargs, fmt='{value}', sep='/')[1:-1]
    return f'{func}/{info}'


def current_missing(**kwargs) -> int:
    """
    Check number of trials for missing values

    Returns:
        int: number of trials already tried
    """
    data_path = os.environ.get(BBG_ROOT, '').replace('\\', '/')
    if not data_path: return 0
    return len(files.all_files(f'{data_path}/Logs/{missing_info(**kwargs)}'))


def update_missing(**kwargs):
    """
    Update number of trials for missing values
    """
    data_path = os.environ.get(BBG_ROOT, '').replace('\\', '/')
    if not data_path: return
    if len(kwargs) == 0: return

    log_path = f'{data_path}/Logs/{missing_info(**kwargs)}'

    cnt = len(files.all_files(log_path)) + 1
    files.create_folder(log_path)
    open(f'{log_path}/{cnt}.log', 'a').close()
