import json
import os
import time

from xbbg.io import files
from xbbg.core.assist import BBG_ROOT, info_key

_MISSING_ = '_missing_'


def current_missing(**kwargs):
    """
    Check number of trials for missing values

    Returns:
        dict
    """
    cur_miss = globals().get(_MISSING_, dict())
    if not cur_miss:
        data_path = os.environ.get(BBG_ROOT, '').replace('\\', '/')
        empty_log = f'{data_path}/Logs/EmptyQueries.json'
        if not files.exists(empty_log): return 0
        with open(empty_log, 'r') as fp:
            cur_miss = json.load(fp=fp)
        globals()[_MISSING_] = cur_miss

    return cur_miss.get(info_key(**kwargs), 0)


def update_missing(**kwargs):
    """
    Update number of trials for missing values

    Returns:
        dict
    """
    key = info_key(**kwargs)

    data_path = os.environ.get(BBG_ROOT, '').replace('\\', '/')
    empty_log = f'{data_path}/Logs/EmptyQueries.json'

    cur_miss = dict()
    if files.exists(empty_log):
        with open(empty_log, 'r') as fp:
            cur_miss = json.load(fp=fp)

    cur_miss[key] = cur_miss.get(key, 0) + 1
    while not os.access(empty_log, os.W_OK): time.sleep(1)
    with open(empty_log, 'w') as fp:
        json.dump(cur_miss, fp=fp, indent=2)

    globals()[_MISSING_] = cur_miss
    return cur_miss
