import pandas as pd

import os

from typing import Union
from ruamel.yaml import YAML
from xbbg.io import files

PKG_PATH = files.abspath(__file__, 1)


def config_files(cat: str) -> list:
    """
    Category files

    Args:
        cat: category

    Returns:
        list of files that exist
    """
    return [
        f'{r}/markets/{cat}.yml'
        for r in [
            PKG_PATH,
            os.environ.get('BBG_ROOT', '').replace('\\', '/'),
        ]
        if files.exists(f'{r}/markets/{cat}.yml')
    ]


def load_config(cat: str) -> pd.DataFrame:
    """
    Load market info that can apply pd.Series directly

    Args:
        cat: category name

    Returns:
        pd.DataFrame
    """
    cfg_files = config_files(cat=cat)
    cache_cfg = f'{PKG_PATH}/markets/cached/{cat}_cfg.pkl'
    last_mod = max(map(files.modified_time, cfg_files))
    if files.exists(cache_cfg) and files.modified_time(cache_cfg) > last_mod:
        return pd.read_pickle(cache_cfg)

    config = (
        pd.concat([
            load_yaml(cf).apply(pd.Series)
            for cf in cfg_files
        ], sort=False)
    )
    files.create_folder(cache_cfg, is_file=True)
    config.to_pickle(cache_cfg)
    return config


def load_yaml(yaml_file: str) -> pd.Series:
    """
    Load yaml from cache

    Args:
        yaml_file: YAML file name

    Returns:
        pd.Series
    """
    cache_file = (
        yaml_file
        .replace('/markets/', '/markets/cached/')
        .replace('.yml', '.pkl')
    )
    cur_mod = files.modified_time(yaml_file)
    if files.exists(cache_file) and files.modified_time(cache_file) > cur_mod:
        return pd.read_pickle(cache_file)

    with open(yaml_file, 'r') as fp:
        data = pd.Series(YAML().load(fp))
        files.create_folder(cache_file, is_file=True)
        data.to_pickle(cache_file)
        return data


def to_hours(num_ts: Union[str, list, int, float]) -> Union[str, list]:
    """
    Convert YAML input to hours

    Args:
        num_ts: list of number in YMAL file, e.g., 900, 1700, etc.

    Returns:
        str

    Examples:
        >>> to_hours([900, 1700])
        ['09:00', '17:00']
        >>> to_hours(901)
        '09:01'
        >>> to_hours('XYZ')
        'XYZ'
    """
    if isinstance(num_ts, str): return num_ts
    if isinstance(num_ts, (int, float)):
        return f'{int(num_ts / 100):02d}:{int(num_ts % 100):02d}'
    return [to_hours(num) for num in num_ts]
