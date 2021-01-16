import pandas as pd

import os
import re
import time

from typing import List
from pathlib import Path

DATE_FMT = r'\d{4}-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])'


def exists(path) -> bool:
    """
    Check path or file exists (use os.path.exists)

    Args:
        path: path or file
    """
    if not path: return False
    return Path(path).is_dir() or Path(path).is_file()


def abspath(cur_file, parent=0) -> Path:
    """
    Absolute path

    Args:
        cur_file: __file__ or file or path str
        parent: level of parent to look for

    Returns:
        str
    """
    p = Path(cur_file)
    cur_path = p.parent if p.is_file() else p
    if parent == 0: return str(cur_path).replace('\\', '/')
    return abspath(cur_file=cur_path.parent, parent=parent - 1)


def create_folder(path_name: str, is_file=False):
    """
    Make folder as well as all parent folders if not exists

    Args:
        path_name: full path name
        is_file: whether input is name of file
    """
    p = Path(path_name).parent if is_file else Path(path_name)
    p.mkdir(parents=True, exist_ok=True)


def all_files(
        path_name, keyword='', ext='', full_path=True,
        has_date=False, date_fmt=DATE_FMT
) -> List[str]:
    """
    Search all files with criteria
    Returned list will be sorted by last modified

    Args:
        path_name: full path name
        keyword: keyword to search
        ext: file extensions, split by ','
        full_path: whether return full path (default True)
        has_date: whether has date in file name (default False)
        date_fmt: date format to check for has_date parameter

    Returns:
        list: all file names with criteria fulfilled
    """
    p = Path(path_name)
    if not p.is_dir(): return []

    keyword = f'*{keyword}*' if keyword else '*'
    keyword += f'.{ext}' if ext else '.*'
    r = re.compile(f'.*{date_fmt}.*')
    return [
        str(f).replace('\\', '/') if full_path else f.name
        for f in p.glob(keyword)
        if f.is_file() and (f.name[0] != '~') and ((not has_date) or r.match(f.name))
    ]


def all_folders(
        path_name, keyword='', has_date=False, date_fmt=DATE_FMT
) -> List[str]:
    """
    Search all folders with criteria
    Returned list will be sorted by last modified

    Args:
        path_name: full path name
        keyword: keyword to search
        has_date: whether has date in file name (default False)
        date_fmt: date format to check for has_date parameter

    Returns:
        list: all folder names fulfilled criteria
    """
    p = Path(path_name)
    if not p.is_dir(): return []

    r = re.compile(f'.*{date_fmt}.*')
    return [
        str(f).replace('\\', '/')
        for f in p.glob(f'*{keyword}*' if keyword else '*')
        if f.is_dir() and (f.name[0] != '~') and ((not has_date) or r.match(f.name))
    ]


def sort_by_modified(files_or_folders: list) -> list:
    """
    Sort files or folders by modified time

    Args:
        files_or_folders: list of files or folders

    Returns:
        list
    """
    return sorted(files_or_folders, key=os.path.getmtime, reverse=True)


def filter_by_dates(files_or_folders: list, date_fmt=DATE_FMT) -> list:
    """
    Filter files or dates by date patterns

    Args:
        files_or_folders: list of files or folders
        date_fmt: date format

    Returns:
        list
    """
    r = re.compile(f'.*{date_fmt}.*')
    return list(filter(
        lambda v: r.match(Path(v).name) is not None,
        files_or_folders,
    ))


def latest_file(path_name, keyword='', ext='', **kwargs) -> str:
    """
    Latest modified file in folder

    Args:
        path_name: full path name
        keyword: keyword to search
        ext: file extension

    Returns:
        str: latest file name
    """
    files = sort_by_modified(
        all_files(path_name=path_name, keyword=keyword, ext=ext, full_path=True)
    )

    if not files:
        from xbbg.io import logs

        logger = logs.get_logger(latest_file, level=kwargs.pop('log', 'warning'))
        logger.debug(f'no file in folder: {path_name}')
        return ''

    return str(files[0]).replace('\\', '/')


def modified_time(file_name):
    """
    File modified time in python

    Args:
        file_name: file name

    Returns:
        pd.Timestamp
    """
    return pd.to_datetime(time.ctime(os.path.getmtime(filename=file_name)))
