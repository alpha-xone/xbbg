import pandas as pd

import os
import re
import glob
import time

DATE_FMT = r'\d{4}-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])'


def exists(path) -> bool:
    """
    Check path or file exists (use os.path.exists)

    Args:
        path: path or file
    """
    return os.path.exists(path=path)


def abspath(cur_file, parent=0) -> str:
    """
    Absolute path

    Args:
        cur_file: __file__ or file or path str
        parent: level of parent to look for

    Returns:
        str
    """
    file_path = os.path.abspath(cur_file).replace('\\', '/')
    if os.path.isdir(file_path) and parent == 0: return file_path
    adj = 1 - os.path.isdir(file_path)
    return '/'.join(file_path.split('/')[:-(parent + adj)])


def create_folder(path_name: str, is_file=False):
    """
    Make folder as well as all parent folders if not exists

    Args:
        path_name: full path name
        is_file: whether input is name of file
    """
    path_sep = path_name.replace('\\', '/').split('/')
    for i in range(1, len(path_sep) + (0 if is_file else 1)):
        cur_path = '/'.join(path_sep[:i])
        if not cur_path: continue
        if not os.path.exists(cur_path): os.mkdir(cur_path)


def all_files(
        path_name, keyword='', ext='', full_path=True,
        has_date=False, date_fmt=DATE_FMT
) -> list:
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
    if not os.path.exists(path=path_name): return []
    path_name = path_name.replace('\\', '/')

    if keyword or ext:
        keyword = f'*{keyword}*' if keyword else '*'
        if not ext: ext = '*'
        files = sort_by_modified([
            f.replace('\\', '/') for f in glob.iglob(f'{path_name}/{keyword}.{ext}')
            if os.path.isfile(f) and (f.replace('\\', '/').split('/')[-1][0] != '~')
        ])

    else:
        files = sort_by_modified([
            f'{path_name}/{f}' for f in os.listdir(path=path_name)
            if os.path.isfile(f'{path_name}/{f}') and (f[0] != '~')
        ])

    if has_date:
        files = filter_by_dates(files, date_fmt=date_fmt)

    return files if full_path else [f.split('/')[-1] for f in files]


def all_folders(
        path_name, keyword='', has_date=False, date_fmt=DATE_FMT
) -> list:
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
    if not os.path.exists(path=path_name): return []
    path_name = path_name.replace('\\', '/')

    if keyword:
        folders = sort_by_modified([
            f.replace('\\', '/') for f in glob.iglob(f'{path_name}/*{keyword}*')
            if os.path.isdir(f) and (f.replace('\\', '/').split('/')[-1][0] != '~')
        ])

    else:
        folders = sort_by_modified([
            f'{path_name}/{f}' for f in os.listdir(path=path_name)
            if os.path.isdir(f'{path_name}/{f}') and (f[0] != '~')
        ])

    if has_date:
        folders = filter_by_dates(folders, date_fmt=date_fmt)

    return folders


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
        lambda vv: r.match(vv.replace('\\', '/').split('/')[-1]) is not None,
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
    files = all_files(
        path_name=path_name, keyword=keyword, ext=ext, full_path=True
    )

    if not files:
        from xbbg.io import logs

        logger = logs.get_logger(latest_file, level=kwargs.pop('log', 'warning'))
        logger.debug(f'file is not found in folder: {path_name}')
        return ''

    mod_time = [os.path.getmtime(f) for f in files]
    files = [f for (dt, f) in sorted(zip(mod_time, files))]

    return files[-1]


def modified_time(file_name):
    """
    File modified time in python

    Args:
        file_name: file name

    Returns:
        pd.Timestamp
    """
    return pd.to_datetime(time.ctime(os.path.getmtime(filename=file_name)))
