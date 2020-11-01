import os

from xbbg.io import files, db
from xbbg.core import utils
from xbbg.core.overrides import BBG_ROOT


TRIALS_TABLE = """
    CREATE TABLE IF NOT EXISTS trials (
        func varchar(20),
        ticker varchar(30),
        dt varchar(10),
        typ varchar(20),
        cnt int,
        PRIMARY KEY (func, ticker, dt, typ)
    )
"""


def root_path() -> str:
    """
    Root data path of Bloomberg
    """
    return os.environ.get(BBG_ROOT, '').replace('\\', '/')


def convert_exisiting():
    """
    Update existing missing logs to database
    """
    data_path = root_path()
    if not data_path: return

    with db.SQLite(f'{data_path}/Logs/xbbg.db') as con:
        con.execute(TRIALS_TABLE)
        for item in all_trials():
            con.execute(db.replace_into(table='trials', **item))


def all_trials() -> dict:
    """
    All missing logs

    Yields:
        dict
    """
    data_path = root_path()
    if data_path:
        for sub1 in files.all_folders(f'{data_path}/Logs/bdib'):
            for sub2 in files.all_folders(sub1, has_date=True):
                for sub3 in files.all_folders(sub2):
                    cnt = len(files.all_files(sub3, ext='log'))
                    if cnt:
                        yield dict(
                            func='bdib',
                            ticker=sub1.split('/')[-1],
                            dt=sub2.split('/')[-1],
                            typ=sub3.split('/')[-1],
                            cnt=cnt,
                        )


def trail_info(**kwargs) -> dict:
    """
    Convert info to proper format for databse

    Returns:
        dict
    """
    kwargs['func'] = kwargs.pop('func', 'unknown')
    if 'ticker' in kwargs:
        kwargs['ticker'] = kwargs['ticker'].replace('/', '_')
    for dt in ['dt', 'start_dt', 'end_dt', 'start_date', 'end_date']:
        if dt not in kwargs: continue
        kwargs[dt] = utils.fmt_dt(kwargs[dt])
    return kwargs


def missing_info(**kwargs) -> str:
    """
    Full infomation for missing query
    """
    func = kwargs.pop('func', 'unknown')
    if 'ticker' in kwargs: kwargs['ticker'] = kwargs['ticker'].replace('/', '_')
    for dt in ['dt', 'start_dt', 'end_dt', 'start_date', 'end_date']:
        if dt not in kwargs: continue
        kwargs[dt] = utils.fmt_dt(kwargs[dt])
    info = utils.to_str(kwargs, fmt='{value}', sep='/')[1:-1]
    return f'{func}/{info}'


def num_trials(**kwargs) -> int:
    """
    Check number of trials for missing values

    Returns:
        int: number of trials already tried
    """
    data_path = root_path()
    if not data_path: return 0

    with db.SQLite(f'{data_path}/Logs/xbbg.db') as con:
        con.execute(TRIALS_TABLE)
        num = con.execute(db.select(
            table='trials',
            **trail_info(**kwargs),
        )).fetchall()
        if not num: return 0
        return num[0][-1]


def update_trials(**kwargs):
    """
    Update number of trials for missing values
    """
    data_path = root_path()
    if not data_path: return

    if 'cnt' not in kwargs:
        kwargs['cnt'] = num_trials(**kwargs) + 1

    with db.SQLite(f'{data_path}/Logs/xbbg.db') as con:
        con.execute(TRIALS_TABLE)
        con.execute(db.replace_into(
            table='trials',
            **trail_info(**kwargs),
        ))


def current_missing(**kwargs) -> int:
    """
    Check number of trials for missing values

    Returns:
        int: number of trials already tried
    """
    data_path = root_path()
    if not data_path: return 0
    return len(files.all_files(f'{data_path}/Logs/{missing_info(**kwargs)}'))


def update_missing(**kwargs):
    """
    Update number of trials for missing values
    """
    data_path = root_path()
    if not data_path: return
    if len(kwargs) == 0: return

    log_path = f'{data_path}/Logs/{missing_info(**kwargs)}'

    cnt = len(files.all_files(log_path)) + 1
    files.create_folder(log_path)
    open(f'{log_path}/{cnt}.log', 'a').close()
