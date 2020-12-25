import pandas as pd

import sqlite3
import json

WAL_MODE = 'PRAGMA journal_mode=WAL'
ALL_TABLES = 'SELECT name FROM sqlite_master WHERE type="table"'


class Singleton(type):

    _instances_ = {}

    def __call__(cls, *args, **kwargs):
        # Default values for class init
        default_keys = ['db_file', 'keep_live']
        kw = {**dict(zip(default_keys, args)), **kwargs}
        kw['keep_live'] = kw.get('keep_live', False)

        # Singleton instance
        key = json.dumps(kw)
        if key not in cls._instances_:
            cls._instances_[key] = super(Singleton, cls).__call__(**kw)
        return cls._instances_[key]


class SQLite(metaclass=Singleton):
    """
    Examples:
        >>> from xbbg.io import files
        >>>
        >>> db_file_ = f'{files.abspath(__file__, 1)}/tests/xone.db'
        >>> with SQLite(db_file_) as con_:
        ...     _ = con_.execute('DROP TABLE IF EXISTS xone')
        ...     _ = con_.execute('CREATE TABLE xone (rowid int)')
        >>> db_ = SQLite(db_file_)
        >>> db_.tables()
        ['xone']
        >>> db_.replace_into(table='xone', rowid=1)
        >>> db_.select(table='xone')
           rowid
        0      1
        >>> db_.replace_into(
        ...     table='xone',
        ...     data=pd.DataFrame([{'rowid': 2}, {'rowid': 3}])
        ... )
        >>> db_.select(table='xone')
           rowid
        0      1
        1      2
        2      3
    """

    def __init__(self, db_file, keep_live=False):

        self.db_file = db_file
        self.keep_live = keep_live
        self._con_ = None

    def tables(self) -> list:
        """
        All tables within database
        """
        keep_live = self.is_live
        res = self.con.execute(ALL_TABLES).fetchall()
        if not keep_live: self.close()
        return [r[0] for r in res]

    def select(self, table: str, cond='', **kwargs) -> pd.DataFrame:
        """
        SELECT query
        """
        keep_live = self.is_live
        q_str = select(table=table, cond=cond, **kwargs)
        data = self.con.execute(q_str).fetchall()
        if not keep_live: self.close()
        return pd.DataFrame(data, columns=self.columns(table=table))

    def select_recent(
            self,
            table: str,
            dateperiod: str,
            date_col: str = 'modified_date',
            cond='',
            **kwargs
    ) -> pd.DataFrame:
        """
        Select recent

        Args:
            table: table name
            dateperiod: time period, e.g., 1M, 1Q, etc.
            date_col: column for time period
            cond: conditions
            **kwargs: other select criteria

        Returns:
            pd.DataFrame
        """
        cols = self.columns(table=table)
        if date_col not in cols: return pd.DataFrame()
        start_dt = (
            pd.date_range(
                end='today', freq=dateperiod, periods=2, normalize=True,
            )[0]
            .strftime('%Y-%m-%d')
        )
        return (
            self.select(table=table, cond=cond, **kwargs)
            .query(f'{date_col} >= {start_dt}')
            .reset_index(drop=True)
        )

    def columns(self, table: str):
        """
        Table columns
        """
        return [
            info[1] for info in (
                self.con.execute(f'PRAGMA table_info (`{table}`)').fetchall()
            )
        ]

    def replace_into(self, table: str, data: pd.DataFrame = None, **kwargs):
        """
        Replace records into table

        Args:
            table: table name
            data: DataFrame - if given, **kwargs will be ignored
            **kwargs: record values
        """
        if isinstance(data, pd.DataFrame):
            keep_live = self.is_live
            cols = ', '.join(map(lambda v: f'`{v}`', data.columns))
            vals = ', '.join(['?'] * data.shape[1])
            # noinspection PyTypeChecker
            self.con.executemany(
                f'REPLACE INTO `{table}` ({cols}) values ({vals})',
                data.apply(tuple, axis=1).tolist()
            )
        else:
            keep_live = self.is_live or kwargs.get('_live_', False)
            self.con.execute(replace_into(table=table, **{
                k: v for k, v in kwargs.items() if k != '_live_'
            }))
        if not keep_live: self.close()

    @property
    def is_live(self) -> bool:
        if not isinstance(self._con_, sqlite3.Connection):
            return False
        try:
            self._con_.execute(ALL_TABLES)
            return True
        except sqlite3.Error:
            return False

    @property
    def con(self) -> sqlite3.Connection:
        if not self.is_live:
            self._con_ = sqlite3.connect(self.db_file)
            self._con_.execute(WAL_MODE)
        return self._con_

    def close(self, keep_live=False):
        try:
            self._con_.commit()
            if not keep_live: self._con_.close()
        except sqlite3.ProgrammingError:
            pass
        except sqlite3.Error as e:
            print(e)

    def __enter__(self):
        return self.con.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close(keep_live=self.keep_live)


def db_value(val) -> str:
    """
    Database value as in query string
    """
    if isinstance(val, str):
        return json.dumps(val.replace('\"', '').strip())
    return json.dumps(val, default=str)


def select(table: str, cond='', **kwargs) -> str:
    """
    Query string of SELECT statement

    Args:
        table: table name
        cond: conditions
        **kwargs: data as kwargs

    Examples:
        >>> q1 = select('daily', ticker='ES1 Index', price=3000)
        >>> q1.splitlines()[-2].strip()
        'ticker="ES1 Index" AND price=3000'
        >>> q2 = select('daily', cond='price > 3000', ticker='ES1 Index')
        >>> q2.splitlines()[-2].strip()
        'price > 3000 AND ticker="ES1 Index"'
        >>> q3 = select('daily', cond='price > 3000')
        >>> q3.splitlines()[-2].strip()
        'price > 3000'
        >>> select('daily')
        'SELECT * FROM `daily`'
    """
    all_cond = [cond] + [
        f'{key}={db_value(value)}'
        for key, value in kwargs.items()
    ]
    where = ' AND '.join(filter(bool, all_cond))
    s = f'SELECT * FROM `{table}`'
    if where:
        return f"""
            {s}
            WHERE
            {where}
        """
    return s


def replace_into(table: str, **kwargs) -> str:
    """
    Query string of REPLACE INTO statement

    Args:
        table: table name
        **kwargs: data as kwargs

    Examples:
        >>> query = replace_into('daily', ticker='ES1 Index', price=3000)
        >>> query.splitlines()[1].strip()
        'REPLACE INTO `daily` (ticker, price)'
        >>> query.splitlines()[2].strip()
        'VALUES ("ES1 Index", 3000)'
    """
    return f"""
        REPLACE INTO `{table}` ({', '.join(list(kwargs.keys()))})
        VALUES ({', '.join(map(db_value, list(kwargs.values())))})
    """
