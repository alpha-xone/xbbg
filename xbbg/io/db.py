import sqlite3
import json

WAL_MODE = 'PRAGMA journal_mode=WAL'
ALL_TABLES = 'SELECT name FROM sqlite_schema WHERE type="table"'


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

    def select(self, table: str, **kwargs) -> list:
        """
        SELECT query
        """
        keep_live = self.is_live
        res = self.con.execute(select(table=table, **kwargs)).fetchall()
        if not keep_live: self.close()
        return res

    def replace_into(self, table: str, **kwargs):
        """
        REPLACE INTO
        """
        keep_live = self.is_live
        self.con.execute(replace_into(table=table, **kwargs))
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
    return f'"{val}"' if isinstance(val, str) else str(val)


def select(table: str, **kwargs) -> str:
    """
    Query string of SELECT statement

    Args:
        table: table name
        **kwargs: data as kwargs

    Examples:
        >>> query = select('daily', ticker='ES1 Index', price=3000)
        >>> query.splitlines()[-2].strip()
        'ticker="ES1 Index" AND price=3000'
        >>> select('daily')
        'SELECT * FROM daily'
    """
    where = ' AND '.join(
        f'{key}={db_value(value)}'
        for key, value in kwargs.items()
    )
    s = f'SELECT * FROM {table}'
    if kwargs:
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
        'REPLACE INTO daily (ticker, price)'
        >>> query.splitlines()[2].strip()
        'VALUES ("ES1 Index", 3000)'
    """
    return f"""
        REPLACE INTO {table} ({', '.join(list(kwargs.keys()))})
        VALUES ({', '.join(map(db_value, list(kwargs.values())))})
    """
