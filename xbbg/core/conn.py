import pytest

from xbbg.core import utils
from xbbg.io import files
from functools import wraps

try:
    import pdblp
except ImportError:
    pdblp = utils.load_module(f'{files.abspath(__file__)}/pdblp.py')

_CON_SYM_ = '_xcon_'

if hasattr(pytest, 'config'):
    if not pytest.config.option.with_bbg:
        pytest.skip('no Bloomberg')


def with_bloomberg(func):
    """
    Wrapper function for Bloomberg connection

    Args:
        func: function to wrap
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        _, new = create_connection()
        res = func(*args, **kwargs)
        if new: delete_connection()
        return res
    return wrapper


def create_connection():
    """
    Create Bloomberg connection

    Returns:
        (Bloomberg connection, if connection is new)
    """
    if _CON_SYM_ in globals():
        if not isinstance(globals()[_CON_SYM_], pdblp.BCon):
            del globals()[_CON_SYM_]

    if _CON_SYM_ in globals():
        con = globals()[_CON_SYM_]
        if getattr(con, '_session').start(): con.start()
        return con, False

    else:
        con = pdblp.BCon(port=8194, timeout=5000)
        globals()[_CON_SYM_] = con
        con.start()
        return con, True


def delete_connection():
    """
    Stop and destroy Bloomberg connection
    """
    if _CON_SYM_ in globals():
        con = globals().pop(_CON_SYM_)
        if not getattr(con, '_session').start(): con.stop()
