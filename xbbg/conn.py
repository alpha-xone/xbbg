import pdblp

from functools import wraps


def with_bloomberg(func):
    """
    Wrapper function for Bloomberg connection

    Args:
        func: function to wrap
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        con, new = create_connection()
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
    if '_xcon_' in globals():
        con = globals()['_xcon_']
        assert isinstance(con, pdblp.BCon)
        if getattr(con, '_session').start(): con.start()
        return con, False

    else:
        con = pdblp.BCon(port=8194, timeout=5000)
        globals()['_xcon_'] = con
        con.start()
        return con, True


def delete_connection():
    """
    Stop and destroy Bloomberg connection
    """
    if '_xcon_' in globals():
        con = globals().pop('_xcon_')
        if not getattr(con, '_session').start(): con.stop()
