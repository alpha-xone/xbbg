import blpapi

from xbbg.io import logs

_CON_SYM_ = '_xcon_'
_PORT_ = 8194


def connect_bbg(**kwargs) -> blpapi.session.Session:
    """
    Create Bloomberg session and make connection
    """
    logger = logs.get_logger(connect_bbg, **kwargs)

    sess_opts = blpapi.SessionOptions()
    sess_opts.setServerHost('localhost')
    sess_opts.setServerPort(kwargs.get('port', _PORT_))
    session = blpapi.Session(sess_opts)
    logger.debug('Connecting to Bloomberg ...')
    if session.start(): return session
    else: raise ConnectionError('Cannot connect to Bloomberg')


def bbg_session(**kwargs) -> blpapi.session.Session:
    """
    Bloomberg session - initiate if not given

    Args:
        **kwargs:
            port: port number (default 8194)
            restart: whether to restart session

    Returns:
        Bloomberg session instance
    """
    port = kwargs.get('port', _PORT_)
    con_sym = f'{_CON_SYM_}//{port}'

    if con_sym in globals():
        if not isinstance(globals()[con_sym], blpapi.session.Session):
            del globals()[con_sym]

    if con_sym not in globals():
        globals()[con_sym] = connect_bbg(port=port, **kwargs)

    return globals()[con_sym]


def _service_symbol_(service: str, **kwargs) -> str:
    """
    Service symbol for global storage

    Args:
        service: service name
        **kwargs:
            port: port number

    Returns:
        str: name of service symbol
    """
    port = kwargs.get('port', _PORT_)
    return f'{_CON_SYM_}/{port}{service}'


def bbg_service(service: str, **kwargs) -> blpapi.service.Service:
    """
    Initiate service

    Args:
        service: service name
        **kwargs:
            port: port number

    Returns:
        Bloomberg service
    """
    serv_sym = _service_symbol_(service=service, **kwargs)
    if serv_sym in globals():
        if isinstance(globals()[serv_sym], blpapi.service.Service):
            return globals()[serv_sym]
        else: del globals()[serv_sym]
    return _init_service_(service=service, **kwargs)


def event_types() -> dict:
    """
    Bloomberg event types
    """
    return {
        getattr(blpapi.Event, ev_typ): ev_typ
        for ev_typ in dir(blpapi.Event) if ev_typ.isupper()
    }


# noinspection PyBroadException
def _init_service_(service: str, **kwargs) -> blpapi.service.Service:
    """
    Initiate service
    """
    logger = logs.get_logger(_init_service_, **kwargs)
    try:
        _get_service_(service=service, **kwargs)
    except Exception:
        logger.debug(f'Initiating {service} ...')
        bbg_session(**kwargs).openService(service)
        try:
            return _get_service_(service=service, **kwargs)
        except Exception:
            raise ConnectionError(f'Cannot initiate {service}')


def _get_service_(service: str, **kwargs) -> blpapi.service.Service:
    """
    Get service after initiation
    """
    serv_sym = _service_symbol_(service=service, **kwargs)
    globals()[serv_sym] = bbg_session(**kwargs).getService(service)
    return globals()[serv_sym]
