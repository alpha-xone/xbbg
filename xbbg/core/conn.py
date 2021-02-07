import sys
import os

try:
    ver = sys.version_info
    if f'{ver.major}.{ver.minor}' == '3.8':
        dll_path = os.environ.get('BBG_DLL', 'C:/blp/DAPI')
        if os.path.exists(dll_path):
            with os.add_dll_directory(dll_path):
                import blpapi
        else:
            raise ImportError(
                'Please add BBG_DLL to your PATH variable'
            )
    else:
        import blpapi
except (ImportError, AttributeError):
    import pytest
    blpapi = pytest.importorskip('blpapi')

from xbbg.io import logs

_CON_SYM_ = '_xcon_'
_PORT_ = 8194


def connect(max_attempt=3, auto_restart=True, **kwargs) -> blpapi.session.Session:
    """
    Use alternative method to connect to blpapi. If a session object is passed, arguments
    max_attempt and auto_restart will be ignored.

    referecing to blpapi example for full lists of available authentication methods:
        https://github.com/msitt/blpapi-python/blob/master/examples/ConnectionAndAuthExample.py
    """
    if isinstance(kwargs.get('sess', None), blpapi.session.Session):
        return bbg_session(sess=kwargs['sess'])

    sess_opts = blpapi.SessionOptions()
    sess_opts.setNumStartAttempts(numStartAttempts=max_attempt)
    sess_opts.setAutoRestartOnDisconnection(autoRestart=auto_restart)

    if isinstance(kwargs.get('auth_method', None), str):
        auth_method = kwargs['auth_method']
        auth = None

        if auth_method == 'user':
            user = blpapi.AuthUser.createWithLogonName()
            auth = blpapi.AuthOptions.createWithUser(user=user)
        elif auth_method == 'app':
            auth = blpapi.AuthOptions.createWithApp(appName=kwargs['app_name'])
        elif auth_method == 'userapp':
            user = blpapi.createWithLogonName()
            auth = blpapi.AuthOptions.createWithUserAndApp(user=user, appName=kwargs['app_name'])
        elif auth_method == 'dir':
            user = blpapi.AuthUser.createWithActiveDirectoryProperty(propertyName=kwargs['dir_property'])
            auth = blpapi.AuthOptions.createWithUser(user=user)
        elif auth_method == 'manual':
            user = blpapi.AuthUser.createWithManualOptions(userId=kwargs['user_id'], ipAddress=kwargs['ip_address'])
            auth = blpapi.AuthOptions.createWithUserAndApp(user=user, appName=kwargs['app_name'])
        else:
            raise ValueError(
                'Received invalid value for auth_method. '
                'auth_method must be one of followings: user, app, userapp, dir, manual'
            )

        sess_opts.setSessionIdentityOptions(authOptions=auth)

    if isinstance(kwargs.get('server_host', None), str):
        sess_opts.setServerHost(serverHost=kwargs['server_host'])

    if isinstance(kwargs.get('server_port', None), str):
        sess_opts.setServerPort(serverPort=kwargs['server_post'])

    if isinstance(kwargs.get('tls_options', None), blpapi.sessionoptions.TlsOptions):
        sess_opts.setTlsOptions(tlsOptions=kwargs['tlsOptions'])

    return bbg_session(sess=blpapi.Session(sess_opts))


def connect_bbg(**kwargs) -> blpapi.session.Session:
    """
    Create Bloomberg session and make connection
    """
    logger = logs.get_logger(connect_bbg, **kwargs)

    if isinstance(kwargs.get('sess', None), blpapi.session.Session):
        session = kwargs['sess']
        logger.debug(f'Using Bloomberg session {session} ...')
    else:
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
        if getattr(globals()[con_sym], '_Session__handle', None) is None:
            del globals()[con_sym]

    if con_sym not in globals():
        globals()[con_sym] = connect_bbg(**kwargs)

    return globals()[con_sym]


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
    logger = logs.get_logger(bbg_service, **kwargs)

    port = kwargs.get('port', _PORT_)
    serv_sym = f'{_CON_SYM_}/{port}{service}'

    log_info = f'Initiating service {service} ...'
    if serv_sym in globals():
        if getattr(globals()[serv_sym], '_Service__handle', None) is None:
            log_info = f'Restarting service {service} ...'
            del globals()[serv_sym]

    if serv_sym not in globals():
        logger.debug(log_info)
        bbg_session(**kwargs).openService(service)
        globals()[serv_sym] = bbg_session(**kwargs).getService(service)

    return globals()[serv_sym]


def event_types() -> dict:
    """
    Bloomberg event types
    """
    return {
        getattr(blpapi.Event, ev_typ): ev_typ
        for ev_typ in dir(blpapi.Event) if ev_typ.isupper()
    }


def send_request(request: blpapi.request.Request, **kwargs):
    """
    Send request to Bloomberg session

    Args:
        request: Bloomberg request
    """
    logger = logs.get_logger(send_request, **kwargs)
    try:
        bbg_session(**kwargs).sendRequest(request=request)
    except blpapi.InvalidStateException as e:
        logger.exception(e)

        # Delete existing connection and send again
        port = kwargs.get('port', _PORT_)
        con_sym = f'{_CON_SYM_}//{port}'
        if con_sym in globals(): del globals()[con_sym]

        # No error handler for 2nd trial
        bbg_session(**kwargs).sendRequest(request=request)
