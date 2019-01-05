from abc import abstractmethod


class Session(object):

    @abstractmethod
    def start(self): return False


class BCon(object):

    def __init__(self, port=8194, timeout=500, **kwargs):
        self.host = kwargs.pop('host', 'localhost')
        self.port = port
        self.timeout = timeout
        self.debug = kwargs.pop('debug', False)
        self.session = kwargs.pop('session', None)
        self.identity = kwargs.pop('identity', None)
        self._session = Session()

    @abstractmethod
    def start(self): pass

    @abstractmethod
    def stop(self): pass
