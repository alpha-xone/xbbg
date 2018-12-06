from abc import abstractmethod


class BCon(object):

    def __init__(
            self, host='localhost', port=8194, debug=False,
            timeout=500, session=None, identity=None
    ):
        self.host = host
        self.port = port
        self.debug = debug
        self.timeout = timeout
        self.session = session
        self.identity = identity

    @abstractmethod
    def start(self): pass

    @abstractmethod
    def stop(self): pass
