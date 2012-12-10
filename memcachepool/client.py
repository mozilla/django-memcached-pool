import socket
import time
from errno import EISCONN, EINVAL
from functools import wraps

from umemcache import Client as OriginalClient
from umemcache import MemcachedError


_RETRY = ('set', 'get', 'gets', 'get_multi', 'gets_multi',
          'add', 'replace', 'append', 'prepend', 'delete',
          'cas', 'incr', 'decr', 'stats', 'flush_all',
          'version')
_ERRORS = (IOError, RuntimeError, MemcachedError, socket.error)


class Client(object):
    """On connection errors, tries to reconnect
    """
    def __init__(self, address, max_item_size=None, max_connect_retries=5,
                 reconnect_delay=.5):
        self.address = address
        self.max_item_size = max_item_size
        self._client = None
        self.funcs = []
        self._create_client()
        self.max_connect_retries = max_connect_retries
        self.reconnect_delay = reconnect_delay

    def _create_connector(self):
        if self.max_item_size is not None:
            self._client = OriginalClient(self.address, self.max_item_size)
        else:
            self._client = OriginalClient(self.address)

        self.funcs = [func for func in dir(self._client)
                      if not func.startswith('_')]

    def _create_client(self):
        reconnect = self._client is not None

        if reconnect:
            try:
                self._client.close()
            except Exception:
                pass

        self._create_connector()

        if reconnect:
            retries = 0
            delay = self.reconnect_delay
            while retries < self.max_connect_retries:
                try:
                    self._client.connect()
                except socket.error, exc:
                    if exc.errno == EISCONN:
                        return   # we're good
                    if exc.errno == EINVAL:
                        # we're doomed, retry
                        self._create_connector()

                    time.sleep(delay)
                    retries += 1
                    delay *= 2      # growing the delay

            raise exc

    def _with_retry(self, func):
        @wraps(func)
        def __with_retry(*args, **kw):
            retries = 0
            delay = self.reconnect_delay
            current_func = func

            while retries < self.max_connect_retries:
                try:
                    return current_func(*args, **kw)
                except _ERRORS, exc:
                    self._create_client()
                    current_func = getattr(self._client, func.__name__)
                    time.sleep(delay)
                    retries += 1
                    delay *= 3      # growing the delay

            raise exc
        return __with_retry

    def __getattr__(self, name):
        if not name in self.funcs:
            return self.__dict__[name]

        original = getattr(self._client, name)

        if name in _RETRY:
            return self._with_retry(original)

        return original
