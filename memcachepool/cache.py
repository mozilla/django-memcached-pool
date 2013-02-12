try:
    import cPickle as pickle        # NOQA
except ImportError:
    import pickle                   # NOQA

import errno
import socket
import time

from django.core.cache.backends.memcached import MemcachedCache
from memcachepool.pool import ClientPool


DEFAULT_ITEM_SIZE = 1000 * 1000


# XXX not sure if keeping the base BaseMemcachedCache class has anymore value
class UMemcacheCache(MemcachedCache):
    "An implementation of a cache binding using python-memcached"

    _FLAG_SERIALIZED = 1
    _FLAG_INT = 1 << 1
    _FLAG_LONG = 1 << 2

    def __init__(self, server, params):
        from memcachepool import client
        kls = super(MemcachedCache, self)
        kls.__init__(server, params, library=client,
                     value_not_found_exception=ValueError)
        # see how to pass the pool value
        self.maxsize = int(params.get('MAX_POOL_SIZE', 35))
        self.blacklist_time = int(params.get('BLACKLIST_TIME', 60))
        self.socktimeout = int(params.get('SOCKET_TIMEOUT', 4))
        self.max_item_size = long(params.get('MAX_ITEM_SIZE',
                                             DEFAULT_ITEM_SIZE))
        self._pool = ClientPool(self._get_client, maxsize=self.maxsize,
                                wait_for_connection=self.socktimeout)
        self._blacklist = {}
        self.retries = int(params.get('MAX_RETRIES', 3))
        self._pick_index = 0

    def call(self, func, *args, **kwargs):
        retries = 0
        while retries < self.retries:
            with self._pool.reserve() as conn:
                try:
                    return getattr(conn, func)(*args, **kwargs)
                except Exception, exc:
                    # log
                    retries += 1
        raise exc

    # XXX using python-memcached style pickling
    # but maybe we could use something else like
    # json
    #
    # at least this makes it compatible with
    # existing data
    def serialize(self, data):
        return pickle.dumps(data, pickle.HIGHEST_PROTOCOL)

    def unserialize(self, data):
        return pickle.loads(data)

    def _get_memcache_timeout(self, timeout):
        if timeout == 0:
            return timeout
        return super(UMemcacheCache, self)._get_memcache_timeout(timeout)

    def _pick_server(self):
        # update the blacklist
        for server, age in self._blacklist.items():
            if time.time() - age > self.blacklist_time:
                del self._blacklist[server]

        # build the list of available servers
        choices = list(set(self._servers) ^ set(self._blacklist.keys()))

        if not choices:
            return None

        if self._pick_index >= len(choices):
            self._pick_index = 0

        choice = choices[self._pick_index]
        self._pick_index += 1
        return choice

    def _blacklist_server(self, server):
        self._blacklist[server] = time.time()

    def _get_client(self):
        server = self._pick_server()
        last_error = None

        def create_client(server):
            cli = self._lib.Client(server, max_item_size=self.max_item_size)
            cli.sock.settimeout(self.socktimeout)
            return cli

        while server is not None:
            cli = create_client(server)
            try:
                cli.connect()
                return cli
            except (socket.timeout, socket.error), e:
                if not isinstance(e, socket.timeout):
                    if e.errno != errno.ECONNREFUSED:
                        # unmanaged case yet
                        raise

                # well that's embarrassing, let's blacklist this one
                # and try again
                self._blacklist_server(server)
                server = self._pick_server()
                last_error = e

        if last_error is not None:
            raise last_error
        else:
            raise socket.timeout('No server left in the pool')

    def _flag_for_value(self, value):
        if isinstance(value, int):
            return self._FLAG_INT
        elif isinstance(value, long):
            return self._FLAG_LONG
        return self._FLAG_SERIALIZED

    def _value_for_flag(self, value, flag):
        if flag == self._FLAG_INT:
            return int(value)
        elif flag == self._FLAG_LONG:
            return long(value)
        return self.unserialize(value)

    def add(self, key, value, timeout=0, version=None):
        flag = self._flag_for_value(value)
        if flag == self._FLAG_SERIALIZED:
            value = self.serialize(value)
        else:
            value = '%d' % value

        key = self.make_key(key, version=version)

        return self.call('add', value, self._get_memcache_timeout(timeout),
                         flag)

    def get(self, key, default=None, version=None):
        key = self.make_key(key, version=version)
        val = self.call('get', key)

        if val is None:
            return default

        return self._value_for_flag(value=val[0], flag=val[1])

    def set(self, key, value, timeout=0, version=None):
        flag = self._flag_for_value(value)
        if flag == self._FLAG_SERIALIZED:
            value = self.serialize(value)
        else:
            value = '%d' % value
        key = self.make_key(key, version=version)
        self.call('set', key, value, self._get_memcache_timeout(timeout), flag)

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        self.call('delete', key)

    def get_many(self, keys, version=None):
        if keys == {}:
            return {}

        new_keys = map(lambda x: self.make_key(x, version=version), keys)

        ret = {}

        for key in new_keys:
            res = self.call('get', key)
            if res is None:
                continue
            ret[key] = res

        if ret:
            res = {}
            m = dict(zip(new_keys, keys))

            for k, v in ret.items():
                res[m[k]] = self._value_for_flag(value=v[0], flag=v[1])

            return res

        return ret

    def close(self, **kwargs):
        # XXX none of your business Django
        pass

    def incr(self, key, delta=1, version=None):
        key = self.make_key(key, version=version)
        try:
            val = self.call('incr', key, delta)

        # python-memcache responds to incr on non-existent keys by
        # raising a ValueError, pylibmc by raising a pylibmc.NotFound
        # and Cmemcache returns None. In all cases,
        # we should raise a ValueError though.
        except self.LibraryValueNotFoundException:
            val = None
        if val is None:
            raise ValueError("Key '%s' not found" % key)
        return val

    def decr(self, key, delta=1, version=None):
        key = self.make_key(key, version=version)
        try:
            val = self.call('decr', key, delta)

        # python-memcache responds to incr on non-existent keys by
        # raising a ValueError, pylibmc by raising a pylibmc.NotFound
        # and Cmemcache returns None. In all cases,
        # we should raise a ValueError though.
        except self.LibraryValueNotFoundException:
            val = None
        if val is None:
            raise ValueError("Key '%s' not found" % key)
        return val

    def set_many(self, data, timeout=0, version=None):
        safe_data = {}
        for key, value in data.items():
            key = self.make_key(key, version=version)
            flag = self._flag_for_value(value)
            if flag == self._FLAG_SERIALIZED:
                value = self.serialize(value)
            else:
                value = '%d' % value
            safe_data[key] = value

        for key, value in safe_data.items():
            self.call('set', key, value, self._get_memcache_timeout(timeout),
                      flag)

    def delete_many(self, keys, version=None):
        for key in keys:
            self.call('delete', self.make_key(key, version=version))

    def clear(self):
        self.call('flush_all')
