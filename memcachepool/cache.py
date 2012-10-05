try:
    import cPickle as pickle        # NOQA
except ImportError:
    import pickle                   # NOQA

import errno
import random
import socket
import time

from django.core.cache.backends.memcached import MemcachedCache

from memcachepool.pool import ClientPool


DEFAULT_ITEM_SIZE = 1000 * 1000


# XXX using python-memcached style pickling
# but maybe we could use something else like
# json
#
# at least this makes it compatible with
# existing data
def serialize(data):
    return pickle.dumps(data)


def unserialize(data):
    return pickle.loads(data)


# XXX not sure if keeping the base BaseMemcachedCache class has anymore value
class UMemcacheCache(MemcachedCache):
    "An implementation of a cache binding using python-memcached"
    def __init__(self, server, params):
        import umemcache
        super(MemcachedCache, self).__init__(server, params,
                                         library=umemcache,
                                         value_not_found_exception=ValueError)
        # see how to pass the pool value
        self.maxsize = int(params.get('MAX_POOL_SIZE', 35))
        self.blacklist_time = int(params.get('BLACKLIST_TIME', 60))
        self.socktimeout = int(params.get('SOCKET_TIMEOUT', 4))
        self.max_item_size = long(params.get('MAX_ITEM_SIZE',
                                             DEFAULT_ITEM_SIZE))
        self._pool = ClientPool(self._get_client, maxsize=self.maxsize)
        self._blacklist = {}

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

        return random.choice(choices)

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

    def add(self, key, value, timeout=0, version=None):
        value = serialize(value)
        key = self.make_key(key, version=version)

        with self._pool.reserve() as conn:
            return conn.add(key, value, self._get_memcache_timeout(timeout))

    def get(self, key, default=None, version=None):
        key = self.make_key(key, version=version)
        with self._pool.reserve() as conn:
            val = conn.get(key)

        if val is None:
            return default

        return unserialize(val[0])

    def set(self, key, value, timeout=0, version=None):
        value = serialize(value)
        key = self.make_key(key, version=version)
        with self._pool.reserve() as conn:
            conn.set(key, value, self._get_memcache_timeout(timeout))

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        with self._pool.reserve() as conn:
            conn.delete(key)

    def get_many(self, keys, version=None):
        if keys == {}:
            return {}

        new_keys = map(lambda x: self.make_key(x, version=version), keys)

        ret = {}
        with self._pool.reserve() as conn:
            for key in new_keys:
                res = conn.get(key)
                if res is None:
                    continue
                ret[key] = res[0]

        if ret:
            res = {}
            m = dict(zip(new_keys, keys))

            for k, v in ret.items():
                res[m[k]] = unserialize(v)

            return res

        return ret

    def close(self, **kwargs):
        # XXX none of your business Django
        pass

    def incr(self, key, delta=1, version=None):
        key = self.make_key(key, version=version)
        try:
            with self._pool.reserve() as conn:
                val = conn.incr(key, delta)

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
            with self._pool.reserve() as conn:
                val = conn.decr(key, delta)

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
            safe_data[key] = serialize(value)

        with self._pool.reserve() as conn:
            for key, value in safe_data.items():
                conn.set(key, value, self._get_memcache_timeout(timeout))

    def delete_many(self, keys, version=None):
        with self._pool.reserve() as conn:
            for key in keys:
                conn.delete(self.make_key(key, version=version))

    def clear(self):
        with self._pool.reserve() as conn:
            conn.flush_all()
