import sys
try:
    import cPickle as pickle
except ImportError:
    import pickle

from django.core.cache.backends.memcached import MemcachedCache
from memcachepool.pool import ClientPool

# XXX not sure if keeping the base BaseMemcachedCache class has anymore value
class UMemcacheCache(MemcachedCache):
    "An implementation of a cache binding using python-memcached"
    def __init__(self, server, params):
        import umemcache
        super(MemcachedCache, self).__init__(server, params,
                                         library=umemcache,
                                         value_not_found_exception=ValueError)
        # see how to pass the pool value
        self._pool = ClientPool(self._get_client)

    def _get_client(self):
        if len(self._servers) != 1:
            raise ValueError('umemcached does not support several servers')

        cli = self._lib.Client(self._servers[0])
        cli.connect()
        return cli

    def add(self, key, value, timeout=0, version=None):
        key = self.make_key(key, version=version)

        with self._pool.reserve() as conn:
            return conn.add(key, value, self._get_memcache_timeout(timeout))

    def get(self, key, default=None, version=None):
        key = self.make_key(key, version=version)
        with self._pool.reserve() as conn:
            val = conn.get(key)

        if val is None:
            return default
        return val[0]

    def set(self, key, value, timeout=0, version=None):
        if not isinstance(value, str):
            raise ValueError('Only string supported - you should serialize '
                             'your data')

        key = self.make_key(key, version=version)
        with self._pool.reserve() as conn:
            conn.set(key, value, self._get_memcache_timeout(timeout))

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        with self._pool.reserve() as conn:
            conn.delete(key)

    def get_many(self, keys, version=None):
        new_keys = map(lambda x: self.make_key(x, version=version), keys)
        with self._pool.reserve() as conn:
            ret = conn.get_multi(new_keys)

        if ret:
            _ = {}
            m = dict(zip(new_keys, keys))
            for k, v in ret.items():
                _[m[k]] = v
            ret = _
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
            safe_data[key] = value

        with self._pool.reserve() as conn:
            conn.set_multi(safe_data, self._get_memcache_timeout(timeout))

    def delete_many(self, keys, version=None):
        l = lambda x: self.make_key(x, version=version)
        with self._pool.reserve() as conn:
            conn.delete_multi(map(l, keys))

    def clear(self):
        with self._pool.reserve() as conn:
            conn._cache.flush_all()
