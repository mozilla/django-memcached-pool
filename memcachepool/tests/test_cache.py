import socket
import time
from unittest import TestCase


class TestCache(TestCase):

    def test_pool(self):
        from memcachepool.cache import UMemcacheCache

        # creating the cache class
        cache = UMemcacheCache('127.0.0.1:11211', {})

        # simple calls
        cache.set('a', '1')
        self.assertEqual(cache.get('a'), '1')

        # should support any type and deal with serialization
        # like python-memcached does
        cache.set('a', 1)
        self.assertEqual(cache.get('a'), 1)
        cache.delete('a')
        self.assertEqual(cache.get('a'), None)

    def test_many(self):
        # make sure all the 'many' APIs work
        from memcachepool.cache import UMemcacheCache

        # creating the cache class
        cache = UMemcacheCache('127.0.0.1:11211', {})

        cache.set_many({'a': 1, 'b': 2})

        res = cache.get_many(['a', 'b']).values()
        self.assertTrue(1 in res)
        self.assertTrue(2 in res)

        cache.delete_many(['a', 'b'])
        self.assertEqual(cache.get_many(['a', 'b']), {})

    def test_loadbalance(self):
        from memcachepool.cache import UMemcacheCache

        # creating the cache class with two backends (one is off)
        params = {'SOCKET_TIMEOUT': 1, 'BLACKLIST_TIME': 1}
        cache = UMemcacheCache('127.0.0.1:11214;127.0.0.2:11213', params)

        # the load balancer should blacklist both IPs.
        # and return an error
        self.assertRaises(socket.error, cache.set, 'a', '1')
        self.assertTrue(len(cache._blacklist), 2)

        # wait for two seconds.
        time.sleep(1.1)

        # calling _pick_server should purge the blacklist
        cache._pick_server()
        self.assertEqual(len(cache._blacklist), 0)
