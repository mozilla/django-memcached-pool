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
