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
