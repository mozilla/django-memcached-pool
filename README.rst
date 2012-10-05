django-memcached-pool
=====================

An efficient fast Django Memcached backend with a pool of connectors, based on
ultramemcache.

See https://github.com/esnme/ultramemcache

Each connection added in the pool stays connected to Memcache or Membase,
drastically limiting the number of reconnections and open sockets your
application will use on high load.

If you configure more than one Memcache server, each new connection
will randomly pick one.

Everytime a socket timeout occurs on a server, it's blacklisted so
new connections avoid picking it for a while.

To use this backend, make sure the package is installed in your environment
then use `memcachepool.cache.UMemcacheCache` as backend in your settings.

**Also, make sure you use umemcache >= 1.5**

Here's an example::


    CACHES = {
        'default': {
            'BACKEND': 'memcachepool.cache.UMemcacheCache',
            'LOCATION': '127.0.0.1:11211',
            'OPTIONS': {
                    'MAX_POOL_SIZE': 100,
                    'BLACKLIST_TIME': 20,
                    'SOCKET_TIMEOUT': 5,
                    'MAX_ITEM_SIZE': 1000*100,
                }
            }
        }


Options:

- **MAX_POOL_SIZE:** -- The maximum number of connectors in the pool. default: 35.
- **BLACKLIST_TIME** -- The time in seconds a server stays in the blacklist. default: 60
- **SOCKET_TIMEOUT** -- the time in seconds for the socket timeout. default: 4
- **MAX_ITEM_SIZE** -- The maximum size for an item in Memcache.

