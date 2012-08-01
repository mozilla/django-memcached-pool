import os


def setUp():
    os.environ['DJANGO_SETTINGS_MODULE'] = 'memcachepool.tests.settings'
