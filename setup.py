import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

with open(os.path.join(here, 'CHANGES.rst')) as f:
    CHANGES = f.read()


requires = ['Django', 'umemcache']
test_requires = ['nose']


setup(name='django-memcached-pool',
      version='0.3.1',
      description='A Memcached Pool for Django',
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
        "Programming Language :: Python",
        "Framework :: Pylons",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        "License :: OSI Approved :: Apache Software License",
        ],
      author='Mozilla Services',
      author_email='services-dev@mozilla.org',
      url='https://github.com/mozilla/django-memcached-pool',
      keywords='django memcached pool',
      packages=find_packages(),
      zip_safe=False,
      install_requires=requires,
      tests_require=test_requires,
      test_suite="memcachepool.tests")
