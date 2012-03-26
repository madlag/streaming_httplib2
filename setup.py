#!/usr/bin/env python
from setuptools import setup, find_packages
import sys, os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README')).read()
NEWS = open(os.path.join(here, 'CHANGELOG')).read()


version = '0.7.5'

install_requires = [
    # List your project dependencies here.
    # For more details, see:
    # http://packages.python.org/distribute/setuptools.html#declaring-dependencies
]


setup(name='streaming_httplib2',
    version=version,
    description="A comprehensive HTTP client library modified to add response streaming support.",
    long_description=README + '\n\n' + NEWS,
    classifiers=[
      # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    ],
    keywords='http streaming',
    author='Joe Gregorio',
    author_email='joe@bitworking.org',
    url='https://github.com/madlag/streaming_httplib2',
    license='MIT',
    packages=find_packages("python2"),
    package_dir = {'': 'python2'},
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    entry_points={
        'console_scripts':
            ['streaming_httplib2=streaming_httplib2:main']
    }
)
