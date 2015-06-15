from setuptools import setup, find_packages

__pkg_name__ = 'pylastic'

import os

version = '0.0.7'

base_dir = os.path.dirname(__file__)

setup(
    author='Kyle Amos',
    author_email='kylebamos@gmail.com',
    name='pylastic',
    version=version,
    packages=find_packages(exclude=['tests', 'tests.*']),
    include_package_data=True,
    description='Helper Functions for the Python Elastic Client',
    install_requires=[
        'elasticsearch'
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    tests_require=[
        'elasticsearch',
        'mock'
    ],
    test_suite='tests'
)
