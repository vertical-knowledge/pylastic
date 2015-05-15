__pkg_name__ = 'pylastic'

from setuptools import setup, find_packages

import os

version = '0.0.1'

base_dir = os.path.dirname(__file__)

setup(
    author='Vertical Knowledge',
    author_email='kyle.amos@vertical-knowledge.com',
    name='pylastic',
    version=version,
    packages=find_packages(exclude=['tests', 'tests.*']),
    include_package_data=True,
    description='Python Elastic',
    # long_description=long_description,
    install_requires=[
        'elasticsearch'
    ],
    classifiers=[
        'Development Status :: 3 - Pre-Alpha',
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
    test_suite="tests"
)