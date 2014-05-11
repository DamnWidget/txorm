#!/usr/bin/env python

# Copyright (c) 2014 - 2013 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details.

"""
Distutils installer for TxORM.
"""

import sys
if not hasattr(sys, "version_info") or sys.version_info < (2, 6):
    raise RuntimeError("TxORM requires Python 2.6 or later.")

CFLAGS = []
if 'darwin' in sys.platform:
    import os
    cflags = os.environ.get('CFLAGS', [])
    cflags.append('-Qunused-arguments')
    os.environ['CFLAGS'] = ' '.join(cflags)

from setuptools import setup, Extension, find_packages


def get_txorm_version():
    with open('txorm.ver', 'r') as txorm_ver_file:
        txorm_ver = txorm_ver_file.read()

    return txorm_ver.strip()

_variable = Extension('txorm._variable', sources=['txorm/_variable.c'])

setup(
    name='txorm',
    version=get_txorm_version(),
    description='',
    author='Oscar Campos',
    author_email='oscar.campos@member.fsf.org',
    maintainer='TxORM Developers',
    license='LGPL',
    packages=find_packages(),
    ext_modules=[_variable],
    tests_require=['twisted>=10.2.0'],
    install_requires=['twisted>=10.2.0'],
    requires=['twisted(>=10.2.0)', 'zope.component'],
    zip_safe=False,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.1',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Framework :: Twisted',
        'Intended Audience :: Developers',
        ('License :: OSI Approved :: '
            'GNU Lesser General Public License v3 or later (LGPLv3+)'),
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: POSIX :: Linux',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
