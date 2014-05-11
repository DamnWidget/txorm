
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Python Compatibility Module
"""

import sys

if '__pypy__' not in sys.modules:
    _PYPY = False
    try:
        import ujson as json
    except ImportError:
        import json

    assert json
else:
    _PYPY = True

_PY3 = False if sys.version_info < (3, 0) else True

if _PY3:
    from ._compat.python3_ import pickle, StringIO

    text_type = str
    binary_type = bytes
    integer_types = int

else:
    from _compat.python2_ import pickle, StringIO

    text_type = unicode
    binary_type = str
    integer_types = (int, long)


def u(string):
    return text_type(string)


def b(string):
    return binary_type(string)


def is_basestring(value):
    try:
        return isinstance(value, basestring)
    except NameError:
        return isinstance(value, str)


__all__ = ['b', 'json', 'pickle', 'StringIO', 'text_type', 'binary_type']
