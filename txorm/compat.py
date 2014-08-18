
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
    import json

_PY3 = False if sys.version_info < (3, 0) else True

if _PY3:
    from ._compat.python3_ import pickle, StringIO, urlparse

    text_type = str
    binary_type = bytes
    integer_types = (int, )

else:
    from _compat.python2_ import pickle, StringIO, urlparse

    text_type = unicode
    binary_type = str
    integer_types = (int, long)


def u(string):
    return text_type(string)


def b(string):
    if _PY3:
        if isinstance(string, memoryview):
            return bytes(string)
        return bytes(string.encode('utf8'))
    return str(string)


def is_basestring(value):
    try:
        return isinstance(value, basestring)
    except NameError:
        return isinstance(value, str)


def itervalues(d):
    return iter(getattr(d, 'values' if _PY3 else 'itervalues')())


def iterkeys(d):
    return iter(getattr(d, 'keys' if _PY3 else 'iterkeys')())


def iteritems(d):
    return iter(getattr(d, 'items' if _PY3 else 'iteritems')())


__all__ = [
    'b', 'json', 'pickle', 'StringIO', 'text_type', 'binary_type',
    'itervalues', 'iterkeys', 'iteritems', 'urlparse'
]
