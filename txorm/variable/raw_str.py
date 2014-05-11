# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from txorm.compat import _PYPY, _PY3
from txorm.compat import binary_type, b
from txorm import c_extensions_available

if not _PYPY and c_extensions_available:
    try:
        from txorm._variable import Variable
    except ImportError:
        from .base import Variable
else:
    from .base import Variable


if _PY3 is True:
    buffer = memoryview


class RawStrVariable(Variable):
    """Raw/Bytes string representation
    """
    __slots__ = ()

    def parse_set(self, value, from_db):
        if isinstance(value, buffer):
            value = b(value)
        elif not isinstance(value, binary_type):
            raise TypeError('Expected {}, found {}: {}'.format(
                binary_type, type(value), value
            ))

        return value
