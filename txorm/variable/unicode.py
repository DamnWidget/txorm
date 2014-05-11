# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from txorm.compat import _PYPY
from txorm.compat import text_type
from txorm import c_extensions_available

if not _PYPY and c_extensions_available:
    try:
        from txorm._variable import Variable
    except ImportError:
        from .base import Variable
else:
    from .base import Variable


class UnicodeVariable(Variable):
    """Unicode variable representation
    """
    __slots__ = ()

    def parse_set(self, value, from_db):
        if not isinstance(value, text_type):
            raise TypeError('Expected {}, found {}: {}'.format(
                text_type, type(value), value
            ))

        return value
