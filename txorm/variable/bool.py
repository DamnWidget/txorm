# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from decimal import Decimal

from txorm.compat import _PYPY
from txorm import c_extensions_available

if not _PYPY and c_extensions_available:
    try:
        from _variable import Variable
    except ImportError:
        from .base import Variable
else:
    from .base import Variable


class BoolVariable(Variable):
    """Boolean variable representation
    """
    __slots__ = ()

    def parse_set(self, value, from_db):
        if not isinstance(value, (int, float, Decimal)):
            raise TypeError('Expected bool, found {}: {}'.format(
                type(value), value
            ))

        return bool(value)
