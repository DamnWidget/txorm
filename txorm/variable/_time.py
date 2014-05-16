# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from datetime import datetime, time

from txorm.compat import _PYPY
from txorm import c_extensions_available
from txorm.compat import text_type, binary_type

if not _PYPY and c_extensions_available:
    try:
        from txorm._variable import Variable
    except ImportError:
        from .base import Variable
else:
    from .base import Variable

from ._datetime import _parse_time


class TimeVariable(Variable):
    """Time variable representation
    """
    __slots__ = ()

    def parse_set(self, value, from_db):
        if from_db is True:
            if value is None:
                return value
            if isinstance(value, time):
                return value
            if not isinstance(value, (text_type, binary_type)):
                raise TypeError('Expected time, found {}'.format(repr(value)))
            if ' ' in value:
                date_str, value = value.split(' ')
            return time(*_parse_time(value))
        else:
            if isinstance(value, datetime):
                return value.time()
            if not isinstance(value, time):
                raise TypeError('Expected time, found {}'.format(repr(value)))
            return value
