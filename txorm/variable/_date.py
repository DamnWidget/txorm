# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from datetime import datetime, date

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

from ._datetime import _parse_date


class DateVariable(Variable):
    """Date variable representation
    """
    __slots__ = ()

    def parse_set(self, value, from_db):
        if isinstance(value, datetime):
            return value.date()

        if from_db is True:
            if value is None:
                return value
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, date):
                return value
            if not isinstance(value, (text_type, binary_type)):
                raise TypeError('Expexted date, found {}'.format(repr(value)))
            if ' ' in value:
                value, time_str = value.split(' ')
            return date(*_parse_date(value))

        if not isinstance(value, date):
            raise TypeError('Expected date, found {}'.format(repr(value)))

        return value
