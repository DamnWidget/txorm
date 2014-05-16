# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

import re
from datetime import timedelta

from txorm.compat import _PYPY
from txorm import c_extensions_available
from txorm.compat import binary_type, text_type

if not _PYPY and c_extensions_available:
    try:
        from txorm._variable import Variable
    except ImportError:
        from .base import Variable
else:
    from .base import Variable

from ._datetime import _parse_time


class TimeDeltaVariable(Variable):
    __slots__ = ()

    def parse_set(self, value, from_db):
        if from_db is True:
            if value is None:
                return value
            if isinstance(value, timedelta):
                return value
            if not isinstance(value, (binary_type, text_type)):
                raise TypeError('Expected timedelta, found {}'.format(
                    repr(value))
                )
            return _parse_interval(value)
        else:
            if not isinstance(value, timedelta):
                raise TypeError('Expected timedelta, found {}'.format(
                    repr(value))
                )

            return value


def _parse_interval_table():
    table = {}
    for units, delta in (
        ("d day days", timedelta),
        ("h hour hours", lambda x: timedelta(hours=x)),
        ("m min minute minutes", lambda x: timedelta(minutes=x)),
        ("s sec second seconds", lambda x: timedelta(seconds=x)),
        ("ms millisecond milliseconds", lambda x: timedelta(milliseconds=x)),
        ("microsecond microseconds", lambda x: timedelta(microseconds=x))
    ):
        for unit in units.split():
            table[unit] = delta

    return table

_parse_interval_table = _parse_interval_table()
_parse_interval_re = re.compile(
    r"[\s,]*"
    r"([-+]?(?:\d\d?:\d\d?(?::\d\d?)?(?:\.\d+)?"
    r"|\d+(?:\.\d+)?))"
    r"[\s,]*"
)


def _parse_interval(interval):
    result = timedelta(0)
    value = None
    for token in _parse_interval_re.split(interval):
        if not token:
            pass
        elif ":" in token:
            if value is not None:
                result += timedelta(days=value)
                value = None
            h, m, s, ms = _parse_time(token)
            result += timedelta(hours=h, minutes=m, seconds=s, microseconds=ms)
        elif value is None:
            try:
                value = float(token)
            except ValueError:
                raise ValueError("Expected an interval value rather than "
                                 "%r in interval %r" % (token, interval))
        else:
            unit = _parse_interval_table.get(token)
            if unit is None:
                raise ValueError("Unsupported interval unit %r in interval %r"
                                 % (token, interval))
            result += unit(value)
            value = None
    if value is not None:
        result += timedelta(seconds=value)

    return result
