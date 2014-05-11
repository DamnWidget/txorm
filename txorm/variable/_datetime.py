# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

import re
from datetime import datetime, timedelta

from txorm.compat import _PYPY
from txorm import c_extensions_available
from txorm.compat import text_type, integer_types

if not _PYPY and c_extensions_available:
    try:
        from _variable import Variable
    except ImportError:
        from .base import Variable
else:
    from .base import Variable


class DateTimeVariable(Variable):
    """DateTime variable representation
    """
    __slots__ = ()

    def parse_set(self, value, from_db):
        if from_db is True:
            if isinstance(value, datetime):
                pass
            elif isinstance(value, text_type):
                if ' ' not in value:
                    raise ValueError('Unknown date/time format: {}'.format(
                        repr(value)
                    ))

                _date, _time = value.split(' ')
                value = datetime(*(_parse_date(_date) + _parse_time(_time)))
            else:
                raise TypeError('Expected datetime, found {}: {}'.format(
                    type(value), repr(value)
                ))
        else:
            if isinstance(value, integer_types):
                value = datetime.utcfromtimestamp(value)
            elif not isinstance(value, datetime):
                raise TypeError('Expected datetime, found {}: {}'.format(
                    type(value), repr(value)
                ))

            if self._tzinfo is not None:
                value = value.astimezone(self._tzinfo)

        return value


def _parse_time(time_str):
    # TODO Add support for timezones.
    colons = time_str.count(":")
    if not 1 <= colons <= 2:
        raise ValueError("Unknown time format: %r" % time_str)
    if colons == 2:
        hour, minute, second = time_str.split(":")
    else:
        hour, minute = time_str.split(":")
        second = "0"
    if "." in second:
        second, microsecond = second.split(".")
        second = int(second)
        microsecond = int(int(microsecond) * 10 ** (6 - len(microsecond)))
        return int(hour), int(minute), second, microsecond
    return int(hour), int(minute), int(second), 0


def _parse_date(date_str):
    if "-" not in date_str:
        raise ValueError("Unknown date format: %r" % date_str)
    year, month, day = date_str.split("-")
    return int(year), int(month), int(day)


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
