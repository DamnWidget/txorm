# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from datetime import datetime

from txorm.compat import _PYPY
from txorm import c_extensions_available
from txorm.compat import text_type, binary_type, integer_types

if not _PYPY and c_extensions_available:
    try:
        from txorm._variable import Variable
    except ImportError:
        from .base import Variable
else:
    from .base import Variable


class DateTimeVariable(Variable):
    """DateTime variable representation
    """
    __slots__ = ('_tzinfo')

    def __init__(self, *args, **kwargs):
        self._tzinfo = kwargs.pop('tzinfo', None)
        super(DateTimeVariable, self).__init__(*args, **kwargs)

    def parse_set(self, value, from_db):
        if from_db is True:
            if isinstance(value, datetime):
                pass
            elif isinstance(value, (text_type, binary_type)):
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
            if self._tzinfo is not None:
                if value.tzinfo is None:
                    value = value.replace(tzinfo=self._tzinfo)
                else:
                    value = value.astimezone(self._tzinfo)
        else:
            if isinstance(value, integer_types + (float,)):
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

