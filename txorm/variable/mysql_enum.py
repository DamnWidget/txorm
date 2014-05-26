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


class MysqlEnumVariable(Variable):
    """Reprsentation of a native MySQL enum
    """

    __slots__ = ('_set')

    def __init__(self, _set, *args, **kwargs):
        self._set = _set
        super(Variable, self).__init__(*args, **kwargs)

    def parse_set(self, value, from_db):
        if from_db is True:
            return value

        return self._parse_common(value)

    def parse_get(self, value, to_db):
        if to_db is True:
            return text_type(value)

        return self._parse_common(value)

    def _parse_common(self, value):
        if value not in self._set:
            raise ValueError('Invalid enum value: {}'.format(value))

        return value
