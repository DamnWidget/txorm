# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from .base import Variable
from txorm.compat import _PY3
from txorm.compat import binary_type, b


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
