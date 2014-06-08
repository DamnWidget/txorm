# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from decimal import Decimal

from .base import Variable
from txorm.compat import is_basestring, integer_types, u


class DecimalVariable(Variable):
    """Decimal variable representation
    """
    __slots__ = ()

    @staticmethod
    def parse_set(value, from_db):
        if (from_db and is_basestring(value)
                or isinstance(value, integer_types)):
            value = Decimal(value)
        elif not isinstance(value, Decimal):
            raise TypeError('Expected Decimal, found {}: {}'.format(
                type(value), value
            ))

        return value

    @staticmethod
    def parse_get(value, to_db):
        if to_db is True:
            return u(value)

        return value
