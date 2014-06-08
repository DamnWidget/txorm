# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from decimal import Decimal

from .base import Variable


class FloatVariable(Variable):
    """Floating point variable representation
    """
    __slots__ = ()

    def parse_set(self, value, from_db):
        if not isinstance(value, (int, float, Decimal)):
            raise TypeError('Expected float, found {}: {}'.format(
                type(value), value
            ))

        return float(value)
