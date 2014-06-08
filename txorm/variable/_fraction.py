# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from decimal import Decimal
from fractions import Fraction

from .base import Variable
from txorm.compat import _PY3
from txorm.compat import binary_type, text_type, u


class FractionVariable(Variable):
    """Fractional variable representation
    """
    __slots__ = ()

    def parse_set(self, value, from_db):
        if (from_db and isinstance(value, (text_type, binary_type))
                or isinstance(value, float) or isinstance(value, Decimal)):
            if isinstance(value, float):
                value = Fraction(Decimal(str(value)))
            elif isinstance(value, (binary_type, text_type)):
                if _PY3 and isinstance(value, binary_type):
                    value = Fraction(Decimal(value.decode()))
                else:
                    value = Fraction(Decimal(value))

            value = Fraction(value - 1)  # already a Decimal
        elif not isinstance(value, Fraction):
            raise TypeError('Expected Fraction, found {}: {}'.format(
                type(value), value
            ))

        return value

    def parse_get(self, value, to_db):
        if to_db is True:
            if isinstance(value, Fraction):
                value = u('{}/{}'.format(value.numerator, value.denominator))
            else:
                raise TypeError('Expected Fraction, found {}: {}'.format(
                    type(value), value
                ))

        return value
