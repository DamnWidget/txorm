# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from .base import Variable
from txorm.compat import text_type


class UnicodeVariable(Variable):
    """Unicode variable representation
    """
    __slots__ = ()

    def parse_set(self, value, from_db):
        if not isinstance(value, text_type):
            raise TypeError('Expected {}, found {}: {}'.format(
                text_type, type(value), value
            ))

        return value
