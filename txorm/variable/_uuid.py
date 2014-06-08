# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

import uuid

from .base import Variable
from txorm.compat import _PY3
from txorm.compat import binary_type, text_type


class UUIDVariable(Variable):
    """UUID variable representation
    """

    __slots__ = ()

    def parse_set(self, value, from_db):
        if from_db and isinstance(value, (text_type, binary_type)):
            if _PY3 is True and isinstance(value, binary_type):
                value = uuid.UUID(value.decode())
            else:
                value = uuid.UUID(value)
        elif not isinstance(value, uuid.UUID):
            raise TypeError('Expected UUID, found {}: {}'.format(
                type(value), value
            ))

        return value

    def parse_get(self, value, to_db):
        if to_db is True:
            return text_type(value)

        return value
