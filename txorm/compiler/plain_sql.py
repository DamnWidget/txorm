# -*- test-case-name: txorm.test.test_expressions -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

import re

from txorm import Undef
from txorm.compat import binary_type
from .comparable import ComparableExpression


class SQL(ComparableExpression):
    """SQL expression
    """

    __slots__ = ('expression', 'params', 'tables')

    def __init__(self, expression, params=Undef, tables=Undef):
        self.expression = expression
        self.params = params
        self.tables = tables


class SQLRaw(binary_type):
    """Used to mark a binary string as something that shouldn't be compiled

    This is handled internally by the compiler
    """
    __slots__ = ()


class SQLToken(binary_type):
    """
    Used to mark a binary strin that should be considered as a single SQL
    token.

    These strings should be quoted, when needed
    """
    __slots__ = ()

is_safe_token = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*$').match
