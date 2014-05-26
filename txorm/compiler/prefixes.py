# -*- test-case-name: txorm.test.test_expressions -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from .expressions import PrefixExpression


class Not(PrefixExpression):
    """Expression representing a 'NOT' prefix
    """
    __slots__ = ()
    prefix = 'NOT'


class Exists(PrefixExpression):
    """Expression representing an 'EXISTS' prefix
    """
    __slots__ = ()
    prefix = 'EXISTS'


class Neg(PrefixExpression):
    """Expression representing a negative symbol prefix
    """
    __slots__ = ()
    prefix = '-'
