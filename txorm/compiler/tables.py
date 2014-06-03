# -*- test-case-name: txorm.test.test_expressions -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from txorm import Undef
from .expressions import Expression, FromExpression, ExpressionError


class Table(FromExpression):
    """Expression representing a 'FROM @table_name' expression

    :param name: the table name
    """

    __slots__ = ('name', 'compile_cache', 'compile_id')

    def __init__(self, name):
        self.name = name
        self.compile_cache = None
        self.compile_id = None


class JoinExpression(FromExpression):
    """Expression representing a 'JOIN FROM @table_name' expression
    :param arg1: the first argument of the sentence
    :param right: the seconf argument of the sentence
    :param on: the JOIN ON statement conditional
    """

    __slots__ = ('left', 'right', 'on')
    operator = ('unknown')

    def __init__(self, arg1, arg2=Undef, on=Undef):
        # http://www.postgresql.org/docs/8.1/interactive/explicit-joins.html
        if arg2 is Undef:
            self.left = Undef
            self.right = arg1
            self.on = on
        elif (not isinstance(arg2, Expression)
                or isinstance(arg2, (FromExpression,))):
            self.left = arg1
            self.right = arg2
            self.on = on
        else:
            self.left = Undef
            self.right = arg1
            self.on = arg2
            if on is not Undef:
                raise ExpressionError(
                    'Improper join arguments: ({!r}, {!r}, {!r})'.format(
                        arg1, arg2, on
                    )
                )


class Join(JoinExpression):
    """Expression representing a 'JOIN'
    """
    __slots__ = ()
    operator = 'JOIN'


class LeftJoin(JoinExpression):
    """Expression representing a 'LEFT JOIN'
    """
    __slots__ = ()
    operator = 'LEFT JOIN'


class RightJoin(JoinExpression):
    """Expression representing a 'RIGHT JOIN'
    """
    __slots__ = ()
    operator = 'RIGHT JOIN'


class NaturalJoin(JoinExpression):
    """Expression representing a 'NATURAL JOIN'
    """
    __slots__ = ()
    operator = 'NATURAL JOIN'


class NaturalLeftJoin(JoinExpression):
    """Expression representing a 'NATURAL LEFT JOIN'
    """
    __slots__ = ()
    operator = 'NATURAL LEFT JOIN'


class NaturalRightJoin(JoinExpression):
    """Expression representing a 'NATURAL RIGHT JOIN'
    """
    __slots__ = ()
    operator = 'NATURAL RIGHT JOIN'
