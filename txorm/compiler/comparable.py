# -*- test-case-name: txorm.test.test_expressions -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from txorm import Undef
from .prefixes import Neg
from txorm.variable import Variable
from txorm.compat import u, text_type
from .expressions import ExpressionError, Expression


class Comparable(object):
    __slots__ = ()

    # A translation table that can escape a unicode string for use in a
    # Like() expression that uses "!" as the escape character.
    like_escape = {
        ord(u('!')): u('!!'),
        ord(u('_')): u('!_'),
        ord(u('%')): u('!%')
    }

    def __eq__(self, other):
        if other is not None and not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return Eq(self, other)

    def __ne__(self, other):
        if other is not None and not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return Ne(self, other)

    def __gt__(self, other):
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return Gt(self, other)

    def __ge__(self, other):
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return Ge(self, other)

    def __lt__(self, other):
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return Lt(self, other)

    def __le__(self, other):
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return Le(self, other)

    def __rshift__(self, other):
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return RShift(self, other)

    def __lshift__(self, other):
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return LShift(self, other)

    def __and__(self, other):
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return And(self, other)

    def __or__(self, other):
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return Or(self, other)

    def __add__(self, other):
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return Add(self, other)

    def __sub__(self, other):
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return Sub(self, other)

    def __mul__(self, other):
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return Mul(self, other)

    def __div__(self, other):
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return Div(self, other)

    def __mod__(self, other):
        from .operators import Mod
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return Mod(self, other)

    def __neg__(self):
        return Neg(self)

    def is_in(self, others):
        if not isinstance(others, Expression):
            others = list(others)
            if not others:
                return False
            variable_factory = getattr(self, 'variable_factory', Variable)
            for i, other in enumerate(others):
                if not isinstance(other, (Expression, Variable)):
                    others[i] = variable_factory(value=other)
        return In(self, others)

    def like(self, other, escape=Undef, case_sensitive=None):
        if not isinstance(other, (Expression, Variable)):
            other = getattr(self, 'variable_factory', Variable)(value=other)
        return Like(self, other, escape, case_sensitive)

    def lower(self):
        return Lower(self)

    def upper(self):
        return Upper(self)

    def startswith(self, prefix):
        if not isinstance(prefix, text_type):
            raise ExpressionError('Expected unicode argument, got {!r}'.format(
                type(prefix)
            ))
        pattern = prefix.translate(self.like_escape) + u('%')
        return Like(self, pattern, u('!'))

    def endswith(self, suffix):
        if not isinstance(suffix, text_type):
            raise ExpressionError('Expected unicode argument, got {!r}'.format(
                type(suffix)
            ))
        pattern = u('%') + suffix.translate(self.like_escape)
        return Like(self, pattern, u('!'))

    def contains_string(self, substring):
        if not isinstance(substring, text_type):
            raise ExpressionError('Expected unicode argument, got {}'.format(
                type(substring)
            ))
        pattern = u('%') + substring.translate(self.like_escape) + u('%')
        return Like(self, pattern, u('!'))


class ComparableExpression(Expression, Comparable):
    """Expression representing a comparison
    """
    __slots__ = ()


class BinaryExpression(ComparableExpression):
    """Expression representing a binary comparison

    :param expression1: the first expression to compare
    :param expression2: the second expression to compare
    """

    __slots__ = ('expressions')

    def __init__(self, expression1, expression2):
        self.expressions = (expression1, expression2)


class CompoundExpression(ComparableExpression):
    """Expression representing a compound comparable

    :param expressions: the compound expressions
    """

    __slots__ = ('expressions',)

    def __init__(self, *expressions):
        self.expressions = expressions


class BinaryOperator(BinaryExpression):
    """Binary operator expression
    """
    __slots__ = ()
    operator = ' (unknown) '


class NonAssocBinaryOperator(BinaryOperator):
    """Non associative binary operator
    """
    __slots__ = ()
    opertor = ' (unknown) '


class CompoundOperator(CompoundExpression):
    """Compound operator
    """
    __slots__ = ()
    operator = ' (unknown) '


class Eq(BinaryOperator):
    """Equality operator
    """
    __slots__ = ()
    operator = ' = '


class Ne(BinaryOperator):
    """Not equality operator
    """
    __slots__ = ()
    operator = ' != '


class Gt(BinaryOperator):
    """Greather than operator
    """
    __slots__ = ()
    operator = ' > '


class Ge(BinaryOperator):
    """Greather or equal than operator
    """
    __slots__ = ()
    operator = ' >= '


class Lt(BinaryOperator):
    """Less than operator
    """
    __slots__ = ()
    operator = ' < '


class Le(BinaryOperator):
    """Less or equal than operator
    """
    __slots__ = ()
    operator = ' <= '


class RShift(BinaryOperator):
    """Right shift operator
    """
    __slots__ = ()
    operator = '>>'


class LShift(BinaryOperator):
    """Left shift operator
    """
    __slots__ = ()
    operator = '<<'


class Like(BinaryOperator):
    """Like operator

    :param expression1: the first expression
    :param expression2: the second expression
    :param escape: scape statement
    :param sensitive: is case sensitive?
    """

    __slots__ = ('escape', 'case_sensitive')
    operator = ' LIKE '

    def __init__(self, expression1, expression2, escape=Undef, sensitive=None):
        self.expressions = (expression1, expression2)
        self.escape = escape
        self.case_sensitive = sensitive


class In(BinaryOperator):
    """In statement
    """
    __slots__ = ()
    operator = ' IN '


class Add(CompoundOperator):
    """Add operator
    """
    __slots__ = ()
    operator = '+'


class Sub(NonAssocBinaryOperator):
    """Substract operator
    """
    __slots__ = ()
    operator = '-'


class Mul(CompoundOperator):
    """Multiplication operator
    """
    __slots__ = ()
    operator = '*'


class Div(NonAssocBinaryOperator):
    """Division operator
    """
    __slots__ = ()
    operator = '/'


class Mod(NonAssocBinaryOperator):
    """Division module operator
    """
    __slots__ = ()
    operator = '%'


class And(CompoundOperator):
    """And statement
    """
    __slots__ = ()
    operator = ' AND '


class Or(CompoundOperator):
    """Or statement
    """
    __slots__ = ()
    operator = ' OR '


class FuncExpression(ComparableExpression):
    __slots__ = ()
    name = '(unknown)'


class Func(FuncExpression):
    """Func mixin used in compiling decorators
    """

    __slots__ = ('name', 'args')

    def __init__(self, name, *args):
        self.name = name
        self.args = args


class NamedFunc(FuncExpression):
    """Mixing function used in compiling decorators
    """

    __slots__ = ('args',)

    def __init__(self, *args):
        self.args = args


class Count(FuncExpression):
    """Expression representing 'COUNT' named func
    """

    __slots__ = ('column', 'distinct')
    name = 'COUNT'

    def __init__(self, column=Undef, distinct=False):
        if distinct and column is Undef:
            raise ValueError('Must specify column when using distinct count')
        self.column = column
        self.distinct = distinct


class Max(NamedFunc):
    """Expression representing 'MAX' named func
    """
    __slots__ = ()
    name = 'MAX'


class Min(NamedFunc):
    """Expression representing 'MIN' named func
    """
    __slots__ = ()
    name = 'MIN'


class Avg(NamedFunc):
    """Expression representing 'AVG' named func
    """
    __slots__ = ()
    name = 'AVG'


class Sum(NamedFunc):
    """Expression representing 'SUM' named func
    """
    __slots__ = ()
    name = 'SUM'


class Lower(NamedFunc):
    """Expression representing 'LOWER' named func
    """
    __slots__ = ()
    name = 'LOWER'


class Upper(NamedFunc):
    """Expression representing 'UPPER' named func
    """
    __slots__ = ()
    name = 'UPPER'


class Coalesce(NamedFunc):
    """Expression representing 'COALESCE' named func
    """
    __slots__ = ()
    name = 'COALESCE'


class Row(NamedFunc):
    """Expression representing 'ROW' named func
    """
    __slots__ = ()
    name = 'ROW'


class Cast(FuncExpression):
    """Expression representing 'CAST' named func
    """

    __slots__ = ('column', 'type')
    name = 'CAST'

    def __init__(self, column, type):
        self.column = column
        self.type = type
