# -*- test-case-name: txorm.test.test_expressions -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from txorm import Undef
# from txorm.compat import _PY3
from txorm.variable import Variable
from .comparable import ComparableExpression


class Field(ComparableExpression):
    """Expression representing a field in some table

    :param name: the field name
    :param table: field table (maybe another expression)
    :param primary: integer representing the primary key position of the field
        or zero if it's not a primary key. May be provided as a boolean.
    :parma variable_factory: factory producing :class:`txorm.variable.Variable`
        instances typed according to this field
    """

    __slots__ = (
        'name', 'table', 'primary', 'variable_factory',
        'compile_cache', 'compile_id'
    )

    def __init__(self, name=Undef, table=Undef,
                 primary=False, variable_factory=None):
        self.name = name
        self.table = table
        self.primary = int(primary)
        self.compile_cache = None
        self.compile_id = None
        if variable_factory is not None:
            self.variable_factory = variable_factory
        else:
            self.variable_factory = Variable

    def __hash__(self):
        return hash((self.name, self.table, self.primary))


class Alias(ComparableExpression):
    """A expression representing an 'AS' alias clause

    :param expression: the :class:`Expression` to create
    :param name: the name to give to the alias, if is not given, a new one
        will be automatically generated
    """

    __slots__ = ('expression', 'name')
    auto_counter = 0

    def __init__(self, expression, name=Undef):
        self.expression = expression
        if name is Undef:
            Alias.auto_counter += 1
            name = '_{}'.format(Alias.auto_counter)

        self.name = name
