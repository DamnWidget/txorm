# -*- test-case-name: txorm.test.test_expressions -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from txorm import Undef
from txorm.utils.construct import parse_args


class ExpressionError(Exception):
    """Fired when there is some exception in the expressions
    """


class Expression(object):
    '''Empty base expression class
    '''

    __slots__ = ()


class Select(Expression):
    '''Expression representing a select statement

    :param fields: the fields to return back from the table
    :param where: a sequence of tuples or a expression to conditional select
    :param tables: a tuple of tables from where select data
    :param default_tables: default tables to use if no tables are passed
    :param order_by: a sequence of tuples or an expression to order results
    :param group_by: a sequence of tuples or an expression to group values
    :param limit: an integer to limit the results from the database
    :param offset: an integer that set how many results we get back
    :param distinct: a bolean value that determine if distinct is use or not
    :param having: a having expression
    '''

    __slots__ = (
        'fields', 'where', 'tables', 'default_tables', 'order_by',
        'group_by', 'limit', 'offset', 'distinct', 'having'
    )

    def __init__(self, fields, *args, **kwargs):
        self.fields = fields
        parse_args(self, *args, **kwargs)
        if getattr(self, 'distinct', Undef) is Undef:
            self.distinct = False


class Insert(Expression):
    '''Expression representing an insert statement

    :param map: dictionary mapping fields to values, or a sequence of fields
    :param table: table where the row will be inserted
    :param default_table: table to use if not table is explicitly provided
    :param primary_fields: tuple of fields forming the primary key
    :param primary_variables: tuple of variables with values for the PK
    :param values: expression or sequence of tuples of values to insert
    '''

    __slots__ = (
        'map', 'table', 'default_table',
        'primary_fields', 'primary_variables', 'values'
    )

    def __init__(self, map, *args, **kwargs):
        self.map = map
        parse_args(self, *args, **kwargs)


class Update(Expression):
    '''Expression representing an update statement

    :param map: dictionary mapping fields to values, or a sequence of fields
    :param where: a sequence of tuples or an expression to conditional update
    :param table: table where the row will be updated
    :param default_table: table to use if not table is explicitly provided
    :param primary_fields: tuple of fields forming the primary key
    '''

    __slots__ = ('map', 'where', 'table', 'default_table', 'primary_fields')

    def __init__(self, map, *args, **kwargs):
        self.map = map
        parse_args(self, *args, **kwargs)


class Delete(Expression):
    """Expression representing a delete statement

    :param where: a sequence of tuples or an expression to conditional update
    :param table: table where the row will be updated
    :param default_table: table to use if not table is explicitly provided
    """

    __slots__ = ('where', 'table', 'default_table')

    def __init__(self, *args, **kwargs):
        parse_args(self, *args, **kwargs)


class Distinct(Expression):
    """Add the 'DISTINCT' prefix to an expression

    :param expression: the expression to prefix
    """
    __slots__ = ('expression',)

    def __init__(self, expression):
        self.expression = expression


class SetExpression(Expression):
    """Expression used to add the 'SET' operation on update statements

    :param expressions: a tuple of expressions to
    :param all: a boolean value that ???
    :param order_by: a sequence of tuples or an expression to order operation
    :param limit: an integer to limit the results from the database
    :param offset: an integer that set how many results we get back
    """

    __slots__ = ('expressions', 'all', 'order_by', 'limit', 'offset')
    operator = ' (unknown) '

    def __init__(self, *expressions, **kwargs):
        self.expressions = expressions
        self.all = kwargs.get('all', False)
        self.order_by = kwargs.get('order_by', Undef)
        self.limit = kwargs.get('limit', Undef)
        self.offset = kwargs.get('offset', Undef)

        if len(self.expressions) > 0:
            first = self.expressions[0]
            if (isinstance(first, self.__class__)
                and first.all == self.all and first.limit is Undef
                    and first.offset is Undef):
                self._include_sub_expressions(first)

    def _include_sub_expressions(self, expression):
        """Include expression expressions as sub expressions
        """

        self.expressions = expression.expressions + self.expressions[1:]


class Union(SetExpression):
    """Expression representing an Union clause
    """
    __slots__ = ()
    operator = ' UNION '


class Except(SetExpression):
    """Expression representing an Except clause
    """
    __slots__ = ()
    operator = ' EXCEPT '


class Intersect(SetExpression):
    """Expression representing an Insertsect clause
    """
    __slots__ = ()
    operator = ' INTERSECT '


class FromExpression(Expression):
    """Expression representing a 'FROM' stetement
    """
    __slots__ = ()


class PrefixExpression(Expression):
    """Expression representing a prefix

    :param expression: the expression to be prefixed
    """

    __slots__ = ('expression',)
    prefix = '(unknown)'

    def __init__(self, expression):
        self.expression = expression


class SuffixExpression(Expression):
    """Expression representing a suffix

    :param expression: the expression to be suffixed
    """

    __slots__ = ('expression',)
    sufix = '(unknown)'

    def __init__(self, expression):
        self.expression = expression


class AutoTables(Expression):
    """This class will inject one or more entries in state.auto_tables

    If the param replace is set as True (False bu default),it will also
    discard any auto_table entries injected by compiling the given expression
    """

    __slots__ = ('expression', 'tables', 'replace')

    def __init__(self, expression, tables, replace=True):
        assert type(tables) in (list, tuple)
        self.expression = expression
        self.tables = tables
        self.replace = replace


class Sequence(Expression):
    """Expression representing auto-incrementing support from the databases

    This should be translated into the *next* value of the named
    auto-incrementing sequence. There's no standard way to compile a sequence,
    since it's very database dependant.

    Example of usage:

    .. sourcecode:: python

        classs Class(object):
            (...)
            id = Int(default=Sequence('sequence_name'))
    """

    __slots__ = ('name')

    def __init__(self, name):
        self.name = name
