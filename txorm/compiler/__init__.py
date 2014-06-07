
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Compiler
"""

from __future__ import unicode_literals

import re
from decimal import Decimal
from collections import defaultdict
from datetime import datetime, date, time, timedelta

from txorm.compat import itervalues
from txorm.compat import text_type, binary_type, integer_types, _PY3

from txorm import Undef
from .base import Context
from .tables import Table
from .fields import Field, Alias
from .expressions import AutoTables
from .plain_sql import SQL, SQLToken
from .comparable import LShift, RShift
from .comparable import Func, NamedFunc, Cast
from .comparable import Add, Sub, Mul, Div, Mod, Neg
from txorm.exceptions import CompileError, NoTableError
from .expressions import Union, Except, Intersect, Distinct
from .tables import Join, LeftJoin, RightJoin, JoinExpression
from .base import Compile, txorm_compile, txorm_compile_python
from .tables import NaturalJoin, NaturalLeftJoin, NaturalRightJoin
from .expressions import Select, Insert, Update, Delete, SetExpression
from .expressions import Expression, PrefixExpression, SuffixExpression
from .comparable import Or, And, Eq, Ne, Gt, Ge, Lt, Le, Like, In, Count
from .comparable import (
    CompoundOperator, NonAssocBinaryOperator, BinaryOperator
)
from txorm.variable import (
    Variable, RawStrVariable, UnicodeVariable, DateTimeVariable,
    DateVariable, TimeVariable, TimeDeltaVariable, BoolVariable,
    IntVariable, DecimalVariable, FloatVariable
)

# expression contexts
TABLE = Context('TABLE')
EXPR = Context('EXPR')
FIELD = Context('FIELD')
FIELD_PREFIX = Context('FIELD_PREFIX')
FIELD_NAME = Context('FIELD_NAME')
SELECT = Context('SELECT')

is_safe_token = re.compile('^[a-zA-Z][a-zA-Z0-9_]*$').match


# basic expression
@txorm_compile.when(Expression)
def compile_python_unsupported(compile, expression, state):
    raise CompileError('Can not compile python expression with {!r}'.format(
        type(expression)
    ))


# compile functions
# the decorator builds the expressions disptach table
@txorm_compile.when(binary_type)
def compile_str(compile, expression, state):
    """Compile a binary_type argument storing a valid value
    """

    state.parameters.append(RawStrVariable(expression))
    return '?'


@txorm_compile.when(text_type)
def compile_text_type(compile, expression, state):
    """Compile a text_type argument storing a valid value
    """
    state.parameters.append(UnicodeVariable(expression))
    return '?'


@txorm_compile.when(*integer_types)
def compile_int(compile, expression, state):
    """Compile int and long (or int in Python3) argumnt storing a valid value
    """
    state.parameters.append(IntVariable(expression))
    return '?'


@txorm_compile.when(float)
def compile_float(compile, expression, state):
    """Compile a float argument storing a valid value
    """
    state.parameters.append(FloatVariable(expression))
    return '?'


@txorm_compile.when(Decimal)
def compile_decimal(compile, expression, state):
    """Compile a :class:`decimal.Decimal` argument storing a valid value
    """
    state.parameters.append(DecimalVariable(expression))
    return '?'


@txorm_compile.when(bool)
def compile_bool(compile, expression, state):
    """Compile a bool argument storing a valid value
    """
    state.parameters.append(BoolVariable(expression))
    return '?'


@txorm_compile.when(datetime)
def compile_datetime(compile, expression, state):
    """Compile a datetime argument storing a valid value
    """
    state.parameters.append(DateTimeVariable(expression))
    return '?'


@txorm_compile.when(date)
def compile_date(compile, expression, state):
    """Compile a date argument storing a valid value
    """
    state.parameters.append(DateVariable(expression))
    return '?'


@txorm_compile.when(time)
def compile_time(compile, expression, state):
    """Compile a time argument storing a valid value
    """
    state.parameters.append(TimeVariable(expression))
    return '?'


@txorm_compile.when(timedelta)
def compile_timedelta(compile, expression, state):
    """Compile a timedelta argument storing a valid value
    """
    state.parameters.append(TimeDeltaVariable(expression))
    return '?'


@txorm_compile.when(type(None))
def compile_none(compile, expression, state):
    """Compile None into NULL
    """
    return 'NULL'


@txorm_compile_python.when(
    float, type(None), text_type, binary_type, *integer_types)
def compile_python_builtin(compile, expression, state):
    """Compile builting python (2 and 3) types into the right representation
    """
    return repr(expression)


@txorm_compile_python.when(bool, datetime, date, time, timedelta)
def compile_python_bool_and_dates(compile, expression, state):
    """Compile python boolean and date/time types into right representation
    """
    index = len(state.parameters)
    state.parameters.append(expression)
    return '_{}'.format(index)


@txorm_compile.when(Variable)
def compile_variables(compile, variable, state):
    """Compile any type of Variable argument storing a valid value
    """
    state.parameters.append(variable)
    return '?'


@txorm_compile_python.when(Variable)
def compile_python_variable(compile, variable, state):
    """Compile any other type of variable to the right representation
    """
    index = len(state.parameters)
    state.parameters.append(variable.get())
    return '_{}'.format(index)


@txorm_compile.when(Cast)
def compile_cast(compile, cast, state):
    """Compile CAST function
    """

    state.push('context', EXPR)
    field = compile(cast.field, state)
    state.pop()
    return 'CAST({} AS {})'.format(field, cast.type)


@txorm_compile_python.when(Neg)
def compile_python_neg(compile, neg, state):
    """Compile Neg to python right representation
    """
    return '-{}'.format(compile(neg.expression, state, raw=True))


# distinct expression
@txorm_compile.when(Distinct)
def compile_distinct(compile, distinct, state):
    """Compile a DISTINCT prefix
    """
    return 'DISTINCT {}'.format(compile(distinct.expression, state))


# compile fields
@txorm_compile.when(Field)
def compile_field(compile, field, state):
    """Compile a database field
    """

    def _check_cache():
        if field.compile_id != id(compile):
            field.compile_cache = compile(field.name, state, token=True)
            field.compile_id = id(compile)

    if field.table is not Undef:
        state.auto_tables.append(field.table)

    if field.table is Undef or state.context is FIELD_NAME:
        if state.aliases is not None:
            alias = state.aliases.get(field)
            if alias is not None:
                return compile(alias.name, state, token=True)

        _check_cache()
        return field.compile_cache

    state.push('context', FIELD_PREFIX)
    table = compile(field.table, state, token=True)
    state.pop()

    _check_cache()
    return '{}.{}'.format(table, field.compile_cache)


@txorm_compile_python.when(Field)
def compile_python_field(compile, field, state):
    """Compile a Field variable into the right representation
    """
    index = len(state.parameters)
    state.parameters.append(field)
    return 'get_field(_{})'.format(index)


@txorm_compile.when(Count)
def compile_count(compile, count, state):
    """Compile COUNT function
    """

    if count.field is not Undef:
        state.push('context', EXPR)
        field = compile(count.field, state)
        state.pop()

        if count.distinct:
            return 'COUNT(DISTINCT {})'.format(field)

        return 'COUNT({})'.format(field)

    return 'COUNT(*)'


# compile expressions
@txorm_compile_python.when(Expression)
def compile_python_non_supported(compile, expression, state):
    """Compile expressions into python is not supported
    """
    raise CompileError('Can not compile python expressions with {!r}'.format(
        type(expression)
    ))


@txorm_compile.when(Select)
def compile_select(compile, select, state):
    """Compile a SELECT statement
    """

    tokens = ['SELECT ']
    state.push('auto_tables', [])
    state.push('context', FIELD)

    if select.distinct:
        tokens.append('DISTINCT ')
        if isinstance(select.distinct, (tuple, list)):
            tokens.append('ON ({}) '.format(
                compile(select.distinct, state, raw=True))
            )

    tokens.append(compile(select.fields, state))
    tables_pos = len(tokens)
    parameters_pos = len(state.parameters)
    state.context = EXPR

    tlist = ('where', 'group_by', 'having', 'order_by', 'limit', 'offset')
    for token in tlist:
        if getattr(select, token, Undef) is not Undef:
            tokens.append(' {} {}'.format(
                token.upper().replace('_', ' '),
                getattr(select, token) if token in ('offset', 'limit') else '')
            )
            if token not in ('offset', 'limit'):
                tokens.append(compile(getattr(select, token), state, raw=True))

    if has_tables(state, select):
        state.context = TABLE
        state.push('parameters', [])
        tokens.insert(tables_pos, ' FROM ')
        tokens.insert(
            tables_pos + 1, build_tables(
                compile, select.tables, select.default_tables, state
            )
        )
        parameters = state.parameters
        state.pop()
        state.parameters[parameters_pos:parameters_pos] = parameters
    state.pop()
    state.pop()

    if _PY3:
        _tokens = []
        for token in tokens:
            if isinstance(token, binary_type):
                _tokens.append(token.decode())
            else:
                _tokens.append(token)
        return ''.join(_tokens)

    return ''.join(tokens)


@txorm_compile.when(Insert)
def compile_insert(compile, insert, state):
    """Compile a INSERT statement
    """

    state.push('context', FIELD_NAME)
    fields = compile(tuple(insert.map), state, token=True)
    state.context = TABLE
    table = build_tables(compile, insert.table, insert.default_table, state)
    state.context = EXPR
    values = insert.values
    if values is Undef:
        values = [tuple(itervalues(insert.map))]

    if isinstance(values, Expression):
        compiled = compile(values, state)
    else:
        compiled = (
            'VALUES ({})'.format(
                '), ('.join(compile(value, state) for value in values)
            )
        )
    state.pop()
    return ''.join(['INSERT INTO ', table, ' (', fields, ') ', compiled])


@txorm_compile.when(Update)
def compile_update(compile, update, state):
    """Compile a UPDATE statement
    """

    _map = update.map
    state.push('context', FIELD_NAME)
    sets = ['{}={}'.format(
        compile(field, state, token=True), compile(_map[field], state))
        for field in _map
    ]
    state.context = TABLE
    tokens = ['UPDATE ', build_tables(
        compile, update.table,
        update.default_table, state), ' SET ', ', '.join(sets)
    ]

    if update.where is not Undef:
        state.context = EXPR
        tokens.append(' WHERE ')
        tokens.append(compile(update.where, state, raw=True))

    state.pop()
    return ''.join(tokens)


@txorm_compile.when(Delete)
def compile_delete(compile, delete, state):
    """Compile a DELETE statement
    """

    tokens = ['DELETE FROM ', None]
    state.push('context', EXPR)
    if delete.where is not Undef:
        tokens.append(' WHERE ')
        tokens.append(compile(delete.where, state, raw=True))

    # compile later for auto_tables support
    state.context = TABLE
    tokens[1] = build_tables(
        compile, delete.table, delete.default_table, state)

    state.pop()
    return ''.join(tokens)


@txorm_compile.when(Func, NamedFunc)
def compile_func(compile, func, state):
    """Compile a function or named function (like SUM, SUB, ADD...)
    """

    state.push('context', EXPR)
    args = compile(func.args, state)
    state.pop()
    return '{}({})'.format(func.name, args)


# compile comparables
@txorm_compile.when(CompoundOperator)
def compile_compound_operator(compile, expression, state):
    """Compile common compound operators
    """
    return compile(expression.expressions, state, join=expression.operator)


@txorm_compile_python.when(CompoundOperator)
def compile_python_and_or(compile, expression, state):
    """Compile and & or python statements
    """
    join = expression.operator.lower()
    return compile(expression.expressions, state, join=join)


@txorm_compile.when(And, Or)
def compile_and_or(compile, expression, state):
    """Compile compound operators AND & OR
    """
    join = expression.operator
    return compile(expression.expressions, state, join=join, raw=True)


@txorm_compile.when(NonAssocBinaryOperator)
@txorm_compile_python.when(NonAssocBinaryOperator)
def compile_non_assoc_binary_operator(compile, expression, state):
    """Compile non binary associative operators like (Sub, Add, etc)
    """

    expression1 = compile(expression.expressions[0], state)
    state.precedence += 0.5   # enforce parenthesis
    expression2 = compile(expression.expressions[1], state)
    return '{}{}{}'.format(expression1, expression.operator, expression2)


# from expressions
@txorm_compile.when(Table)
def compile_table(compile, table, state):
    """Compile a table expression
    """

    if table.compile_id != id(compile):
        table.compile_cache = compile(table.name, state, token=True)
        table.compile_id = id(compile)

    return table.compile_cache


# alias expressions
@txorm_compile.when(Alias)
def compile_alias(compile, alias, state):
    """Compule an aias
    """

    name = compile(alias.name, state, token=True)
    if state.context is FIELD or state.context is TABLE:
        return '{} AS {}'.format(compile(alias.expression, state), name)

    return name


# set expressions
@txorm_compile.when(SetExpression)
def compile_set_expression(compile, expression, state):
    """Compile a set expression
    """

    # usually, databases have trouble using fully qualified column names
    # when ORDER BY is present. We transform pure column names into aliases
    # and use them in the ORDER BY to pevent this
    aliases = {}
    for subexpression in expression.expressions:
        if isinstance(subexpression, Select):
            fields = subexpression.fields
            if not isinstance(fields, (tuple, list)):
                fields = [fields]
            else:
                fields = list(fields)

            for i, field in enumerate(fields):
                if field not in aliases:
                    if isinstance(field, Field):
                        aliases[field] = fields[i] = Alias(field)
                    elif isinstance(field, Alias):
                        aliases[field.expression] = field

            subexpression.fields = fields

    state.push('context', SELECT)
    # In the statement:
    #   SELECT foo UNION SELECT bar LIMIT 1
    # The LIMIT 1 applies to the union results, not the SELECT bar
    # This ensures that parentheses will be placed around the
    # sub-selects in the expression.
    state.precedence += 0.5
    operator = expression.operator
    if expression.all:
        operator += 'ALL '

    statement = compile(expression.expressions, state, join=operator)
    state.precedence -= 0.5
    if expression.order_by is not Undef:
        state.context = FIELD_NAME
        if state.aliases is None:
            state.push('aliases', aliases)
        else:
            # previously defined aliases have precedence
            aliases.update(state.aliases)
            state.aliases = aliases
            aliases = None

        statement += ' ORDER BY {}' .format(
            compile(expression.order_by, state)
        )
        if aliases is not None:
            state.pop()

    if expression.limit is not Undef:
        statement += ' LIMIT {}'.format(expression.limit)
    if expression.offset is not Undef:
        statement += ' OFFSET {}'.format(expression.offset)

    state.pop()
    return statement


# compile operators
@txorm_compile.when(BinaryOperator)
@txorm_compile_python.when(BinaryOperator)
def compile_binary_operator(compile, expression, state):
    """Compile binary operators
    """

    return '{}{}{}'.format(
        compile(expression.expressions[0], state),
        expression.operator,
        compile(expression.expressions[1], state),
    )


@txorm_compile.when(Eq)
def compile_eq(compile, eq, state):
    """Compile Eq operator
    """

    if eq.expressions[1] is None:
        return '{} IS NULL'.format(compile(eq.expressions[0], state))

    return '{} = {}'.format(
        compile(eq.expressions[0], state), compile(eq.expressions[1], state)
    )


@txorm_compile_python.when(Eq)
def compile_python_eq(compile, eq, state):
    """Compile Eq operator to the right python representation
    """
    return '{} == {}'.format(
        compile(eq.expressions[0], state), compile(eq.expressions[1], state)
    )


@txorm_compile.when(Ne)
def compile_ne(compile, ne, state):
    """Compile Ne operator
    """

    if ne.expressions[1] is None:
        return '{} IS NOT NULL'.format(compile(ne.expressions[0], state))

    return '{} != {}'.format(
        compile(ne.expressions[0], state), compile(ne.expressions[1], state)
    )


@txorm_compile.when(In)
def compile_in(compile, expression, state):
    """Compile In operator
    """

    expression1 = compile(expression.expressions[0], state)
    state.precedence = 0  # enforce parentehsis here
    return '{} IN ({})'.format(
        expression1, compile(expression.expressions[1], state)
    )


@txorm_compile_python.when(In)
def compile_python_in(compile, expression, state):
    """Compile In operator into the right python representation
    """

    expression1 = compile(expression.expressions[0], state)
    state.precedence = 0  # enforce parentehsis here
    return '{} in ({},)'.format(
        expression1, compile(expression.expressions[1], state)
    )


@txorm_compile.when(Like)
def compile_like(compile, like, state, operator=None):
    """Compile a LIKE operator
    """

    statement = '{}{}{}'.format(
        compile(like.expressions[0], state),
        operator if operator is not None else like.operator,
        compile(like.expressions[1], state)
    )

    if like.escape is not Undef:
        statement = '{} ESCAPE {}'.format(
            statement, compile(like.escape, state)
        )

    return statement


# prefix and suffix
@txorm_compile.when(PrefixExpression)
def compile_prefix(compile, expression, state):
    """Compile a prefix expression
    """
    return '{} {}'.format(
        expression.prefix, compile(expression.expression, state, raw=True))


@txorm_compile.when(SuffixExpression)
def compile_suffix(compile, expression, state):
    """Compile a suffix  expression
    """
    return '{} {}'.format(
        compile(expression.expression, state, raw=True), expression.suffix
    )


# autotable
@txorm_compile.when(AutoTables)
def compile_auto_tables(compile, expression, state):
    """Compile auto tables
    """

    if expression.replace is True:
        state.push('auto_tables', [])

    statement = compile(expression.expression, state)
    if expression.replace is True:
        state.pop()

    state.auto_tables.extend(expression.tables)
    return statement


# joins
@txorm_compile.when(JoinExpression)
def compile_join(compile, join, state):
    """Compile a JOIN expression
    """

    result = []
    if join.left is not Undef:
        statement = compile(join.left, state, token=True)
        result.append(statement)

        if state.join_tables is not None:
            state.join_tables.add(statement)

    result.append(join.operator)

    # joins are left associative so ensure joins in the right hand
    # argument get parenthesis enforcing it
    state.precedence += 0.5
    statement = compile(join.right, state, token=True)
    result.append(statement)

    if state.join_tables is not None:
        state.join_tables.add(statement)
    if join.on is not Undef:
        state.push('context', EXPR)
        result.append('ON')
        result.append(compile(join.on, state, raw=True))
        state.pop()

    return ' '.join(result)


# plain SQL
@txorm_compile.when(SQLToken)
def compile_sql_token(compile, expression, state):
    if is_safe_token(expression) and not compile.is_reserved_word(expression):
        return expression

    return '"{}"'.format(expression.replace('"', '""'))


@txorm_compile_python.when(SQLToken)
def compile_python_sql_token(compile, expression, state):
    return expression


@txorm_compile.when(SQL)
def compile_sql(compile, expression, state):
    """Compile SQL expression
    """

    if expression.params is not Undef:
        if not isinstance(expression.params, (tuple, list)):
            raise CompileError(
                'Parameters should be a list or a tuple not {!r}'.format(
                    type(expression.params)
                )
            )

        for param in expression.params:
            state.parameters.append(param)

    if expression.tables is not Undef:
        state.auto_tables.append(expression.tables)

    return expression.expression


# statement expressions
def has_tables(state, expression):
    """Determine if a given expression has tables
    """
    return (
        expression.tables is not Undef
        or expression.default_tables is not Undef
        or state.auto_tables
    )


def build_tables(compile, tables, default_tables, state):
    """Compile the given tables

    Tables will be built from either `tables`, `default_tables` or
    `state.auto_tables`.
    """

    tables = _parse_tables(tables, default_tables, state)

    # single element
    if type(tables) not in (list, tuple) or len(tables) == 1:
        return txorm_compile(tables, state, token=True)

    # coumpound element
    return _compile_coumpound(compile, tables, state)


def _compile_coumpound(compile, tables, state):
    """Compile a coumpound expression
    """

    if _no_joins_in(tables):
        if tables is state.auto_tables:
            tables = set(compile(table, state, token=True) for table in tables)
            return ', '.join(sorted(tables))

        return compile(tables, state, token=True)

    if tables is state.auto_tables:
        statements = defaultdict(lambda: set())
        # push a join_tables onto the state: compile calls below will populate
        # this set so we can know what tables not to include
        state.push('join_tables', set())

        for elem in tables:
            statement_name = 'table'
            statement = compile(elem, state, token=True)
            if isinstance(elem, JoinExpression):
                statement_name = 'half_join' if elem.left is Undef else 'join'

            statements[statement_name].add(statement)

        # remove tables that were seen in join statements
        statements['table'] -= state.join_tables
        state.pop()  # remove the join_tables set from the state

        result = ', '.join(
            sorted(statements['table']) + sorted(statements['join'])
        )

        if statements['half_join']:
            result = '{} {}'.format(
                result, ' '.join(sorted(statements['half_join']))
            )
    else:
        result = []
        for elem in tables:
            if len(result) > 0:
                if isinstance(elem, JoinExpression) and elem.left is Undef:
                    result.append(' ')
                else:
                    result.append(', ')

            result.append(compile(elem, state, token=True))

    return ''.join(result)


def _parse_tables(tables, default_tables, state):
    """
    If `tables` is not `Undef`, it will be used.
    If `tables` is `Undef` and `state.auto_tables` is available, that's used
    instead. If neither `tables` nor `state.auto_tables` are available,
    `default_tables` is tried as a last failback. If none of them are
    available, `NoTableError` is raised
    """

    if tables is Undef:
        if state.auto_tables:
            tables = state.auto_tables
        elif default_tables is not Undef:
            tables = default_tables
        else:
            raise NoTableError('Could not find any tables')

    return tables


def _no_joins_in(tables):
    """Return True is there is no Joins in the given expression tables
    """

    for table in tables:
        if isinstance(table, JoinExpression):
            return False

    return True


# set operator precedence
txorm_compile.set_precedence(10, Select, Insert, Update, Delete)
txorm_compile.set_precedence(10, Join, LeftJoin, RightJoin)
txorm_compile.set_precedence(
    10, NaturalJoin, NaturalLeftJoin, NaturalRightJoin)
txorm_compile.set_precedence(10, Union, Except, Intersect)
txorm_compile.set_precedence(20, SQL)
txorm_compile.set_precedence(30, Or)
txorm_compile.set_precedence(40, And)
txorm_compile.set_precedence(50, Eq, Ne, Gt, Ge, Lt, Le, Like, In)
txorm_compile.set_precedence(60, LShift, RShift)
txorm_compile.set_precedence(70, Add, Sub)
txorm_compile.set_precedence(80, Mul, Div, Mod)

txorm_compile_python.set_precedence(10, Or)
txorm_compile_python.set_precedence(20, And)
txorm_compile_python.set_precedence(30, Eq, Ne, Gt, Ge, Lt, Le, Like, In)
txorm_compile_python.set_precedence(40, LShift, RShift)
txorm_compile_python.set_precedence(50, Add, Sub)
txorm_compile_python.set_precedence(60, Mul, Div, Mod)

# reserved words, from SQL1992

txorm_compile.add_reserved_words(
    """
    absolute action add all allocate alter and any are as asc assertion at
    authorization avg begin between bit bit_length both by cascade cascaded
    case cast catalog char character char_ length character_length check close
    coalesce collate collation column commit connect connection constraint
    constraints continue convert corresponding count create cross current
    current_date current_time current_timestamp current_ user cursor date day
    deallocate dec decimal declare default deferrable deferred delete desc
    describe descriptor diagnostics disconnect distinct domain double drop
    else end end-exec escape except exception exec execute exists external
    extract false fetch first float for foreign found from full get global go
    goto grant group having hour identity immediate in indicator initially
    inner input insensitive insert int integer intersect interval into is
    isolation join key language last leading left level like local lower
    match max min minute module month names national natural nchar next no
    not null nullif numeric octet_length of on only open option or order
    outer output overlaps pad partial position precision prepare preserve
    primary prior privileges procedure public read real references relative
    restrict revoke right rollback rows schema scroll second section select
    session session_ user set size smallint some space sql sqlcode sqlerror
    sqlstate substring sum system_user table temporary then time timestamp
    timezone_ hour timezone_minute to trailing transaction translate
    translation trim true union unique unknown update upper usage user using
    value values varchar varying view when whenever where with work write
    year zone
    """.split())


__all__ = [
    'CompileError', 'Compile', 'txorm_compile', 'txorm_compile_python',
    'SQL', 'LShift', 'RShift', 'Join', 'LeftJoin', 'RightJoin', 'NaturalJoin',
    'NaturalLeftJoin', 'NaturalRightJoin', 'Union', 'Except', 'Intersect',
    'Or', 'And', 'Eq', 'Ne', 'Gt', 'Ge', 'Lt', 'Le', 'Like', 'In', 'Mul',
    'Div', 'Mod', 'Add', 'Sub', 'NoTableError'
]
