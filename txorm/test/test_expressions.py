
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Expressions Unit Tests
"""

from __future__ import unicode_literals

from datetime import datetime, date, time, timedelta

from twisted.trial import unittest

from txorm import Undef
from txorm.variable import Variable
from txorm.compiler.state import State
from txorm.compiler.tables import Join
from txorm.compiler.fields import Field, Alias
from txorm.compiler import CompileError, NoTableError
from txorm.compiler.expressions import FromExpression
from txorm.compiler.base import txorm_compile, Compile
from txorm.compiler.tables import JoinExpression, Table
from txorm.compiler.plain_sql import SQLRaw, SQLToken, SQL
from txorm.compat import _PY3, b, u, binary_type, text_type
from txorm.compiler.comparable import Add, Sub, Mul, Div, Row
from txorm.compiler.expressions import Select, Insert, Update, Delete
from txorm.compiler import TABLE, EXPR, FIELD, FIELD_NAME, FIELD_PREFIX
from txorm.compiler.expressions import Union, Except, Intersect, Sequence
from txorm.compiler.comparable import And, Or, Func, NamedFunc, Like, Eq, In
from txorm.compiler.comparable import Ne, Gt, Ge, Lt, Le, LShift, RShift
from txorm.compiler.expressions import ExpressionError, Expression, AutoTables
from txorm.variable import (
    RawStrVariable, UnicodeVariable, IntVariable, BoolVariable, FloatVariable,
    DecimalVariable, DateTimeVariable, DateVariable, TimeVariable,
    TimeDeltaVariable
)


class ExpressionsTest(unittest.TestCase):

    def test_select_default(self):
        expression = Select(())
        self.assertEqual(expression.fields, ())
        self.assertEqual(expression.where, Undef)
        self.assertEqual(expression.tables, Undef)
        self.assertEqual(expression.default_tables, Undef)
        self.assertEqual(expression.order_by, Undef)
        self.assertEqual(expression.group_by, Undef)
        self.assertEqual(expression.limit, Undef)
        self.assertEqual(expression.offset, Undef)
        self.assertEqual(expression.distinct, False)
        self.assertEqual(expression.having, Undef)

    def test_select_constructor(self):
        objects = [object() for i in range(10)]
        expression = Select(**dict(zip(Select.__slots__, objects)))
        self.assertEqual(expression.fields, objects[0])
        self.assertEqual(expression.where, objects[1])
        self.assertEqual(expression.tables, objects[2])
        self.assertEqual(expression.default_tables, objects[3])
        self.assertEqual(expression.order_by, objects[4])
        self.assertEqual(expression.group_by, objects[5])
        self.assertEqual(expression.limit, objects[6])
        self.assertEqual(expression.offset, objects[7])
        self.assertEqual(expression.distinct, objects[8])
        self.assertEqual(expression.having, objects[9])

    def test_insert_default(self):
        expression = Insert(None)
        self.assertEqual(expression.map, None)
        self.assertEqual(expression.table, Undef)
        self.assertEqual(expression.default_table, Undef)
        self.assertEqual(expression.primary_fields, Undef)
        self.assertEqual(expression.primary_variables, Undef)
        self.assertEqual(expression.values, Undef)

    def test_insert_constructor(self):
        objects = [object() for i in range(6)]
        expression = Insert(**dict(zip(Insert.__slots__, objects)))
        self.assertEqual(expression.map, objects[0])
        self.assertEqual(expression.table, objects[1])
        self.assertEqual(expression.default_table, objects[2])
        self.assertEqual(expression.primary_fields, objects[3])
        self.assertEqual(expression.primary_variables, objects[4])
        self.assertEqual(expression.values, objects[5])

    def test_update_default(self):
        expression = Update(None)
        self.assertEqual(expression.map, None)
        self.assertEqual(expression.table, Undef)
        self.assertEqual(expression.where, Undef)
        self.assertEqual(expression.table, Undef)
        self.assertEqual(expression.default_table, Undef)
        self.assertEqual(expression.primary_fields, Undef)

    def test_update_constructor(self):
        objects = [object() for i in range(5)]
        expression = Update(**dict(zip(Update.__slots__, objects)))
        self.assertEqual(expression.map, objects[0])
        self.assertEqual(expression.where, objects[1])
        self.assertEqual(expression.table, objects[2])
        self.assertEqual(expression.default_table, objects[3])
        self.assertEqual(expression.primary_fields, objects[4])

    def test_delete_default(self):
        expression = Delete()
        self.assertEqual(expression.where, Undef)
        self.assertEqual(expression.table, Undef)

    def test_delete_constructor(self):
        objects = [object() for i in range(3)]
        expression = Delete(**dict(zip(Delete.__slots__, objects)))
        self.assertEqual(expression.where, objects[0])
        self.assertEqual(expression.table, objects[1])
        self.assertEqual(expression.default_table, objects[2])

    def test_and(self):
        expression = And(elem1, elem2, elem3)
        self.assertEqual(expression.expressions, (elem1, elem2, elem3))

    def test_or(self):
        expression = Or(elem1, elem2, elem3)
        self.assertEqual(expression.expressions, (elem1, elem2, elem3))

    def test_field_default(self):
        expression = Field()
        self.assertEqual(expression.name, Undef)
        self.assertEqual(expression.table, Undef)
        self.assertEqual(expression.compile_cache, None)
        self.assertTrue(expression.primary is 0)
        self.assertEqual(expression.variable_factory, Variable)

    def test_field_constructor(self):
        objects = [object() for i in range(3)]
        objects.insert(2, True)
        expression = Field(**dict(zip(Field.__slots__, objects)))
        self.assertEqual(expression.name, objects[0])
        self.assertEqual(expression.table, objects[1])
        self.assertTrue(expression.primary is 1)
        self.assertEqual(expression.variable_factory, objects[3])

    def test_func(self):
        expression = Func('dafunc', elem1, elem2)
        self.assertEqual(expression.name, 'dafunc')
        self.assertEqual(expression.args, (elem1, elem2))

    def test_named_func(self):
        class MyFunc(NamedFunc):
            name = 'dafunc'
        expression = MyFunc(elem1, elem2)
        self.assertEqual(expression.name, 'dafunc')
        self.assertEqual(expression.args, (elem1, elem2))

    def test_like(self):
        expression = Like(elem1, elem2)
        self.assertEqual(expression.expressions[0], elem1)
        self.assertEqual(expression.expressions[1], elem2)

    def test_like_escape(self):
        expression = Like(elem1, elem2, elem3)
        self.assertEqual(expression.expressions[0], elem1)
        self.assertEqual(expression.expressions[1], elem2)
        self.assertEqual(expression.escape, elem3)

    def test_like_case(self):
        expression = Like(elem1, elem2, elem3)
        self.assertEqual(expression.case_sensitive, None)
        expression = Like(elem1, elem2, elem3, True)
        self.assertEqual(expression.case_sensitive, True)
        expression = Like(elem1, elem2, elem3, False)
        self.assertEqual(expression.case_sensitive, False)

    def test_startswith(self):
        expression = Func1()
        self.assertRaises(
            ExpressionError, expression.startswith, b('not a unicode string')
        )

        like_expression = expression.startswith(u('abc!!_%'))
        self.assertTrue(isinstance(like_expression, Like))
        self.assertTrue(like_expression.expressions[0] is expression)
        self.assertEqual(like_expression.expressions[1], u('abc!!!!!_!%%'))
        self.assertEqual(like_expression.escape, u('!'))

    def test_endswith(self):
        expression = Func1()
        self.assertRaises(
            ExpressionError, expression.startswith, b('not a unicode string')
        )

        like_expression = expression.endswith(u('abc!!_%'))
        self.assertTrue(isinstance(like_expression, Like))
        self.assertTrue(like_expression.expressions[0] is expression)
        self.assertTrue(like_expression.expressions[1], u('%abc!!!!!_!%'))
        self.assertEqual(like_expression.escape, u('!'))

    def test_contains_sring(self):
        expression = Func1()
        self.assertRaises(
            ExpressionError,
            expression.contains_string, b('not a unicode string')
        )

        like_expression = expression.contains_string(u('abc!!_%'))
        self.assertTrue(isinstance(like_expression, Like))
        self.assertTrue(like_expression.expressions[0] is expression)
        self.assertEqual(like_expression.expressions[1], u('%abc!!!!!_!%%'))
        self.assertEqual(like_expression.escape, u('!'))

    def test_eq(self):
        expression = Eq(elem1, elem2)
        self.assertEqual(expression.expressions[0], elem1)
        self.assertEqual(expression.expressions[1], elem2)

    def test_sql_default(self):
        expression = SQL(None)
        self.assertEqual(expression.expression, None)
        self.assertEqual(expression.params, Undef)
        self.assertEqual(expression.tables, Undef)

    def test_sql_constructor(self):
        objects = [object() for i in range(3)]
        expression = SQL(**dict(zip(SQL.__slots__, objects)))
        self.assertEqual(expression.expression, objects[0])
        self.assertEqual(expression.params, objects[1])
        self.assertEqual(expression.tables, objects[2])

    def test_join_expression_right(self):
        expression = JoinExpression(None)
        self.assertEqual(expression.right, None)
        self.assertEqual(expression.left, Undef)
        self.assertEqual(expression.on, Undef)

    def test_join_expression_on(self):
        on = Expression()
        expression = JoinExpression(None, on)
        self.assertEqual(expression.right, None)
        self.assertEqual(expression.left, Undef)
        self.assertEqual(expression.on, on)

    def test_join_expression_on_keyword(self):
        on = Expression()
        expression = JoinExpression(None, on=on)
        self.assertEqual(expression.right, None)
        self.assertEqual(expression.left, Undef)
        self.assertEqual(expression.on, on)

    def test_join_expression_on_invalid(self):
        on = Expression()
        self.assertRaises(ExpressionError, JoinExpression, None, on, None)

    def test_join_expression_right_left(self):
        objects = [object() for i in range(2)]
        expression = JoinExpression(*objects)
        self.assertEqual(expression.left, objects[0])
        self.assertEqual(expression.right, objects[1])
        self.assertEqual(expression.on, Undef)

    def test_join_expression_right_left_on(self):
        objects = [object() for i in range(3)]
        expression = JoinExpression(*objects)
        self.assertEqual(expression.left, objects[0])
        self.assertEqual(expression.right, objects[1])
        self.assertEqual(expression.on, objects[2])

    def test_join_expression_right_join(self):
        join = JoinExpression(None)
        expression = JoinExpression(None, join)
        self.assertEqual(expression.right, join)
        self.assertEqual(expression.left, None)
        self.assertEqual(expression.on, Undef)

    def test_table(self):
        objects = [object() for i in range(1)]
        expression = Table(*objects)
        self.assertEqual(expression.name, objects[0])

    def test_alias_default(self):
        expression = Alias(None)
        self.assertEqual(expression.expression, None)
        self.assertTrue(isinstance(expression.name, text_type))

    def test_alias_constructor(self):
        objects = [object() for i in range(2)]
        expression = Alias(*objects)
        self.assertEqual(expression.expression, objects[0])
        self.assertEqual(expression.name, objects[1])

    def test_union(self):
        expression = Union(elem1, elem2, elem3)
        self.assertEqual(expression.expressions, (elem1, elem2, elem3))

    def test_union_with_kwargs(self):
        expression = Union(
            elem1, elem2, all=True, order_by=(), limit=1, offset=2
        )
        self.assertEqual(expression.expressions, (elem1, elem2))
        self.assertEqual(expression.all, True)
        self.assertEqual(expression.order_by, ())
        self.assertEqual(expression.limit, 1)
        self.assertEqual(expression.offset, 2)

    def test_union_collapse(self):
        expression = Union(Union(elem1, elem2), elem3)
        self.assertEqual(expression.expressions, (elem1, elem2, elem3))

        # only first expression is collapsed
        expression = Union(elem1, Union(elem2, elem3))
        self.assertEqual(expression.expressions[0], elem1)
        self.assertTrue(isinstance(expression.expressions[1], Union))

        # don't collapse if all is different
        expression = Union(Union(elem1, elem2, all=True), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Union))
        expression = Union(Union(elem1, elem2), elem3, all=True)
        self.assertTrue(isinstance(expression.expressions[0], Union))
        expression = Union(Union(elem1, elem2, all=True), elem3, all=True)
        self.assertEqual(expression.expressions, (elem1, elem2, elem3))

        # don't collapse if limit or offset are set
        expression = Union(Union(elem1, elem2, limit=1), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Union))
        expression = Union(Union(elem1, elem2, offset=3), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Union))

        # don't collapse other set expressions
        expression = Union(Except(elem1, elem2), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Except))
        expression = Union(Intersect(elem1, elem2), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Intersect))

    def test_except(self):
        expression = Except(elem1, elem2, elem3)
        self.assertEqual(expression.expressions, (elem1, elem2, elem3))

    def test_except_with_kwargs(self):
        expression = Except(
            elem1, elem2, all=True, order_by=(), limit=1, offset=2
        )
        self.assertEqual(expression.expressions, (elem1, elem2))
        self.assertEqual(expression.all, True)
        self.assertEqual(expression.order_by, ())
        self.assertEqual(expression.limit, 1)
        self.assertEqual(expression.offset, 2)

    def test_except_collapse(self):
        expression = Except(Except(elem1, elem2), elem3)
        self.assertEqual(expression.expressions, (elem1, elem2, elem3))

        # only first expression is collapsed
        expression = Except(elem1, Except(elem2, elem3))
        self.assertEqual(expression.expressions[0], elem1)
        self.assertTrue(isinstance(expression.expressions[1], Except))

        # don't collapse if all is different
        expression = Except(Except(elem1, elem2, all=True), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Except))
        expression = Except(Except(elem1, elem2), elem3, all=True)
        self.assertTrue(isinstance(expression.expressions[0], Except))
        expression = Except(Except(elem1, elem2, all=True), elem3, all=True)
        self.assertEqual(expression.expressions, (elem1, elem2, elem3))

        # don't collapse if limit or offset are set
        expression = Except(Except(elem1, elem2, limit=1), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Except))
        expression = Except(Except(elem1, elem2, offset=3), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Except))

        # don't collapse other set expressions
        expression = Except(Union(elem1, elem2), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Union))
        expression = Except(Intersect(elem1, elem2), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Intersect))

    def test_intersect(self):
        expression = Intersect(elem1, elem2, elem3)
        self.assertEqual(expression.expressions, (elem1, elem2, elem3))

    def test_intersect_with_kwargs(self):
        expression = Intersect(
            elem1, elem2, all=True, order_by=(), limit=1, offset=2
        )
        self.assertEqual(expression.expressions, (elem1, elem2))
        self.assertEqual(expression.all, True)
        self.assertEqual(expression.order_by, ())
        self.assertEqual(expression.limit, 1)
        self.assertEqual(expression.offset, 2)

    def test_intersect_collapse(self):
        expression = Intersect(Intersect(elem1, elem2), elem3)
        self.assertEqual(expression.expressions, (elem1, elem2, elem3))

        # only first expression is collapsed
        expression = Intersect(elem1, Intersect(elem2, elem3))
        self.assertEqual(expression.expressions[0], elem1)
        self.assertTrue(isinstance(expression.expressions[1], Intersect))

        # don't collapse if all is different
        expression = Intersect(Intersect(elem1, elem2, all=True), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Intersect))
        expression = Intersect(Intersect(elem1, elem2), elem3, all=True)
        self.assertTrue(isinstance(expression.expressions[0], Intersect))
        expression = Intersect(
            Intersect(elem1, elem2, all=True), elem3, all=True)
        self.assertEqual(expression.expressions, (elem1, elem2, elem3))

        # don't collapse if limit or offset are set
        expression = Intersect(Intersect(elem1, elem2, limit=1), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Intersect))
        expression = Intersect(Intersect(elem1, elem2, offset=3), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Intersect))

        # don't collapse other set expressions
        expression = Intersect(Union(elem1, elem2), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Union))
        expression = Intersect(Except(elem1, elem2), elem3)
        self.assertTrue(isinstance(expression.expressions[0], Except))

    def test_auto_tables(self):
        expression = AutoTables(elem1, (elem2,))
        self.assertEqual(expression.expression, elem1)
        self.assertEqual(expression.tables, (elem2,))

    def test_sequence(self):
        expression = Sequence(elem1)
        self.assertEqual(expression.name, elem1)


class StateTest(unittest.TestCase):

    def setUp(self):
        self.state = State()

    def test_attributes(self):
        self.assertEqual(self.state.parameters, [])
        self.assertEqual(self.state.auto_tables, [])
        self.assertEqual(self.state.context, None)

    def test_push_pop(self):
        self.state.parameters.extend([1, 2])
        self.state.push('parameters', [])
        self.assertEqual(self.state.parameters, [])
        self.state.pop()
        self.assertEqual(self.state.parameters, [1, 2])
        self.state.push('parameters')
        self.assertEqual(self.state.parameters, [1, 2])
        self.state.parameters.append(3)
        self.assertEqual(self.state.parameters, [1, 2, 3])
        self.state.pop()
        self.assertEqual(self.state.parameters, [1, 2])

    def test_push_pop_unexistent(self):
        self.state.push('nonexistent')
        self.assertEqual(self.state.nonexistent, None)
        self.state.nonexistent = 'something'
        self.state.pop()
        self.assertEqual(self.state.nonexistent, None)


class CompileTest(unittest.TestCase):

    def test_simple_inheritance(self):
        custom_compile = txorm_compile.create_child()
        statement = custom_compile(Func1())
        self.assertEqual(statement, 'func1()')

    def test_customize(self):
        custom_compile = txorm_compile.create_child()

        @custom_compile.when(type(None))
        def compile_none(compile, state, expression):
            return 'None'

        statement = custom_compile(Func1(None))
        self.assertEqual(statement, 'func1(None)')

    def test_customize_inheritance(self):
        class C(object):
            pass

        compile_parent = Compile()
        compile_child = compile_parent.create_child()

        @compile_parent.when(C)
        def compile_in_parent(compile, state, expression):
            return 'parent'

        statement = compile_child(C())
        self.assertEqual(statement, 'parent')

        @compile_child.when(C)
        def compile_in_child(compile, state, expression):
            return 'child'

        statement = compile_child(C())
        self.assertEqual(statement, 'child')

    def test_precedence(self):
        expression = And(
            e1, Or(e2, e3), Add(e4, Mul(e5, Sub(e6, Div(e7, Div(e8, e9)))))
        )

        statement = txorm_compile(expression)
        self.assertEqual(statement, '1 AND (2 OR 3) AND 4+5*(6-7/(8/9))')

    def test_get_precedence(self):
        c = txorm_compile
        self.assertTrue(c.get_precedence(Or) < c.get_precedence(And))
        self.assertTrue(c.get_precedence(Add) < c.get_precedence(Mul))
        self.assertTrue(c.get_precedence(Sub) < c.get_precedence(Div))

    def test_customize_precedence(self):
        expression = And(elem1, Or(elem2, elem3))
        custom_compile = txorm_compile.create_child()

        custom_compile.set_precedence(10, And)
        custom_compile.set_precedence(11, Or)
        statement = custom_compile(expression)
        self.assertEqual(statement, 'elem1 AND elem2 OR elem3')

        custom_compile.set_precedence(10, Or)
        statement = custom_compile(expression)
        self.assertEqual(statement, 'elem1 AND elem2 OR elem3')

        custom_compile.set_precedence(9, Or)
        statement = custom_compile(expression)
        self.assertEqual(statement, 'elem1 AND (elem2 OR elem3)')

    def test_customize_precedence_inheritance(self):
        compile_parent = txorm_compile.create_child()
        compile_child = compile_parent.create_child()

        expression = And(elem1, Or(elem2, elem3))

        compile_parent.set_precedence(10, And)
        compile_parent.set_precedence(11, Or)
        self.assertEqual(compile_child.get_precedence(Or), 11)
        self.assertEqual(compile_parent.get_precedence(Or), 11)
        statement = compile_child(expression)
        self.assertEqual(statement, 'elem1 AND elem2 OR elem3')

        compile_parent.set_precedence(10, Or)
        self.assertEqual(compile_child.get_precedence(Or), 10)
        self.assertEqual(compile_parent.get_precedence(Or), 10)
        statement = compile_child(expression)
        self.assertEqual(statement, 'elem1 AND elem2 OR elem3')

        compile_child.set_precedence(9, Or)
        self.assertEqual(compile_child.get_precedence(Or), 9)
        self.assertEqual(compile_parent.get_precedence(Or), 10)
        statement = compile_child(expression)
        self.assertEqual(statement, 'elem1 AND (elem2 OR elem3)')

    def test_compile_sequence(self):
        expression = [elem1, Func1(), (Func2(), None)]
        statement = txorm_compile(expression)
        self.assertEqual(statement, 'elem1, func1(), func2(), NULL')

    def test_compile_invalid(self):
        self.assertRaises(CompileError, txorm_compile, object())
        self.assertRaises(CompileError, txorm_compile, [object()])

    def test_compile_str(self):
        state = State()
        statement = txorm_compile(b('str'), state)
        self.assertEqual(statement, '?')
        assert_variables(self, state.parameters, [RawStrVariable(b('str'))])

    def test_compile_unicode(self):
        state = State()
        statement = txorm_compile(u('unicode'), state)
        self.assertEqual(statement, '?')
        assert_variables(self, state.parameters, [UnicodeVariable('unicode')])

    def test_compile_int_long(self):
        state = State()
        statement = txorm_compile(1, state)
        self.assertEqual(statement, '?')
        assert_variables(self, state.parameters, [IntVariable(1)])

    def test_compile_bool(self):
        state = State()
        statement = txorm_compile(True, state)
        self.assertEqual(statement, '?')
        assert_variables(self, state.parameters, [BoolVariable(True)])

    def test_compile_float(self):
        state = State()
        statement = txorm_compile(1.1, state)
        self.assertEqual(statement, '?')
        assert_variables(self, state.parameters, [FloatVariable(1.1)])

    def test_compile_decimal(self):
        state = State()
        statement = txorm_compile(1.1, state)
        self.assertEqual(statement, '?')
        assert_variables(self, state.parameters, [FloatVariable(1.1)])

    def test_compile_datetime(self):
        dt = datetime(1936, 7, 17, 12, 0)
        state = State()
        statement = txorm_compile(dt, state)
        self.assertEqual(statement, '?')
        assert_variables(self, state.parameters, [DateTimeVariable(dt)])

    def test_compile_date(self):
        d = date(1936, 7, 17)
        state = State()
        statement = txorm_compile(d, state)
        self.assertEqual(statement, '?')
        assert_variables(self, state.parameters, [DateVariable(d)])

    def test_compile_time(self):
        t = time(12, 0)
        state = State()
        statement = txorm_compile(t, state)
        self.assertEqual(statement, '?')
        assert_variables(self, state.parameters, [TimeVariable(t)])

    def test_compile_timedelta(self):
        td = timedelta(days=1, seconds=2, microseconds=3)
        state = State()
        statement = txorm_compile(td, state)
        self.assertEqual(statement, '?')
        assert_variables(self, state.parameters, [TimeDeltaVariable(td)])

    def test_compile_none(self):
        state = State()
        statement = txorm_compile(None, state)
        self.assertEqual(statement, 'NULL')
        self.assertEqual(state.parameters, [])

    def test_compile_select(self):
        expression = Select([field1, field2])
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'SELECT field1, field2')
        self.assertEqual(state.parameters, [])

    def test_compile_select_distinct(self):
        expression = Select([field1, field2], Undef, [table1], distinct=True)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement, 'SELECT DISTINCT field1, field2 FROM "table 1"'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_select_distinct_on(self):
        expression = Select(
            [field1, field2], Undef, [table1], distinct=[field2, field1])
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'SELECT DISTINCT ON (field2, field1) field1, field2 FROM "table 1"'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_select_with_strings(self):
        expression = Select(
            field1, b('1 = 2'), table1, order_by='field1', group_by='field2')
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'SELECT field1 FROM "table 1" WHERE 1 = 2 GROUP BY field2 '
            'ORDER BY field1'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_select_with_unicode(self):
        expression = Select(
            field1, '1 = 2', table1, order_by='field1', group_by='field2')
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'SELECT field1 FROM "table 1" WHERE 1 = 2 GROUP BY field2 '
            'ORDER BY field1'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_select_join(self):
        expression = Select(
            [field1, Func1()], Func1(), [table1, Join(table2), Join(table3)])
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'SELECT field1, func1() FROM "table 1" JOIN "table 2" '
            'JOIN "table 3" WHERE func1()'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_select_where(self):
        expression = Select(
            [field1, Func1()],
            Func1(),
            [table1, Func1()],
            order_by=[field2, Func1()],
            group_by=[field3, Func1()],
            limit=3, offset=4
        )
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'SELECT field1, func1() FROM "table 1", func1() WHERE func1() '
            'GROUP BY field3, func1() ORDER BY field2, func1() '
            'LIMIT 3 OFFSET 4'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_select_having(self):
        expression = Select(
            field1, tables=table1, order_by='field1',
            group_by=['field2'], having='1 = 2'
        )
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'SELECT field1 FROM "table 1" GROUP BY field2 HAVING 1 = 2 '
            'ORDER BY field1'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_select_contexts(self):
        field, where, table, order_by, group_by = track_contexts(5)
        expression = Select(
            field, where, table, order_by=order_by, group_by=group_by)
        txorm_compile(expression)
        self.assertEqual(field.context, FIELD)
        self.assertEqual(where.context, EXPR)
        self.assertEqual(table.context, TABLE)
        self.assertEqual(order_by.context, EXPR)
        self.assertEqual(group_by.context, EXPR)

    def test_compile_select_join_where(self):
        expression = Select(
            field1, Func1() == 'value1', Join(table1, Func2() == 'value2'))
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'SELECT field1 FROM JOIN "table 1" ON func2() = ? '
            'WHERE func1() = ?'
        )
        self.assertEqual(
            [variable.get() for variable in state.parameters],
            ['value2', 'value1']
        )

    def test_compile_select_join_right_left(self):
        expression = Select(
            [field1, Func1()], Func1(), [table1, Join(table2, table3)])
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'SELECT field1, func1() FROM "table 1", "table 2" '
            'JOIN "table 3" WHERE func1()'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_select_auto_table_default(self):
        expression = Select(
            Field(field1), Field(field2) == 1, default_tables=table1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement, 'SELECT field1 FROM "table 1" WHERE field2 = ?'
        )
        assert_variables(self, state.parameters, [Variable(1)])

    def test_compile_select_auto_table(self):
        expression = Select(Field(field1, table1), Field(field2, table2) == 1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'SELECT "table 1".field1 FROM "table 1", "table 2" '
            'WHERE "table 2".field2 = ?'
        )
        assert_variables(self, state.parameters, [Variable(1)])

    def test_compile_select_auto_table_unknown(self):
        statement = txorm_compile(Select(elem1))
        self.assertEqual(statement, 'SELECT elem1')

    def test_compile_select_auto_table_duplicated(self):
        expression = Select(Field(field1, table1), Field(field2, table1) == 1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'SELECT "table 1".field1 FROM "table 1" WHERE "table 1".field2 = ?'
        )
        assert_variables(self, state.parameters, [Variable(1)])

    def test_compile_select_auto_table_default_with_joins(self):
        expression = Select(
            Field(field1), default_tables=[table1, Join(table2)])
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement, 'SELECT field1 FROM "table 1" JOIN "table 2"'
        )
        assert_variables(self, state.parameters, [])

    def test_compile_select_auto_table_sub(self):
        f1 = Field(field1, table1)
        f2 = Field(field2, table2)
        expression = Select(f1, In(elem1, Select(f2, f1 == f2, f2.table)))
        statement = txorm_compile(expression)
        self.assertEqual(
            statement,
            'SELECT "table 1".field1 FROM "table 1" WHERE elem1 IN ('
            'SELECT "table 2".field2 FROM "table 2" '
            'WHERE "table 1".field1 = "table 2".field2)'
        )

    def test_compile_insert(self):
        expression = Insert({field1: elem1, Func1(): Func2()}, Func2())
        state = State()
        statement = txorm_compile(expression, state)
        self.assertTrue(
            statement in (
                'INSERT INTO func2() (field1, func1()) '
                'VALUES (elem1, func2())',
                'INSERT INTO func2() (func1(), field1) '
                'VALUES (func2(), elem1)'
            ), statement
        )
        self.assertEqual(state.parameters, [])

    def test_compile_insert_with_fields(self):
        expression = Insert({
            Field(field1, table1): elem1, Field(field2, table1): elem2
        }, table2)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertTrue(
            statement in (
                'INSERT INTO "table 2" (field1, field2) VALUES (elem1, elem2)',
                'INSERT INTO "table 2" (field2, field1) VALUES (elem2, elem1)'
            ), statement
        )
        self.assertEqual(state.parameters, [])

    def test_compile_insert_with_fields_to_escape(self):
        expression = Insert({Field('field 1', table1): elem1}, table2)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement, 'INSERT INTO "table 2" ("field 1") VALUES (elem1)')
        self.assertEqual(state.parameters, [])

    def test_compile_insert_with_fields_as_raw_strings(self):
        expression = Insert({r'field 1': elem1}, table2)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement, 'INSERT INTO "table 2" ("field 1") VALUES (elem1)'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_insert_with_fields_as_literal_strings(self):
        expression = Insert({'field 1': elem1}, table2)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement, 'INSERT INTO "table 2" ("field 1") VALUES (elem1)'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_insert_auto_table(self):
        expression = Insert({Field(field1, table1): elem1})
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement, 'INSERT INTO "table 1" (field1) VALUES (elem1)'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_insert_auto_table_default(self):
        expression = Insert({Field(field1): elem1}, default_table=table1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement, 'INSERT INTO "table 1" (field1) VALUES (elem1)'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_insert_auto_table_unknown(self):
        expression = Insert({Field(field1): elem1})
        self.assertRaises(NoTableError, txorm_compile, expression)

    def tets_compile_insert_contexts(self):
        field, value, table = track_contexts(3)
        expression = Insert({field: value}, table)
        txorm_compile(expression)
        self.assertEqual(field.context, FIELD_NAME)
        self.assertEqual(value.context, EXPR)
        self.assertEqual(table.context, TABLE)

    def test_compile_insert_bulk(self):
        expression = Insert(
            (Field(field1, table1), Field(field2, table1)),
            values=[(elem1, elem2), (elem3, elem4)]
        )
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'INSERT INTO "table 1" (field1, field2) '
            'VALUES (elem1, elem2), (elem3, elem4)'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_insert_select(self):
        expression = Insert(
            (Field(field1, table1), Field(field2, table1)),
            values=Select((Field(field3, table3), Field(field4, table4)))
        )
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'INSERT INTO "table 1" (field1, field2) SELECT "table 3".field3, '
            '"table 4".field4 FROM "table 3", "table 4"'
        )
        self.assertEqual(state.parameters, [])

    def test_compile_update(self):
        expression = Update({field1: elem1, Func1(): Func2()}, table=Func1())
        state = State()
        statement = txorm_compile(expression, state)
        self.assertTrue(statement in (
            'UPDATE func1() SET field1=elem1, func1()=func2()',
            'UPDATE func1() SET func1()=func2(), field1=elem1'
        ), statement)

    def test_compile_update_with_fields(self):
        expression = Update({Field(field1, table1): elem1}, table=table1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'UPDATE "table 1" SET field1=elem1')
        self.assertEqual(state.parameters, [])

    def test_compile_update_with_fields_to_escape(self):
        expression = Update({Field('field x', table1): elem1}, table=table1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'UPDATE "table 1" SET "field x"=elem1')
        self.assertEqual(state.parameters, [])

    def test_compile_update_with_fields_as_raw_string(self):
        expression = Update({r'field 1': elem1}, table=table2)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'UPDATE "table 2" SET "field 1"=elem1')
        self.assertEqual(state.parameters, [])

    def test_compile_update_with_fields_as_literal_string(self):
        expression = Update({'field 1': elem1}, table=table2)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'UPDATE "table 2" SET "field 1"=elem1')
        self.assertEqual(state.parameters, [])

    def test_compile_update_where(self):
        expression = Update({field1: elem1}, Func1(), Func2())
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement, 'UPDATE func2() SET field1=elem1 WHERE func1()')
        self.assertEqual(state.parameters, [])

    def test_compile_update_auto_table(self):
        expression = Update({Field(field1, table1): elem1})
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'UPDATE "table 1" SET field1=elem1')
        self.assertEqual(state.parameters, [])

    def test_compile_update_auto_table_default(self):
        expression = Update({Field(field1): elem1}, default_table=table1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'UPDATE "table 1" SET field1=elem1')
        self.assertEqual(state.parameters, [])

    def test_compile_update_auto_table_unknown(self):
        expression = Update({Field(field1): elem1})
        self.assertRaises(CompileError, txorm_compile, expression)

    def test_update_with_strings(self):
        expression = Update({field1: elem1}, "1 = 2", table1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement, 'UPDATE "table 1" SET field1=elem1 WHERE 1 = 2')
        self.assertEqual(state.parameters, [])

    def test_update_contexts(self):
        set_left, set_right, where, table = track_contexts(4)
        expression = Update({set_left: set_right}, where, table)
        txorm_compile(expression)
        self.assertEqual(set_left.context, FIELD_NAME)
        self.assertEqual(set_right.context, FIELD_NAME)
        self.assertEqual(where.context, EXPR)
        self.assertEqual(table.context, TABLE)

    def test_compile_delete(self):
        expression = Delete(table=table1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'DELETE FROM "table 1"')
        self.assertEqual(state.parameters, [])

    def test_compile_delete_where(self):
        expression = Delete(Func1(), Func2())
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'DELETE FROM func2() WHERE func1()')
        self.assertEqual(state.parameters, [])

    def test_compile_delete_with_strings(self):
        expression = Delete('1 = 2', table1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'DELETE FROM "table 1" WHERE 1 = 2')
        self.assertEqual(state.parameters, [])

    def test_compile_delete_auto_table(self):
        expression = Delete(Field(field1, table1) == 1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement, 'DELETE FROM "table 1" WHERE "table 1".field1 = ?')
        assert_variables(self, state.parameters, [Variable(1)])

    def test_compile_delete_auto_table_default(self):
        expression = Delete(Field(field1) == 1, default_table=table1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'DELETE FROM "table 1" WHERE field1 = ?')
        assert_variables(self, state.parameters, [Variable(1)])

    def test_compile_delete_auto_table_unknown(self):
        expression = Delete(Field(field1) == 1)
        self.assertRaises(NoTableError, txorm_compile, expression)

    def test_compile_delete_contexts(self):
        where, table = track_contexts(2)
        expression = Delete(where, table)
        txorm_compile(expression)
        self.assertEqual(where.context, EXPR)
        self.assertEqual(table.context, TABLE)

    def test_compile_field(self):
        expression = Field(field1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'field1')
        self.assertEqual(state.parameters, [])
        self.assertEqual(expression.compile_cache, 'field1')

    def test_compile_field_table(self):
        field = Field(field1, Func1())
        expression = Select(field)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'SELECT func1().field1 FROM func1()')
        self.assertEqual(state.parameters, [])
        self.assertEqual(field.compile_cache, 'field1')

    def test_compile_field_contexts(self):
        table, = track_contexts(1)
        expr = Field(field1, table)
        txorm_compile(expr)
        self.assertEqual(table.context, FIELD_PREFIX)

    def test_compile_field_with_reserved_word(self):
        expression = Select(Field('name 1', 'table 1'))
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'SELECT "table 1"."name 1" FROM "table 1"')

    def test_compile_row(self):
        expression = Row(field1, field2)
        statement = txorm_compile(expression)
        self.assertEqual(statement, 'ROW(field1, field2)')

    def test_compile_variable(self):
        expression = Variable('Value')
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, '?')
        assert_variables(self, state.parameters, [Variable('Value')])

    def test_compile_eq(self):
        expression = Eq(Func1(), Func2())
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() = func2()')
        self.assertEqual(state.parameters, [])

        expression = Func1() == 'value'
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() = ?')
        assert_variables(self, state.parameters, [Variable('value')])

    def test_compile_is_in(self):
        expression = Func1().is_in(['Hello', 'World'])
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() IN (?, ?)')
        assert_variables(
            self, state.parameters, [Variable('Hello'), Variable('World')])

    def test_compile_is_in_empty(self):
        expression = Func1().is_in([])
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, '?')
        assert_variables(self, state.parameters, [BoolVariable(False)])

    def test_compile_is_in_expr(self):
        expression = Func1().is_in(Select(field1))
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() IN (SELECT field1)')
        self.assertEqual(state.parameters, [])

    def test_compile_eq_none(self):
        expression = Func1() == None  # noqa
        self.assertTrue(expression.expressions[1] is None)

        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() IS NULL')
        self.assertEqual(state.parameters, [])

    def test_compile_ne(self):
        expression = Ne(Func1(), Func2())
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() != func2()')
        self.assertEqual(state.parameters, [])

        expression = Func1() != 'value'
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() != ?')
        assert_variables(self, state.parameters, [Variable('value')])

    def test_compile_ne_none(self):
        expression = Func1() != None  # noqa

        self.assertTrue(expression.expressions[1] is None)

        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() IS NOT NULL')
        self.assertEqual(state.parameters, [])

    def test_compile_gt(self):
        expression = Gt(Func1(), Func2())
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() > func2()')
        self.assertEqual(state.parameters, [])

        expression = Func1() > 'value'
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() > ?')
        assert_variables(self, state.parameters, [Variable('value')])

    def test_compile_ge(self):
        expression = Ge(Func1(), Func2())
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() >= func2()')
        self.assertEqual(state.parameters, [])

        expression = Func1() >= 'value'
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() >= ?')
        assert_variables(self, state.parameters, [Variable('value')])

    def test_compile_lt(self):
        expression = Lt(Func1(), Func2())
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() < func2()')
        self.assertEqual(state.parameters, [])

        expression = Func1() < 'value'
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() < ?')
        assert_variables(self, state.parameters, [Variable('value')])

    def test_compile_le(self):
        expression = Le(Func1(), Func2())
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() <= func2()')
        self.assertEqual(state.parameters, [])

        expression = Func1() <= 'value'
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1() <= ?')
        assert_variables(self, state.parameters, [Variable('value')])

    def test_compile_lshift(self):
        expression = LShift(Func1(), Func2())
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1()<<func2()')
        self.assertEqual(state.parameters, [])

        expression = Func1() << 'value'
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1()<<?')
        assert_variables(self, state.parameters, [Variable('value')])

    def test_compile_rshift(self):
        expression = RShift(Func1(), Func2())
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1()>>func2()')
        self.assertEqual(state.parameters, [])

        expression = Func1() >> 'value'
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(statement, 'func1()>>?')
        assert_variables(self, state.parameters, [Variable('value')])

    def test_compile_in(self):
        expression = In(Func1(), b('value'))
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEquals(statement, 'func1() IN (?)')
        assert_variables(self, state.parameters, [RawStrVariable(b('value'))])

        expression = In(Func1(), elem1)
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEquals(statement, 'func1() IN (elem1)')
        self.assertEquals(state.parameters, [])

    def test_compile_and(self):
        expression = And(elem1, elem2, And(elem3, elem4))
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEquals(statement, 'elem1 AND elem2 AND elem3 AND elem4')
        self.assertEquals(state.parameters, [])

        expression = Func1() & 'value'
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEquals(statement, 'func1() AND ?')
        assert_variables(self, state.parameters, [Variable('value')])

    def test_compile_or(self):
        expression = Or(elem1, elem2, Or(elem3, elem4))
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEquals(statement, 'elem1 OR elem2 OR elem3 OR elem4')
        self.assertEquals(state.parameters, [])

        expression = Func1() | 'value'
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEquals(statement, 'func1() OR ?')
        assert_variables(self, state.parameters, [Variable('value')])

    def test_compile_and_with_strings(self):
        expression = And('elem1', 'elem2')
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEquals(statement, 'elem1 AND elem2')
        self.assertEquals(state.parameters, [])

    def test_compile_or_with_strings(self):
        expression = Or('elem1', 'elem2')
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEquals(statement, 'elem1 OR elem2')
        self.assertEquals(state.parameters, [])


def assert_variables(test, checked, expected):
    test.assertEqual(len(checked), len(expected))
    for check, expect in zip(checked, expected):
        test.assertEqual(check.__class__, expect.__class__)
        test.assertEqual(check.get(), expect.get())


class TrackContext(FromExpression):
    context = None


@txorm_compile.when(TrackContext)
def compile_track_context(compile, expression, state):
    expression.context = state.context
    return ''


def track_contexts(n):
    return [TrackContext() for i in range(n)]

# I don't like dynamic variables because the linter give me fake possitives
elem1 = SQLToken('elem1')
elem2 = SQLToken('elem2')
elem3 = SQLToken('elem3')
elem4 = SQLToken('elem4')
elem5 = SQLToken('elem5')
elem6 = SQLToken('elem6')
elem7 = SQLToken('elem7')
elem8 = SQLToken('elem8')
elem9 = SQLToken('elem9')

field1 = SQLToken('field1')
field2 = SQLToken('field2')
field3 = SQLToken('field3')
field4 = SQLToken('field4')
field5 = SQLToken('field5')
field6 = SQLToken('field6')
field7 = SQLToken('field7')
field8 = SQLToken('field8')
field9 = SQLToken('field9')

table1 = 'table 1'
table2 = 'table 2'
table3 = 'table 3'
table4 = 'table 4'
table5 = 'table 5'
table6 = 'table 6'
table7 = 'table 7'
table8 = 'table 8'
table9 = 'table 9'

e1 = SQLRaw('1')
e2 = SQLRaw('2')
e3 = SQLRaw('3')
e4 = SQLRaw('4')
e5 = SQLRaw('5')
e6 = SQLRaw('6')
e7 = SQLRaw('7')
e8 = SQLRaw('8')
e9 = SQLRaw('9')


class Func1(NamedFunc):
    name = 'func1'

    def __hash__(self):
        return hash(self.name)


class Func2(NamedFunc):
    name = 'func2'
