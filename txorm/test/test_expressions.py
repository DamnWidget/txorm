
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Expressions Unit Tests
"""

from __future__ import unicode_literals

from twisted.trial import unittest

from txorm import Undef
from txorm.variable import Variable
from txorm.compiler.state import State
from txorm.compiler.fields import Field, Alias
from txorm.compiler.base import txorm_compile, Compile
from txorm.compiler.tables import JoinExpression, Table
from txorm.compiler.comparable import Add, Sub, Mul, Div
from txorm.compiler.plain_sql import SQLRaw, SQLToken, SQL
from txorm.compat import _PY3, b, u, binary_type, text_type
from txorm.compiler.expressions import Select, Insert, Update, Delete
from txorm.compiler.comparable import And, Or, Func, NamedFunc, Like, Eq
from txorm.compiler.expressions import Union, Except, Intersect, Sequence
from txorm.compiler.expressions import ExpressionError, Expression, AutoTables


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
        self.assertEquals(expression.where, Undef)
        self.assertEquals(expression.table, Undef)

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
            print(state)
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


# I don't like dynamic variables because the linter give me fake possitives
elem1 = SQLToken('elem1' if not _PY3 else b('elem1'))
elem2 = SQLToken('elem2' if not _PY3 else b('elem2'))
elem3 = SQLToken('elem3' if not _PY3 else b('elem3'))
elem4 = SQLToken('elem4' if not _PY3 else b('elem4'))
elem5 = SQLToken('elem5' if not _PY3 else b('elem5'))
elem6 = SQLToken('elem6' if not _PY3 else b('elem6'))
elem7 = SQLToken('elem7' if not _PY3 else b('elem7'))
elem8 = SQLToken('elem8' if not _PY3 else b('elem8'))
elem9 = SQLToken('elem9' if not _PY3 else b('elem9'))

e1 = SQLRaw('1' if not _PY3 else b('1'))
e2 = SQLRaw('2' if not _PY3 else b('2'))
e3 = SQLRaw('3' if not _PY3 else b('3'))
e4 = SQLRaw('4' if not _PY3 else b('4'))
e5 = SQLRaw('5' if not _PY3 else b('5'))
e6 = SQLRaw('6' if not _PY3 else b('6'))
e7 = SQLRaw('7' if not _PY3 else b('7'))
e8 = SQLRaw('8' if not _PY3 else b('8'))
e9 = SQLRaw('9' if not _PY3 else b('9'))


class Func1(NamedFunc):
    name = 'func1'


class Func2(NamedFunc):
    name = 'func2'
