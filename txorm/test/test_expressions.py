
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Expressions Unit Tests
"""

from __future__ import unicode_literals

from twisted.trial import unittest

from txorm import Undef
from txorm.compat import _PY3, b, u
from txorm.variable import Variable
from txorm.compiler.fields import Field
from txorm.compiler.tables import JoinExpression
from txorm.compiler.plain_sql import SQLToken, SQL
from txorm.compiler.expressions import ExpressionError, Expression
from txorm.compiler.expressions import Select, Insert, Update, Delete
from txorm.compiler.comparable import And, Or, Func, NamedFunc, Like, Eq


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


class Func1(NamedFunc):
    name = 'func1'


class Func2(NamedFunc):
    name = 'func2'