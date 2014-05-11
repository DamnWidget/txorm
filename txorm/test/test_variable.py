
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

'''TxORM Variable Unit Tests
'''

from decimal import Decimal

from twisted.trial import unittest

from txorm import Undef
from txorm.variable import *
from txorm.compat import _PY3, b, u

if _PY3 is True:
    buffer = memoryview


class Stub(object):
    pass

marker = Stub()


class DummyVariable(Variable):

    def __init__(self, *args, **kwargs):
        self.gets = []
        self.sets = []
        super(DummyVariable, self).__init__(*args, **kwargs)

    def parse_get(self, variable, to_db):
        self.gets.append((variable, to_db))
        return 'g', variable

    def parse_set(self, variable, from_db):
        self.sets.append((variable, from_db))
        return 's', variable


class VaraibleTest(unittest.TestCase):

    def test_constructor_value(self):
        variable = DummyVariable(marker)
        self.assertEqual(variable.sets, [(marker, False)])

    def test_constructor_value_from_db(self):
        variable = DummyVariable(marker, from_db=True)
        self.assertEqual(variable.sets, [(marker, True)])

    def test_constructor_value_factory(self):
        variable = DummyVariable(value_factory=lambda: marker)
        self.assertEqual(variable.sets, [(marker, False)])

    def test_constructor_value_factory_from_db(self):
        variable = DummyVariable(value_factory=lambda: marker, from_db=True)
        self.assertEqual(variable.sets, [(marker, True)])

    def test_constructor_column(self):
        variable = DummyVariable(column=marker)
        self.assertEqual(variable.column, marker)

    def test_get_default(self):
        variable = DummyVariable()
        self.assertEqual(variable.get(default=marker), marker)

    def test_get_returns_none_if_value_is_none(self):
        variable = DummyVariable(value=None)
        self.assertIsNone(variable.get(default=marker))

    def test_set(self):
        variable = DummyVariable()
        variable.set(marker)
        self.assertEqual(variable.sets, [(marker, False)])
        variable.set(marker, from_db=True)
        self.assertEqual(variable.sets, [(marker, False), (marker, True)])

    def test_get(self):
        variable = DummyVariable()
        variable.set(marker)
        self.assertEqual(variable.get(), ('g', ('s', marker)))
        self.assertEqual(variable.gets, [(('s', marker), False)])

    def test_get_to_db(self):
        variable = DummyVariable()
        variable.set(marker)
        self.assertEqual(variable.get(to_db=True), ('g', ('s', marker)))
        self.assertEqual(variable.gets, [(('s', marker), True)])

    def test_is_defined(self):
        variable = DummyVariable()
        self.assertFalse(variable.is_defined)
        variable.set(marker)
        self.assertTrue(variable.is_defined)

    def test_set_get_none(self):
        variable = DummyVariable()
        variable.set(None)
        self.assertEqual(variable.get(marker), None)
        self.assertEqual(variable.sets, [])
        self.assertEqual(variable.gets, [])

    def test_set_none_without_allow_none(self):
        variable = DummyVariable(allow_none=False)
        self.assertRaises(TypeError, variable.set, None)

    def test_set_none_without_allow_none_and_column(self):
        column = Column('colun_name')
        variable = DummyVariable(allow_none=False, column=column)
        try:
            variable.set(None)
        except TypeError as error:
            pass
        self.assertTrue('column_name' in str(error))

    def test_set_with_validator(self):
        args = []

        def validator(value):
            args.append((value,))
            return value

        variable = DummyVariable(validator=validator)
        variable.set(3)
        self.assertEqual(args, [(3,)])

    def test_set_with_validator_raising_error(self):
        args = []

        def validator(value):
            args.append((value,))
            raise ZeroDivisionError()

        variable = DummyVariable(validator=validator)
        self.assertRaises(ZeroDivisionError, variable.set, marker)
        self.assertEqual(args, [(marker,)])
        self.assertEqual(variable.get(), None)

    def test_set_with_validator_changing_value(self):
        args = []

        def validator(value):
            args.append((value,))
            return 42

        variable = DummyVariable(validator=validator)
        variable.set(marker)
        self.assertEqual(args, [(marker,)])
        self.assertEqual(variable.get(), ('g', ('s', 42)))

    def test_set_from_db_wont_call_validator(self):
        args = []

        def validator(value):
            args.append((value,))
            return 42

        variable = DummyVariable(validator=validator)
        variable.set(marker, from_db=True)
        self.assertEqual(args, [])
        self.assertEqual(variable.get(), ('g', ('s', marker)))


class BoolVariableTest(unittest.TestCase):

    def test_set_get(self):
        variable = BoolVariable()
        variable.set(1)
        self.assertTrue(variable.get() is True)
        variable.set(0)
        self.assertTrue(variable.get() is False)
        variable.set(1.1)
        self.assertTrue(variable.get() is True)
        variable.set(0.0)
        self.assertTrue(variable.get() is False)
        variable.set(Decimal(1))
        self.assertTrue(variable.get() is True)
        variable.set(Decimal(0))
        self.assertTrue(variable.get() is False)
        self.assertRaises(TypeError, variable.set, 'string')


class IntVariableTest(unittest.TestCase):

    def test_set_get(self):
        variable = IntVariable()
        variable.set(1)
        self.assertEquals(variable.get(), 1)
        variable.set(1.1)
        self.assertEquals(variable.get(), 1)
        variable.set(Decimal(2))
        self.assertEquals(variable.get(), 2)
        self.assertRaises(TypeError, variable.set, '1')


class FloatVariableTest(unittest.TestCase):

    def test_set_get(self):
        variable = FloatVariable()
        variable.set(1.1)
        self.assertEquals(variable.get(), 1.1)
        variable.set(1)
        self.assertEquals(variable.get(), 1)
        self.assertEquals(type(variable.get()), float)
        variable.set(Decimal('1.1'))
        self.assertEquals(variable.get(), 1.1)
        self.assertRaises(TypeError, variable.set, '1')


class DecimalVariableTest(unittest.TestCase):

    def test_set_get(self):
        variable = DecimalVariable()
        variable.set(Decimal('1.1'))
        self.assertEquals(variable.get(), Decimal('1.1'))
        variable.set(1)
        self.assertEquals(variable.get(), 1)
        self.assertEquals(type(variable.get()), Decimal)
        variable.set(Decimal('1.1'))
        self.assertEquals(variable.get(), Decimal('1.1'))
        self.assertRaises(TypeError, variable.set, '1')
        self.assertRaises(TypeError, variable.set, 1.1)

    def test_get_set_from_database(self):
        '''Strings used to/from the database.'''
        variable = DecimalVariable()
        variable.set('1.1', from_db=True)
        self.assertEquals(variable.get(), Decimal('1.1'))
        self.assertEquals(variable.get(to_db=True), '1.1')


class RawStrVariableTest(unittest.TestCase):

    def test_set_get(self):
        variable = RawStrVariable()
        variable.set(b('str'))
        self.assertEquals(variable.get(), 'str')
        variable.set(buffer('buffer'))
        self.assertEqual(variable.get(), b('buffer'))
        self.assertRaises(TypeError, variable.set, u('unicode'))


class UnicodeVariableTest(unittest.TestCase):

    def test_set_get(self):
        variable = UnicodeVariable()
        variable.set(u('unicode'))
        self.assertEquals(variable.get(), u('unicode'))
        self.assertRaises(TypeError, variable.set, b('str'))
