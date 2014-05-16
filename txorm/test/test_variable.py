
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

'''TxORM Variable Unit Tests
'''

from decimal import Decimal
from fractions import Fraction
from datetime import datetime, date, time, timedelta

from twisted.trial import unittest

from txorm import Undef
from txorm.variable import *
from txorm.compat import _PY3, b, u
from txorm.utils.tz import tzutc, tzoffset

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

    # def test_set_none_without_allow_none_and_column(self):
    #     column = Column('colun_name')
    #     variable = DummyVariable(allow_none=False, column=column)
    #     try:
    #         variable.set(None)
    #     except TypeError as error:
    #         pass
    #     self.assertTrue('column_name' in str(error))

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
        self.assertEqual(variable.get(), 1)
        variable.set(1.1)
        self.assertEqual(variable.get(), 1)
        variable.set(Decimal(2))
        self.assertEqual(variable.get(), 2)
        self.assertRaises(TypeError, variable.set, '1')


class FloatVariableTest(unittest.TestCase):

    def test_set_get(self):
        variable = FloatVariable()
        variable.set(1.1)
        self.assertEqual(variable.get(), 1.1)
        variable.set(1)
        self.assertEqual(variable.get(), 1)
        self.assertEqual(type(variable.get()), float)
        variable.set(Decimal('1.1'))
        self.assertEqual(variable.get(), 1.1)
        self.assertRaises(TypeError, variable.set, '1')


class DecimalVariableTest(unittest.TestCase):

    def test_set_get(self):
        variable = DecimalVariable()
        variable.set(Decimal('1.1'))
        self.assertEqual(variable.get(), Decimal('1.1'))
        variable.set(1)
        self.assertEqual(variable.get(), 1)
        self.assertEqual(type(variable.get()), Decimal)
        variable.set(Decimal('1.1'))
        self.assertEqual(variable.get(), Decimal('1.1'))
        self.assertRaises(TypeError, variable.set, '1')
        self.assertRaises(TypeError, variable.set, 1.1)

    def test_get_set_from_database(self):
        '''Strings used to/from the database.'''
        variable = DecimalVariable()
        variable.set('1.1', from_db=True)
        self.assertEqual(variable.get(), Decimal('1.1'))
        self.assertEqual(variable.get(to_db=True), '1.1')


class FrationVariableTest(unittest.TestCase):

    def test_set_get(self):
        variable = FractionVariable()
        variable.set(Fraction('0.30'))
        self.assertEqual(variable.get(), Fraction('3/10'))
        self.assertRaises(TypeError, variable.set, marker)

    def test_set_get_from_database(self):
        variable = FractionVariable()
        variable.set(u('1.20'), from_db=True)
        self.assertEqual(variable.get(), Fraction('1/5'))
        variable.set(b('1.20'), from_db=True)
        self.assertEqual(variable.get(), Fraction('1/5'))
        variable.set(1.20, from_db=True)
        self.assertEqual(variable.get(), Fraction('1/5'))
        variable.set(Decimal('1.20'), from_db=True)
        self.assertEqual(variable.get(), Fraction('1/5'))
        self.assertRaises(TypeError, variable.set, marker, from_db=True)


class RawStrVariableTest(unittest.TestCase):

    def test_set_get(self):
        variable = RawStrVariable()
        variable.set(b('str'))
        self.assertEqual(variable.get(), b('str') if _PY3 else 'str')
        variable.set(buffer(b('buffer')))
        self.assertEqual(variable.get(), b('buffer'))
        self.assertRaises(TypeError, variable.set, u('unicode'))


class UnicodeVariableTest(unittest.TestCase):

    def test_set_get(self):
        variable = UnicodeVariable()
        variable.set(u('unicode'))
        self.assertEqual(variable.get(), u('unicode'))
        self.assertRaises(TypeError, variable.set, b('str'))


class DateTimeVariableTest(unittest.TestCase):

    def test_get_set(self):
        epoch = datetime.utcfromtimestamp(0)
        variable = DateTimeVariable()
        variable.set(0)
        self.assertEqual(variable.get(), epoch)
        variable.set(0.0)
        self.assertEqual(variable.get(), epoch)
        variable.set(0)
        self.assertEqual(variable.get(), epoch)
        variable.set(epoch)
        self.assertEqual(variable.get(), epoch)
        self.assertRaises(TypeError, variable.set, marker)

    def test_get_set_from_database(self):
        datetime_str = '1977-05-04 12:34:56.78'
        datetime_uni = u(datetime_str)
        datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000)

        variable = DateTimeVariable()

        variable.set(datetime_str, from_db=True)
        self.assertEqual(variable.get(), datetime_obj)
        variable.set(datetime_uni, from_db=True)
        self.assertEqual(variable.get(), datetime_obj)
        variable.set(datetime_obj, from_db=True)
        self.assertEqual(variable.get(), datetime_obj)

        datetime_str = '1977-05-04 12:34:56'
        datetime_uni = u(datetime_str)
        datetime_obj = datetime(1977, 5, 4, 12, 34, 56)

        variable.set(datetime_str, from_db=True)
        self.assertEqual(variable.get(), datetime_obj)
        variable.set(datetime_uni, from_db=True)
        self.assertEqual(variable.get(), datetime_obj)
        variable.set(datetime_obj, from_db=True)
        self.assertEqual(variable.get(), datetime_obj)

        self.assertRaises(TypeError, variable.set, 0, from_db=True)
        self.assertRaises(TypeError, variable.set, marker, from_db=True)
        self.assertRaises(ValueError, variable.set, 'foobar', from_db=True)
        self.assertRaises(ValueError, variable.set, 'foo bar', from_db=True)

    def test_get_set_with_tzinfo(self):
        datetime_str = '1977-05-04 12:34:56.78'
        datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000, tzinfo=tzutc())

        variable = DateTimeVariable(tzinfo=tzutc())

        # Naive timezone, from_db=True.
        variable.set(datetime_str, from_db=True)
        self.assertEqual(variable.get().replace(tzinfo=tzutc()), datetime_obj)
        variable.set(datetime_obj, from_db=True)
        self.assertEqual(variable.get(), datetime_obj)

        # Naive timezone, from_db=False (doesn't work).
        datetime_obj = datetime(1977, 5, 4, 12, 34, 56, 780000)
        self.assertRaises(ValueError, variable.set, datetime_obj)

        # Different timezone, from_db=False.
        datetime_obj = datetime(
            1977, 5, 4, 12, 34, 56, 780000, tzinfo=tzoffset('1h', 3600)
        )
        variable.set(datetime_obj, from_db=False)
        converted_obj = variable.get()
        self.assertEqual(converted_obj, datetime_obj)
        self.assertEqual(type(converted_obj.tzinfo), tzutc)

        # Different timezone, from_db=True.
        datetime_obj = datetime(
            1977, 5, 4, 12, 34, 56, 780000, tzinfo=tzoffset('1h', 3600)
        )
        variable.set(datetime_obj, from_db=True)
        converted_obj = variable.get()
        self.assertEqual(converted_obj, datetime_obj)
        self.assertEqual(type(converted_obj.tzinfo), tzutc)


class DateVariableTest(unittest.TestCase):

    def test_get_set(self):
        epoch = datetime.utcfromtimestamp(0)
        epoch_date = epoch.date()

        variable = DateVariable()

        variable.set(epoch)
        self.assertEqual(variable.get(), epoch_date)
        variable.set(epoch_date)
        self.assertEqual(variable.get(), epoch_date)

        self.assertRaises(TypeError, variable.set, marker)

    def test_get_set_from_database(self):
        date_str = '1977-05-04'
        date_uni = u(date_str)
        date_obj = date(1977, 5, 4)
        datetime_obj = datetime(1977, 5, 4, 0, 0, 0)

        variable = DateVariable()

        variable.set(date_str, from_db=True)
        self.assertEqual(variable.get(), date_obj)
        variable.set(date_uni, from_db=True)
        self.assertEqual(variable.get(), date_obj)
        variable.set(date_obj, from_db=True)
        self.assertEqual(variable.get(), date_obj)
        variable.set(datetime_obj, from_db=True)
        self.assertEqual(variable.get(), date_obj)

        self.assertRaises(TypeError, variable.set, 0, from_db=True)
        self.assertRaises(TypeError, variable.set, marker, from_db=True)
        self.assertRaises(ValueError, variable.set, 'foobar', from_db=True)

    def test_set_with_datetime(self):
        datetime_str = '1977-05-04 12:34:56.78'
        date_obj = date(1977, 5, 4)
        variable = DateVariable()
        variable.set(datetime_str, from_db=True)
        self.assertEqual(variable.get(), date_obj)


class TimeVariableTest(unittest.TestCase):

    def test_get_set(self):
        epoch = datetime.utcfromtimestamp(0)
        epoch_time = epoch.time()

        variable = TimeVariable()

        variable.set(epoch)
        self.assertEqual(variable.get(), epoch_time)
        variable.set(epoch_time)
        self.assertEqual(variable.get(), epoch_time)

        self.assertRaises(TypeError, variable.set, marker)

    def test_get_set_from_database(self):
        time_str = '12:34:56.78'
        time_uni = u(time_str)
        time_obj = time(12, 34, 56, 780000)

        variable = TimeVariable()

        variable.set(time_str, from_db=True)
        self.assertEqual(variable.get(), time_obj)
        variable.set(time_uni, from_db=True)
        self.assertEqual(variable.get(), time_obj)
        variable.set(time_obj, from_db=True)
        self.assertEqual(variable.get(), time_obj)

        time_str = '12:34:56'
        time_uni = u(time_str)
        time_obj = time(12, 34, 56)

        variable.set(time_str, from_db=True)
        self.assertEqual(variable.get(), time_obj)
        variable.set(time_uni, from_db=True)
        self.assertEqual(variable.get(), time_obj)
        variable.set(time_obj, from_db=True)
        self.assertEqual(variable.get(), time_obj)

        self.assertRaises(TypeError, variable.set, 0, from_db=True)
        self.assertRaises(TypeError, variable.set, marker, from_db=True)
        self.assertRaises(ValueError, variable.set, 'foobar', from_db=True)

    def test_set_with_datetime(self):
        datetime_str = '1977-05-04 12:34:56.78'
        time_obj = time(12, 34, 56, 780000)
        variable = TimeVariable()
        variable.set(datetime_str, from_db=True)
        self.assertEqual(variable.get(), time_obj)

    def test_microsecond_error(self):
        time_str = '15:14:18.598678'
        time_obj = time(15, 14, 18, 598678)
        variable = TimeVariable()
        variable.set(time_str, from_db=True)
        self.assertEqual(variable.get(), time_obj)

    def test_microsecond_error_less_digits(self):
        time_str = '15:14:18.5986'
        time_obj = time(15, 14, 18, 598600)
        variable = TimeVariable()
        variable.set(time_str, from_db=True)
        self.assertEqual(variable.get(), time_obj)

    def test_microsecond_error_more_digits(self):
        time_str = '15:14:18.5986789'
        time_obj = time(15, 14, 18, 598678)
        variable = TimeVariable()
        variable.set(time_str, from_db=True)
        self.assertEqual(variable.get(), time_obj)


class TimeDeltaVariableTest(unittest.TestCase):

    def test_get_set(self):
        delta = timedelta(days=42)

        variable = TimeDeltaVariable()

        variable.set(delta)
        self.assertEquals(variable.get(), delta)

        self.assertRaises(TypeError, variable.set, marker)

    def test_get_set_from_database(self):
        delta_str = '42 days 12:34:56.78'
        delta_uni = u(delta_str)
        delta_obj = timedelta(days=42, hours=12, minutes=34,
                              seconds=56, microseconds=780000)

        variable = TimeDeltaVariable()

        variable.set(delta_str, from_db=True)
        self.assertEquals(variable.get(), delta_obj)
        variable.set(delta_uni, from_db=True)
        self.assertEquals(variable.get(), delta_obj)
        variable.set(delta_obj, from_db=True)
        self.assertEquals(variable.get(), delta_obj)

        delta_str = '1 day, 12:34:56'
        delta_uni = u(delta_str)
        delta_obj = timedelta(days=1, hours=12, minutes=34, seconds=56)

        variable.set(delta_str, from_db=True)
        self.assertEquals(variable.get(), delta_obj)
        variable.set(delta_uni, from_db=True)
        self.assertEquals(variable.get(), delta_obj)
        variable.set(delta_obj, from_db=True)
        self.assertEquals(variable.get(), delta_obj)

        self.assertRaises(TypeError, variable.set, 0, from_db=True)
        self.assertRaises(TypeError, variable.set, marker, from_db=True)
        self.assertRaises(ValueError, variable.set, 'foobar', from_db=True)

        self.assertRaises(ValueError, variable.set, '42 months', from_db=True)
        self.assertRaises(ValueError, variable.set, '42 years', from_db=True)
