
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Variable Unit Tests
"""

import uuid
from decimal import Decimal
from fractions import Fraction
from datetime import datetime, date, time, timedelta

from twisted.trial import unittest

from txorm.variable import *
from txorm.compat import _PY3, b, u
from txorm.exceptions import NoneError
from txorm.compiler.fields import Field
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


class VariableTest(unittest.TestCase):

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

    def test_constructor_field(self):
        variable = DummyVariable(field=marker)
        self.assertEqual(variable.field, marker)

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
        self.assertRaises(NoneError, variable.set, None)

    def test_set_none_without_allow_none_and_column(self):
        p = None
        field = Field('field_name')
        variable = DummyVariable(allow_none=False, field=field)
        try:
            variable.set(None)
        except NoneError as error:
            p = error

        self.assertTrue('field_name' in str(p))

    def test_set_with_validator(self):
        args = []

        def validator(obj, attr, value):
            args.append((obj, attr, value))
            return value

        variable = DummyVariable(validator=validator)
        variable.set(3)
        self.assertEqual(args, [(None, None, 3)])

    def test_set_with_validator_and_validator_arguments(self):
        args = []

        def validator(obj, attr, value):
            args.append((obj, attr, value))
            return value

        variable = DummyVariable(
            validator=validator,
            validator_factory=lambda: 1,
            validator_attribute=2
        )
        variable.set(3)
        self.assertEqual(args, [(1, 2, 3)])

    def test_set_with_validator_raising_error(self):
        args = []

        def validator(obj, attr, value):
            args.append((obj, attr, value))
            raise ZeroDivisionError()

        variable = DummyVariable(validator=validator)
        self.assertRaises(ZeroDivisionError, variable.set, marker)
        self.assertEqual(args, [(None, None, marker)])
        self.assertEqual(variable.get(), None)

    def test_set_with_validator_changing_value(self):
        args = []

        def validator(obj, attr, value):
            args.append((obj, attr, value))
            return 42

        variable = DummyVariable(validator=validator)
        variable.set(marker)
        self.assertEqual(args, [(None, None, marker)])
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


class FractionVariableTest(unittest.TestCase):

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
        self.assertEqual(variable.get(), delta)

        self.assertRaises(TypeError, variable.set, marker)

    def test_get_set_from_database(self):
        delta_str = '42 days 12:34:56.78'
        delta_uni = u(delta_str)
        delta_obj = timedelta(days=42, hours=12, minutes=34,
                              seconds=56, microseconds=780000)

        variable = TimeDeltaVariable()

        variable.set(delta_str, from_db=True)
        self.assertEqual(variable.get(), delta_obj)
        variable.set(delta_uni, from_db=True)
        self.assertEqual(variable.get(), delta_obj)
        variable.set(delta_obj, from_db=True)
        self.assertEqual(variable.get(), delta_obj)

        delta_str = '1 day, 12:34:56'
        delta_uni = u(delta_str)
        delta_obj = timedelta(days=1, hours=12, minutes=34, seconds=56)

        variable.set(delta_str, from_db=True)
        self.assertEqual(variable.get(), delta_obj)
        variable.set(delta_uni, from_db=True)
        self.assertEqual(variable.get(), delta_obj)
        variable.set(delta_obj, from_db=True)
        self.assertEqual(variable.get(), delta_obj)

        self.assertRaises(TypeError, variable.set, 0, from_db=True)
        self.assertRaises(TypeError, variable.set, marker, from_db=True)
        self.assertRaises(ValueError, variable.set, 'foobar', from_db=True)

        self.assertRaises(ValueError, variable.set, '42 months', from_db=True)
        self.assertRaises(ValueError, variable.set, '42 years', from_db=True)


class UUIDVariableTest(unittest.TestCase):

    def test_get_set(self):
        value = uuid.UUID('{0609f76b-878f-4546-baf5-c1b135e8de72}')

        variable = UUIDVariable()

        variable.set(value)
        self.assertEqual(variable.get(), value)
        self.assertEqual(
            variable.get(to_db=True), '0609f76b-878f-4546-baf5-c1b135e8de72')

        self.assertRaises(TypeError, variable.set, marker)
        self.assertRaises(TypeError, variable.set,
                          '0609f76b-878f-4546-baf5-c1b135e8de72')
        self.assertRaises(TypeError, variable.set,
                          u('0609f76b-878f-4546-baf5-c1b135e8de72'))

    def test_get_set_from_database(self):
        value = uuid.UUID("{0609f76b-878f-4546-baf5-c1b135e8de72}")

        variable = UUIDVariable()

        # Strings and UUID objects are accepted from the database.
        variable.set(value, from_db=True)
        self.assertEqual(variable.get(), value)
        variable.set('0609f76b-878f-4546-baf5-c1b135e8de72', from_db=True)
        self.assertEqual(variable.get(), value)
        variable.set(u('0609f76b-878f-4546-baf5-c1b135e8de72'), from_db=True)
        self.assertEqual(variable.get(), value)

        # Some other representations for UUID values.
        variable.set('{0609f76b-878f-4546-baf5-c1b135e8de72}', from_db=True)
        self.assertEqual(variable.get(), value)
        variable.set('0609f76b878f4546baf5c1b135e8de72', from_db=True)
        self.assertEqual(variable.get(), value)


class MysqlEnumVariableTest(unittest.TestCase):

    def test_set_get(self):
        variable = MysqlEnumVariable({'foo', 'bar'})
        variable.set('foo')
        self.assertEqual(variable.get(), 'foo')
        self.assertEqual(variable.get(to_db=True), u('foo'))
        variable.set('bar', from_db=True)
        self.assertEqual(variable.get(), 'bar')
        self.assertEqual(variable.get(to_db=True), u('bar'))
        self.assertRaises(ValueError, variable.set, 'foobar')
        self.assertRaises(ValueError, variable.set, 2)


class EnumVariableTest(unittest.TestCase):

    def test_set_get(self):
        variable = EnumVariable({1: 'foo', 2: 'bar'}, {'foo': 1, 'bar': 2})
        variable.set('foo')
        self.assertEqual(variable.get(), 'foo')
        self.assertEqual(variable.get(to_db=True), 1)
        variable.set(2, from_db=True)
        self.assertEqual(variable.get(), 'bar')
        self.assertEqual(variable.get(to_db=True), 2)
        self.assertRaises(ValueError, variable.set, 'foobar')
        self.assertRaises(ValueError, variable.set, 2)

    def test_in_map(self):
        variable = EnumVariable({1: 'foo', 2: 'bar'}, {'one': 1, 'two': 2})
        variable.set('one')
        self.assertEqual(variable.get(), 'foo')
        self.assertEqual(variable.get(to_db=True), 1)
        variable.set(2, from_db=True)
        self.assertEqual(variable.get(), 'bar')
        self.assertEqual(variable.get(to_db=True), 2)
        self.assertRaises(ValueError, variable.set, 'foo')
        self.assertRaises(ValueError, variable.set, 2)


class ParseIntervalTest(unittest.TestCase):

    def check(self, interval, td):
        self.assertEqual(TimeDeltaVariable(interval, from_db=True).get(), td)

    def test_zero(self):
        self.check('0:00:00', timedelta(0))

    def test_one_microsecond(self):
        self.check('0:00:00.000001', timedelta(0, 0, 1))

    def test_twelve_centiseconds(self):
        self.check('0:00:00.120000', timedelta(0, 0, 120000))

    def test_one_second(self):
        self.check('0:00:01', timedelta(0, 1))

    def test_twelve_seconds(self):
        self.check('0:00:12', timedelta(0, 12))

    def test_one_minute(self):
        self.check('0:01:00', timedelta(0, 60))

    def test_twelve_minutes(self):
        self.check('0:12:00', timedelta(0, 12*60))

    def test_one_hour(self):
        self.check('1:00:00', timedelta(0, 60*60))

    def test_twelve_hours(self):
        self.check('12:00:00', timedelta(0, 12*60*60))

    def test_one_day(self):
        self.check('1 day, 0:00:00', timedelta(1))

    def test_twelve_days(self):
        self.check('12 days, 0:00:00', timedelta(12))

    def test_twelve_twelve_twelve_twelve_twelve(self):
        self.check(
            '12 days, 12:12:12.120000',
            timedelta(12, 12*60*60 + 12*60 + 12, 120000)
        )

    def test_minus_twelve_centiseconds(self):
        self.check('-1 day, 23:59:59.880000', timedelta(0, 0, -120000))

    def test_minus_twelve_days(self):
        self.check('-12 days, 0:00:00', timedelta(-12))

    def test_minus_twelve_hours(self):
        self.check('-12:00:00', timedelta(hours=-12))

    def test_one_day_and_a_half(self):
        self.check('1.5 days', timedelta(days=1, hours=12))

    def test_seconds_without_unit(self):
        self.check('1h123', timedelta(hours=1, seconds=123))

    def test_d_h_m_s_ms(self):
        self.check(
            '1d1h1m1s1ms',
            timedelta(days=1, hours=1, minutes=1, seconds=1, microseconds=1000)
        )

    def test_days_without_unit(self):
        self.check(
            '-12 1:02 3s',
            timedelta(days=-12, hours=1, minutes=2, seconds=3)
        )

    def test_unsupported_unit(self):
        try:
            self.check('1 month', None)
        except ValueError as e:
            self.assertEqual(
                str(e),
                'Unsupported interval unit \'month\' in interval \'1 month\''
            )
        else:
            self.fail('ValueError not raised')

    def test_missing_value(self):
        try:
            self.check('day', None)
        except ValueError as e:
            self.assertEqual(
                str(e),
                'Expected an interval value rather than \'day\' '
                'in interval \'day\''
            )
        else:
            self.fail('ValueError not raised')
