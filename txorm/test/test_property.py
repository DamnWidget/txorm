
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Property Unit Tests
"""

import uuid
from decimal import Decimal as decimal
from datetime import datetime, date, time, timedelta

from twisted.trial import unittest

from txorm.compat import b, u
from txorm.exceptions import NoneError
from txorm.compiler.state import State
from txorm.object_data import get_obj_data
from txorm.compiler.plain_sql import SQLRaw
from txorm.compiler.expressions import Select
from txorm.property.base import SimpleProperty
from txorm.compiler import Field, txorm_compile
from txorm.property import (
    Int, Bool, Float, Decimal, RawStr, Unicode, DateTime, Date, Time,
    TimeDelta, Enum, MysqlEnum, UUID, Fraction
)
from txorm.variable import (
    Variable, BoolVariable, IntVariable, FloatVariable, DecimalVariable,
    RawStrVariable, UnicodeVariable, DateTimeVariable, DateVariable,
    TimeVariable, TimeDeltaVariable, EnumVariable, MysqlEnumVariable,
    UUIDVariable, FractionVariable
)

from .test_expressions import assert_variables


class Wrapper(object):

    def __init__(self, obj):
        self.obj = obj

    __object_data__ = property(lambda self: self.obj.__object_data__)


class DummyVariable(Variable):
    """Dummy varaible used for testing purposes
    """
    pass


class Custom(SimpleProperty):
    """Dummy property used for testing purposes
    """
    variable_class = DummyVariable


class PropertyTest(unittest.TestCase):

    def setUp(self):

        class Dummy(object):
            """Dummy class for testing purposes
            """

            __database_table__ = 'dummytable'
            prop1 = Custom('field1', primary=True, size=11, unsigned=True)
            prop2 = Custom()
            prop3 = Custom('field3', default=50, allow_none=False)
            prop4 = Custom(
                'field4', index=True, unique=True, auto_increment=True,
                array={'other_value': 1}
            )

        class SubDummy(Dummy):
            """SubDummy class for testing purposes
            """
            __database_table__ = 'subdummytable'

        self.Dummy = Dummy
        self.SubDummy = SubDummy

    def test_field(self):
        self.assertTrue(isinstance(self.Dummy.prop1, Field))

    def test_cls(self):
        self.assertEqual(self.Dummy.prop1.cls, self.Dummy)
        self.assertEqual(self.Dummy.prop2.cls, self.Dummy)
        self.assertEqual(self.SubDummy.prop1.cls, self.SubDummy)
        self.assertEqual(self.SubDummy.prop2.cls, self.SubDummy)

    def test_name(self):
        self.assertEqual(self.Dummy.prop1.name, 'field1')

    def test_automatic_name(self):
        self.assertEqual(self.Dummy.prop2.name, 'prop2')

    def test_size(self):
        self.assertEqual(self.Dummy.prop1.size, 11)

    def test_unsigned(self):
        self.assertTrue(self.Dummy.prop1.unsigned)

    def test_auto_unsigned(self):
        self.assertFalse(self.Dummy.prop2.unsigned)

    def test_index(self):
        self.assertTrue(self.Dummy.prop4.index)

    def test_auto_index(self):
        self.assertFalse(self.Dummy.prop2.index)

    def test_unique(self):
        self.assertTrue(self.Dummy.prop4.unique)

    def test_auto_unique(self):
        self.assertFalse(self.Dummy.prop2.unique)

    def test_autoincrement(self):
        self.assertTrue(self.Dummy.prop4.auto_increment)

    def test_auto_autoincrement(self):
        self.assertFalse(self.Dummy.prop2.auto_increment)

    def test_array(self):
        self.assertEqual(self.Dummy.prop4.array['other_value'], 1)

    def test_auto_array(self):
        self.assertIsNone(self.Dummy.prop1.array)

    def test_auto_table(self):
        self.assertEqual(self.Dummy.prop1.table, self.Dummy)
        self.assertEqual(self.Dummy.prop2.table, self.Dummy)

    def test_auto_table_subclass(self):
        self.assertEqual(self.Dummy.prop1.table, self.Dummy)
        self.assertEqual(self.Dummy.prop2.table, self.Dummy)
        self.assertEqual(self.SubDummy.prop1.table, self.SubDummy)
        self.assertEqual(self.SubDummy.prop2.table, self.SubDummy)

    def test_variable_factory(self):
        variable = self.Dummy.prop1.variable_factory()
        self.assertTrue(isinstance(variable, DummyVariable))
        self.assertFalse(variable.is_defined)

        variable = self.Dummy.prop3.variable_factory()
        self.assertTrue(isinstance(variable, DummyVariable))
        self.assertTrue(variable.is_defined)

    def test_variable_factory_validator_attribute(self):
        prop = Custom()

        class Class1(object):
            __database_table__ = 'table1'
            prop1 = prop

        class Class2(object):
            __database_table__ = 'table2'
            prop2 = prop

        args = []

        def validator(obj, attr, value):
            args.append((obj, attr, value))

        variable1 = Class1.prop1.variable_factory(validator=validator)
        variable2 = Class2.prop2.variable_factory(validator=validator)

        variable1.set(1)
        variable2.set(2)
        self.assertEqual(args, [(None, 'prop1', 1), (None, 'prop2', 2)])

    def test_default(self):
        obj = self.SubDummy()
        self.assertEqual(obj.prop1, None)
        self.assertEqual(obj.prop2, None)
        self.assertEqual(obj.prop3, 50)
        self.assertEqual(obj.prop4, None)

    def test_set_get(self):
        obj = self.Dummy()
        obj.prop1 = 10
        obj.prop2 = 20
        obj.prop3 = 30
        obj.prop4 = 40
        self.assertEqual(obj.prop1, 10)
        self.assertEqual(obj.prop2, 20)
        self.assertEqual(obj.prop3, 30)
        self.assertEqual(obj.prop4, 40)

    def test_set_get_none(self):
        obj = self.Dummy()
        obj.prop1 = None
        obj.prop2 = None
        self.assertEqual(obj.prop1, None)
        self.assertEqual(obj.prop2, None)
        self.assertRaises(NoneError, setattr, obj, 'prop3', None)

    def test_set_with_validator(self):
        args = []

        def validator(obj, attr, value):
            args[:] = obj, attr, value
            return 42

        class Class(object):
            __database_table__ = 'mytable'
            prop = Custom('column', primary=True, validator=validator)

        obj = Class()
        obj.prop = 21

        self.assertEqual(args, [obj, 'prop', 21])
        self.assertEqual(obj.prop, 42)

    def test_set_get_subclass(self):
        obj = self.SubDummy()
        obj.prop1 = 10
        obj.prop2 = 20
        obj.prop3 = 30
        obj.prop4 = 40
        self.assertEqual(obj.prop1, 10)
        self.assertEqual(obj.prop2, 20)
        self.assertEqual(obj.prop3, 30)
        self.assertEqual(obj.prop4, 40)

    def test_set_get_explicitly(self):
        obj = self.Dummy()
        prop1 = self.Dummy.prop1
        prop2 = self.Dummy.prop2
        prop3 = self.Dummy.prop3
        prop4 = self.Dummy.prop4
        prop1.__set__(obj, 10)
        prop2.__set__(obj, 20)
        prop3.__set__(obj, 30)
        prop4.__set__(obj, 40)
        self.assertEqual(prop1.__get__(obj), 10)
        self.assertEqual(prop2.__get__(obj), 20)
        self.assertEqual(prop3.__get__(obj), 30)
        self.assertEqual(prop4.__get__(obj), 40)

    def test_set_get_subclass_explicitly(self):
        obj = self.SubDummy()
        prop1 = self.Dummy.prop1
        prop2 = self.Dummy.prop2
        prop3 = self.Dummy.prop3
        prop4 = self.Dummy.prop4
        prop1.__set__(obj, 10)
        prop2.__set__(obj, 20)
        prop3.__set__(obj, 30)
        prop4.__set__(obj, 40)
        self.assertEqual(prop1.__get__(obj), 10)
        self.assertEqual(prop2.__get__(obj), 20)
        self.assertEqual(prop3.__get__(obj), 30)
        self.assertEqual(prop4.__get__(obj), 40)

    def test_delete(self):
        obj = self.Dummy()
        obj.prop1 = 10
        obj.prop2 = 20
        obj.prop3 = 30
        obj.prop4 = 40
        del obj.prop1
        del obj.prop2
        del obj.prop3
        del obj.prop4
        self.assertEqual(obj.prop1, None)
        self.assertEqual(obj.prop2, None)
        self.assertEqual(obj.prop3, None)
        self.assertEqual(obj.prop4, None)

    def test_delete_subclass(self):
        obj = self.SubDummy()
        obj.prop1 = 10
        obj.prop2 = 20
        obj.prop3 = 30
        obj.prop4 = 40
        del obj.prop1
        del obj.prop2
        del obj.prop3
        del obj.prop4
        self.assertEqual(obj.prop1, None)
        self.assertEqual(obj.prop2, None)
        self.assertEqual(obj.prop3, None)
        self.assertEqual(obj.prop4, None)

    def test_delete_explicitly(self):
        obj = self.Dummy()
        obj.prop1 = 10
        obj.prop2 = 20
        obj.prop3 = 30
        obj.prop4 = 40
        self.Dummy.prop1.__delete__(obj)
        self.Dummy.prop2.__delete__(obj)
        self.Dummy.prop3.__delete__(obj)
        self.Dummy.prop4.__delete__(obj)
        self.assertEqual(obj.prop1, None)
        self.assertEqual(obj.prop2, None)
        self.assertEqual(obj.prop3, None)
        self.assertEqual(obj.prop4, None)

    def test_delete_subclass_explicitly(self):
        obj = self.SubDummy()
        obj.prop1 = 10
        obj.prop2 = 20
        obj.prop3 = 30
        obj.prop4 = 40
        self.Dummy.prop1.__delete__(obj)
        self.Dummy.prop2.__delete__(obj)
        self.Dummy.prop3.__delete__(obj)
        self.Dummy.prop4.__delete__(obj)
        self.assertEqual(obj.prop1, None)
        self.assertEqual(obj.prop2, None)
        self.assertEqual(obj.prop3, None)
        self.assertEqual(obj.prop4, None)

    def test_comparable_expression(self):
        prop1 = self.Dummy.prop1
        prop2 = self.Dummy.prop2
        prop3 = self.Dummy.prop3
        prop4 = self.Dummy.prop4
        expression = Select(
            SQLRaw('*'), (prop1 == 'value1') &
            (prop2 == 'value2') &
            (prop3 == 'value3') &
            (prop4 == 'value4')
        )
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'SELECT * FROM dummytable WHERE '
            'dummytable.field1 = ? AND '
            'dummytable.prop2 = ? AND '
            'dummytable.field3 = ? AND '
            'dummytable.field4 = ?'
        )
        assert_variables(self, state.parameters, [
            DummyVariable('value1'),
            DummyVariable('value2'),
            DummyVariable('value3'),
            DummyVariable('value4')
        ])

    def test_comparable_expression_subclass(self):
        prop1 = self.SubDummy.prop1
        prop2 = self.SubDummy.prop2
        prop3 = self.SubDummy.prop3
        prop4 = self.SubDummy.prop4
        expression = Select(
            SQLRaw('*'), (prop1 == 'value1') &
            (prop2 == 'value2') &
            (prop3 == 'value3') &
            (prop4 == 'value4')
        )
        state = State()
        statement = txorm_compile(expression, state)
        self.assertEqual(
            statement,
            'SELECT * FROM subdummytable WHERE '
            'subdummytable.field1 = ? AND '
            'subdummytable.prop2 = ? AND '
            'subdummytable.field3 = ? AND '
            'subdummytable.field4 = ?'
        )
        assert_variables(self, state.parameters, [
            DummyVariable('value1'),
            DummyVariable('value2'),
            DummyVariable('value3'),
            DummyVariable('value4')
        ])

    def test_set_get_delete_with_wrapper(self):
        obj = self.Dummy()
        get_obj_data(obj)   # ensure the object data exsts
        self.Dummy.prop1.__set__(Wrapper(obj), 10)
        self.assertEqual(self.Dummy.prop1.__get__(Wrapper(obj)), 10)
        self.Dummy.prop1.__delete__(Wrapper(obj))
        self.assertEqual(self.Dummy.prop1.__get__(Wrapper(obj)), None)

    def test_reuse_of_instance(self):
        prop = Custom()

        class Class1(object):
            __database_table__ = 'table1'
            prop1 = prop

        class Class2(object):
            __database_table__ = 'table2'
            prop2 = prop

        self.assertEqual(Class1.prop1.name, 'prop1')
        self.assertEqual(Class1.prop1.table, Class1)
        self.assertEqual(Class2.prop2.name, 'prop2')
        self.assertEqual(Class2.prop2.table, Class2)
        self.assertEqual(Class1.prop1, Class2.prop2)

    def test_creattion_counter(self):
        self.assertTrue(
            self.Dummy.prop3._creation_order > self.Dummy.prop2._creation_order
        )
        self.assertTrue(
            self.Dummy.prop2._creation_order > self.Dummy.prop1._creation_order
        )
        self.assertTrue(
            self.Dummy.prop4._creation_order > self.Dummy.prop3._creation_order
        )


class PropertyKindsTest(unittest.TestCase):

    def setup(self, property, *args, **kwargs):
        prop2_kwargs = kwargs.pop('prop2_kwargs', {})
        kwargs['primary'] = True

        class Class(object):
            __database_table__ = 'mytable'
            prop1 = property('field1', *args, **kwargs)
            prop2 = property(**prop2_kwargs)

        class SubClass(Class):
            pass

        self.Class = Class
        self.SubClass = SubClass
        self.obj = SubClass()
        self.obj_data = get_obj_data(self.obj)
        self.field1 = self.SubClass.prop1
        self.field2 = self.SubClass.prop2
        self.variable1 = self.obj_data.variables[self.field1]
        self.variable2 = self.obj_data.variables[self.field2]

    def commons(self, variable_type):
        self.assertTrue(isinstance(self.field1, Field))
        self.assertTrue(isinstance(self.field2, Field))
        self.assertEqual(self.field1.name, 'field1')
        self.assertEqual(self.field1.table, self.SubClass)
        self.assertEqual(self.field2.name, 'prop2')
        self.assertEqual(self.field2.table, self.SubClass)
        self.assertTrue(isinstance(self.variable1, variable_type))
        self.assertTrue(isinstance(self.variable2, variable_type))

    def test_bool(self):
        self.setup(Bool, default=50, allow_none=False)

        self.commons(BoolVariable)

        self.assertEqual(self.obj.prop1, True)
        self.assertRaises(NoneError, setattr, self.obj, 'prop1', None)
        self.obj.prop2 = None
        self.assertEqual(self.obj.prop2, None)

        self.obj.prop1 = 1
        self.assertTrue(self.obj.prop1 is True)
        self.obj.prop1 = 0
        self.assertTrue(self.obj.prop1 is False)

    def test_int(self):
        self.setup(Int, default=50, allow_none=False)

        self.commons(IntVariable)

        self.assertEqual(self.obj.prop1, 50)
        self.assertRaises(NoneError, setattr, self.obj, 'prop1', None)
        self.obj.prop2 = None
        self.assertEqual(self.obj.prop2, None)

        self.obj.prop1 = False
        self.assertTrue(self.obj.prop1 == 0)
        self.obj.prop1 = True
        self.assertTrue(self.obj.prop1 == 1)

    def test_float(self):
        self.setup(Float, default=50.5, allow_none=False)

        self.commons(FloatVariable)

        self.assertEqual(self.obj.prop1, 50.5)
        self.assertRaises(NoneError, setattr, self.obj, 'prop1', None)
        self.obj.prop2 = None
        self.assertEqual(self.obj.prop2, None)

        self.obj.prop1 = 1
        self.assertTrue(isinstance(self.obj.prop1, float))

    def test_decimal(self):
        self.setup(Decimal, default=decimal('50.5'), allow_none=False)

        self.commons(DecimalVariable)

        self.assertEqual(self.obj.prop1, decimal('50.5'))
        self.assertRaises(NoneError, setattr, self.obj, 'prop1', None)
        self.obj.prop2 = None
        self.assertEqual(self.obj.prop2, None)

        self.obj.prop1 = 1
        self.assertTrue(isinstance(self.obj.prop1, decimal))

    def test_str(self):
        self.setup(RawStr, default=b('def'), allow_none=False)

        self.commons(RawStrVariable)

        self.assertEqual(self.obj.prop1, b('def'))
        self.assertRaises(NoneError, setattr, self.obj, 'prop1', None)
        self.obj.prop2 = None
        self.assertEqual(self.obj.prop2, None)

        self.assertRaises(TypeError, setattr, self.obj, 'prop1', u('unicode'))

    def test_unicode(self):
        self.setup(Unicode, default=u('unicode'), allow_none=False)

        self.commons(UnicodeVariable)

        self.assertEqual(self.obj.prop1, u('unicode'))
        self.assertRaises(NoneError, setattr, self.obj, 'prop1', None)
        self.obj.prop2 = None
        self.assertEqual(self.obj.prop2, None)

        self.assertRaises(TypeError, setattr, self.obj, 'prop1', b('str'))

    def test_datetime(self):
        self.setup(DateTime, default=0, allow_none=False)

        self.commons(DateTimeVariable)

        self.assertEqual(self.obj.prop1, datetime.utcfromtimestamp(0))
        self.assertRaises(NoneError, setattr, self.obj, 'prop1', None)
        self.obj.prop2 = None
        self.assertEqual(self.obj.prop2, None)

        self.obj.prop1 = 0.0
        self.assertEqual(self.obj.prop1, datetime.utcfromtimestamp(0))
        self.obj.prop1 = datetime(2014, 7, 31, 0, 31)
        self.assertEqual(self.obj.prop1, datetime(2014, 7, 31, 0, 31))

        self.assertRaises(TypeError, setattr, self.obj, 'prop1', object())

    def test_date(self):
        self.setup(Date, default=date(2014, 8, 1), allow_none=False)

        self.commons(DateVariable)

        self.assertEqual(self.obj.prop1, date(2014, 8, 1))
        self.assertRaises(NoneError, setattr, self.obj, 'prop1', None)
        self.obj.prop2 = None
        self.assertEqual(self.obj.prop2, None)

        self.obj.prop1 = datetime(2014, 8, 1, 18, 44, 12)
        self.assertEqual(self.obj.prop1, date(2014, 8, 1))
        self.obj.prop1 = date(2014, 8, 1)
        self.assertEqual(self.obj.prop1, date(2014, 8, 1))

        self.assertRaises(TypeError, setattr, self.obj, 'prop1', object())

    def test_time(self):
        self.setup(Time, default=time(18, 50), allow_none=False)

        self.commons(TimeVariable)

        self.assertEqual(self.obj.prop1, time(18, 50))
        self.assertRaises(NoneError, setattr, self.obj, 'prop1', None)
        self.obj.prop2 = None
        self.assertEqual(self.obj.prop2, None)

        self.obj.prop1 = datetime(2014, 8, 1, 18, 44, 12)
        self.assertEqual(self.obj.prop1, time(18, 44, 12))
        self.obj.prop1 = time(18, 44, 12)
        self.assertEqual(self.obj.prop1, time(18, 44, 12))

        self.assertRaises(TypeError, setattr, self.obj, 'prop1', object())

    def test_timedelta(self):
        self.setup(
            TimeDelta,
            default=timedelta(days=1, seconds=2, microseconds=3),
            allow_none=False
        )

        self.commons(TimeDeltaVariable)

        self.assertEqual(
            self.obj.prop1, timedelta(days=1, seconds=2, microseconds=3)
        )
        self.assertRaises(NoneError, setattr, self.obj, 'prop1', None)
        self.obj.prop2 = None
        self.assertEqual(self.obj.prop2, None)

        self.obj.prop1 = timedelta(days=42, seconds=42, microseconds=42)
        self.assertEqual(
            self.obj.prop1, timedelta(days=42, seconds=42, microseconds=42)
        )

        self.assertRaises(TypeError, setattr, self.obj, 'prop1', object())

    def test_enum(self):
        self.setup(
            Enum,
            map={'sausage': 1, 'spam': 2},
            default='spam',
            allow_none=False,
            prop2_kwargs={'map': {'susage': 1, 'spam': 2}}
        )

        self.commons(EnumVariable)

        self.assertEqual(self.obj.prop1, 'spam')
        self.assertRaises(NoneError, setattr, self.obj, 'prop1', None)
        self.obj.prop2 = None
        self.assertEqual(self.obj.prop2, None)

        self.obj.prop1 = 'sausage'
        self.assertEqual(self.obj.prop1, 'sausage')
        self.obj.prop1 = 'spam'
        self.assertEqual(self.obj.prop1, 'spam')

        self.assertRaises(ValueError, setattr, self.obj, 'prop1', 'circus')
        self.assertRaises(ValueError, setattr, self.obj, 'prop1', 1)

    def test_enum_with_set_map(self):
        self.setup(
            Enum,
            map={'sausage': 1, 'spam': 2},
            set_map={'bacon': 1, 'chorizo': 2},
            default='chorizo',
            allow_none=False,
            prop2_kwargs={'map': {'susage': 1, 'spam': 2}}
        )

        self.commons(EnumVariable)

        self.assertEqual(self.obj.prop1, 'spam')
        self.assertRaises(NoneError, setattr, self.obj, 'prop1', None)
        self.obj.prop2 = None
        self.assertEqual(self.obj.prop2, None)

        self.obj.prop1 = 'bacon'
        self.assertEqual(self.obj.prop1, 'sausage')
        self.obj.prop1 = 'chorizo'
        self.assertEqual(self.obj.prop1, 'spam')

        self.assertRaises(ValueError, setattr, self.obj, 'prop1', 'sausage')
        self.assertRaises(ValueError, setattr, self.obj, 'prop1', 1)

    def test_mysql_enum(self):
        column = MysqlEnum(set={'foo', 'bar'}, default='foo')
        self.assertEqual(column._variable_kwargs['_set'], set(['foo', 'bar']))

        class EnumTest(object):
            __storm_table__ = 'testtable'
            prop1 = MysqlEnum(set={'foo', 'bar'}, default='foo', primary=True)

        obj = EnumTest()

        self.assertEqual(obj.prop1, 'foo')
        obj.prop1 = 'bar'
        self.assertEqual(obj.prop1, 'bar')

        self.assertRaises(ValueError, setattr, obj, 'prop1', 'baz')
        self.assertRaises(ValueError, setattr, obj, 'prop1', 1)

    def test_uuid(self):
        value1 = uuid.UUID('{b50cb608-450d-469b-9e11-6d18c916d3d0}')
        value2 = uuid.UUID('{3b01f35c-8368-484e-bc70-406cde3ea693}')
        self.setup(UUID, default=value1, allow_none=False)

        self.commons(UUIDVariable)

        self.assertEqual(self.obj.prop1, value1)
        self.assertRaises(NoneError, setattr, self.obj, 'prop1', None)
        self.obj.prop2 = None
        self.assertEqual(self.obj.prop2, None)

        self.obj.prop1 = value1
        self.assertEqual(self.obj.prop1, value1)
        self.obj.prop1 = value2
        self.assertEqual(self.obj.prop1, value2)

        self.assertRaises(
            TypeError, setattr, self.obj, 'prop1',
            '{b50cb608-450d-469b-9e11-6d18c916d3d0}'
        )

    def test_variable_factory_arguments(self):

        class Class(object):
            __database_table__ = 'test'
            id = Int(primary=True)

        validator_args = []

        def validator(obj, attr, value):
            validator_args[:] = obj, attr, value
            return value

        for func, cls, value in [
            (Bool, BoolVariable, True),
            (Int, IntVariable, 1),
            (Float, FloatVariable, 1.1),
            (RawStr, RawStrVariable, b('str')),
            (Unicode, UnicodeVariable, u('unicode')),
            (DateTime, DateTimeVariable, datetime.now()),
            (Date, DateVariable, date.today()),
                (Time, TimeVariable, datetime.now().time())]:

            # Test no default and allow_none=True.
            Class.prop = func(name='name')
            column = Class.prop.__get__(None, Class)
            self.assertEquals(column.name, 'name')
            self.assertEquals(column.table, Class)

            variable = column.variable_factory()
            self.assertTrue(isinstance(variable, cls))
            self.assertEquals(variable.get(), None)
            variable.set(None)
            self.assertEquals(variable.get(), None)

            # Test default and allow_none=False.
            Class.prop = func(name='name', default=value, allow_none=False)
            column = Class.prop.__get__(None, Class)
            self.assertEquals(column.name, 'name')
            self.assertEquals(column.table, Class)

            variable = column.variable_factory()
            self.assertTrue(isinstance(variable, cls))
            self.assertRaises(NoneError, variable.set, None)
            self.assertEquals(variable.get(), value)

            # Test validator.
            Class.prop = func(name='name', validator=validator, default=value)
            column = Class.prop.__get__(None, Class)
            self.assertEquals(column.name, 'name')
            self.assertEquals(column.table, Class)

            del validator_args[:]
            variable = column.variable_factory()
            self.assertTrue(isinstance(variable, cls))
            # Validator is not called on instantiation.
            self.assertEquals(validator_args, [])
            # But is when setting the variable.
            variable.set(value)
            self.assertEquals(validator_args, [None, 'prop', value])
