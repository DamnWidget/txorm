
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Field Data Unit Tests
"""

from twisted.trial import unittest

from txorm.variable import Variable
from txorm.property import Property
from txorm.exceptions import ClassDataError
from txorm.object_data import ClassData, ObjectData
from txorm.object_data import get_obj_data, get_cls_data, set_obj_data


class FieldDataTest(unittest.TestCase):

    def setUp(self):

        self.Dummy = Dummy
        self.obj = Dummy()
        self.obj_data = get_obj_data(self.obj)
        self.cls_data = get_cls_data(Dummy)
        self.variable1 = self.obj_data.variables[Dummy.prop1]
        self.variable2 = self.obj_data.variables[Dummy.prop2]

    def test_hashing(self):
        self.assertEqual(
            hash(self.obj_data), hash(self.obj_data)
        )

    def test_equals(self):
        obj_data1 = self.obj_data
        obj_data2 = get_obj_data(self.Dummy())
        self.assertFalse(obj_data1 == obj_data2)

    def test_not_equals(self):
        obj_data1 = self.obj_data
        obj_data2 = get_obj_data(self.Dummy())
        self.assertTrue(obj_data1 != obj_data2)

    def test_dict_subclass(self):
        self.assertTrue(isinstance(self.obj_data, dict))

    def test_variables(self):
        self.assertTrue(isinstance(self.obj_data.variables, dict))

        for field in self.cls_data.fields:
            variable = self.obj_data.variables.get(field)
            self.assertTrue(isinstance(variable, Variable))
            self.assertTrue(variable.field is field)

        self.assertEqual(
            len(self.obj_data.variables), len(self.cls_data.fields)
        )

    def test_variable_has_valiator_object_factory(self):
        args = []

        def validator(obj, attr, value):
            args.append((obj, attr, value))

        class Dummy(object):
            __database_table__ = 'dummy'
            prop = Property(
                primary=True, variable_kwargs={'validator': validator}
            )

        obj = Dummy()
        get_obj_data(obj).variables[Dummy.prop].set(123)
        self.assertEqual(args, [(obj, 'prop', 123)])

    def test_primary_vars(self):
        self.assertTrue(isinstance(self.obj_data.primary_vars, tuple))
        for field, variable in zip(
                self.cls_data.primary_key, self.obj_data.primary_vars):
            self.assertEqual(
                self.obj_data.variables.get(field), variable)

        self.assertEqual(
            len(self.obj_data.primary_vars),
            len(self.cls_data.primary_key)
        )

    def test_get_obj(self):
        self.assertTrue(self.obj_data.get_object() is self.obj)

    def test_get_obj_reference(self):
        get_object = self.obj_data.get_object
        self.assertTrue(get_object() is self.obj)
        another_obj = self.Dummy()
        self.obj_data.set_object(another_obj)
        self.assertTrue(get_object() is another_obj)

    def test_set_obj(self):
        obj = self.Dummy()
        self.obj_data.set_object(obj)
        self.assertTrue(self.obj_data.get_object() is obj)

    def test_weak_reference(self):
        obj = self.Dummy()
        obj_data = get_obj_data(obj)
        del obj
        self.assertEqual(obj_data.get_object(), None)


class ClassDataTest(unittest.TestCase):

    def setUp(self):

        self.Dummy = Dummy
        self.cls_data = get_cls_data(Dummy)

    def test_invalid_class(self):
        class Dummy(object):
            pass

        self.assertRaises(ClassDataError, ClassData, Dummy)

    def tets_cls(self):
        self.assertEqual(self.cls_data.cls, self.Dummy)

    def test_fields(self):
        self.assertEqual(
            self.cls_data.fields, (self.Dummy.prop1, self.Dummy.prop2)
        )

    def test_table(self):
        self.assertEqual(self.cls_data.table.name, 'dummy')

    def test_primary_key(self):
        self.assertTrue(self.cls_data.primary_key[0] is self.Dummy.prop1)
        self.assertEqual(len(self.cls_data.primary_key), 1)

    def test_primary_key_with_attribute(self):

        class SubClass(self.Dummy):
            __table_primary__ = 'prop2'

        cls_data = get_cls_data(SubClass)

        self.assertTrue(cls_data.primary_key[0] is SubClass.prop2)
        self.assertEqual(len(self.cls_data.primary_key), 1)

    def test_primary_key_composed(self):

        class Dummy(object):
            __database_table__ = 'dummy'
            prop1 = Property('field1', primary=2)
            prop2 = Property('field2', primary=1)

        cls_data = ClassData(Dummy)

        self.assertTrue(cls_data.primary_key[0] is Dummy.prop2)
        self.assertTrue(cls_data.primary_key[1] is Dummy.prop1)
        self.assertEqual(len(cls_data.primary_key), 2)

    def test_primary_key_composed_with_attribute(self):

        class Dummy(object):
            __database_table__ = 'dummy'
            __table_primary__ = ('prop2', 'prop1')
            prop1 = Property('field1', primary=True)
            prop2 = Property('field2')

        cls_data = ClassData(Dummy)

        self.assertTrue(cls_data.primary_key[0] is Dummy.prop2)
        self.assertTrue(cls_data.primary_key[1] is Dummy.prop1)
        self.assertEqual(len(cls_data.primary_key), 2)

    def test_primary_key_composed_duplicated(self):

        class Dummy(object):
            __database_table__ = 'dummy'
            prop1 = Property('field1', primary=True)
            prop2 = Property('field2', primary=True)
        self.assertRaises(ClassDataError, ClassData, Dummy)

    def test_primary_key_missing(self):

        class Dummy(object):
            __database_table__ = 'dummy'
            prop1 = Property('field1')
            prop2 = Property('field2')
        self.assertRaises(ClassDataError, ClassData, Dummy)

    def test_primary_key_attribute_missing(self):

        class Dummy(object):
            __database_table__ = 'dummy'
            __table_primary__ = ()
            prop1 = Property('field1', primary=True)
            prop2 = Property('field2')
        self.assertRaises(ClassDataError, ClassData, Dummy)

    def test_primary_key_pos(self):

        class Dummy(object):
            __database_table__ = 'dummy'
            prop1 = Property('field1', primary=2)
            prop2 = Property('field2')
            prop3 = Property('field3', primary=1)

        cls_data = ClassData(Dummy)
        self.assertEqual(cls_data.primary_key_pos, (2, 0))


class GetTest(unittest.TestCase):

    def setUp(self):
        self.Dummy = Dummy
        self.obj = Dummy()

    def test_get_cls_data(self):
        cls_data = get_cls_data(self.Dummy)
        self.assertIsInstance(cls_data, ClassData)
        self.assertIs(cls_data, get_cls_data(self.Dummy))

    def test_get_obj_data(self):
        obj_data = get_obj_data(self.obj)
        self.assertIsInstance(obj_data, ObjectData)
        self.assertIs(obj_data, get_obj_data(self.obj))

    def test_get_obj_data_on_obj_data(self):
        obj_data = get_obj_data(self.obj)
        self.assertIs(get_obj_data(obj_data), obj_data)

    def test_set_obj_data(self):
        obj_data1 = get_obj_data(self.obj)
        obj_data2 = ObjectData(self.obj)

        self.assertEqual(get_obj_data(self.obj), obj_data1)
        set_obj_data(self.obj, obj_data2)
        self.assertEqual(get_obj_data(self.obj), obj_data2)


class Dummy(object):
    """Dummy class for testing purposes
    """

    __database_table__ = 'dummy'
    prop1 = Property('field1', primary=True)
    prop2 = Property('field2')
