
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Field Data Unit Tests
"""

import gc
from weakref import ref

from twisted.trial import unittest

from txorm.variable import Variable
from txorm.property import Property
from txorm.compiler import txorm_compile
from txorm.exceptions import ClassDataError
from txorm.compiler.expressions import Select
from txorm.object_data import ClassData, ObjectData, ClassAlias
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
        self.assertEqual(self.cls_data.table.name, 'table')

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


class ClassAliasTest(unittest.TestCase):

    def setUp(self):
        self.Dummy = Dummy
        self.ClassAlias = ClassAlias(self.Dummy, 'alias_dummy')

    def test_cls_data_cls(self):
        cls_data = get_cls_data(self.ClassAlias)
        self.assertEqual(cls_data.cls, self.Dummy)
        self.assertEqual(cls_data.table.name, 'alias_dummy')
        self.assertEqual(self.ClassAlias.prop1.name, 'field1')
        self.assertEqual(self.ClassAlias.prop2.name, 'field2')
        self.assertEqual(self.ClassAlias.prop1.table, self.ClassAlias)
        self.assertEqual(self.ClassAlias.prop2.table, self.ClassAlias)

    def test_compile(self):
        statement = txorm_compile(self.ClassAlias)
        self.assertEqual(statement, 'alias_dummy')

    def test_compile_with_reserved_keyword(self):
        Alias = ClassAlias(self.Dummy, 'select')
        statement = txorm_compile(Alias)
        self.assertEqual(statement, '"select"')

    def test_compile_in_select(self):
        expression = Select(
            self.ClassAlias.prop1, self.ClassAlias.prop1 == 1, self.ClassAlias
        )
        statement = txorm_compile(expression)
        self.assertEqual(
            statement,
            'SELECT alias_dummy.field1 FROM "table" AS alias_dummy '
            'WHERE alias_dummy.field1 = ?'
        )

    def test_compile_in_select_with_reserved_word(self):
        Alias = ClassAlias(self.Dummy, 'select')
        expression = Select(Alias.prop1, Alias.prop1 == 1, Alias)
        statement = txorm_compile(expression)
        self.assertEqual(
            statement,
            'SELECT "select".field1 FROM "table" AS "select" '
            'WHERE "select".field1 = ?'
        )

    def test_metaclass_messing_around(self):

        class MetaClass(type):
            def __new__(meta_cls, name, bases, dict):
                cls = type.__new__(meta_cls, name, bases, dict)
                cls.__database_table__ = 'WHAT THE...'
                return cls

        class Class(object):
            __metaclass__ = MetaClass
            __database_table__ = 'table'
            prop1 = Property('field1', primary=True)

        Alias = ClassAlias(Class, 'USE_THIS')
        self.assertEqual(Alias.__database_table__, 'USE_THIS')

    def test_cached_aliases(self):
        alias1 = ClassAlias(self.Dummy, 'something_unlikely')
        alias2 = ClassAlias(self.Dummy, 'something_unlikely')
        self.assertIdentical(alias1, alias2)
        alias3 = ClassAlias(self.Dummy, 'something_unlikely2')
        self.assertNotIdentical(alias1, alias3)
        alias4 = ClassAlias(self.Dummy, 'something_unlikely2')
        self.assertIdentical(alias3, alias4)

    def test_unnamed_aliases_not_cached(self):
        alias1 = ClassAlias(self.Dummy)
        alias2 = ClassAlias(self.Dummy)
        self.assertNotIdentical(alias1, alias2)

    def test_alias_cache_is_per_class(self):

        class LocalClass(self.Dummy):
            pass

        alias1 = ClassAlias(self.Dummy, 'something_unikely')
        alias2 = ClassAlias(LocalClass, 'something_unikely')
        self.assertNotIdentical(alias1, alias2)

    def test_aliases_only_last_as_long_as_class(self):

        class LocalClass(self.Dummy):
            pass

        alias = ClassAlias(LocalClass, 'something_unikely3')
        alias_ref = ref(alias)
        class_ref = ref(LocalClass)
        del alias
        del LocalClass

        for i in range(4):
            gc.collect()

        self.assertIsNone(class_ref())
        self.assertIsNone(alias_ref())


class TypeCompileTest(unittest.TestCase):

    def test_nested_classes(self):

        class Class1(object):
            __database_table__ = 'class1'
            id = Property(primary=True)

        class Class2(object):
            __database_table__ = Class1
            id = Property(primary=True)

        statement = txorm_compile(Class2)
        self.assertEqual(statement, 'class1')
        alias = ClassAlias(Class2, 'alias')
        statement = txorm_compile(Select(alias.id))
        self.assertEqual(statement, 'SELECT alias.id FROM class1 AS alias')


class Dummy(object):
    """Dummy class for testing purposes
    """

    __database_table__ = 'table'
    prop1 = Property('field1', primary=True)
    prop2 = Property('field2')
