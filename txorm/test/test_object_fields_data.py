
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Field Data Unit Tests
"""

from twisted.trial import unittest

from txorm.property import Property
from txorm.exceptions import ClassDataError
from txorm.object_fields_data import ClassData
from txorm.object_fields_data import get_obj_fields_data, get_cls_data


class FieldDataTest(unittest.TestCase):

    def setUp(self):

        class Dummy(object):
            """Dummy class for testing purposes
            """

            __database_table__ = 'dummy'
            prop1 = Property('field1', primary=True)
            prop2 = Property('field2')

        self.Dummy = Dummy
        self.obj = Dummy()
        self.obj_fields_data = get_obj_fields_data(self.obj)
        self.cls_fields_data = get_cls_data(Dummy)
        self.variable1 = self.obj_fields_data.variables[Dummy.prop1]
        self.variable2 = self.obj_fields_data.variables[Dummy.prop2]

    def test_hashing(self):
        self.assertEqual(
            hash(self.obj_fields_data), hash(self.obj_fields_data)
        )


class ClassDataTest(unittest.TestCase):

    def setUp(self):

        class Dummy(object):
            """Dummy class for testing purposes
            """

            __database_table__ = 'dummy'
            prop1 = Property('field1', primary=True)
            prop2 = Property('field2')

        self.Dummy = Dummy
        self.cls_data = get_cls_data(Dummy)

    def test_invalid_class(self):
        class Class(object):
            pass

        self.assertRaises(ClassDataError, ClassData, Class)
