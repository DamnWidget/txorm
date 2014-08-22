
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from twisted.trial import unittest

from txorm.property import Property
from txorm.property.registry import PropertyRegisterMeta, PropertyPathError


class BaseClassMetaTest(unittest.TestCase):

    def setUp(self):

        class Base(object):
            __metaclass__ = PropertyRegisterMeta

        class Class(Base):
            __database_table__ = 'mytable'
            prop1 = Property('field1', primary=True)
            prop2 = Property()

        class SubClass(Class):
            __database_table__ = 'mysubtable'

        self.Class = Class
        self.SubClass = SubClass

        class Class(Class):
            __module__ += '.foo'
            prop3 = Property('field3')

        self.AnotherClass = Class
        self.registry = Base._txorm_property_registry

    def test_get_empty(self):
        self.assertRaises(PropertyPathError, self.registry.get, 'unexistent')

    def test_get_subclass(self):
        prop1 = self.registry.get('SubClass.prop1')
        prop2 = self.registry.get('SubClass.prop2')
        self.assertTrue(prop1 is self.SubClass.prop1)
        self.assertTrue(prop2 is self.SubClass.prop2)

    def test_get_ambiguous(self):
        self.assertRaises(PropertyPathError, self.registry.get, 'Class.prop1')
        self.assertRaises(PropertyPathError, self.registry.get, 'Class.prop2')
