
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from twisted.trial import unittest

from txorm.property import Property
from txorm.property.registry import PropertyRegisterMeta


class Base(object):
    __metaclass__ = PropertyRegisterMeta


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
        self.registry = Class._txorm_property_registry

    def test_get_subclass(self):
        prop1 = self.registry.get('SubClass.prop1')
        prop2 = self.registry.get('SubClass.prop2')
        self.assertTrue(prop1 is self.SubClass.prop1)
        self.assertTrue(prop2 is self.SubClass.prop2)
