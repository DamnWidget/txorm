
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Property Unit Tests
"""

from twisted.trial import unittest

from txorm.compiler import Field
from txorm.variable import Variable
from txorm.property.base import SimpleProperty


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
