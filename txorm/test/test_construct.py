
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM constuct helper test
"""

from twisted.trial import unittest

from txorm import Undef
from txorm.utils.construct import parse_args


class ConstructTest(unittest.TestCase):

    def test_construct_default(self):
        obj = object()
        dummy = Dummy(obj)
        self.assertEqual(dummy.dummy, obj)
        self.assertIs(dummy.foo, Undef)
        self.assertIs(dummy.bar, Undef)

    def test_construct_with_arguments(self):
        obj = object()
        dummy = Dummy(obj, 1)
        self.assertEqual(dummy.dummy, obj)
        self.assertEqual(dummy.foo, 1)
        self.assertIs(dummy.bar, Undef)

        dummy = Dummy(obj, 1, 2)
        self.assertEqual(dummy.dummy, obj)
        self.assertEqual(dummy.foo, 1)
        self.assertIs(dummy.bar, 2)

    def test_construct_with_keyword_arguments(self):
        obj = object()
        dummy = Dummy(obj, bar=True)
        self.assertEqual(dummy.dummy, obj)
        self.assertEqual(dummy.foo, Undef)
        self.assertEqual(dummy.bar, True)

    def test_construct_with_arguments_and_keyword_arguments(self):
        obj = object()
        dummy = Dummy(obj, 'foo', bar=True)
        self.assertEqual(dummy.dummy, obj)
        self.assertEqual(dummy.foo, 'foo')
        self.assertEqual(dummy.bar, True)

    def test_construct_with_arguments_non_skip(self):
        obj = object()
        dummy = Dummy(obj, 'foo', skip_first_slot=False)
        self.assertEqual(dummy.dummy, 'foo')  # obj get overriden
        self.assertTrue(not hasattr(dummy, 'skip_first_slot'))
        self.assertIs(dummy.foo, Undef)
        self.assertIs(dummy.bar, Undef)

    def test_construct_with_key_arguments_non_skip_has_no_effect(self):
        obj = object()
        dummy = Dummy(obj, skip_first_slot=False, foo='bar', bar='foo')
        self.assertEqual(dummy.dummy, obj)
        self.assertTrue(not hasattr(dummy, 'skip_first_slot'))
        self.assertIs(dummy.foo, 'bar')
        self.assertIs(dummy.bar, 'foo')

    def test_key_arguments_non_override_arguments(self):
        obj = object()
        dummy = Dummy(obj, True, foo=False, bar=10)
        self.assertEqual(dummy.dummy, obj)
        self.assertEqual(dummy.foo, True)
        self.assertEqual(dummy.bar, 10)


class Dummy(object):
    """Dummy class for testing purposes
    """

    __slots__ = ('dummy', 'foo', 'bar')

    def __init__(self, dummy, *args, **kwargs):
        self.dummy = dummy
        parse_args(self, *args, **kwargs)
