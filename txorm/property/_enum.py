# -*- test-case-name: txorm.test.test_property -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from .base import SimpleProperty
from ..variable._enum import EnumVariable


class Enum(SimpleProperty):
    """Enumration property, allowing used values to differ from stored ones

    Example::

        class Class(object):
            prop = Enum(map={'one': 1, 'two': 2})

        obj = Class()
        obj.prop = 'one'
        assert obj.prop == 'one'

        obj.prop = 1  # raises error

    Another example::

        class Class(object):
            prop = Enum(map={'one': 1, 'two': 2}, set_map={'um': 1})

        obj = Class()
        obj.prop = 'um'
        assert obj.prop is 'one'

        obj.prop = 'one'  # raises an error
    """
    variable_class = EnumVariable

    def __init__(self, name=None, primary=False, **kwargs):
        set_map = dict(kwargs.pop('map'))
        get_map = dict((value, key) for key, value in set_map.items())
        if 'set_map' in kwargs:
            set_map = dict(kwargs.pop('set_map'))

        kwargs['get_map'] = get_map
        kwargs['set_map'] = set_map
        SimpleProperty.__init__(self, name, primary, **kwargs)
