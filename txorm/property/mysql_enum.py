# -*- test-case-name: txorm.test.test_property -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals
from .base import SimpleProperty
from ..variable.mysql_enum import MysqlEnumVariable


class MysqlEnum(SimpleProperty):
    """
    Enumeration property, allowing the use of native enum types in MySQL
    and PostgreSQL.

    For instance::

        class Class(Storm):
            prop = NativeEnum(set={'one', 'two'})

        obj.prop = "one"
        assert obj.prop == "one"

        obj.prop = 1 # Raises error.
    """

    variable_class = MysqlEnumVariable

    def __init__(self, name=None, primary=False, **kwargs):
        _set = set(kwargs.pop('set'))
        kwargs['_set'] = _set

        super(MysqlEnum, self).__init__(name, primary, **kwargs)
