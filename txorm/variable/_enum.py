# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from .base import Variable


class EnumVariable(Variable):
    """Representation of an agnostic database enum that works as a map
    """

    __slots__ = ('_get_map', '_set_map')

    def __init__(self, get_map, set_map, *args, **kwargs):
        self._get_map = get_map
        self._set_map = set_map
        super(EnumVariable, self).__init__(*args, **kwargs)

    def parse_set(self, value, from_db):
        if from_db is True:
            return value

        value_ = self._set_map.get(value)
        if value_ is None:
            raise ValueError('Invalid enum value: {}'.format(repr(value)))

        return value_

    def parse_get(self, value, to_db):
        if to_db is True:
            return value

        value_ = self._get_map.get(value)
        if value_ is None:
            raise ValueError('Invalid enum value: {}'.format(repr(value)))

        return value_
