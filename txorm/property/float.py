# -*- test-case-name: txorm.test.test_property -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from .base import SimpleProperty
from ..variable.float import FloatVariable


class Float(SimpleProperty):
    variable_class = FloatVariable
