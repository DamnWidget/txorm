# -*- test-case-name: txorm.test.test_expressions -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from .expressions import SuffixExpression


class Asc(SuffixExpression):
    """Expression representing an 'ASC' suffix
    """
    __slots__ = ()
    suffix = 'ASC'


class Desc(SuffixExpression):
    """Expression representing an 'DESC' suffix
    """
    __slots__ = ()
    suffix = 'DESC'
