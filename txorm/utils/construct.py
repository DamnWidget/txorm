# -*- test-case-name: txorm.test.test_construct -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""Construtor convenience helpers
"""

from txorm import Undef


def parse_args(obj, *args, **kwargs):
    """Dynamically parse arguments for the given object and set defaults
    """

    skip_first_slot = kwargs.pop('skip_first_slot', True)
    for index, value in enumerate(args):
        if skip_first_slot is True:
            setattr(obj, obj.__slots__[index+1], value)
            continue

        setattr(obj, obj.__slots__[index], value)

    for field in obj.__slots__:
        if not hasattr(obj, field):  # has not been set already
            setattr(obj, field, kwargs.get(field, Undef))


__all__ = ['parse_args']
