# -*- test-case-name: txorm.test.test_property -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from ..property import PropertyRegisterMeta


class TxORM(object, metaclass=PropertyRegisterMeta):
    """Causes subclasses to be associated with a TxORM PropertyRegister

    This is necessary to be able to spcify References with strings
    """
