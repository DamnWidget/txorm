
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals


class TxormError(Exception):
    """Base class for all the rest of errors
    """


class CopileError(TxormError):
    """Raised on compile errors
    """


class NoneError(TxormError):
    """Raised when None is not allowed for a variable value
    """
