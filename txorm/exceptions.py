
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals


class TxormError(Exception):
    """Base class for all the rest of errors
    """


class CompileError(TxormError):
    """Raised on compile exceptions
    """


class NoTableError(CompileError):
    """Raised when no table is avaibale on compile time
    """


class NoneError(TxormError):
    """Raised when None is not allowed for a variable value
    """


class FieldInfoError(TxormError):
    """Raised when errors on fields data are detected
    """


class ClassDataError(TxormError):
    """Raised when errors on class info are detected
    """
