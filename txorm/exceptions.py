
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


class ObjectDataError(TxormError):
    """Raised when errors on object data are detected
    """


class ClassDataError(TxormError):
    """Raised when errors on class info are detected
    """


class PropertyPathError(TxormError):
    """Raised when errors on PropertyRegistry paths resolution are found
    """


class URIError(TxormError):
    """Raised when errors on URI parsing are found
    """
