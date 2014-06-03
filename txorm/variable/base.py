# -*- test-case-name: txorm.test.test_variable -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from txorm import Undef
from txorm.compat import _PY3
from txorm.exceptions import NoneError

if _PY3 is True:
    buffer = memoryview


def raise_none_error(field):
    if not field:
        raise NoneError('None isn\'t acceptable as a value')
    else:
        from txorm.compiler import txorm_compile, CompileError
        name = field.name
        if field.table is not Undef:
            try:
                table = txorm_compile(field.table)
                name = '{}.{}'.format(table, name)
            except CompileError:
                pass
        raise NoneError("None isn't acceptable as a value for %s" % name)


class Variable(object):
    """Representation of a database value in Python.

    :param value: the initial value for this variable
    :param value_factory: a callable that is called to set the default value
    :type value_factory: callable
    :param allow_none: if `True` allow `None` as a valid value to this var
    :type allow_none: boolean
    :param field: the real field name in the database if mapping is needed
    :type field: string
    :param validator: callable object that will be used whenever we try to
        set the variable to a non-db value (that doesn't comes from the
        database). The function signature wil be as described:

        .. sourcecode:: python

            func(value)

        where the first and unique value is the value to validate, if the value
        is not acceptable an error should be raised, the value is returned
        otherwise
    """

    _value = Undef
    _allow_none = True
    _validator = None

    field = None

    def __init__(self, value=Undef, value_factory=Undef,
                 from_db=False, allow_none=True, field=None, validator=None):

        if allow_none is not True:
            self._allow_none = False

        if value is not Undef:
            self.set(value, from_db)
        elif value_factory is not Undef:
            self.set(value_factory(), from_db)

        if validator is not None:
            self._validator = validator

        self.field = field

    @property
    def is_defined(self):
        """Returns `True` if the internal value is defined, `False` otherwise
        """

        return True if self._value is not Undef else False

    def get(self, default=None, to_db=False):
        """Get the value of this variable.

        :param default: returned if no value has been set yet
        :param to_db: indicate if the value is destined to the database
        :type to_db: boolean
        """

        value = None
        if self._value is Undef:
            value = default
        elif self._value is not None:
            value = self.parse_get(self._value, to_db)

        return value

    def set(self, value, from_db=False):
        """Set a new value.

        This method is called when we manually set a value for this attribute
        or we load from the database.

        If the value comes from the database we do nothing but if it comes
        from the Python code and a validator is set, we call it.

        :param value: the value to set
        :param from_db: indicate if the value comes from the database
        :type from_db: boolean
        """

        if from_db is False and self._validator is not None:
            value = self._validator(value)

        if value is None:
            if self._allow_none is False:
                raise raise_none_error(self.field)
            new_value = None
        else:
            new_value = self.parse_set(value, from_db)

        self._value = new_value

    def delete(self):
        """Delete the internal value
        """

        if self._value is not Undef:
            self._value = Undef

    def parse_get(self, value, to_db):
        """Convert the internal value to an external value

        Take a representation of this value form Python or from the database

        :parma value: the value to be converted
        :param to_db: indicate if the value is destined to the database
        """

        return value

    def parse_set(self, value, from_db):
        """Convert an external value to an internal value

        A value is being set either from Python or from the database. Then is
        parsed into its internal representation.

        :param value: the value from Python or the database
        """

        return value
