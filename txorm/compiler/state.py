# -*- test-case-name: txorm.test.test_expressions -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from copy import copy

from txorm import Undef


class State(object):
    """All the data necessary duting compilation of an expression

    The `aliases` parameter specify how fields should be compiled as aliases in
    very specific situations. This is typically used to work around strange
    deficiencies in various database backends.

    The `auto_tables` parameter is a list of all the implicitly used tables,
    for example in the following expression:

    .. sourcecode:: python

        store.find(Foo, Foo.attr==Bar.id)

    the tables of `Bar` and `Foo` are implicitly used because fields in them
    are referenced. This is used when building tables.

    When `join_tables` is not None, when :class:`compiler.expressions.Join`
    expressions are compiled, tables seen will be added to this set. This
    acts as a blacklist againts `auto_tables` when compiling joins because
    the generated statement should not refer to the table twice.

    The `precedence` is restored by the compiler. If any inner precedence is
    lower than an outer precedence, parenthesis around the inner expression
    are automatically emitted.

    :var aliases: dict of :class:`compiler.expressions.Field` instances to
                    :class:`compiler.expressions.Alias` instances
    :param auto_tables: the list of all implicitly used tables
    :param join_tables: a set or None
    :param context: an instance of :class:`Context`, specifying the context of
        the expression currently being compiled
    :param precedence: current precedence
    """

    def __init__(self):
        self._stack = []
        self.precedence = 0
        self.parameters = []
        self.auto_tables = []
        self.join_tables = None
        self.context = None
        self.aliases = None

    def push(self, attr, new_value=Undef):
        """Set an attribite in a way that can later be reverted with `pop`
        """

        old_value = getattr(self, attr, None)
        self._stack.append((attr, old_value))
        if new_value is Undef:
            new_value = copy(old_value)

        setattr(self, attr, new_value)
        return old_value

    def pop(self):
        """Revert the topmost `push`
        """

        setattr(self, *self._stack.pop(-1))
