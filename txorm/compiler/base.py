# -*- test-case-name: txorm.test.test_expressions -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from weakref import WeakKeyDictionary

from txorm.exceptions import CompileError
from txorm.compat import binary_type, text_type

from .state import State
from .plain_sql import SQLRaw, SQLToken

MAX_PRECEDENCE = 1000


def _when(self, types):
    """Check Compile.when.

    That's defined here to make easier he C Implementation of the Compile
    object.

    .. note::

        We could use here the new singledispatch decorator and it's backport
        from Python 3.4.0 but we need more than just dispatch generic functions
        from this decorator. For example we have to setup the precedence of
        the operations so at the end of the day we need some custom way to
        hook into he execution chain
    """

    def wrapper(func):
        for t in types:
            self._local_dispatch_table[t] = func
        self._update_cache()

        return func

    return wrapper


class Context(object):
    """
    An object used to specify the nature of expected SQL expressions
    being compiled in a given context.
    """

    def __init__(self, name):
        self._name = name

    def __repr__(self):
        return '{}({!r})'.format(self.__class__.__name__, self._name)


class Compile(object):
    """Compiler based on the comcept of generic functions implemented by Storm
    """

    def __init__(self, parent=None):
        self._local_dispatch_table = {}
        self._local_precedence = {}
        self._local_reserved_words = {}
        self._dispatch_table = {}
        self._precedence = {}
        self._reserved_words = {}
        self._children = WeakKeyDictionary()
        self._parents = []

        if parent is not None:
            self._parents.extend(parent._parents)
            self._parents.append(parent)
            parent._children[self] = True
            self._update_cache()

    def __call__(self, expression, state=None,
                 join=', ', raw=False, token=False):
        """Compile the given expression into a SQL statement

        :param expression: the expression to compile
        :param state: an instance of State or None in which case it's created
                      internally (and thus can't be accessed)
        :param join: the string token to put between subexpressions.
        :param raw: if True, any string or unicode expression or subexpression
                    will not be further compiled
        :param token: if True, any string or unicode expression will be
                      considerd as a SQLToken, and quoted properly
        """

        expression_type = type(expression)

        if (expression_type is SQLRaw or raw
                and (expression_type in (binary_type, text_type))):
            return expression

        if token and (expression_type in (binary_type, text_type)):
            expression = SQLToken(expression)

        if state is None:
            state = State()

        outer_precedence = state.precedence
        if expression_type in (tuple, list):
            compiled = []
            for subexpression in expression:
                subexpression_type = type(subexpression)
                if subexpression_type is SQLRaw or raw and (
                        subexpression_type in (binary_type, text_type)):
                    statement = subexpression
                elif subexpression_type in (tuple, list):
                    state.precedence = outer_precedence
                    statement = self(subexpression, state, join, raw, token)
                else:
                    if token and (
                            subexpression_type in (binary_type, text_type)):
                        subexpression = SQLToken(subexpression)

                    statement = self._compile_single(
                        subexpression, state, outer_precedence
                    )

                compiled.append(statement)

            statement = join.join((text_type(v) for v in compiled))
        else:
            statement = self._compile_single(
                expression, state, outer_precedence
            )

        state.precedence = outer_precedence
        return statement

    def when(self, *types):
        """Decorator to include a type handler in this compiler

        :param types: the types to compile

        Example of usage:

        .. sourcecode:: python

            @compile.when(TypeA, TypeB)
            def compile_type_a_or_b(compie, expr, state):
                ...
                return 'THE COMPILED SQL STATEMENT'
        """

        return _when(self, types)

    def add_reserved_words(self, words):
        """Include words to be considered reserved and thus scaped.

        Reserved words are escaped during compilation when they're
        seen in a SQLToken expression

        :params words: a list of words to add
        """

        self._local_reserved_words.update(
            (word.lower(), True) for word in words
        )
        self._update_cache()

    def remove_reserved_words(self, words):
        """Remove words from the reserved words cache

        :param words: a list of words to remove
        """

        self._local_reserved_words.update(
            (word.lower(), None) for word in words
        )
        self._update_cache()

    def is_reserved_word(self, word):
        """Determine if a word is in the reserved words cache

        :param word: thw word to check
        """

        return self._reserved_words.get(word.lower()) is not None

    def create_child(self):
        """Create a new instance of :class:`Compile` which inherits from this

        This is most commonly used to customize a compiler for database
        specific compilation strategies
        """

        return self.__class__(self)

    def get_precedence(self, expression_type):
        """Get the operators precedence of the given type

        :param expression_type: the expression type to check precedence for
        """

        return self._precedence.get(expression_type, MAX_PRECEDENCE)

    def set_precedence(self, precedence, *expression_types):
        """Set the precedence of one or more expression types

        :param precedence: the precedence to set
        :expression_types: a list of expression types
        """

        for expression_type in expression_types:
            self._local_precedence[expression_type] = precedence

        self._update_cache()

    def _compile_single(self, expression, state, outer_precedence):
        """Compile a single expression
        """

        cls = expression.__class__
        dispatch_table = self._dispatch_table
        if cls in dispatch_table:
            handler = dispatch_table[cls]
        else:
            for mro in cls.__mro__:
                # first interaction always fails because we've already tested
                # that the class itself isn't in the dispatch table
                if mro in dispatch_table:
                    handler = dispatch_table[mro]
                    break
            else:
                raise CompileError(
                    'Don\'t know how to compile type {!r} of {!r}'.format(
                        expression.__class__, expression
                    )
                )

        inner_precedence = state.precedence = self._precedence.get(
            cls, MAX_PRECEDENCE
        )
        statement = handler(self, expression, state)
        if inner_precedence < outer_precedence:
            return '({})'.format(statement)

        return statement

    def _update_cache(self):
        """Update internal compile cache
        """

        for parent in self._parents:
            self._dispatch_table.update(parent._local_dispatch_table)
            self._precedence.update(parent._local_precedence)
            self._reserved_words.update(parent._local_reserved_words)

        self._dispatch_table.update(self._local_dispatch_table)
        self._precedence.update(self._local_precedence)
        self._reserved_words.update(self._local_reserved_words)

        for child in self._children:
            child._update_cache()


class CompilePython(Compile):

    def get_matcher(self, expr):
        state = State()
        source = self(expr, state)
        namespace = {}
        lparams = len(state.parameters)
        code = (
            'def closure(parameters, bool):\n'
            '   [{}] = parameters\n'
            '   def match(get_field):\n'
            '       return bool({})\n'
            '   return match'.format(
                ','.join('_{}'.format(i) for i in range(lparams)), source)
        )

        exec(code, namespace)
        return namespace['closure'](state.parameters, bool)

txorm_compile = Compile()
txorm_compile_python = CompilePython()
