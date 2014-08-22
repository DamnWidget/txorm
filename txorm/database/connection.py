# -*- test-case-name: txorm.test.test_database -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from twisted.python import log
from twisted.internet import defer, threads

from txorm.compiler.state import State
from txorm.signal import signal, Signal
from txorm.database.result import Result
from txorm.compiler import txorm_compile
from txorm.compiler.expressions import Expression


class Connection(object):
    """A connection pool to a database
    """

    result_factory = Result
    compile = txorm_compile

    def __init__(self, database):
        self._database = database
        self._raw_connection = self._database._raw_connect()
        self.register_transaction = Signal(self)

    @defer.inlineCallbacks
    def execute(self, statement, params=None, noresult=False, **kwargs):
        """
        Execute a statement with the given parameters and return a defer
        object which will fire the return value or a Failure.

        .. note::

            This method will automatically commit, so don't use it if
            you need transactional behavior, in that case, use
            :method:`Connection.execute_transact`

        :param statement: the statement or expression to execute
        :type statement: :class:`Expression` or string
        :param params: the params to fill the satement query with
        :type params: list
        :param noresult: if True, just for and forget
        :type noresult: boolean
        """

        if isinstance(statement, Expression):
            if params is not None:
                raise ValueError('Can\'t pass parameters with expressions')
            state = State()
            statement = self.compile(statement, state)
            if noresult is False:
                result = yield self._execute(statement, *params, **kwargs)
                defer.returnValue(self.result_factory(result))
            else:
                self._raw_connection.runOperation(
                    *self._execution_args(params, statement), **kwargs)

    @defer.inlineCallbacks
    def execute_transact(self, transact_chain, *args, **kwargs):
        """
        Execute a transaction calling the transact_chain that defines
        what operations are going to be performed into the database.

        This method commits at the end or rollback if there is some
        failure.

        If you call this from user code, you have to make sure to make:

            state = State()
            txorm_compile(statement, state)

        in some moment to compile your expressions

        :param transact_chain: a callable object whose first argument
            is a :class:`twisted.adbapi.Transaction`
        :type transact_chain: callable
        :param args: additional positional arguments to be passed to
            the transaction
        :type args: list
        :param kwargs: keyword arguments to be passed to the interaction
        :type kwargs: dict
        :return: a Deferred which will fire the return value of
            `transact_chain(Transaction(...), *args, **kwargs)` or
            Failure
        """

        return self._raw_connection.rinInteraction(
            transact_chain, *args, **kwargs
        )

    def _execute(self, statement, *params, **kwargs):
        """Execute raw statement using twisted adbapi
        """

        args = self._execution_args(params, statement)
        return self._raw_connection.runQuery(*args, **kwargs)

    def _execution_args(self, params, statement):
        """Get the appropiate statement execution arguments
        """

        if params:
            args = (statement, tuple(self.to_database(params)))
        else:
            args = (statement, )

        return args
