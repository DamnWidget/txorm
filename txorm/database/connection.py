# -*- test-case-name: txorm.test.test_database -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from twisted.python import log
from twisted.internet import defer

from txorm.compiler.state import State
from txorm.signal import signal, Signal
from txorm.database.result import Result
from txorm.compiler import txorm_compile
from txorm.compiler.expressions import Expression


class ConnectionPool(object):
    """A connection pool to a database
    """

    result_factory = Result
    compile = txorm_compile

    def __init__(self, database):
        self._database = database
        self._raw_connection = self._database._raw_connect()
        self.register_transaction = Signal(self)

    @defer.inlineCallbacks
    @signal('register_transaction')
    def execute(self, statement, params=None, noresult=False, **kwargs):
        """
        Execute a statement with the given parameters and return a defer
        object which will fire the return value or a Failure

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
            result = yield self._raw_connection.runQuery()
            statement = self.compile(statement, state)
            if noresult is False:
                result = yield self._execute(statement, *params, **kwargs)
                defer.returnValue(self.result_factory(result))
            else:
                self._raw_connection.runOperation(statement, *params, **kwargs)
