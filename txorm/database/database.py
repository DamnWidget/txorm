# -*- test-case-name: txorm.test.test_database -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

import txorm
from txorm.utils.uri import URI
from txorm.variable import Variable
from txorm.compat import is_basestring
from txorm.compiler.state import State
from txorm.signal import signal, Signal
from txorm.database.connection import Connection
from txorm.compiler import Expression, txorm_compile


class Database(object):
    """A database that can be connected to.
    """

    connection_factory = Connection

    def __init__(self):
        self.connected = Signal(self)

    @signal('connected')
    def connect(self):
        """Create a connection to the database using a factory
        """

        return self.connection_factory(self)


def create_database(uri):
    """Create a database instance.

    :param uri: the uri to get connection parameters from
    :type uri: string
    """

    if is_basestring(uri):
        uri = URI(uri)

    if uri.scheme not in _database_schemes: