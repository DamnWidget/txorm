# -*- test-case-name: txorm.test.test_database -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

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

    def raw_connect(self):
        """Create a raw database connection

        This is used by :class:`txorm.database.connection.Connection`
        objects to connect to the database. It should be overriden in
        subclasses to do any database-specific setup
        """
        raise NotImplementedError


_database_schemes = {}


def create_database(uri):
    """Create a database instance.

    :param uri: the uri to get connection parameters from
    :type uri: string
    """

    if is_basestring(uri):
        uri = URI(uri)

    if uri.scheme in _database_schemes:
        factory = _database_schemes[uri.scheme]
    else:
        module = __import__(
            '{}.databases.{}'.format(txorm.__name__, uri.scheme),
            None, None, ['']
        )
        factory = module.create_from_uri

    return factory(uri)
