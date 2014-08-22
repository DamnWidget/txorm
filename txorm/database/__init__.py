# -*- test-case-name: txorm.test.test_database -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from contextlib import contextmanager

from .database import Database
from .connection import Connection


@contextmanager
def transaction(pool):
    """Context that creates a new transaction context

    :param pool: the adbapi connection pool
    :type pool: :class:`twisted.entreprise.dbapi.ConnectionPool`
    """

    conn = pool.connectionFactory(pool)
    txn = pool.transactionFactory(pool, conn)

    try:
        yield txn
    except:
        conn.rollback()
    else:
        txn.close()
        conn.commit()


__all__ = ['Database', 'Connection', 'transaction']
