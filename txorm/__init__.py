# -*- test-case-name: txorm.test.test_txorm -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for more details

"""
TxORM is an implementation of the Active Record pattern using Twisted
For more details take a loof at the documentation at http://txorm.pymamba.com

.. note:: TxORM is hardly inspired in the fantastic canonical Storm ORM
"""

from __future__ import unicode_literals

import sys

if not hasattr(sys, "version_info") or sys.version_info < (2, 6):
    raise RuntimeError("TxORM requires Python 2.6 or later.")

del sys

# setup version
from _version import version
__version__ = version.short()


class UndefBaseType(object):
    """Undef is an instance of this class and there is only one Undef instance
    """

    def __repr__(self):
        """Representaton of the `Undef` type
        """
        return 'Undef'

    def __reduce__(self):
        """This is called when we try to pickle an `Undef` object
        """
        return 'Undef'

# Instance of Undef
Undef = UndefBaseType()
