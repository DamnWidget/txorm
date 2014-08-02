
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

import sys

if sys.version_info < (3, 0):
    from .txorm_python2 import TxORM
else:
    from .txorm_python3 import TxORM

__all__ = ['TxORM']
