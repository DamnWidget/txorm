
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Variable
"""

from .int import IntVariable
from .bool import BoolVariable
from ._date import DateVariable
from ._uuid import UUIDVariable
from ._time import TimeVariable
from ._enum import EnumVariable
from .float import FloatVariable
from .raw_str import RawStrVariable
from .unicode import UnicodeVariable
from ._decimal import DecimalVariable
from ._datetime import DateTimeVariable
from ._fraction import FractionVariable
from .timedelta import TimeDeltaVariable
from .mysql_enum import MysqlEnumVariable

from txorm.compat import _PYPY
from txorm import c_extensions_available

if not _PYPY and c_extensions_available:
    try:
        from txorm._variable import Variable
    except ImportError:
        from .base import Variable
else:
    from .base import Variable


__all__ = [
    'Variable', 'IntVariable', 'BoolVariable', 'FloatVariable',
    'RawStrVariable', 'DecimalVariable', 'UnicodeVariable', 'FractionVariable',
    'DateTimeVariable', 'DateVariable', 'TimeVariable', 'TimeDeltaVariable',
    'UUIDVariable', 'MysqlEnumVariable', 'EnumVariable'
]
