
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Variable
"""

from .int import IntVariable
from .bool import BoolVariable
from .float import FloatVariable
from .raw_str import RawStrVariable
from .unicode import UnicodeVariable
from ._decimal import DecimalVariable
from ._datetime import DateTimeVariable
from ._fraction import FractionVariable

from txorm.compat import _PYPY
from txorm import c_extensions_available

if not _PYPY and c_extensions_available:
    try:
        from _variable import Variable
    except ImportError:
        from .base import Variable
else:
    from .base import Variable


__all__ = [
    'Variable', 'IntVariable', 'BoolVariable', 'FloatVariable',
    'RawStrVariable', 'DecimalVariable', 'UnicodeVariable', 'FractionVariable',
    'DateTimeVariable'
]
