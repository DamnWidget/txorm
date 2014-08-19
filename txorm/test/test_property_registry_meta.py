
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM PropertyRegistryMeta Unit Tests
"""

from txorm.compat import _PY3


if _PY3:
    from txorm.test import _meta_python3 as test
else:
    from txorm.test import _meta_python2 as test


class BaseClassMetaTest(test.BaseClassMetaTest):
    pass
