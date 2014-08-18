
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

import re
import urlparse

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    import cStringIO as StringIO
except ImportError:
    from StringIO import StringIO


_queryprog = None


def __splitquery(url):
    """splitquery('/path?query') --> '/path', 'query'."""
    global _queryprog
    if _queryprog is None:
        _queryprog = re.compile('^(.*)\?([^?]*)$')

    match = _queryprog.match(url)
    if match:
        return match.group(1, 2)
    return url, None

urlparse.splitquery = __splitquery


__all__ = ['pickle', 'StringIO', 'urlparse']
