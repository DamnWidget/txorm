
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

import re
import sys
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

# urlparse prior to Python 2.7.6 have a bug in parsing the port, fix it
if sys.version_info < (2, 7, 6):
    def port(self):
        netloc = self.netloc.split('@')[-1].split(']')[-1]
        if ':' in netloc:
            port = netloc.split(':')[1]
            if port:
                port = int(port, 10)
                # verify legal port
                if (0 <= port <= 65535):
                    return port
        return None

    urlparse.ResultMixin.port = property(port)


__all__ = ['pickle', 'StringIO', 'urlparse']
