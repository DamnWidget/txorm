# -*- test-case-name: txorm.test.test_uri -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from txorm.compat import urlparse
from txorm.exceptions import URIError


class URI(object):
    """Parse database uris
    """

    username = None
    password = None
    host = None
    port = None
    database = None

    def __init__(self, uri):
        self.__uri = uri
        try:
            self.scheme, _ = uri.split(':', 1)
        except ValueError:
            raise URIError('URI has no scheme: {}'.format(uri))

        self.options = {}
        try:
            _, query = urlparse.splitquery(uri)
            self.options.update(urlparse.parse_qs(query))
            for k, v in self.options.items():
                self.options[k] = v[0]
        except AttributeError:
            pass

        rest = urlparse.urlparse(uri)
        self.host, self.port, self.username, self.password, self.database = (
            self._escape(rest)
        )
        if (self.database is not None and self.database.count('/') == 1
                and self.database.startswith('/')):
            self.database = self.database.split('/', 1)[1]

    def copy(self):
        """Return a copy of the object
        """

        uri = object.__new__(self.__class__)
        uri.__dict__.update(self.__dict__)
        uri.options = self.options.copy()
        return uri

    def _escape(self, r):
        """Escape strings
        """

        strings = (r.hostname, r.port, r.username, r.password, r.path or None)

        def __inner_escape(s):
            if s is None or str(s) == '' or "%" not in str(s):
                return s

            i = 0
            j = s.find("%")
            r = []
            while j != -1:
                r.append(s[i:j])
                i = j+3
                r.append(chr(int(s[j+1:i], 16)))
                j = s.find("%", i)

            r.append(s[i:])
            return "".join(r)

        return [__inner_escape(s) for s in strings]

    def __str__(self):
        """Return a string representation of the object
        """

        return self.__uri
