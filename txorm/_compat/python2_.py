
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    import cStringIO as StringIO
except ImportError:
    from StringIO import StringIO


__all__ = ['pickle', 'StringIO']
