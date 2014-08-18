
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""TxORM Signals Unit Tests
"""

from twisted.trial import unittest

from txorm.signal import signal, Signal


class SignalTest(unittest.TestCase):

    def setUp(self):
        self.display = DisplaySomeData()
        self.somedata = SomeData()

    def test_signal_connects(self):
        self.somedata.modified.connect(self.display.update_somedata)
        self.assertEqual(len(self.somedata.modified.listeners), 1)
        self.assertEqual(
            self.somedata.modified.listeners[0], self.display.update_somedata)

    def test_signal_disconnects(self):
        self.somedata.modified.connect(self.display.update_somedata)
        self.assertEqual(len(self.somedata.modified.listeners), 1)
        self.somedata.modified.disconnect(self.display.update_somedata)
        self.assertEqual(len(self.somedata.modified.listeners), 0)

    def test_signal_fires(self):
        self.somedata.modified.connect(self.display.update_somedata)
        self.somedata.set_some_data('firing', True)
        self.assertEqual(self.display.data, {'firing': True})


class SomeData:
    def __init__(self):
        self.data = {}
        self.modified = Signal()

    @signal('modified')
    def set_some_data(self, foo, bar):
        self.data[foo] = bar


class DisplaySomeData:

    def __init__(self):
        self.data = None

    def update_somedata(self, some_data):
        self.data = some_data.data
