
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
            self.somedata.modified.listeners[0],
            (self.display.update_somedata, (), {})
        )

    def test_signal_connects_with_arguments(self):
        self.somedata.modified.connect(self.display.update_somedata, 1, 2)
        self.assertEqual(len(self.somedata.modified.listeners), 1)
        self.assertEqual(
            self.somedata.modified.listeners[0],
            (self.display.update_somedata, (1, 2), {})
        )

    def test_signal_connects_with_keyword_arguments(self):
        self.somedata.modified.connect(self.display.update_somedata, one=2)
        self.assertEqual(len(self.somedata.modified.listeners), 1)
        self.assertEqual(
            self.somedata.modified.listeners[0],
            (self.display.update_somedata, (), {'one': 2})
        )

    def test_signal_connects_with_arguments_and_keyword_arguments(self):
        self.somedata.modified.connect(self.display.update_somedata, 1, one=2)
        self.assertEqual(len(self.somedata.modified.listeners), 1)
        self.assertEqual(
            self.somedata.modified.listeners[0],
            (self.display.update_somedata, (1,), {'one': 2})
        )

    def test_signal_disconnects(self):
        self.somedata.modified.connect(self.display.update_somedata)
        self.assertEqual(len(self.somedata.modified.listeners), 1)
        self.somedata.modified.disconnect(self.display.update_somedata)
        self.assertEqual(len(self.somedata.modified.listeners), 0)

    def test_signal_disconnects_with_arguments(self):
        self.somedata.modified.connect(self.display.update_somedata, 1, 2)
        self.assertEqual(len(self.somedata.modified.listeners), 1)
        self.somedata.modified.disconnect(self.display.update_somedata, 1, 2)
        self.assertEqual(len(self.somedata.modified.listeners), 0)

    def test_signal_disconnects_with_keyword_arguments(self):
        self.somedata.modified.connect(self.display.update_somedata, one=2)
        self.assertEqual(len(self.somedata.modified.listeners), 1)
        self.somedata.modified.disconnect(self.display.update_somedata, one=2)
        self.assertEqual(len(self.somedata.modified.listeners), 0)

    def test_signal_disconnects_with_arguments_and_keyword_arguments(self):
        self.somedata.modified.connect(self.display.update_somedata, 1, one=2)
        self.assertEqual(len(self.somedata.modified.listeners), 1)
        self.somedata.modified.disconnect(self.display.update_somedata, 1, one=2)  # noqa
        self.assertEqual(len(self.somedata.modified.listeners), 0)

    def test_signal_fires(self):
        self.somedata.modified.connect(self.display.update_somedata)
        self.somedata.set_some_data('firing', True)
        self.assertEqual(self.display.data, {'firing': True})

    def test_signal_fires_with_arguments(self):
        self.somedata.modified.connect(self.display.update_with_args, 'one', 2)
        self.somedata.set_some_data('firing', True)
        self.assertEqual(self.display.data, {'one': 2, 'firing': True})

    def test_signal_fires_with_keyword_arguments(self):
        self.somedata.modified.connect(self.display.update_with_kwargs, one=2)
        self.somedata.set_some_data(None, None, firing=True)
        self.assertEqual(self.display.data, {'one': 2, 'firing': True})

    def test_signal_fires_with_arguments_and_keyword_arguments(self):
        self.somedata.modified.connect(
            self.display.update_with_args_and_kwargs, 'one', 2, three=4)
        self.somedata.set_some_data('firing', True)
        self.assertEqual(
            self.display.data, {'one': 2, 'three': 4, 'firing': True}
        )

    def test_disconnect_by_returning_false(self):
        called = []

        def callback(*args):
            called.append(args)
            return len(called) < 2

        self.somedata.modified.connect(callback)
        self.somedata.set_some_data(1, 2)
        self.somedata.set_some_data(3, 4)
        self.somedata.set_some_data(5, 6)
        self.somedata.set_some_data(7, 8)

        self.assertEqual(called, [(1, 2), (3, 4)])

    def test_weak_reference(self):
        pass


class SomeData:
    def __init__(self):
        self.data = {}
        self.modified = Signal(self)

    @signal('modified')
    def set_some_data(self, foo, bar, **kwargs):
        self.data[foo] = bar


class DisplaySomeData:

    def __init__(self):
        self.data = None

    def update_somedata(self, *args, **kwargs):
        self.data = {args[0]: args[1]}

    def update_with_args(self, *args, **kwargs):
        self.data = {args[0]: args[1], args[2]: args[3]}

    def update_with_kwargs(self, *args, **kwargs):
        self.data = kwargs

    def update_with_args_and_kwargs(self, *args, **kwargs):
        self.data = {args[0]: args[1], args[2]: args[3]}
        self.data.update(kwargs)
