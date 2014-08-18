# -*- test-case-name: txorm.test.test_signal -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""Simple signaling system
"""

import functools


class Signal(object):
    """Objects that inherits from this class can be connected and fired

    How to use:

        class SomeData:
            def __init__(self, owner):
                self.data = {}
                self.modified = Signal(owner)

            @signal('modified')
            def set_some_data(self, foo, bar):
                self.data[foo] = bar

        display = SomeDataDisplay()
        some_data.modified.connect(display.update_somedata)
    """

    def __init__(self):
        self.listeners = []

    def connect(self, listener):
        """Connect a new listener

        :param listener: the listener to connect
        """

        if listener not in self.listeners:
            self.listeners.append(listener)

    def disconnect(self, listener):
        """Disconnects a listener

        :param listener: the listener to disconnect
        """

        if listener in self.listeners:
            self.listeners.remove(listener)

    def fire(self, *args, **kwargs):
        """Fire up the signal
        """

        [listener(*args, **kwargs) for listener in self.listeners]

    def fire_signal(self, signal):
        """Decorate `func` to fire signals when ready

        :param signal: the name of the signal to be fired
        :type signal: str
        """

        def decorator(func):
            """Internal decorator so we can handle arguments
            """

            @functools.wraps(func)
            def wrapper(obj, *args, **kwargs):
                result = func(obj, *args, **kwargs)
                getattr(obj, signal).fire(obj)
                return result

            return wrapper

        return decorator

signal = Signal().fire_signal


__all__ = ['Signal', 'signal']
