# -*- test-case-name: txorm.test.test_signal -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

"""Simple signaling system
"""

import weakref
import functools


class Signal(object):
    """Objects that inherits from this class can be connected and fired

    How to use:

        class SomeData:
            def __init__(self):
                self.data = {}
                self.modified = Signal(self)

            @signal('modified')
            def set_some_data(self, foo, bar):
                self.data[foo] = bar

        some_data = SomeData()
        display = SomeDataDisplay()
        some_data.modified.connect(display.update_somedata)
    """

    def __init__(self, owner):
        self._owner_ref = weakref.ref(owner)
        self.listeners = []

    @property
    def owner(self):
        """Return back the Signal owner (not a ref to the signal owner)
        """

        return self._owner_ref()

    def connect(self, listener, *args, **kwargs):
        """Connect a new listener

        :param listener: the listener to connect
        """

        if (listener, args, kwargs) not in self.listeners:
            self.listeners.append((listener, args, kwargs))

    def disconnect(self, listener, *args, **kwargs):
        """Disconnects a listener

        :param listener: the listener to disconnect
        """

        if (listener, args, kwargs) in self.listeners:
            self.listeners.remove((listener, args, kwargs))

    def fire(self, *args, **kwargs):
        """Fire up the signal
        """

        owner = self._owner_ref()
        if owner is not None:
            for listener in self.listeners:
                callback, data, kwdata = listener
                kwdata.update(kwargs)
                result = callback(*(data+args), **kwdata)
                if result is False:
                    self.disconnect(callback, *data, **kwdata)

    @classmethod
    def fire_signal(cls, signal):
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
                getattr(obj, signal).fire(*args, **kwargs)
                return result

            return wrapper

        return decorator

signal = Signal.fire_signal


__all__ = ['Signal', 'signal']
