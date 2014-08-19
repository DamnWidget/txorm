# -*- test-case-name: txorm.test.test_property -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

import sys
import weakref
from bisect import insort_left, bisect_left

from txorm.object_data import get_cls_data
from txorm.exceptions import PropertyPathError


class PropertyRegisterMeta(type):
    """A metaclass that associates TxORM with `PropertyRegistry`.
    """

    def __init__(cls, name, bases, dict):
        if not hasattr(cls, "_txorm_property_registry"):
            cls._txorm_property_registry = PropertyRegistry()
        elif (hasattr(cls, '__database_table__')
                or hasattr(cls, '__storm_table__')):  # Storm compatibility
            cls._txorm_property_registry.add_class(cls)


class PropertyRegistry(object):
    """
    This object remembers the TxORM properties specified on classes, and
    is able to translate names to these properties.
    """

    def __init__(self):
        self._properties = []

    def get(self, name, namespace=None):
        """Translate a property name path to the actual property (if possible)

        This method accepts a property name like `id` or `Class.id` or
        `module.path.Class.id`, and tries to find a unique class/property
        with the given name.

        When the `namespace` agument is passed, the registry wi be able to
        disambiguate names by choosing the one that is closer to the given
        namespace.
        """

        key = '{}.'.format('.'.join(reversed(name.split('.'))))
        i = bisect_left(self._properties, (key,))
        l = len(self._properties)
        best_props = []
        if namespace is None:
            while i < l and self._properties[i][0].startswith(key):
                path, prop_ref = self._properties[i]
                prop = prop_ref()
                if prop is not None:
                    best_props.append((path, prop))
                i += 1
        else:
            namespace_parts = ('.{}').format(namespace).split('.')
            best_path_info = (0, sys.maxint)
            while i < l and self._properties[i][0].startswith(key):
                path, prop_ref = self._properties[i]
                prop = prop_ref()
                if prop is None:
                    i += 1
                    continue

                path_parts = path.split('.')
                path_parts.reverse()
                common_prefix = 0
                for part, ns_part in zip(path_parts, namespace_parts):
                    if part == ns_part:
                        common_prefix += 1
                    else:
                        break

                path_info = (-common_prefix, len(path_parts)-common_prefix)
                if path_info < best_path_info:
                    best_path_info = path_info
                    best_props = [(path, prop)]
                elif path_info == best_path_info:
                    best_props.append((path, prop))

                i += 1

        if not best_props:
            raise PropertyPathError(
                'Path \'{}\' matches no known property.'.format(name))
        elif len(best_props) > 1:
            paths = ['.'.join(reversed(path.split('.')[:-1]))
                     for path, prop in best_props]
            raise PropertyPathError(
                'Path \'{}\' matches multiple properties: {}'.format(
                    name, ', '.join(paths)
                )
            )

        return best_props[0][1]

    def add_class(self, cls):
        """Register properties of `cls` so that they may be found by `get()`
        """

        # create a list that contains the path components of the class
        # and the class name in reversed order so package.module.Classs
        # should be stored as ['Class', 'module', 'package'] and then
        # is cast to the string 'Class.module.package'
        suffix = cls.__module__.split('.')
        suffix.append(cls.__name__)
        suffix.reverse()
        suffix = '.{}.'.format('.'.join(suffix))

        cls_data = get_cls_data(cls)
        for attr in cls_data.attributes:
            prop = cls_data.attributes[attr]
            prop_ref = weakref.KeyedRef(prop, self._remove, None)
            pair = ('{}{}'.format(attr, suffix), prop_ref)
            prop_ref.key = pair
            insort_left(self._properties, pair)

    def add_property(self, cls, prop, attr_name):
        """Registry a propert of the given class that may be found by `get()`
        """

        suffix = cls.__module__.split('.')
        suffix.append(cls.__name__)
        suffix.reverse()
        suffix = '.{}.'.format('.'.join(suffix))
        prop_ref = weakref.KeyedRef(prop, self._remove, None)
        pair = ('{}{}'.format(attr_name, suffix), prop_ref)
        prop_ref.key = pair
        insort_left(self._properties, pair)

    def clear(self):
        """Clean up all properties in the registry
        """

        del self._properties[:]

    def _remove(self, ref):
        self._properties.remove(ref.key)
