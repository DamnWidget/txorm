# -*- test-case-name: txorm.test.test_property -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from functools import partial

from txorm import Undef
from txorm.compiler import Field
from txorm.compat import iteritems
from txorm.variable import Variable
from txorm.object_data import get_obj_data


class Property(object):
    """Property class wraps and maps python object values with table fields

    Class scope variable creation_counter is used to keep track of the order
    of appearance of the fields in the generated SQL schema when we dump it.

    :param name: the property name
    :param primary: bool that determine if the mapped field is a primary key
    :param variable_class: variable class for this porperty
    :param variable_kwargs: key arguments for variable class factory
    """

    creation_counter = 0

    def __init__(self, name=None,
                 primary=False, variable_class=Variable, variable_kwargs={}):
        self._name = name
        self._primary = primary
        self._variable_class = variable_class
        self._variable_kwargs = variable_kwargs
        self._creation_order = Property.creation_counter
        Property.creation_counter += 1

    def __get__(self, obj, cls=None):
        """Return the right variable type for this property field data
        """

        if obj is None:
            return self._get_field(cls)

        obj_fields_data = get_obj_data(obj)
        if cls is None:
            # don't get obj.__class__ because we don't trust if
            cls = obj_fields_data.cls_data.cls

        field = self._get_field(cls)
        return obj_fields_data.variables[field].get()

    def __set__(self, obj, value):
        """Set the given value right variable type for this property field data
        """

        obj_fields_data = get_obj_data(obj)
        # don't get obj.__class__ because we don't trust if
        field = self._get_field(obj_fields_data.cls_data.cls)
        obj_fields_data.variables[field].set(value)

    def __delete__(self, obj):
        """Delete the wrapped variable value
        """

        obj_fields_data = get_obj_data(obj)
        # don't get obj.__class__ because we don't trust if
        field = self._get_field(obj_fields_data.cls_data.cls)
        obj_fields_data.variables[field].delete()

    def _get_field(self, cls):
        """
        Cache per-class fields values in the class itself, to avoid holding
        a strong reference to it here, and thus rendering classes uncollectable
        in certain situations like for example, subclasses where the property
        is stored in the base
        """

        # use class dictionary explicitly to get sensibe results on subclasses
        if '_txorm_fields' in cls.__dict__:
            field = cls.__dict__['_txorm_fields'].get(self)
        else:
            cls._txorm_fields = {}
            field = None

        if field is None:
            attr = self._infere_attr_name(cls)
            name = attr if self._name is None else self._name
            field = PropertyField(
                self, cls, attr, name, self._primary,
                self._variable_class, self._variable_kwargs
            )
            cls._txorm_fields[self] = field

        return field

    def _infere_attr_name(self, used_cls):
        """Infere the attribute name in the given used class
        """

        self_id = id(self)
        for cls in used_cls.__mro__:
            for attr, prop in iteritems(cls.__dict__):
                if id(prop) == self_id:
                    return attr

        raise RuntimeError('Property used in an unknown class')


class PropertyField(Field):
    """
    Properties base class that knows how to compile to SQL itself
    using a VariableFactory
    """

    def __init__(self, prop, cls, attr, name,
                 primary, variable_class, variable_kwargs):
        self.size = variable_kwargs.pop('size', Undef)
        self.unsigned = variable_kwargs.pop('unsigned', False)
        self.index = variable_kwargs.pop('index', None)
        self.unique = variable_kwargs.pop('unique', None)
        self.auto_increment = variable_kwargs.pop('auto_increment', False)
        self.array = variable_kwargs.pop('array', None)

        Field.__init__(self, name, cls, primary, partial(
            variable_class, field=self,
            validator_attribute=attr, **variable_kwargs
        ))

        self.cls = cls   # used by references

        # copy attributes from the property to avoid one additional function
        # call on each access
        for attr in ['__get__', '__set__', '__delete__', '_creation_order']:
            setattr(self, attr, getattr(prop, attr))


class SimpleProperty(Property):
    """The siplest possible property
    """

    variable_class = None

    def __init__(self, name=None, primary=False, **kwargs):
        kwargs['value'] = kwargs.pop('default', Undef)
        kwargs['value_factory'] = kwargs.pop('value_factory', Undef)
        super(SimpleProperty, self).__init__(
            name, primary, self.variable_class, kwargs
        )
