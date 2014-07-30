# -*- test-case-name: txorm.test.test_object_data -*-
# Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
# See LICENSE for details

from __future__ import unicode_literals

from weakref import ref

from txorm import Undef
from txorm.compat import binary_type, text_type, _PY3, b
from txorm.exceptions import ObjectDataError, ClassDataError
from txorm.compiler import Field, Desc, Table, TABLE, txorm_compile


def get_obj_data(obj):
    """Extract fields data information from te given object
    """

    if hasattr(obj, '__object_data__'):
        return obj.__object_data__

    # instantiate ObjectData first so that it breaks gracefully in case
    # that the object is not a TxORM object
    obj_data = ObjectData(obj)
    return obj.__dict__.setdefault('__object_data__', obj_data)


def get_cls_data(cls):
    """Extract fields data information from the given class
    """

    if '__class_data__' in cls.__dict__:
        # can't use attribute access here, otherwise subclassing won't work
        return cls.__dict__['__class_data__']
    else:
        cls.__class_data__ = ClassData(cls)
        return cls.__class_data__


def set_obj_data(obj, data):
    """Set the given data as __object_data__ of the given obj
    """
    obj.__dict__['__object_data__'] = data


class ClassData(dict):
    """Store useful information of a class

    :param table: expression from where columns will be looked up
    :param cls: class which should be used to build objects
    :param fields: tuple of field properties found in the class
    :param primary_key_pos: position of `primary_key` items in the fileds tuple
    """

    def __init__(self, cls):
        self.__pairs = None
        self.__primary_key = None

        # look for __database_table__, fi not found check storm compatibility
        self.table = getattr(
            cls, '__database_table__', getattr(cls, '__storm_table__', None))
        if self.table is None:
            raise ClassDataError(
                '{}.__database_table__ missing'.format(repr(cls))
            )
        self.cls = cls

        if isinstance(self.table, (text_type, binary_type)):
            self.table = Table(self.table)

        self.fields = tuple(pair[1] for pair in self.pairs)
        self.attributes = dict(self.pairs)

        # fields have __eq__ implementation that do things we don't want
        # look at these up in a dict and use identity semantics
        id_positions = dict((id(f), i) for i, f in enumerate(self.fields))
        self.primary_key_idx = dict(
            (id(f), i) for i, f in enumerate(self.fields))
        self.primary_key_pos = tuple(
            id_positions[id(f)] for f in self.primary_key)

        __order__ = getattr(cls, '__txorm_order__', None)
        if __order__ is None:
            self.default_order = Undef
        else:
            if type(__order__) is not tuple:
                __order__ = (__order__,)

            self.default_order = []
            for item in __order__:
                desc = False
                if isinstance(item, (binary_type, text_type)):
                    if _PY3 is True and isinstance(item, binary_type):
                        if item.startswith(b('-')):
                            desc = True
                    elif _PY3 is True and isinstance(item, text_type):
                        if item.startswith('-'):
                            desc = True
                    elif _PY3 is not True:
                        if item.startswith('-'):
                            desc = True

                prop = Desc(getattr(cls, item)) if desc is True else item
                self.default_order.append(prop)

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return self is not other

    @property
    def pairs(self):
        """Calculate class pairs (if needed) and return it back
        """

        if self.__pairs is not None:
            return self.__pairs

        pairs = []
        for attr in dir(self.cls):
            field = getattr(self.cls, attr, None)
            if isinstance(field, Field):
                pairs.append((attr, field))

        pairs.sort()
        self.__pairs = pairs
        return self.pairs

    @property
    def primary_key(self):
        """Calculate class primary key (if needed) and return it back
        """

        if self.__primary_key is not None:
            return self.__primary_key

        # get the __table_primary__, if not found try storm compatibility
        table_primary = getattr(
            self.cls, '__table_primary__',
            getattr(self.cls, '__storm_primary__', None)
        )
        if table_primary is not None:
            if type(table_primary) is not tuple:
                table_primary = (table_primary, )

            self.__primary_key = tuple(
                self.attributes[attr] for attr in table_primary
            )
        else:
            primary = []
            primary_attrs = {}
            for attr, field in self.pairs:
                if field.primary != 0:
                    if field.primary in primary_attrs:
                        raise ClassDataError(
                            '{} has two fields with the same primary id: '
                            '{} and {}'.format(
                                repr(self.cls), attr,
                                primary_attrs[field.primary]
                            )
                        )

                    primary.append((field.primary, field))
                    primary_attrs.update({field.primary: attr})

            primary.sort()
            self.__primary_key = tuple(field for i, field in primary)

        if len(self.__primary_key) == 0:
            raise ClassDataError(
                '{} has no primary key information'.format(repr(self.cls))
            )

        return self.primary_key


class ObjectData(dict):
    """Store useful information about objects that define TxORM Properties

    :param obj: the object to store data from
    """

    __hash__ = object.__hash__

    # for get_obj_data, a FiedsData is its own obj_data
    __object_data__ = property(lambda self: self)

    def __init__(self, obj):
        # first thing, try to create a ClassInfo for the object's class.
        # this ensures that obj is the kind of object we expect.
        self.cls_data = get_cls_data(type(obj))

        self.set_object(obj)
        self.variables = variables = {}

        for field in self.cls_data.fields:
            variables[field] = field.variable_factory(
                field=field, validator_factory=self.get_object
            )

        self.primary_vars = tuple(
            variables[field] for field in self.cls_data.primary_key
        )

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return self is not other

    def get_object(self):
        return self._ref()

    def set_object(self, obj):
        self._ref = ref(obj, None)


class ClassAlias(object):
    """Create a named alias for a TxORM class to use in queries.

    This is useful wehn the SQL 'AS' feature is desired in code using TxORM
    queries. ClassAliases which are explicitly named are cached for as long
    as the class exists, such that the alias returned from the constructor
    should be the same object no matter how many times it's called.

    :param cls: the class to create an alias from
    :param name: if provided, specify the name of the alias to create
    """

    alias_count = 0

    def __new__(self_cls, cls, name=Undef):
        if name is Undef:
            use_cache = False
            ClassAlias.alias_count += 1
            name = '_{:x}'.format(ClassAlias.alias_count)
        else:
            use_cache = True
            cache = cls.__dict__.get('_alias_cache')
            if cache is None:
                cls._alias_cache = {}
            elif name in cache:
                return cache[name]

        if _PY3 is False:
            alias_name = b('{}Alias'.format(cls.__name__))
        else:
            alias_name = '{}Alias'.format(cls.__name__)
        alias_cls = type(alias_name, (self_cls,), {'__database_table__': name})
        alias_cls.__bases__ = (cls, self_cls)
        alias_cls_data = get_cls_data(alias_cls)
        alias_cls_data.cls = cls

        if use_cache:
            cls._alias_cache[name] = alias_cls

        return alias_cls


@txorm_compile.when(type)
def compile_type(compile, expression, state):
    cls_data = get_cls_data(expression)
    table = compile(cls_data.table, state)
    if state.context is TABLE and issubclass(expression, ClassAlias):
        return '{} AS {}'.format(compile(cls_data.cls, state), table)

    return table
