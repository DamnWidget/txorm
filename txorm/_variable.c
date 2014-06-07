/*
 * Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
 * See LICENSE for details
 */

#include "include/txorm.h"

#if PY_MAJOR_VERSION >= 3
    #define _PY3
#endif

/*
    class Variable(object):
        _value = Undef
        _allow_none = True
        _validator = None

        field = None
*/
static PyObject *
Variable_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    VariableObject *self = (VariableObject *)type->tp_alloc(type, 0);

    if (!initialize_globals())
        return NULL;

    /* Undef is defined in txorm.h this is why we don't use Py_XINCREF here */
    Py_INCREF(Undef);
    self->_value = Undef;
    Py_INCREF(Py_True);
    self->_allow_none = Py_True;
    Py_INCREF(Py_None);
    self->_validator = Py_None;
    Py_INCREF(Py_None);
    self->field = Py_None;

    return (PyObject *)self;
}

/*
    def __init__(self, value=Undef, value_factory=Undef,
                 from_db=False, allow_none=True, field=None, validator=None):
 */
static int
Variable_init(VariableObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {
        "value", "value_factory", "from_db", "allow_none", "field",
        "validator", "validator_factory", "validator_attribute", NULL
    };

    PyObject *value = Undef;
    PyObject *value_factory = Undef;
    PyObject *from_db = Py_False;
    PyObject *allow_none = Py_True;
    PyObject *field = Py_None;
    PyObject *validator = Py_None;
    PyObject *validator_factory = Py_None;
    PyObject *validator_attribute = Py_None;
    PyObject *tmp;

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "|OOOOOOOO", kwlist, &value, &value_factory, &from_db,
            &allow_none, &field, &validator, &validator_factory, &value_factory))
        return -1;

    /* if allow_none is not True: */
    if (!PyObject_IsTrue(allow_none)) {
        /*  self._allow_none = False  */
        Py_INCREF(Py_False);
        REPLACE(self->_allow_none, Py_False);  /* Py_DECREF old value */
    }

    /* if value is not Undef */
    if (value != Undef) {
        /*  self.set(value, from_db) */
        CATCH(NULL, tmp = PyObject_CallMethod(
            (PyObject *)self, "set", "OO", value, from_db));
        Py_DECREF(tmp);
    }
    /* elif value_factory is not Undef */
    else if (value_factory != Undef) {
        /*  self.set(value_factory(), from_db) */
        CATCH(NULL, value = PyObject_CallFunctionObjArgs(value_factory, NULL));
        tmp = PyObject_CallMethod(
            (PyObject *)self, "set", "OO", value, from_db);
        Py_DECREF(value);
        CATCH(NULL, tmp);   /* jump out if tmp is NULL */
        Py_DECREF(tmp);
    }

    /* if validator is not None: */
    if (validator != Py_None) {
        /*  self._validator = validator */
        Py_INCREF(validator);
        self->_validator = validator;
        /*  self._validator_factory = validator_factory */
        Py_INCREF(validator_factory);
        self->_validator_factory = validator_factory;
        /*  self._validator_attribute = validator_attribute */
        Py_INCREF(validator_attribute);
        self->_validator_attribute = validator_attribute;
    }

    /* self.field = field */
    Py_DECREF(self->field);
    Py_INCREF(field);
    self->field = field;

    return 0;

 /* Used to jump out from the context of Variable_init */
error:
    return -1;
}

/*
    This is being called by the garbage collector:
    https://docs.python.org/2/c-api/gcsupport.html#supporting-cycle-detection
 */
static int
Variable_traverse(VariableObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->_value);
    Py_VISIT(self->_allow_none);
    Py_VISIT(self->_validator);
    Py_VISIT(self->_validator_factory);
    Py_VISIT(self->_validator_attribute);
    Py_VISIT(self->field);
    return 0;
}

static int
Variable_clear(VariableObject *self)
{
    Py_CLEAR(self->_value);
    Py_CLEAR(self->_allow_none);
    Py_CLEAR(self->_validator);
    Py_CLEAR(self->_validator_factory);
    Py_CLEAR(self->_validator_attribute);
    Py_CLEAR(self->field);
    return 0;
}

/* This is called when a Variable object get out of scope */
static void
Variable_dealloc(VariableObject *self)
{
    Variable_clear(self);
    Py_TYPE(self)->tp_free((PyObject *)self);  /* low level free resources */
}

/*
    def parse_get(self, value, to_db):
 */
static PyObject *
Variable_parse_get(VariableObject *self, PyObject *args)
{
    /* return value */
    PyObject *value, *to_db;
    if (!PyArg_ParseTuple(args, "OO:parse_get", &value, &to_db))
        return NULL;
    Py_INCREF(value);
    return value;
}

/*
    def parse_set(self, value, from_db):
 */
static PyObject *
Variable_parse_set(VariableObject *self, PyObject *args)
{
    /* return value */
    PyObject *value, *from_db;
    if (!PyArg_ParseTuple(args, "OO:parse_set", &value, &from_db))
        return NULL;
    Py_INCREF(value);
    return value;
}

/*
    def get(self, default=None, to_db=False):
 */
static PyObject *
Variable_get(VariableObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"default", "to_db", NULL};
    PyObject *default_ = Py_None;
    PyObject *to_db = Py_False;

    if (!PyArg_ParseTupleAndKeywords(
        args, kwargs, "|OO:get", kwlist, &default_, &to_db))
        return NULL;

    /* value = None */
    PyObject *value = Py_None;
    /* if self._value is Undef: */
    if (self->_value == Undef) {
        /* value = default */
        if (default_ != Py_None)
            Py_INCREF(default_);
        value = default_;
    }
    /* elif self._value is not None: */
    else if (self->_value != Py_None) {
        /* value = self.parse_get(self._value, to_db) */
        value = PyObject_CallMethod(
            (PyObject *)self, "parse_get", "OO", self->_value, to_db);
    }

    if (value == Py_None)
        Py_INCREF(Py_None);
    return value;
}

/*
    def set(self, value, from_db=False):
*/
static PyObject *
Variable_set(VariableObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"value", "from_db", NULL};
    PyObject *value = Py_None;
    PyObject *from_db = Py_False;
    PyObject *new_value = Py_None;
    PyObject *tmp;

    if (!PyArg_ParseTupleAndKeywords(
        args, kwargs, "|OO:set", kwlist, &value, &from_db))
        return NULL;

    Py_INCREF(value);

    /* if from_db is False and self._validator is not None: */
    if (!PyObject_IsTrue(from_db) && self->_validator != Py_None) {
        /*
            value = self._validator(
                self._validator_factory and self._validator_factory(),
                self._validator_attribute, value
            )
         */
        PyObject *validator_object, *tmp;
        if (self->_validator_factory == Py_None)    {
            Py_INCREF(Py_None);
            validator_object = Py_None;
        } else{
            CATCH(NULL, validator_object = PyObject_CallFunctionObjArgs(
                self->_validator_factory, NULL
            ));
        }
        tmp = PyObject_CallFunctionObjArgs(
            self->_validator, validator_object, self->_validator_attribute,
            value, NULL
        );
        Py_DECREF(validator_object);
        CATCH(NULL, tmp);

        Py_DECREF(value);
        value = tmp;
    }

    /* if value is None: */
    if (value == Py_None) {
        /* if self._allow_none is False */
        if (!PyObject_IsTrue(self->_allow_none)) {
            /*  raise raise_none_error(self.field) */
            tmp = PyObject_CallFunctionObjArgs(
                raise_none_error, self->field, NULL
            );

            Py_XDECREF(tmp);
            goto error;
        }
        /*  new_value = None */
        Py_INCREF(Py_None);
        new_value = Py_None;
    }
    /* else: */
    else {
        /* new_value = self.parse_set(value, from_db) */
        CATCH(NULL, new_value = PyObject_CallMethod(
            (PyObject *)self, "parse_set", "OO", value, from_db));

    }

    Py_INCREF(new_value);
    self->_value = new_value;

    Py_DECREF(value);

    Py_RETURN_NONE;

/* Used to jump out from the context of Variable_set */
error:
    Py_XDECREF(value);
    Py_XDECREF(new_value);
    return NULL;
}

/*
    def delete(self):
 */
static PyObject *
Variable_delete(VariableObject *self, PyObject *args)
{
    /* if self._value is not Undef */
    if (self->_value != Undef) {
        Py_DECREF(self->_value);
        Py_INCREF(Undef);
        self->_value = Undef;
    }

    Py_RETURN_NONE;
}

/*
    def is_defined(self):
*/
static PyObject *
Variable_is_defined_(VariableObject *self, void *closure)
{
    /* return True if self._value is not Undef else False */
    return PyBool_FromLong(self->_value != Undef);
}

/* @property for is_defined */
static PyGetSetDef Variable_getseters[] = {
    {"is_defined", (getter)Variable_is_defined_, NULL, NULL, NULL},
    {NULL}  /* sentinel */
};

/* register Variable methods */
static PyMethodDef Variable_methods[] = {
    {"get", (PyCFunction)Variable_get, METH_VARARGS | METH_KEYWORDS, NULL},
    {"set", (PyCFunction)Variable_set, METH_VARARGS | METH_KEYWORDS, NULL},
    {"delete",
        (PyCFunction)Variable_delete, METH_VARARGS | METH_KEYWORDS, NULL},
    {"parse_get", (PyCFunction)Variable_parse_get, METH_VARARGS, NULL},
    {"parse_set", (PyCFunction)Variable_parse_set, METH_VARARGS, NULL},
    {NULL}    /* sentinel */
};

static PyMemberDef Variable_members[] = {
    {"_value", T_OBJECT, offsetof(VariableObject, _value), 0, 0},
    {"_allow_none", T_OBJECT, offsetof(VariableObject, _allow_none), 0, 0},
    {"field", T_OBJECT, offsetof(VariableObject, field), 0, 0},
    {NULL}  /* sentinel */
};

static PyTypeObject Variable_Type = {
#ifdef _PY3
    PyVarObject_HEAD_INIT(NULL, 0)
#else
    PyObject_HEAD_INIT(NULL)
    0,                                                  /* ob_size */
#endif
    "txorm.variable.Variable",                          /* tp_name */
    sizeof(VariableObject),                             /* tp_basicsize */
    0,                                                  /* tp_itemsize */
    (destructor)Variable_dealloc,                       /* tp_dealloc */
    0,                                                  /* tp_print */
    0,                                                  /* tp_getattr */
    0,                                                  /* tp_setattr */
    0,                                                  /* tp_compare */
    0,                                                  /* tp_repr */
    0,                                                  /* tp_as_number */
    0,                                                  /* tp_as_sequence */
    0,                                                  /* tp_as_mapping */
    0,                                                  /* tp_hash */
    0,                                                  /* tp_call */
    0,                                                  /* tp_str */
    0,                                                  /* tp_getattro */
    0,                                                  /* tp_setattro */
    0,                                                  /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
        Py_TPFLAGS_HAVE_GC,                             /* tp_flags */
    0,                                                  /* tp_doc */
    (traverseproc)Variable_traverse,                    /* tp_traverse */
    (inquiry)Variable_clear,                            /* tp_clear */
    0,                                                  /* tp_richcompare */
    0,                                                  /* tp_weaklistoffset */
    0,                                                  /* tp_iter */
    0,                                                  /* tp_iternext */
    Variable_methods,                                   /* tp_methods */
    Variable_members,                                   /* tp_members */
    Variable_getseters,                                 /* tp_getset */
    0,                                                  /* tp_base */
    0,                                                  /* tp_dict */
    0,                                                  /* tp_descr_get */
    0,                                                  /* tp_descr_set */
    0,                                                  /* tp_dictoffset */
    (initproc)Variable_init,                            /* tp_init */
    0,                                                  /* tp_alloc */
    Variable_new,                                       /* tp_new */
    0,                                                  /* tp_free */
    0,                                                  /* tp_is_gc */
};

static PyMethodDef _variable_methods[] = {
    {NULL, NULL}      /* sentinel */
};

#ifdef _PY3
static struct PyModuleDef _variable_module = {
    PyModuleDef_HEAD_INIT,
    "_variable",
    "C Implementation of the txorm Variable type",
    -1,
    _variable_methods,
    NULL, NULL, NULL, NULL
};
#endif

#ifndef PyMODINIT_FUNC  /* declarations for DLL import/export */
    #define PyMODINIT_FUNC void
#endif

PyMODINIT_FUNC
#ifdef _PY3
PyInit__variable(void)
#else
init_variable(void)
#endif
{
    PyObject* m;

    if(PyType_Ready(&Variable_Type) < 0)
#ifdef _PY3
        return NULL;
#else
        return;
#endif

#ifdef _PY3
    m = PyModule_Create(&_variable_module);
    if (m == NULL)
        return NULL;
#else
    m = Py_InitModule3(
        "_variable",
         _variable_methods,
          "C Implementation of the txorm Variable type"
    );
    if (m == NULL)
        return;
#endif

    Py_INCREF(&Variable_Type);
    PyModule_AddObject(m, "Variable", (PyObject *)&Variable_Type);
#ifdef _PY3
    return m;
#endif
}
