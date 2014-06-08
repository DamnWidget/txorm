/*
 * Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
 * See LICENSE for details
 */

#include "include/txorm.h"

#if PY_MAJOR_VERSION >= 3
    #define _PY3
#endif

/*
    Class ObjectData(dict)
 */


/* __object_data__ = property(lambda self: self) */
static PyObject *
ObjectData__object_data__(PyObject *self, void *closure)
{
    Py_INCREF(self);
    return self;
}

static int
ObjectData_init(ObjectDataObject *self, PyObject *args)
{
    PyObject *self_get_object = NULL;
    PyObject *empty_args = NULL;
    PyObject *factory_kwargs = NULL;
    PyObject *fields = NULL;
    PyObject *primary_key = NULL;
    PyObject *obj;
    Py_ssize_t i;

    empty_args = PyTuple_New(0);
    CATCH(-1, PyDict_Type.tp_init((PyObject *)self, empty_args, NULL));
    CATCH(0, initialize_globals());

    if (!PyArg_ParseTuple(args, "O", &obj))
        goto error;

    /* self.cls_data = get_cls_data(type(obj)) */
    CATCH(NULL, self->cls_data = PyObject_CallFunctionObjArgs(
        get_cls_data, Py_TYPE(obj), NULL)
    );

    /* self.set_object(obj) */
    CATCH(NULL, self->__obj_ref = PyWeakref_NewRef(obj, Py_None));

    /* self.variables = variables = {} */
    CATCH(NULL, self->variables = PyDict_New());

    CATCH(NULL, self_get_object = PyObject_GetAttrString((PyObject *)self, "get_object"));
    CATCH(NULL, factory_kwargs = PyDict_New());
    CATCH(-1, PyDict_SetItemString(
        factory_kwargs, "validator_factory", self_get_object)
    );
    /*
        for field in self.cls_data.fields:
            variables[field] = field.variable_factory(
                field=field, validator_factory=self.get_object
            )
     */
    CATCH(NULL, fields = PyObject_GetAttrString(self->cls_data, "fields"));
    for (i = 0; i != PyTuple_GET_SIZE(fields); i++) {
        PyObject *field = PyTuple_GET_ITEM(fields, i);
        PyObject *variable, *factory;
        CATCH(-1, PyDict_SetItemString(factory_kwargs, "field", field));
        CATCH(NULL, factory = PyObject_GetAttrString(field, "variable_factory"));

        variable = PyObject_Call(factory, empty_args, factory_kwargs);
        Py_DECREF(factory);
        CATCH(NULL, variable);
        if (PyDict_SetItem(self->variables, field, variable) == -1) {
            Py_DECREF(variable);
            goto error;
        }
        Py_DECREF(variable);
    }

    /*
        self.primary_vars = tuple(
            variables[field] for field in self.cls_data.primary_key
        )
    */
    CATCH(NULL, primary_key = PyObject_GetAttrString(
        (PyObject *)self->cls_data, "primary_key")
    );

    CATCH(NULL, self->primary_vars = PyTuple_New(PyTuple_GET_SIZE(primary_key)));
    for (i = 0; i != PyTuple_GET_SIZE(primary_key); i++) {
        PyObject *field = PyTuple_GET_ITEM(primary_key, i);
        PyObject *variable = PyDict_GetItem(self->variables, field);
        Py_INCREF(variable);
        PyTuple_SET_ITEM(self->primary_vars, i, variable);
    }

    Py_DECREF(self_get_object);
    Py_DECREF(empty_args);
    Py_DECREF(factory_kwargs);
    Py_DECREF(fields);
    Py_DECREF(primary_key);
    return 0;

error:
    Py_XDECREF(self_get_object);
    Py_XDECREF(empty_args);
    Py_XDECREF(factory_kwargs);
    Py_XDECREF(fields);
    Py_XDECREF(primary_key);
    return -1;

}

static PyObject *
ObjectData_get_object(ObjectDataObject *self, PyObject *args)
{
    PyObject *obj = PyWeakref_GET_OBJECT(self->__obj_ref);
    Py_INCREF(obj);
    return obj;
}

static PyObject *
ObjectData_set_object(ObjectDataObject *self, PyObject *args)
{
    PyObject *obj;

    if (!PyArg_ParseTuple(args, "O", &obj))
        return NULL;

    Py_DECREF(self->__obj_ref);
    self->__obj_ref = PyWeakref_NewRef(obj, Py_None);
    if (!self->__obj_ref)
        return NULL;

    Py_RETURN_NONE;
}

static int
ObjectData_traverse(ObjectDataObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->__obj_ref);
    Py_VISIT(self->cls_data);
    Py_VISIT(self->variables);
    Py_VISIT(self->primary_vars);
    // return 0;
    return PyDict_Type.tp_traverse((PyObject *)self, visit, arg);
}

static int
ObjectData_clear(ObjectDataObject *self)
{
    Py_CLEAR(self->__obj_ref);
    Py_CLEAR(self->cls_data);
    Py_CLEAR(self->variables);
    Py_CLEAR(self->primary_vars);
    // return 0;
    return PyDict_Type.tp_clear((PyObject *)self);
}

static PyObject *
ObjectData_richcompare(PyObject *self, PyObject *other, int op)
{
    PyObject *res;

    /* implement equality via identity */
    switch (op) {
        case Py_EQ:
            res = (self == other) ? Py_True : Py_False;
            break;
        case Py_NE:
            res = (self != other) ? Py_True : Py_False;
            break;
        default:
            res = Py_NotImplemented;
    }
    Py_INCREF(res);
    return res;
}

static void
ObjectData_dealloc(ObjectDataObject *self)
{
    if (self->__weakreflist)
        PyObject_ClearWeakRefs((PyObject *)self);

    Py_CLEAR(self->__obj_ref);
    Py_CLEAR(self->cls_data);
    Py_CLEAR(self->variables);
    Py_CLEAR(self->primary_vars);
    PyDict_Type.tp_dealloc((PyObject *)self);
}

static PyMethodDef ObjectData_methods[] = {
    {"get_object", (PyCFunction)ObjectData_get_object, METH_NOARGS, NULL},
    {"set_object", (PyCFunction)ObjectData_set_object, METH_VARARGS, NULL},
    {NULL  /* sentinel */}
};

static PyMemberDef ObjectData_members[] = {
    {"cls_data", T_OBJECT, offsetof(ObjectDataObject, cls_data), 0, 0},
    {"variables", T_OBJECT, offsetof(ObjectDataObject, variables), 0, 0},
    {"primary_vars", T_OBJECT, offsetof(ObjectDataObject, primary_vars), 0, 0},
    {NULL  /* sentinel */}
};

static PyGetSetDef ObjectData_getset[] = {
    {"__object_data__", (getter)ObjectData__object_data__, NULL, NULL},
    {NULL  /* sentinel */}
};

#ifdef _PY3
static PyTypeObject ObjectData_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)                      /*ob_size*/
#else
statichere PyTypeObject ObjectData_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                                                  /*ob_size*/
#endif
    "txorm.object_data.ObjectData",                     /*tp_name*/
    sizeof(ObjectDataObject),                           /*tp_basicsize*/
    0,                                                  /*tp_itemsize*/
    (destructor)ObjectData_dealloc,                     /*tp_dealloc*/
    0,                                                  /*tp_print*/
    0,                                                  /*tp_getattr*/
    0,                                                  /*tp_setattr*/
    0,                                                  /*tp_compare*/
    0,                                                  /*tp_repr*/
    0,                                                  /*tp_as_number*/
    0,                                                  /*tp_as_sequence*/
    0,                                                  /*tp_as_mapping*/
    (hashfunc)_Py_HashPointer,                          /*tp_hash*/
    0,                                                  /*tp_call*/
    0,                                                  /*tp_str*/
    PyObject_GenericGetAttr,                            /*tp_getattro*/
    PyObject_GenericSetAttr,                            /*tp_setattro*/
    0,                                                  /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
        Py_TPFLAGS_HAVE_GC,                             /*tp_flags*/
    0,                                                  /*tp_doc*/
    (traverseproc)ObjectData_traverse,                  /*tp_traverse*/
    (inquiry)ObjectData_clear,                          /*tp_clear*/
    ObjectData_richcompare,                             /*tp_richcompare*/
    offsetof(ObjectDataObject, __weakreflist),          /*tp_weaklistoffset*/
    0,                                                  /*tp_iter*/
    0,                                                  /*tp_iternext*/
    ObjectData_methods,                                 /*tp_methods*/
    ObjectData_members,                                 /*tp_members*/
    ObjectData_getset,                                  /*tp_getset*/
    &PyDict_Type,                                       /*tp_base*/
    0,                                                  /*tp_dict*/
    0,                                                  /*tp_descr_get*/
    0,                                                  /*tp_descr_set*/
    0,                                                  /*tp_dictoffset*/
    (initproc)ObjectData_init,                          /*tp_init*/
    PyType_GenericAlloc,                                /*tp_alloc*/
    PyType_GenericNew,                                  /*tp_new*/
    PyObject_GC_Del,                                    /*tp_free*/
    0,                                                  /*tp_is_gc*/
};

static PyObject *
get_obj_data(PyObject *self, PyObject *obj)
{
    PyObject *obj_data;

    if (Py_TYPE(obj) == &ObjectData_Type) {
        Py_INCREF(obj);
        return obj;
    }

    /* if hasattr(obj, '__object_data__'): */
    if (PyObject_HasAttrString(obj, "__object_data__") == 1) {
        obj_data = PyObject_GetAttrString(obj, "__object_data__");
        return obj_data;
    }

    /* obj_data = ObjectData(obj) */
    obj_data = PyObject_CallFunctionObjArgs((PyObject *)&ObjectData_Type, obj, NULL);
    if (!obj_data)
        return NULL;

    /* return obj.__dict__.setdefault('__object_data__', obj_data) */
    if (PyObject_SetAttrString(obj, "__object_data__", obj_data) == -1)
        return NULL;

    return obj_data;
}

static PyMethodDef _object_data_methods[] = {
    {"get_obj_data", (PyCFunction)get_obj_data, METH_O, NULL},
    {NULL  /* sentinel */}
};

#ifdef _PY3
static struct PyModuleDef _object_data_module = {
    PyModuleDef_HEAD_INIT,
    "_object_data",
    "C Implementation of the txorm ObjectData type",
    -1,
    _object_data_methods,
    NULL, NULL, NULL, NULL
};
#endif

#ifndef PyMODINIT_FUNC  /* declarations for DLL import/export */
    #define PyMODINIT_FUNC void
#endif

PyMODINIT_FUNC
#ifdef _PY3
PyInit__object_data(void)
#else
init_object_data(void)
#endif
{
    PyObject* m;

    if(PyType_Ready(&ObjectData_Type) < 0)
#ifdef _PY3
        return NULL;
#else
        return;
#endif

#ifdef _PY3
    m = PyModule_Create(&_object_data_module);
    if (m == NULL)
        return NULL;
#else
    m = Py_InitModule3(
        "_object_data",
         _object_data_methods,
          "C Implementation of the txorm ObjectData type"
    );
    if (m == NULL)
        return;
#endif

    Py_INCREF(&ObjectData_Type);
    PyModule_AddObject(m, "ObjectData", (PyObject *)&ObjectData_Type);
#ifdef _PY3
    return m;
#endif
}
