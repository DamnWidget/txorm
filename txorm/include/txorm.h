/*
 * Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
 * See LICENSE for details
 */

#include <stdio.h>
#include <Python.h>
#include <structmember.h>

/*
    Support for Python 3

    In Python3, ob_type is in a nested structure so self->ob_type doesn't
    work anymore. We have to use the PyTYPE macro but it doesn't exists in
    Python versions previous to Python 2.6. We don't need any workaround
    as the lowest Python version that we support is Python2.6 but just in
    case
 */
#ifndef PyTYPE
    #define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#endif

#define CATCH(error_value, expression) \
    do { \
        if ((expression) == error_value) { \
            goto error; \
        } \
    } while (0)

#define REPLACE(variable, new_value) \
    do { \
        PyObject *tmp = variable; \
        variable = new_value; \
        Py_DECREF(tmp); \
    } while(0)

/* Definitions */
static PyObject *Undef = NULL;

typedef struct {
    PyObject_HEAD
    PyObject *_value;
    PyObject *_allow_none;
    PyObject *_validator;
    PyObject *column;
} VariableObject;

/* Initialization */
static int
initialize_globals(void)
{
    static int initialized = -1;
    PyObject *module;

    if (initialized >= 0) {
        if (!initialized)
            PyErr_SetString(
                PyExc_RuntimeError,
                "initialize_globals() failed in its first run"
            );

        return initialized;
    }
    initialized = 0;

    /* Import objects from txorm module */
    module = PyImport_ImportModule("txorm");
    if (!module)
        return 0;

    Undef = PyObject_GetAttrString(module, "Undef");
    if (!Undef)
        return 0;

    Py_DECREF(module);

    /* Import objects from txorm.variables package */
    module = PyImport_ImportModule("txorm.variable");
    if (!module)
        return 0;

    initialized = 1;
    return initialized;
}
