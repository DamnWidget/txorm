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
static PyObject *raise_none_error = NULL;
static PyObject *SQLRaw = NULL;
static PyObject *SQLToken = NULL;
static PyObject *State = NULL;
static PyObject *CompileError = NULL;
static PyObject *parenthesis_format = NULL;
static PyObject *default_compile_join = NULL;

/* Varaible */
typedef struct {
    PyObject_HEAD
    PyObject *_value;
    PyObject *_allow_none;
    PyObject *_validator;
    PyObject *field;
} VariableObject;

/* Compile */
typedef struct {
    PyObject_HEAD
    PyObject *__weakreflist;
    PyObject *_local_dispatch_table;
    PyObject *_local_precedence;
    PyObject *_local_reserved_words;
    PyObject *_dispatch_table;
    PyObject *_precedence;
    PyObject *_reserved_words;
    PyObject *_children;
    PyObject *_parents;
} CompileObject;

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
    module = PyImport_ImportModule("txorm.variable.base");
    if (!module)
        return 0;

    raise_none_error = PyObject_GetAttrString(module, "raise_none_error");
    if (!raise_none_error)
        return 0;

    Py_DECREF(module);

    /* Import objects from txorm.compiler package */
    module = PyImport_ImportModule("txorm.compiler.state");
    if (!module)
        return 0;

    State = PyObject_GetAttrString(module, "State");
    if (!State)
        return 0;

    Py_DECREF(module);

    module = PyImport_ImportModule("txorm.compiler.plain_sql");
    if (!module)
        return 0;

    SQLRaw = PyObject_GetAttrString(module, "SQLRaw");
    if (!SQLRaw)
        return 0;

    SQLToken = PyObject_GetAttrString(module, "SQLToken");
    if (!SQLToken)
        return 0;

    Py_DECREF(module);

    module = PyImport_ImportModule("txorm.compiler.base");
    if (!module)
        return 0;

    CompileError = PyObject_GetAttrString(module, "CompileError");
    if (!CompileError)
        return 0;

    Py_DECREF(module);

    /* fast path frequently used objects */
    parenthesis_format = PyUnicode_DecodeASCII("(%s)", 4, "strict");
    default_compile_join = PyUnicode_DecodeASCII(", ", 2, "strict");

    initialized = 1;
    return initialized;
}
