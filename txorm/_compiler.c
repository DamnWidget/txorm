/*
 * Copyright (c) 2014 Oscar Campos <oscar.campos@member.fsf.org>
 * See LICENSE for details
 */

#include "include/txorm.h"

#if PY_MAJOR_VERSION >= 3
    #define _PY3
#endif

/*
    We need this here to calm the compiler
 */
static PyObject *
Compile__update_cache(CompileObject *self, PyObject *args);

/*
    Class Compile(object)
 */

static int
Compile_init(CompileObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"parent", NULL};

    PyObject *parent = Py_None;
    PyObject *module = NULL;
    PyObject *WeakKeyDictionary = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O", kwlist, &parent))
        return -1;

    /*
        self._local_dispatch_table = {}
        self._local_precedence = {}
        self._local_reserved_words = {}
        self._dispatch_table = {}
        self._precedence = {}
        self._reserved_words = {}
    */
    CATCH(NULL, self->_local_dispatch_table = PyDict_New());
    CATCH(NULL, self->_local_precedence = PyDict_New());
    CATCH(NULL, self->_local_reserved_words = PyDict_New());
    CATCH(NULL, self->_dispatch_table = PyDict_New());
    CATCH(NULL, self->_precedence = PyDict_New());
    CATCH(NULL, self->_reserved_words = PyDict_New());

    /* self._children = WeakKeyDictionary() */
    CATCH(NULL, module = PyImport_ImportModule("weakref"));
    CATCH(NULL, WeakKeyDictionary = PyObject_GetAttrString(
        module, "WeakKeyDictionary")
    );
    Py_CLEAR(module);
    CATCH(NULL, self->_children = PyObject_CallFunctionObjArgs(
        WeakKeyDictionary, NULL)
    );
    Py_CLEAR(WeakKeyDictionary);

    /* self._parents = [] */
    CATCH(NULL, self->_parents = PyList_New(0));

    /* if parent is not None */
    if (parent != Py_None) {
        PyObject *tmp;

        /* self._parents.extend(parent._parents)*/
        CompileObject *parent_object = (CompileObject *)parent;
        CATCH(-1,
            PyList_SetSlice(self->_parents, 0, 0, parent_object->_parents));

        /* self._parents.append(parent) */
        CATCH(-1, PyList_Append(self->_parents, parent));

        /* parent._children[self] = True */
        CATCH(-1, PyObject_SetItem(
            parent_object->_children, (PyObject *)self, Py_True));

        /* self._update_cache() */
        CATCH(NULL, tmp = Compile__update_cache(self, NULL));
        Py_DECREF(tmp);
    }

    return 0;

error:
    Py_XDECREF(module);
    Py_XDECREF(WeakKeyDictionary);
    return -1;
}

static int
Compile_traverse(CompileObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->_local_dispatch_table);
    Py_VISIT(self->_local_precedence);
    Py_VISIT(self->_local_reserved_words);
    Py_VISIT(self->_dispatch_table);
    Py_VISIT(self->_precedence);
    Py_VISIT(self->_reserved_words);
    Py_VISIT(self->_children);
    Py_VISIT(self->_parents);
    return 0;
}

static int
Compile_clear(CompileObject *self)
{
    if (self->__weakreflist)
        PyObject_ClearWeakRefs((PyObject *)self);
    Py_CLEAR(self->_local_dispatch_table);
    Py_CLEAR(self->_local_precedence);
    Py_CLEAR(self->_local_reserved_words);
    Py_CLEAR(self->_dispatch_table);
    Py_CLEAR(self->_precedence);
    Py_CLEAR(self->_reserved_words);
    Py_CLEAR(self->_children);
    Py_CLEAR(self->_parents);
    return 0;
}

static void
Compile_dealloc(CompileObject *self)
{
    Compile_clear(self);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *
Compile__update_cache(CompileObject *self, PyObject *args)
{
    PyObject *iter = NULL;
    PyObject *child = NULL;
    Py_ssize_t size;
    int i;

    /* for parent in self._parents: */
    size = PyList_GET_SIZE(self->_parents);
    for (i = 0; i != size; i++) {
        CompileObject *parent = (CompileObject *)PyList_GET_ITEM(
            self->_parents, i
        );
        /* self._dispatch_table.update(parent._local_dispatch_table) */
        CATCH(-1, PyDict_Update(
            self->_dispatch_table, parent->_local_dispatch_table));
        /* self._precedence.update(parent._local_precedence) */
        CATCH(-1, PyDict_Update(
            self->_precedence, parent->_local_precedence));
        /* self._reserved_words.update(parent._local_reserved_words) */
        CATCH(-1, PyDict_Update(
            self->_reserved_words, parent->_local_reserved_words));
    }

    /* self._dispatch_table.update(self._local_dispatch_table) */
    CATCH(-1, PyDict_Update(
        self->_dispatch_table, self->_local_dispatch_table));
    /* self._precedence.update(self._local_precedence) */
    CATCH(-1, PyDict_Update(self->_precedence, self->_local_precedence));
    /* self._reserved_words.update(self._local_reserved_words)     */
    CATCH(-1, PyDict_Update(
        self->_reserved_words, self->_local_reserved_words));

    /* (child._update_cache() for child in self._children) */
    CATCH(NULL, iter = PyObject_GetIter(self->_children));
    while((child = PyIter_Next(iter))) {
        PyObject *tmp;

        CATCH(NULL, tmp = Compile__update_cache((CompileObject *)child, NULL));
        Py_DECREF(tmp);
        Py_DECREF(child);
    }
    if (PyErr_Occurred())
        goto error;
    Py_CLEAR(iter);

    Py_RETURN_NONE;

error:
    Py_XDECREF(child);
    Py_XDECREF(iter);
    return NULL;
}

static PyObject *
Compile_when(CompileObject *self, PyObject *types)
{
    PyObject *result = NULL;
    PyObject *module = PyImport_ImportModule("txorm.compiler.base");
    if (module) {
        PyObject *_when = PyObject_GetAttrString(module, "_when");
        if(_when) {
            result = PyObject_CallFunctionObjArgs(_when, self, types, NULL);
            Py_DECREF(_when);
        }
        Py_DECREF(module);
    }

    return result;
}

static PyObject *
Compile_add_reserved_words(CompileObject *self, PyObject *words)
{
    PyObject *lower_word = NULL;
    PyObject *iter = NULL;
    PyObject *word = NULL;
    PyObject *tmp;

    /*
        self._local_reserved_words.update(
            (word.lower(), True) for word in words
        )
    */
    CATCH(NULL, iter = PyObject_GetIter(words));
    while ((word = PyIter_Next(iter))) {
        CATCH(NULL, lower_word = PyObject_CallMethod(word, "lower", NULL));
        CATCH(-1, PyDict_SetItem(
            self->_local_reserved_words, lower_word, Py_True));
        Py_CLEAR(lower_word);
        Py_DECREF(word);
    }
    if (PyErr_Occurred())
        goto error;
    Py_CLEAR(iter);

    /* self._update_cache() */
    CATCH(NULL, tmp = Compile__update_cache(self, NULL));
    Py_DECREF(tmp);

    Py_RETURN_NONE;

error:
    Py_XDECREF(lower_word);
    Py_XDECREF(word);
    Py_XDECREF(iter);
    return NULL;
}

static PyObject *
Compile_remove_reserved_words(CompileObject *self, PyObject *words)
{
    PyObject *lower_word = NULL;
    PyObject *iter = NULL;
    PyObject *word = NULL;
    PyObject *tmp;

    /*
        self._local_reserved_words.update(
            (word.lower(), None) for word in words
        )
    */
    CATCH(NULL, iter = PyObject_GetIter(words));
    while ((word = PyIter_Next(iter))) {
        CATCH(NULL, lower_word = PyObject_CallMethod(word, "lower", NULL));
        CATCH(-1, PyDict_SetItem(
            self->_local_reserved_words, lower_word, Py_None));
        Py_CLEAR(lower_word);
        Py_DECREF(word);
    }
    if (PyErr_Occurred())
        goto error;
    Py_CLEAR(iter);

    /* self._update_cache() */
    CATCH(NULL, tmp = Compile__update_cache(self, NULL));
    Py_DECREF(tmp);

    Py_RETURN_NONE;

error:
    Py_XDECREF(lower_word);
    Py_XDECREF(word);
    Py_XDECREF(iter);
    return NULL;
}

static PyObject *
Compile_is_reserved_word(CompileObject *self, PyObject *word)
{
    PyObject *lower_word = NULL;
    PyObject *result = Py_False;
    PyObject *tmp;

    /* return self._reserved_words.get(word.lower()) is not None */
    CATCH(NULL, lower_word = PyObject_CallMethod(word, "lower", NULL));
    tmp = PyDict_GetItem(self->_reserved_words, lower_word);
    if (tmp == NULL && PyErr_Occurred()) {
        goto error;
    } else if (tmp != NULL && tmp != Py_None) {
        result = Py_True;
    }
    Py_DECREF(lower_word);
    Py_INCREF(result);
    return result;

error:
    Py_XDECREF(lower_word);
    return NULL;
}

static PyTypeObject Compile_Type;

static PyObject *
Compile_create_child(CompileObject *self, PyObject *args)
{
    /* return self.__class__(parent=self) */
    return PyObject_CallFunctionObjArgs((PyObject*)Py_TYPE(self), self, NULL);
}

static PyObject *
Compile_get_precedence(CompileObject *self, PyObject *expression_type)
{
    PyObject *module = NULL;
    PyObject *MAX_PRECEDENCE = NULL;
    /* return self._precedence.get(expression_type, MAX_PRECEDENCE) */
    PyObject *result = PyDict_GetItem(self->_precedence, expression_type);
    if (result == NULL && !PyErr_Occurred()) {
        module = PyImport_ImportModule("txorm.compiler.base");
        if (module) {
            MAX_PRECEDENCE = PyObject_GetAttrString(module, "MAX_PRECEDENCE");
            Py_DECREF(module);
            return MAX_PRECEDENCE;
        } else {
#ifdef _PY3
            return PyLong_FromLong(1000);
#else
            return PyInt_FromLong(1000);
#endif
        }
    }
    Py_INCREF(result);
    return result;
}

static PyObject *
Compile_set_precedence(CompileObject *self, PyObject *args)
{
    Py_ssize_t size = PyTuple_GET_SIZE(args);
    PyObject *precedence = NULL;
    PyObject *tmp;
    int i;

    if (size < 2) {
        PyErr_SetString(
            PyExc_TypeError, "set_precedence() takes at least 2 arguments"
        );
        return NULL;
    }

    /* for expression_type in expression_types: */
    precedence = PyTuple_GET_ITEM(args, 0);
    for (i = 1; i != size; i++) {
        PyObject *type = PyTuple_GET_ITEM(args, i);
        /* self._local_precedence[expression_type] = precedence */
        CATCH(-1, PyDict_SetItem(self->_local_precedence, type, precedence));
    }

    /* self._udpate_cache() */
    CATCH(NULL, tmp = Compile__update_cache(self, NULL));
    Py_DECREF(tmp);

    Py_RETURN_NONE;

error:
    return NULL;
}

static PyObject *
Compile__single(CompileObject *self,
    PyObject *expression, PyObject *state, PyObject *outer_precedence)
{
    PyObject *inner_precedence = NULL;
    PyObject *statement = NULL;

    /* cls = expression.__class__ */
    PyObject *cls = (PyObject*)Py_TYPE(expression);

    /*
        dispatch_table = self._dispatch_table
        if cls in dispatch_table:
            handler = dispatch_table[cls]
        else:
    */
    PyObject *handler = PyDict_GetItem(self->_dispatch_table, cls);
    if (!handler) {
        PyObject *mro;
        Py_ssize_t size, i;

        if (PyErr_Occurred())
            goto error;

        /* for mro in cls.__mro__: */
        mro = Py_TYPE(expression)->tp_mro;
        size = PyTuple_GET_SIZE(mro);
        for (i = 0; i != size; i++) {
            PyObject *mro_cls = PyTuple_GET_ITEM(mro, i);
            /*
                if mro in dispatch_table:
                    handler = dispatch_table[mro]
                    break
            */
            handler = PyDict_GetItem(self->_dispatch_table, mro_cls);
            if (handler)
                break;

            if (PyErr_Occurred())
                goto error;
        }
        /* else: */
        if (i == size) {
            /*
                raise CompileError(
                    'Don\'t know how to compile type {!r} of {!r}'.format(
                        expression.__class__, expression
                    )
                )
            */
            PyObject *repr = PyObject_Repr(expression);
            if (repr) {
                PyErr_Format(
                    CompileError, "Don't know how to compile type %s of %p",
#ifdef _PY3
                    Py_TYPE(expression)->tp_name, PyBytes_AS_STRING(expression)
                );
#else
                    Py_TYPE(expression)->tp_name, PyString_AS_STRING(repr)
                );
#endif
                Py_DECREF(repr);
            }
            goto error;
        }
    }

    /*
        inner_precedence = state.precedence = self._precedence.get(
            cls, MAX_PRECEDENCE
        )
    */
    CATCH(NULL, inner_precedence = Compile_get_precedence(self, cls));
    CATCH(-1, PyObject_SetAttrString(state, "precedence", inner_precedence));

    /* statement = handler(self, expression, state) */
    CATCH(NULL, statement = PyObject_CallFunctionObjArgs(
        handler, self, expression, state, NULL)
    );

    /* if inner_precedence < outer_precedence: */
#ifdef _PY3
    if (PyObject_RichCompare(inner_precedence, outer_precedence, Py_LT) == Py_True) {
#else
    if (PyObject_Compare(inner_precedence, outer_precedence) == -1) {
#endif
        PyObject *args, *tmp;

        if (PyErr_Occurred())
            goto error;

        /* return '({})'.format(statement) */
        CATCH(NULL, args = PyTuple_Pack(1, statement));
        tmp = PyUnicode_Format(parenthesis_format, args);
        Py_DECREF(args);
        CATCH(NULL, tmp);
        Py_DECREF(statement);
        statement = tmp;
    }

    Py_DECREF(inner_precedence);
    return statement;

error:
    Py_XDECREF(inner_precedence);
    Py_XDECREF(statement);

    return NULL;
}

static PyObject *
Compile_recursive_compile(CompileObject *self, PyObject *expression,
    PyObject *state, PyObject *join, int raw, int token)
{
    PyObject *outer_precedence = NULL;
    PyObject *compiled = NULL;
    PyObject *sequence = NULL;
    PyObject *statement = NULL;
    Py_ssize_t size, i;

    Py_INCREF(expression);

    /*
        if (expression_type is SQLRaw or raw
                and (expression_type in (binary_type, text_type))):
            return expression
    */
    if ((PyObject *)Py_TYPE(expression) == SQLRaw ||
#ifdef _PY3
        (raw && (PyBytes_CheckExact(expression) ||
                                        PyUnicode_CheckExact(expression)))) {
#else
        (raw && (PyString_CheckExact(expression) ||
                                        PyUnicode_CheckExact(expression)))) {
#endif
        return expression;
    }

    /*
        if token and (expression_type in (binary_type, text_type)):
            expression = SQLToken(expression)
    */
#ifdef _PY3
    if (token && (PyBytes_CheckExact(expression) ||
                                        PyUnicode_CheckExact(expression))) {
#else
    if (token && (PyString_CheckExact(expression) ||
                                        PyUnicode_CheckExact(expression))) {
#endif
        PyObject *tmp;
        CATCH(NULL, tmp = PyObject_CallFunctionObjArgs(
            SQLToken, expression, NULL)
        );
        Py_DECREF(expression);
        expression = tmp;
    }

    /* outer_precedence = state.precedence */
    CATCH(NULL, outer_precedence = PyObject_GetAttrString(state, "precedence"));
    /* if expression_type in (tuple, list): */
    if (PyTuple_CheckExact(expression) || PyList_CheckExact(expression)) {
        /* compiled = [] */
        CATCH(NULL, compiled = PyList_New(0));

        /* for subexpression in expression: */
        sequence = PySequence_Fast(expression, "Ouch ouch ouch ouch");
        size = PySequence_Fast_GET_SIZE(sequence);
        for (i = 0; i != size; i++) {
            PyObject *subexpression = PySequence_Fast_GET_ITEM(sequence, i);
            /*
                subexpression_type = type(subexpression)
                if subexpression_type is SQLRaw or raw and (
                        subexpression_type in (binary_type, text_type)):
            */
#ifdef _PY3
            if ((PyObject *)Py_TYPE(subexpression) == (PyObject *)SQLRaw ||
                (raw && (PyBytes_CheckExact(subexpression) ||
                         PyUnicode_CheckExact(subexpression)))) {
#else
            if ((PyObject *)subexpression->ob_type == (PyObject *)SQLRaw ||
                (raw && (PyString_CheckExact(subexpression) ||
                         PyUnicode_CheckExact(subexpression)))) {
#endif
                Py_INCREF(subexpression);
                statement = subexpression;
            /* elif subexpression_type in (tuple, list): */
            } else if (PyTuple_CheckExact(subexpression) ||
                       PyList_CheckExact(subexpression)) {
                /* state.precedence = outer_precedence */
                CATCH(-1, PyObject_SetAttrString(
                    state, "precedence", outer_precedence)
                );

                /* statement = self(subexpression, state, join, raw, token) */
                CATCH(NULL, statement = Compile_recursive_compile(
                    self, subexpression, state, join, raw, token)
                );
            /* else: */
            } else {
                /*
                    if token and (
                            subexpression_type in (binary_type, text_type)):
                */
#ifdef _PY3
                if (token && (PyUnicode_CheckExact(subexpression) ||
                              PyBytes_CheckExact(subexpression))) {
#else
                if (token && (PyUnicode_CheckExact(subexpression) ||
                              PyString_CheckExact(subexpression))) {
#endif
                    /* subexpression = SQLToken(subexpression) */
                    CATCH(NULL, subexpression = PyObject_CallFunctionObjArgs(
                        SQLToken, subexpression, NULL)
                    );
                } else {
                    Py_INCREF(subexpression);
                }

                /*
                    statement = self._compile_single(
                        subexpression, state, outer_precedence
                    )
                */
                statement = Compile__single(
                    self, subexpression, state, outer_precedence
                );

                Py_DECREF(subexpression);
                CATCH(NULL, statement);
            }
            /* compiled.append(statement) */
            CATCH(-1, PyList_Append(compiled, statement));
            Py_CLEAR(statement);
        }
        Py_CLEAR(sequence);

        /* statement = join.join(compiled) */
        CATCH(NULL, statement = PyUnicode_Join(join, compiled));
        Py_CLEAR(compiled);
    } else {
        /*
            statement = self._compile_single(
                expression, state, outer_precedence
            )
        */
        CATCH(NULL, statement = Compile__single(
            self, expression, state, outer_precedence)
        );
    }

    /* state.precedence = outer_precedence */
    CATCH(-1, PyObject_SetAttrString(state, "precedence", outer_precedence));
    Py_CLEAR(outer_precedence);
    Py_DECREF(expression);

    return statement;

error:
    Py_XDECREF(expression);
    Py_XDECREF(outer_precedence);
    Py_XDECREF(compiled);
    Py_XDECREF(sequence);
    Py_XDECREF(statement);

    return NULL;
}

static PyObject *
Compile__call__(CompileObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {
        "expression", "state", "join", "raw", "token", NULL
    };
    PyObject *expression = NULL;
    PyObject *state = Py_None;
    PyObject *join;
    char raw = 0;
    char token = 0;

    PyObject *result = NULL;

    if (!initialize_globals())
        return NULL;

    join = default_compile_join;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OUbb", kwlist,
                                &expression, &state, &join, &raw, &token)) {
        return NULL;
    }

    /*
        if state is None:
            state = State()
    */
    if (state == Py_None) {
        state = PyObject_CallFunctionObjArgs(State, NULL);
    } else {
        Py_INCREF(state);
    }

    if (state) {
        result = Compile_recursive_compile(
            self, expression, state, join, raw, token);
        Py_DECREF(state);
    }
    return result;
}

static PyMethodDef Compile_methods[] = {
    {"when", (PyCFunction)Compile_when, METH_VARARGS, NULL},
    {"get_precedence", (PyCFunction)Compile_get_precedence, METH_O, NULL},
    {"create_child", (PyCFunction)Compile_create_child, METH_NOARGS, NULL},
    {"_update_cache", (PyCFunction)Compile__update_cache, METH_NOARGS, NULL},
    {"is_reserved_word", (PyCFunction)Compile_is_reserved_word, METH_O, NULL},
    {"set_precedence", (PyCFunction)Compile_set_precedence, METH_VARARGS, NULL},
    {"add_reserved_words",
        (PyCFunction)Compile_add_reserved_words, METH_O, NULL},
    {"remove_reserved_words",
        (PyCFunction)Compile_remove_reserved_words, METH_O, NULL},
    {NULL  /* sentinel */}
};

static PyMemberDef Compile_members[] = {
    {"_local_dispatch_table", T_OBJECT, offsetof(
                                CompileObject, _local_dispatch_table), 0, 0},
    {"_local_precedence", T_OBJECT, offsetof(
                                    CompileObject, _local_precedence), 0, 0},
    {"_local_reserved_words", T_OBJECT, offsetof(
                                CompileObject, _local_reserved_words), 0, 0},
    {"_dispatch_table", T_OBJECT, offsetof(
                                      CompileObject, _dispatch_table), 0, 0},
    {"_precedence", T_OBJECT, offsetof(CompileObject, _precedence), 0, 0},
    {"_reserved_words", T_OBJECT, offsetof(CompileObject, _precedence), 0, 0},
    {"_children", T_OBJECT, offsetof(CompileObject, _children), 0, 0},
    {"_parents", T_OBJECT, offsetof(CompileObject, _parents), 0, 0},
    {NULL  /* sentinel */}
};

#ifdef _PY3
static PyTypeObject Compile_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)                      /*ob_size*/
#else
statichere PyTypeObject Compile_Type = {
    PyObject_HEAD_INIT(NULL)
    0,                                                  /*ob_size*/
#endif
    "txorm.compiler.Compile",                           /*tp_name*/
    sizeof(CompileObject),                              /*tp_basicsize*/
    0,                                                  /*tp_itemsize*/
    (destructor)Compile_dealloc,                        /*tp_dealloc*/
    0,                                                  /*tp_print*/
    0,                                                  /*tp_getattr*/
    0,                                                  /*tp_setattr*/
    0,                                                  /*tp_compare*/
    0,                                                  /*tp_repr*/
    0,                                                  /*tp_as_number*/
    0,                                                  /*tp_as_sequence*/
    0,                                                  /*tp_as_mapping*/
    0,                                                  /*tp_hash*/
    (ternaryfunc)Compile__call__,                       /*tp_call*/
    0,                                                  /*tp_str*/
    PyObject_GenericGetAttr,                            /*tp_getattro*/
    PyObject_GenericSetAttr,                            /*tp_setattro*/
    0,                                                  /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE |
        Py_TPFLAGS_HAVE_GC,                             /*tp_flags*/
    0,                                                  /*tp_doc*/
    (traverseproc)Compile_traverse,                     /*tp_traverse*/
    (inquiry)Compile_clear,                             /*tp_clear*/
    0,                                                  /*tp_richcompare*/
    offsetof(CompileObject, __weakreflist),             /*tp_weaklistoffset*/
    0,                                                  /*tp_iter*/
    0,                                                  /*tp_iternext*/
    Compile_methods,                                    /*tp_methods*/
    Compile_members,                                    /*tp_members*/
    0,                                                  /*tp_getset*/
    0,                                                  /*tp_base*/
    0,                                                  /*tp_dict*/
    0,                                                  /*tp_descr_get*/
    0,                                                  /*tp_descr_set*/
    0,                                                  /*tp_dictoffset*/
    (initproc)Compile_init,                             /*tp_init*/
    PyType_GenericAlloc,                                /*tp_alloc*/
    PyType_GenericNew,                                  /*tp_new*/
    PyObject_GC_Del,                                    /*tp_free*/
    0,                                                  /*tp_is_gc*/
};


static PyMethodDef _compiler_methods[] = {
    {NULL, NULL}      /* sentinel */
};

#ifdef _PY3
static struct PyModuleDef _compiler_module = {
    PyModuleDef_HEAD_INIT,
    "_compiler",
    "C Implementation of the txorm Compile type",
    -1,
    _compiler_methods,
    NULL, NULL, NULL, NULL
};
#endif

#ifndef PyMODINIT_FUNC  /* declarations for DLL import/export */
    #define PyMODINIT_FUNC void
#endif

PyMODINIT_FUNC
#ifdef _PY3
PyInit__compiler(void)
#else
init_compiler(void)
#endif
{
    PyObject* m;

    if(PyType_Ready(&Compile_Type) < 0)
#ifdef _PY3
        return NULL;
#else
        return;
#endif

#ifdef _PY3
    m = PyModule_Create(&_compiler_module);
    if (m == NULL)
        return NULL;
#else
    m = Py_InitModule3(
        "_compiler",
         _compiler_methods,
          "C Implementation of the txorm Compile type"
    );
    if (m == NULL)
        return;
#endif

    Py_INCREF(&Compile_Type);
    PyModule_AddObject(m, "Compile", (PyObject *)&Compile_Type);
#ifdef _PY3
    return m;
#endif
}
