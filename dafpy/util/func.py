import collections
import inspect
import dafpy.defs as dfpd

import dafpy as dfp

def func_name(func):
    try:
        name = func.__name__
    except:
        name = func.__class__.__name__
    return name

def set_call_args(func, call_spec, name = None):
    if name is None:
        name = func_name(func)

    CallSpec = collections.namedtuple(name, call_spec)    
        
    setattr(func, dfpd.CALL_SPEC_ATTR, CallSpec)
    return func


def set_call_rets(func, ret_spec, name = None):
    if name is None:
        name = func_name(func)
    
    RetSpec = collections.namedtuple(name, ret_spec)    
    setattr(func, dfpd.RET_SPEC_ATTR, RetSpec)
    return func


def set_call_rets_decorator(ret_names):
    def wrap_rets(func):
        spec = inspect.getargspec(func)
        setted_rets_spec = collections.namedtuple(func_name(func), ret_names)
        original_call_spec = collections.namedtuple(func_name(func), spec.args)
        def wrapper(*l,**args):
            nlist = l
            if args:
                nlist += original_call_spec(**args)
            res = func(*nlist)
            return setted_rets_spec(res)
        set_call_rets(wrapper, ret_names, func_name(func))
        set_call_args(wrapper, spec.args, func_name(func) )
        return wrapper    
    return wrap_rets



def get_call_args(node_callable):
    """
    inpect node_callable input names
    """
    if hasattr(node_callable, dfpd.CALL_SPEC_ATTR):
        if hasattr( getattr(node_callable, dfpd.CALL_SPEC_ATTR),'_fields'):
            return getattr(getattr(node_callable, dfpd.CALL_SPEC_ATTR),'_fields')
        
    try:
        args =  inspect.getargspec(node_callable)[0]
    except:
        args =  inspect.getargspec(getattr(node_callable,'__call__'))[0]
        
    if len(args) and args[0] == 'self':
        args.pop(0)
    return args


def get_call_rets(node_callable):
    """
    inspect if node_callable has named output
    """
    if hasattr(node_callable, dfpd.RET_SPEC_ATTR):
        if hasattr(getattr(node_callable,dfpd.RET_SPEC_ATTR),'_fields'):
            return getattr(getattr(node_callable,dfpd.RET_SPEC_ATTR),'_fields')
    return
