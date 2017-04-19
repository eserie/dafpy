import dafpy.util.filters as dfpf
from functools import wraps
def generator_from_func(func):
    @wraps(func)
    def f_gen(env):
        while True:
            data_ = env.next()
            args, kwargs = dfpf.data_to_args_kwargs(data_)
            yield func(*args, **kwargs)
    return f_gen

class func_from_generator(object):
    def __init__(self, gen):
        """
        :param gen: generator object
        """
        self.gen = gen
    def __call__(self):
        return self.gen.next()
        
def coroutine(func):
    @wraps(func)
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return start

def coroutine_from_func(func):
    @wraps(func)
    @coroutine
    def f_co(env):
        while True:
            data_ = yield
            args, kwargs = dfpf.data_to_args_kwargs(data_)
            res = func(*args, **kwargs)
            env.send(res)
    #setattr(f_co,'funcname', func.funcname)
    return f_co

def coroutine_from_class(cls, *clsargs, **clskwargs):
    call = clskwargs.get('call','__call__')
    @wraps(cls)    
    @coroutine
    def f_co(env,*args, **kwargs):
        if clskwargs:
            if clsargs:
                obj = cls(*clsargs,**clskwargs)
            else:
                obj = cls(**clskwargs)
        else:
            if clsargs:
                obj = cls(*clsargs)
            else:
                obj = cls()

        func = getattr(obj,call)
        assert callable(func)

        while True:
            data = yield
            args, kwargs = data
            res = func(*args, **kwargs)
            env.send(res)
    #setattr(f_co,'funcname', func.funcname)
    return f_co
        
class func_from_coroutine(object):
    def __init__(self, co, *args, **kwargs):
        env = self.env()
        self.co = co(env, *args, **kwargs)
        self.res = None

    @coroutine
    def env(self):
        while True:
            self.res = yield

    def __call__(self,*args, **kwargs):
        data = (args, kwargs)
        self.co.send(data)
        return self.res

    
    
