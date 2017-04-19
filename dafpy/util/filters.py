#import dafpy.defs as dfpd

def data_to_args_kwargs(data):
    if isinstance(data, list) or isinstance(data, tuple):
        args, kwargs = data
    elif isinstance(data,dict):
        args = data.get('args',[])
        kwargs = data.get('kwargs',{})
    else:
        if data is not None:
            args = [data]
        else:
            args = []
        kwargs = {}
    return args, kwargs

