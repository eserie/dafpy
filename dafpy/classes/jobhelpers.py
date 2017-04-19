import inspect

__all__ = ['job_delayed', 'concat_joblists', 'delayed', 'ParallelJobs']


#############################################
# Helper functions
#############################################

#####################################
# copied from joblib and simplified
#####################################

def format_signature(func, *args, **kwargs):
    from joblib.logger import pformat
    # XXX: Should this use inspect.formatargvalues/formatargspec?
    arg_str = list()
    previous_length = 0
    for arg in args:
        arg = pformat(arg, indent=2)
        if len(arg) > 1500:
            arg = '%s...' % arg[:700]
        if previous_length > 80:
            arg = '\n%s' % arg
        previous_length = len(arg)
        arg_str.append(arg)
    arg_str.extend(['%s=%s' % (v, pformat(i)) for v, i in kwargs.items()])
    arg_str = ', '.join(arg_str)

    signature = '%s(%s)' % (func.__name__, arg_str)
    return signature


###############################
# simple decorators
###############################

def delayed(func):
    def job_func(*args, **kwargs):
        return (func, args, kwargs)

    return job_func


###############################
# runner decorators
###############################

def concat_joblists(*joblists):
    if not joblists:
        return
    try:
        concat_joblist = list()
        for joblist in joblists:
            if joblist is not None:
                concat_joblist += joblist
        return concat_joblist
    except TypeError:
        return list(joblists)
        # return list(zip(*[joblist for joblist in joblists if joblist is not None])[0])


def job_delayed(runner, **runner_args):
    """
    Usage
    job_delayed(runner, **runner_args)(func)(*args, **kwargs)(job_ids)
    """

    def delayed_func(func):
        def job_func(*args, **kwargs):
            delayed_func = (func, args, kwargs)

            def slurm_func(*joblists):
                joblist = concat_joblists(*joblists)
                runner.append(delayed_func, depends_on=joblist, **runner_args)
                func_joblist = runner.run()
                return func_joblist

            slurm_func.__name__ = func.__name__ + '_delayed'
            # slurm_func.__name__ = format_signature(func, *args, **kwargs)
            return slurm_func

        return job_func

    return delayed_func


class ParallelJobs(object):
    def __init__(self, runner):
        self.runner = runner

    def __call__(self, jobs, **runner_args):
        def slurm_func(*job_lists):
            for job in jobs:
                self.runner.append(job, depends_on=job_lists, **runner_args)
            return self.runner.run()

        return slurm_func
