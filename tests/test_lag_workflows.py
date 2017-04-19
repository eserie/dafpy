import unittest
import collections
import dafpy as dfp
from dafpy import coroutine, coroutine_from_func, func_from_coroutine, coroutine_from_class
from dafpy import generator_from_func, func_from_generator


###############################
# DEFINITIONS
###############################

def f(x):
    y = x + 10
    return y


def f2(x, y):
    return x + 10 * y


def g(y):
    z = y + 1000
    return z


class DataGenerator(object):
    def __init__(self):
        self.data = range(10)

    def func(self):
        if self.data:
            return self.data.pop(0)
        else:
            raise (StopIteration())

    @coroutine
    def co(self, env):
        yield
        for x in self.data:
            env.send(x)
            yield

    def gen(self):
        for x in self.data:
            yield x


class DataAppender(object):
    def __init__(self):
        self.out = []

    def func(self, z):
        self.out.append(z)

    def gen(self, z_gen):
        while True:
            z = z_gen.next()
            self.out.append(z)
            yield

    @coroutine
    def co(self):
        while True:
            z = yield
            self.out.append(z)


###############################
# TESTS
###############################

def test_coroutine_from_callable_obj():
    mlag = dfp.Lag(0)
    f_lag = func_from_coroutine(coroutine_from_func(mlag))
    assert 0 == f_lag(10)
    assert 10 == f_lag(11)


def test_coroutine_from_class():
    f_lag = func_from_coroutine(coroutine_from_class(dfp.Lag, 0))
    assert 0 == f_lag(10)
    assert 10 == f_lag(11)
    assert callable(f_lag)


def test_coroutine_from_func():
    f_func = func_from_coroutine(coroutine_from_func(f))
    for x in range(10):
        d1, d2 = f_func(x), f(x)
        assert d1 == d2


class TestGenerator(unittest.TestCase):
    def setUp(self):
        self.terminal_node = DataAppender()
        self.mlag = dfp.Lag(0)

        def data_gen():
            for i in range(10):
                yield [i + 1], {}

        self.data_gen = data_gen

    def test_generator_from_func(self):
        """
        Play with generators
        Remarks: we can play a lot whith it be cause when the generator is created, it has already the data i.

        At contrario coroutine can be piped without sending data.
        data is send at the end
        """
        mlag = self.mlag
        data_gen = self.data_gen
        gen_lag = generator_from_func(mlag)
        for i, res in enumerate(gen_lag(data_gen())):
            assert i == res

    def test_generator_from_func2(self):
        mlag = self.mlag
        data_gen = self.data_gen
        gen_lag = generator_from_func(mlag)
        f_func = func_from_generator(gen_lag(data_gen()))
        for i in range(10):
            assert i == f_func()


def test_workflow_with_lag():
    """
    We test a workkflow with a feedback implemented with a lag
    """
    results = [0, 1000, 11001, 111012, 1111123, 11112234, 111123345, 1111234456, 11112345567, 111123456678,
               1111234567789]
    attr_f = {'color': 'red'}
    data_in = DataGenerator()
    data_out = DataAppender()
    mlag = dfp.Lag(0)

    dtf = dfp.DataflowEnvironment()
    dtf.add_gentask('indata', data_in.gen, initial=True)
    dtf.add_task('f', f2,
                 filters=dict(args=['indata', 'lag', ]),
                 **attr_f)
    dtf.add_task('g', g, filters=dict(args=['f']))
    dtf.add_task('lag', mlag, filters=dict(args=['g']))
    dtf.add_task('terminal', data_out.func, filters=dict(args=['lag']))
    dtf.start()
    dtf.run()

    assert data_out.out == results


####################################
# Some tests on different possible Lag implementations
# This test permits to test some more advanced featchures of dataflowEnv such as passing argmuments to generators or coroutines
####################################


class LagEnvGen(dfp.DataflowEnvironment):
    def __init__(self, initial_state=None, **attr):
        dfp.DataflowEnvironment.__init__(self, **attr)

        self.add_gentask('lag', dfp.gen_lag,
                         filters='call_args',
                         gen_args=dict(args=[initial_state]),
                         reset=True)
        self.add_edge_call_rets('lag')
        self.start()

    def reset(self):
        return self()
        # self.task['lag']['co_started'].send(None)
        # return self.task['receive']['gen_started'].next()


def test_lag_env_gen():
    """
    LagEnvGen is iso functioal to dfp.Lag
    """
    lag = LagEnvGen(10)

    assert lag(11) == 10
    assert lag('a') == 11
    assert lag.reset() == 'a'
    assert lag('b') is None
    assert lag.reset() == 'b'
    assert lag('c') == None
    assert ['c'] + range(9) == list(lag.gen(iter(range(10))))


class LagEnvCo(dfp.DataflowEnvironment):
    def __init__(self, initial_state=None, **attr):
        dfp.DataflowEnvironment.__init__(self, **attr)

        self.add_cotask('lag', dfp.co_lag,
                        filters='call_args',
                        co_args=dict(args=[initial_state]),
                        reset=True)
        self.add_edge_call_rets('lag')
        self.start()

    def reset(self):
        return self()
        # self.task['lag']['co_started'].send(None)
        # return self.task['receive']['gen_started'].next()


def test_lag_env_co():
    """
    LagEnvCo is iso functioal to dfp.Lag
    """
    lag = LagEnvCo(10)

    assert lag(11) == 10
    assert lag('a') == 11
    assert lag.reset() == 'a'
    assert lag('b') is None
    assert lag.reset() == 'b'
    assert lag('c') == None
    assert ['c'] + range(9) == list(lag.gen(iter(range(10))))
