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


def test_name_set_in_constructor():
    g = dfp.DataflowEnvironment(name='test')
    assert g.name == 'test'


def test_name_set_attr():
    g = dfp.DataflowEnvironment()
    g.name = 'test'
    assert g.name == 'test'


class TestDataflowEnvironment(unittest.TestCase):
    def setUp(self):
        self.attr_f = {'color': 'red'}
        self.data_in = DataGenerator()
        self.data_out = DataAppender()
        self.results = [1010, 1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019]

    def test_main_callables(self):
        out = []
        for x in self.data_in.gen():
            y = f(x)
            z = g(y)
            out.append(z)
        assert out == self.results

    def test_main_generators(self):
        f_gen = generator_from_func(f)
        g_gen = generator_from_func(g)
        gen = self.data_out.gen(g_gen(f_gen(self.data_in.gen())))
        while True:
            try:
                gen.next()
            except StopIteration:
                break
        assert self.data_out.out == self.results

    def test_main_generators_partial(self):
        f_gen = generator_from_func(f)
        g_gen = generator_from_func(g)
        gen = g_gen(f_gen(self.data_in.gen()))
        out = []
        for z in gen:
            out.append(z)
        assert out == self.results

    def test_main_coroutines(self):
        f_co = coroutine_from_func(f)
        g_co = coroutine_from_func(g)
        co = self.data_in.co(f_co(g_co(self.data_out.co())))
        while True:
            try:
                co.next()
            except StopIteration:
                break
        assert self.data_out.out == self.results

    def test_main_coroutines_partial(self):
        f_co = coroutine_from_func(f)
        g_co = coroutine_from_func(g)
        co = f_co(g_co(self.data_out.co()))
        for x in self.data_in.gen():
            co.send(x)
        assert self.data_out.out == self.results

    def test_DataflowEnvironment1(self):
        """
        datain is a generator
        f is a coroutine
        g is a funcion
        terminal is a function
        """
        attr_f = self.attr_f
        data_in = self.data_in
        data_out = self.data_out

        dtf = dfp.DataflowEnvironment()
        dtf.add_gentask('indata', data_in.gen, initial=True)
        dtf.add_cotask('f', coroutine_from_func(f), filters='indata', **attr_f)
        dtf.add_task('g', g, filters=dict(args=['f']))
        dtf.add_task('terminal', data_out.func, filters=dict(args=['g']))
        dtf.start()
        dtf.run()
        assert data_out.out == self.results

    def test_DataflowEnvironment2(self):
        """
        datain is a function
        f is a coroutine
        g is a funcion
        terminal is a function
        """
        attr_f = self.attr_f
        data_in = self.data_in
        data_out = self.data_out

        dtf = dfp.DataflowEnvironment()
        dtf.add_task('indata', data_in.func)
        dtf.add_cotask('f', coroutine_from_func(f),
                       filters='indata',
                       **attr_f)
        dtf.add_task('g', g,
                     filters=dict(args=['f']))
        dtf.add_task('terminal', data_out.func,
                     filters=dict(args=['g']))
        dtf.start()
        dtf.run()

        assert data_out.out == self.results

    def test_DataflowEnvironment3(self):
        """
        datain is a coroutine
        f is a coroutine
        g is a funcion
        terminal is a function
        """
        attr_f = self.attr_f
        data_in = self.data_in
        data_out = self.data_out

        dtf = dfp.DataflowEnvironment()
        dtf.add_cotask('indata', data_in.co)
        dtf.add_cotask('f', coroutine_from_func(f),
                       filters='indata',
                       **attr_f)
        dtf.add_task('g', g,
                     filters=dict(args=['f']))
        dtf.add_task('terminal', data_out.func,
                     filters=dict(args=['g']))
        dtf.start()
        dtf.run()
        assert data_out.out == self.results

    def test_DataflowEnvironment4(self):
        """
        datain is a coroutine made from datain func with coroutine from func...
        f is a coroutine
        g is a funcion
        terminal is a function
        """
        attr_f = self.attr_f
        data_in = self.data_in
        data_out = self.data_out

        dtf = dfp.DataflowEnvironment()
        dtf.add_cotask('indata', coroutine_from_func(data_in.func))
        dtf.add_cotask('f', coroutine_from_func(f),
                       filters='indata',
                       **attr_f)
        dtf.add_task('g', g,
                     filters=dict(args=['f']))
        dtf.add_task('terminal', data_out.func,
                     filters=dict(args=['g']))
        dtf.start()
        dtf.run()
        assert data_out.out == self.results

    def test_DataflowEnvironmentAutomaticOrderInLock(self):
        """
        The nodes are specified in the wrong order.
        The excution plan is determined in the lock() method.

        
        datain is a coroutine made from datain func with coroutine from func...
        f is a coroutine
        g is a funcion
        terminal is a function
        """
        attr_f = self.attr_f
        data_in = self.data_in
        data_out = self.data_out

        dtf = dfp.DataflowEnvironment()
        dtf.add_task('terminal', data_out.func,
                     filters=dict(args=['g']))
        dtf.add_cotask('f', coroutine_from_func(f),
                       filters='indata',
                       **attr_f)
        dtf.add_task('g', g,
                     filters=dict(args=['f']))
        dtf.add_cotask('indata', coroutine_from_func(data_in.func))
        dtf.start()
        dtf.run()
        assert data_out.out == self.results

    def test_DataflowEnvironment_NotNamedTasks(self):
        """
        The nodes are specified in the wrong order.
        The excution plan is determined in the lock() method.

        
        datain is a coroutine made from datain func with coroutine from func...
        f is a coroutine
        g is a funcion
        terminal is a function
        """
        attr_f = self.attr_f
        data_in = self.data_in
        data_out = self.data_out
        f_co = coroutine_from_func(f)
        in_co = coroutine_from_func(data_in.func)

        dtf = dfp.DataflowEnvironment()
        dtf.add_task(data_out.func, filters=dict(args=[g]))
        dtf.add_cotask(f_co, filters=in_co, **attr_f)
        dtf.add_task(g, filters=dict(args=[f_co]))
        dtf.add_cotask(in_co)
        dtf.start()
        dtf.run()
        assert data_out.out == self.results

    def test_DataflowEnvironment_AplyNonTrivialFilters(self):
        """
        The nodes are specified in the wrong order.
        The excution plan is determined in the lock() method.

        We apply a non-trivial filter that should not be executed in the exection plan determination

        datain is a coroutine made from datain func with coroutine from func...
        f is a coroutine
        g is a funcion
        terminal is a function
        """
        attr_f = self.attr_f
        data_in = self.data_in
        data_out = self.data_out
        f_co = coroutine_from_func(f)
        in_co = coroutine_from_func(data_in.func)
        end_func = data_out.func

        dtf = dfp.DataflowEnvironment()
        dtf.add_task(end_func, filters=dict(args=[(g, lambda x: -float(str(x)))]))
        dtf.add_cotask(in_co)
        dtf.add_task(g, filters=dict(args=[(f_co, lambda x: -float(str(x)))]))
        dtf.add_cotask(f_co, filters=(in_co, lambda x: -float(str(x))), **attr_f)
        dtf.start()
        dtf.run()

        assert data_out.out == [-990.0, -991.0, -992.0, -993.0, -994.0, -995.0, -996.0, -997.0, -998.0, -999.0]

    def test_DataflowEnvironment_WithArgs(self):
        """
        The nodes are specified in the wrong order.
        The excution plan is determined in the lock() method.
    
        There is no data in generator but arguments.
        
        datain is a coroutine made from datain func with coroutine from func...
        f is a coroutine
        g is a funcion
        terminal is a function
        """
        attr_f = self.attr_f
        data_in = self.data_in
        data_out = self.data_out
        f_co = coroutine_from_func(f)

        dtf = dfp.DataflowEnvironment()
        dtf.args = ['my_data']
        dtf.add_cotask(f_co, filters=('call_args', 'my_data'), **attr_f)
        dtf.add_task(g, filters=dict(args=[f_co]))
        dtf.add_task(data_out.func, filters=dict(args=[g]))
        dtf.start()

        # dtf.run()
        for i in range(10):
            dtf(i)
        assert data_out.out == self.results

        in_co = coroutine_from_func(data_in.func)
        # dtf.add_cotask(in_co)

    def test_DataflowEnvironment_WithKwArgs(self):
        """
        The nodes are specified in the wrong order.
        The excution plan is determined in the lock() method.
    
        There is no data in generator but arguments.
        
        datain is a coroutine made from datain func with coroutine from func...
        f is a coroutine
        g is a funcion
        terminal is a function
        """
        attr_f = self.attr_f
        data_in = self.data_in
        data_out = self.data_out
        f_co = coroutine_from_func(f)

        dtf = dfp.DataflowEnvironment()
        dtf.args = ['my_data']
        dtf.add_cotask(f_co, filters=('call_args', 'my_data'), **attr_f)
        dtf.add_task(g, filters=dict(args=[f_co]))
        dtf.add_task(data_out.func, filters=dict(args=[g]))
        dtf.start()

        # dtf.run()
        for i in range(10):
            dtf(my_data=i)
        assert data_out.out == self.results

        in_co = coroutine_from_func(data_in.func)
        # dtf.add_cotask(in_co)

    def test_DataflowEnvironment_ReturnData(self):
        """
        The nodes are specified in the wrong order.
        The excution plan is determined in the lock() method.
    
        There is no data in generator but arguments.
        
        datain is a coroutine made from datain func with coroutine from func...
        f is a coroutine
        g is a funcion
        terminal is a function
        """
        attr_f = self.attr_f
        data_in = self.data_in
        data_out = self.data_out
        f_co = coroutine_from_func(f)

        dtf = dfp.DataflowEnvironment()
        in_co = coroutine_from_func(data_in.func)
        dtf.add_cotask(in_co)
        dtf.add_cotask(f_co, filters=in_co, **attr_f)
        dtf.add_task(g, filters=dict(args=[f_co]))
        dtf.add_edge_call_rets(g)
        dtf.start()

        # dtf.run()
        results = []
        for i in range(10):
            res = dtf()
            results.append(res)
        assert results == self.results
        # dtf.add_cotask(in_co)

    def test_DataflowEnvironment_as_Generator(self):
        """
        The nodes are specified in the wrong order.
        The excution plan is determined in the lock() method.
    
        There is no data in generator but arguments.
        
        datain is a coroutine made from datain func with coroutine from func...
        f is a coroutine
        g is a funcion
        terminal is a function
        """
        attr_f = self.attr_f
        data_in = self.data_in
        data_out = self.data_out
        f_co = coroutine_from_func(f)

        dtf = dfp.DataflowEnvironment()
        in_co = coroutine_from_func(data_in.func)
        dtf.add_cotask(in_co)
        dtf.add_cotask(f_co, filters=in_co, **attr_f)
        dtf.add_task(g, filters=dict(args=[f_co]))
        dtf.add_edge_call_rets(g)
        dtf.start()

        results = []
        for res in dtf.gen():
            results.append(res)
        assert results == self.results

    def test_DataflowEnvironment_as_Generator_Chained_Partial(self):
        """
        The nodes are specified in the wrong order.
        The excution plan is determined in the lock() method.
    
        There is no data in generator but arguments.
        
        datain is a coroutine made from datain func with coroutine from func...
        f is a coroutine
        g is a funcion
        terminal is a function
        """
        attr_f = self.attr_f
        data_in = self.data_in
        data_out = self.data_out
        f_co = coroutine_from_func(f)
        dtf = dfp.DataflowEnvironment()
        dtf.add_cotask(f_co, filters='call_args', **attr_f)
        dtf.add_task(g, filters=dict(args=[f_co]))
        dtf.add_edge_call_rets(g)
        dtf.start()

        results = []
        for res in dtf.gen(data_in.gen()):
            results.append(res)
        assert results == self.results

    def test_DataflowEnvironment_as_Generator_Chained(self):
        """
        The DataflowEnvironment instance is chained with two other generators
        """
        attr_f = self.attr_f
        data_in = self.data_in
        data_out = self.data_out
        f_co = coroutine_from_func(f)
        dtf = dfp.DataflowEnvironment()
        dtf.add_cotask(f_co, filters='call_args', **attr_f)
        dtf.add_task(g, filters=dict(args=[f_co]))
        dtf.add_edge_call_rets(g)
        dtf.start()

        gen = data_out.gen(dtf.gen(data_in.gen()))

        while True:
            try:
                gen.next()
            except StopIteration:
                break

        assert data_out.out == self.results

    def test_DataflowEnvironment_as_Coroutine_Chained(self):
        """
        The DataflowEnvironment instance is chained with two other coroutines
        """
        attr_f = self.attr_f
        dtf = dfp.DataflowEnvironment()
        dtf.call_args = 'x'
        dtf.add_task(f, filters=dict(args=['call_args']), **attr_f)
        dtf.add_task(g, filters=dict(args=[f]))
        dtf.add_edge_call_rets(g)
        dtf.start()

        data_in = self.data_in
        data_out = self.data_out

        co = data_in.co(dtf.co(data_out.co()))
        while True:
            try:
                co.next()
            except StopIteration:
                break

        assert data_out.out == self.results

    def test_DataflowEnvironment_as_Coroutine_Chained_Partial(self):
        """
        The DataflowEnvironment instance is chained with two other coroutines
        """
        attr_f = self.attr_f
        dtf = dfp.DataflowEnvironment()
        dtf.add_task(f, filters=dict(args=['call_args']), **attr_f)
        dtf.add_task(g, filters=dict(args=[f]))
        dtf.add_edge_call_rets(g)
        dtf.start()

        data_in = self.data_in
        data_out = self.data_out

        co = dtf.co(data_out.co())
        for x in data_in.gen():
            co.send(x)
        assert data_out.out == self.results

    def test_DataflowEnvironment_with_generator_task(self):
        """
        The DataflowEnvironment instance is chained with two other coroutines
        """
        attr_f = self.attr_f
        f_gen = generator_from_func(f)
        g_gen = generator_from_func(g)
        dtf = dfp.DataflowEnvironment()
        dtf.add_gentask('f', f_gen, filters='call_args', **attr_f)
        dtf.add_gentask('g', g_gen, filters='f')
        dtf.add_edge_call_rets('g')
        dtf.start()

        data_in = self.data_in
        data_out = self.data_out

        co = data_in.co(dtf.co(data_out.co()))
        while True:
            try:
                co.next()
            except StopIteration:
                break

        assert data_out.out == self.results

    def test_DataflowEnvironment_with_generator_task_unamed(self):
        """
        The DataflowEnvironment instance is chained with two other coroutines
        """
        attr_f = self.attr_f
        f_gen = generator_from_func(f)
        g_gen = generator_from_func(g)
        dtf = dfp.DataflowEnvironment()
        dtf.add_gentask(f_gen, filters='call_args', **attr_f)
        dtf.add_gentask(g_gen, filters=f_gen)
        dtf.add_edge_call_rets(g_gen)
        dtf.start()

        data_in = self.data_in
        data_out = self.data_out

        co = data_in.co(dtf.co(data_out.co()))
        while True:
            try:
                co.next()
            except StopIteration:
                break

        assert data_out.out == self.results
