import unittest
import collections

import dafpy as dfp
import dafpy.exceptions as dfpe


######################################
# Some definitions for the tests
######################################

class DataGenerator(object):
    def __call__(self):
        return 10


def decision(position, observation):
    return 10


def execution(trade):
    return trade * 0.99


def add(a, b):
    return a + b


class DataAppender(object):
    def __init__(self):
        self.data = []

    def __call__(self, data):
        self.data.append(data)


class TestDAFPYSimple(unittest.TestCase):
    def test_terminal_node_data_with_tasks_and_automatic_link_no_names(self):
        # declare functions
        position_update = lambda position, trade: position + trade
        pos_lag = dfp.Lag(0)

        g = dfp.DataflowEnvironment()
        g.args = 'observation'
        g.rets = 'position'
        g.add_task(decision, filters=dict(args=[pos_lag]))
        g.add_edge_call_args(decision)
        g.add_task(execution)
        g.add_task(position_update, filters=dict(args=[execution, pos_lag]))
        g.add_task(pos_lag)
        g.add_edge_call_rets(pos_lag)
        g.start()

        datas = []
        for __ in range(10):
            data = g(observation=10)
            datas.append(data.position)

        assert datas[-1] == 89.10000000000001

    def test_terminal_node_data_no_names(self):
        # declare functions
        position_update = add  # lambda position, trade : position +  trade
        pos_lag = dfp.Lag(0)

        g = dfp.DataflowEnvironment()
        g.args = 'observation'
        g.rets = 'position'
        g.add_node(decision)
        g.add_node(execution, rets='trade')
        g.add_node(position_update, args='position,trade')
        g.add_node(pos_lag, args='position', rets='position')

        g.add_edge(pos_lag, decision)
        g.add_edge(decision, execution)
        g.add_edge(execution, position_update)
        g.add_edge(pos_lag, position_update)
        g.add_edge(position_update, pos_lag)

        g.add_edge_call_args(decision)
        g.add_edge_call_rets(pos_lag)
        g.start()

        datas = []
        for __ in range(10):
            data = g(observation=10)
            datas.append(data.position)

        assert datas[-1] == 89.10000000000001

    def test_terminal_node_data(self):
        g = dfp.DataflowEnvironment()
        g.add_node('Decision', decision, rets='trade')
        g.add_node('Execution', execution, rets='trade')
        g.add_node('PositionUpdate', add, args='position,trade', rets='position')
        g.add_node('LaggedPosition', dfp.Lag(0), args='position', rets='position')

        g.add_edge('LaggedPosition', 'Decision')
        g.add_edge('Decision', 'Execution')
        g.add_edge('Execution', 'PositionUpdate')
        g.add_edge('LaggedPosition', 'PositionUpdate')
        g.add_edge('PositionUpdate', 'LaggedPosition')

        g.set_call_args('observation')
        g.set_call_rets('position')
        g.add_edge_call_args('Decision')
        g.add_edge_call_rets('LaggedPosition')
        g.start()

        datas = []
        for __ in range(10):
            data = g(observation=10)
            datas.append(data.position)

        assert datas[-1] == 89.10000000000001

    def test_terminal_node_data_with_tasks(self):
        g = dfp.DataflowEnvironment()
        g.add_task('Decision', decision, rets='trade', filters=dict(args=[('LaggedPosition', 'position')]))
        g.add_task('Execution', execution, rets='trade', filters=dict(args=[('Decision', 'trade')]))
        g.add_task('PositionUpdate', add, args='position,trade', rets='position',
                   filters=dict(args=[('Execution', 'trade'), ('LaggedPosition', 'position')]))
        g.add_task('LaggedPosition', dfp.Lag(0), args='position', rets='position',
                   filters=dict(args=[('PositionUpdate', 'position')]))
        g.args = 'observation'
        g.add_edge_call_args('Decision')
        g.rets = 'position'
        g.add_edge_call_rets('LaggedPosition')
        g.start()

        datas = []
        for __ in range(10):
            data = g(observation=10)
            datas.append(data.position)

        assert datas[-1] == 89.10000000000001

    def test_terminal_node_data_with_tasks_and_automatic_link(self):
        g = dfp.DataflowEnvironment()
        g.args = 'observation'
        g.add_task('Decision', decision, rets='trade', filters=dict(args=['LaggedPosition']))
        g.add_edge_call_args('Decision')
        g.add_task('Execution', execution, rets='trade')
        g.add_task('PositionUpdate', add, args='trade, position', rets='position',
                   filters=dict(args=['Execution', 'LaggedPosition']))
        g.add_task('LaggedPosition', dfp.Lag(0), args='position', rets='position')
        g.rets = 'position'
        g.add_edge_call_rets('LaggedPosition')
        g.start()

        datas = []
        for __ in range(10):
            data = g(observation=10)
            datas.append(data.position)

        assert datas[-1] == 89.10000000000001

    def test_UnlinkedOutputRunTimeError(self):
        g = dfp.DataflowEnvironment()
        g.add_node('Execution', execution)
        g.set_call_args('obs')
        g.add_edge_call_args('Execution')
        g.start()
        assert None == g(10)
        # f = lambda : g(10)
        # self.assertRaises(dfpe.UnlinkedOutputRunTimeError, f)

    def off_test_raise_call_rets_not_specified(self):
        g = dfp.DataflowEnvironment()
        g.add_node('Execution', execution, rets='trade')
        g.set_call_args('obs')
        f = lambda: g.add_edge_call_rets('Execution')
        self.assertRaises(dfpe.DafpyError, f)

    def test_raise_call_args_not_specified(self):
        g = dfp.DataflowEnvironment()
        g.add_node('Execution', execution, rets='trade')
        f = lambda: g.add_edge_call_args('Execution')
        self.assertRaises(dfpe.DafpyError, f)

    def test_raise_UnlinkedInputError(self):
        g = dfp.DataflowEnvironment()
        g.add_node('Execution', execution, rets='trade')
        g.set_call_rets('obs')
        g.add_edge_call_rets('Execution')
        f = lambda: g.start()
        self.assertRaises(dfpe.UnlinkedInputError, f)


class TestDAFPYSimple2(unittest.TestCase):
    def setUp(self):
        g = dfp.DataflowEnvironment(verbose=True)
        g.set_call_args('u')
        g.set_call_rets('y')

        g.add_node('f', lambda x: x)
        g.add_node('g', lambda x: x)
        g.add_edge('f', 'g',
                   apply=lambda x: -.9 * x)
        g.add_edge_call_args('f')
        g.add_edge_call_rets('g')
        g.start()
        self.g = g

    def test_pprint(self):
        self.g.pprint()

    def test_apply(self):
        assert self.g(10).y == -0.9 * 10
