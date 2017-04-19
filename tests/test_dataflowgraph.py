import unittest
import dafpy as dfp

import collections
import numpy as np
import copy

import dafpy.exceptions as dfpe
import dafpy.defs as dfpd


######################################
# Some definitions for the tests
######################################

class DataGenerator(object):
    def __call__(self):
        return {'observations': 10}


def decision(position, observation):
    return {'trade': 10}


def execution(trade):
    return {'trade': trade * 0.99}


def add(a, b):
    return a + b


class LagWrong(dfp.Lag):
    """
    This class return a value in its call.
    It is used to raise a dfp.DuplicatedReturnedDataError Exception in the call of the graph
    """

    def __call__(self, data):
        out = copy.copy(self.state)
        self.state = data
        return out


class DataAppender(object):
    def __init__(self):
        self.data = []

    def __call__(self, data):
        self.data.append(data)


######################################
# tests
######################################

def test_init():
    g = dfp.DataflowEnvironment()


def test_name_set_in_constructor():
    g = dfp.DataflowEnvironment(name='test')
    assert g.name == 'test'


def test_name_set_attr():
    g = dfp.DataflowEnvironment()
    g.name = 'test'
    assert g.name == 'test'


def test_nodes_property():
    g = dfp.DataflowEnvironment()
    assert isinstance(g.nodes(), list)
    assert len(g.nodes()) == len(g)
    assert len(g) == 0


def test_add_path():
    G = dfp.DataflowEnvironment(name='test')  # or DiGraph, MultiGraph, MultiDiGraph, etc
    G.add_path([0, 1, 2, 3], [lambda x: x] * 4)
    assert len(G) == 4
    G.add_path([10, 11, 12], [lambda x: x] * 3, weight=7)
    assert len(G) == 7
    G.add_edge(3, 10)

    G.set_call_rets('x')
    G.set_call_args('x')
    G.add_edge_call_args(0)
    G.add_edge_call_rets(12)

    G.start()
    assert G(1) == collections.namedtuple('test', 'x')(1)


def test_add_path_common_call_attr():
    G = dfp.DataflowEnvironment(name='test')  # or DiGraph, MultiGraph, MultiDiGraph, etc
    G.add_path([0, 1, 2, 3], func=lambda x: x)
    assert len(G) == 4
    G.add_path([10, 11, 12], [lambda x: x] * 3, weight=7)
    assert len(G) == 7
    G.add_edge(3, 10)

    G.set_call_rets('x')
    G.set_call_args('x')
    G.add_edge_call_args(0)
    G.add_edge_call_rets(12)

    G.start()
    assert G(1) == collections.namedtuple('test', 'x')(1)


def test_add_nodes_from():
    G = dfp.DataflowEnvironment()  # or DiGraph, MultiGraph, MultiDiGraph, etc
    G.add_nodes_from('Helo', func=lambda x: x)
    K3 = dfp.DataflowEnvironment()
    K3.add_path([0, 1, 2], func=lambda x: x)
    G.add_nodes_from(K3)
    assert sorted(G.nodes(), key=str) == [0, 1, 2, 'H', 'e', 'l', 'o']

    G.add_nodes_from([3, 4], func=lambda x: x, size=10)
    G.add_nodes_from([5, 6], func=lambda x: x, weight=0.4)

    # Use (node, attrdict) tuples to update attributes for specific
    #    nodes.
    G.add_nodes_from([(7, dict(size=11)), (8, {'color': 'blue'})], func=lambda x: x)
    assert G.node[7]['size'] == 11
    H = dfp.DataflowEnvironment()
    H.add_nodes_from(G)
    assert H.node[7]['size'] == 11


def test_get_item():
    G = dfp.DataflowEnvironment()  # or DiGraph, MultiGraph, MultiDiGraph, etc
    G.add_path([0, 1, 2, 3], [lambda x: x] * 4)
    assert G[0] == {dfpd.OBJ_CALL_RET: {1: {'x': {}}}}


def test_len():
    G = dfp.DataflowEnvironment()  # or DiGraph, MultiGraph, MultiDiGraph, etc
    G.add_path([0, 1, 2, 3], [lambda x: x] * 4)
    assert len(G) == 4


def test_contains():
    G = dfp.DataflowEnvironment()  # or DiGraph, MultiGraph, MultiDiGraph, etc
    G.add_path([0, 1, 2, 3], [lambda x: x] * 4)
    assert 1 in G


def test_contains_error():
    G = dfp.DataflowEnvironment()  # or DiGraph, MultiGraph, MultiDiGraph, etc
    G.add_path([0, 1, 2, 3], [lambda x: x] * 4)
    assert 1 in G
    assert (4 not in G)
    assert ('b' not in G)
    assert ([] not in G)  # no exception for nonhashable
    assert ({1: 1} not in G)  # no exception for nonhashable


def test_iter():
    G = dfp.DataflowEnvironment()  # or DiGraph, MultiGraph, MultiDiGraph, etc
    nodes = [0, 1, 2, 3]
    G.add_path(nodes, [lambda x: x] * 4)
    for i, (n, _) in enumerate(G):
        assert n == nodes[i]


def test_str():
    G = dfp.DataflowEnvironment(name='foo')
    assert str(G) == 'foo'


class TestDAFPYErrors(unittest.TestCase):
    def test_add_node_OK(self):
        g = dfp.DataflowEnvironment()
        g.add_node(1, lambda: 1, size=10)

    def test_add_node_raise_DafpyError(self):
        g = dfp.DataflowEnvironment()
        f = lambda: g.add_node(1, lambda: 1, size=10, attr_dict=1)
        self.assertRaises(dfpe.DafpyError, f)

    def test_add_edge_OK(self):
        g = dfp.DataflowEnvironment()
        g.add_node('data_appender', DataAppender())
        g.add_node('data_generator', DataGenerator())
        g.add_edge('data_generator', 'data_appender', 'observations', 'data')

    def test_add_edge_OK_attr(self):
        g = dfp.DataflowEnvironment()
        g.add_node('data_appender', DataAppender())
        g.add_node('data_generator', DataGenerator())
        g.add_edge('data_generator', 'data_appender', 'observations', 'data', dict(color='red'))

    def test_add_edge_set_attr_and_missing_node(self):
        g = dfp.DataflowEnvironment()
        g.add_node('data_appender', DataAppender())
        g.add_node('data_generator', DataGenerator())
        f = lambda: g.add_edge('data_generator', 'wrong_node_id', 'observations', 'data', dict(color='red'))
        self.assertRaises(dfpe.UnknownNodeError, f)

    def test_add_edge_raise_DafpyError(self):
        g = dfp.DataflowEnvironment()
        g.add_node('data_appender', DataAppender())
        g.add_node('data_generator', DataGenerator())
        f = lambda: g.add_edge('data_generator', 'data_appender', 'observations', 'data', attr_dict=1)
        self.assertRaises(dfpe.DafpyError, f)


class TestDAFPY(unittest.TestCase):
    def setUp(self):
        self.terminal_node = DataAppender()
        self.nodes = {
            'Data': DataGenerator(),
            'Decision': decision,
            'Execution': execution,
            'PositionUpdate': add,
            'LaggedPosition': dfp.Lag(0),
            'Terminal Node': self.terminal_node,
        }

        self.edges = [
            ('Data', 'Decision', 'observations', 'observation'),
            ('LaggedPosition', 'Decision', 'position'),
            ('Decision', 'Execution', 'trade', 'trade'),
            ('Execution', 'PositionUpdate', 'trade', 'a'),
            ('LaggedPosition', 'PositionUpdate', 'b'),
            ('PositionUpdate', 'LaggedPosition'),
            ('LaggedPosition', 'Terminal Node', 'data'),
        ]

    def test_raise_DataflowNotLockedError(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        f = lambda: g()
        self.assertRaises(dfpe.DataflowNotLockedError, f)

    def test_raise_DataflowLockedError_add_node(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.start()
        f = lambda: g.add_node('decision2', decision)
        self.assertRaises(dfpe.DataflowLockedError, f)

    def test_develop_graph_is_ok(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.start()
        g.develop_graph(1)  # OK

    def test_raise_DataflowNotLockedError_develop_graph(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        f = lambda: g.develop_graph(1)
        self.assertRaises(dfpe.DataflowNotLockedError, f)

    def test_raise_DataflowLockedError_add_edge(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.start()
        f = lambda: g.add_edge('PositionUpdate', 'LaggedPosition')
        self.assertRaises(dfpe.DataflowLockedError, f)

    def off_test_raise_DataflowLockedError_add_edge_call_args(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.start()
        f = lambda: g.add_edge_call_args('Decision', 'observation')
        self.assertRaises(dfpe.DataflowLockedError, f)

        f = lambda: g.add_edge_call_args('Decision', 'observation', 'observation')
        self.assertRaises(dfpe.WrongEdgeArgsError, f)

    def off_test_raise_DataflowLockedError_add_edge_call_rets(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.start()
        f = lambda: g.add_edge_call_rets('SimulExec', 'position', 'position')
        self.assertRaises(dfpe.DataflowLockedError, f)

    def test_terminal_node_data_1(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.start()

        l = [g() for __ in range(10)]

        assert l == [None] * 10
        assert self.terminal_node.data[-1] == 89.10000000000001

    def off_test_raise_DuplicatedReturnedDataError(self):
        self.nodes['LaggedPosition'] = LagWrong(0)
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.start()
        f = lambda: g()
        self.assertRaises(dfpe.DuplicatedReturnedDataError, f)


class TestDAFPY_Open_DataflowEnvironment(unittest.TestCase):
    def setUp(self):
        self.terminal_node = DataAppender()
        self.nodes = {
            'Decision': decision,
            'Execution': execution,
            'PositionUpdate': add,
            'LaggedPosition': dfp.Lag(0),
        }

        self.edges = [
            ('LaggedPosition', 'Decision', 'position'),
            ('Decision', 'Execution', 'trade', 'trade'),
            ('Execution', 'PositionUpdate', 'trade', 'a'),
            ('LaggedPosition', 'PositionUpdate', 'b'),
            ('PositionUpdate', 'LaggedPosition'),
        ]

    def test_raise_UnlinkedInputError_check(self):
        # Try to create the graph: An exception must be raised
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        f = lambda: g.start()
        self.assertRaises(dfpe.UnlinkedInputError, f)

    def test_raise_UnknownNodeError_v_in(self):
        # Try to create the graph: An exception must be raised
        del self.nodes['Decision']
        f = lambda: dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        self.assertRaises(dfpe.UnknownNodeError, f)

    def test_raise_UnknownNodeError_u_out(self):
        # Try to create the graph: An exception must be raised
        del self.nodes['LaggedPosition']
        f = lambda: dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        self.assertRaises(dfpe.UnknownNodeError, f)

    def test_raise_UnlinkedInputError(self):
        # Try to create the graph: An exception must be raised
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        f = lambda: g.start()
        self.assertRaises(dfpe.UnlinkedInputError, f)

    def test_raise_DupicatedEdgeError(self):
        # Try to create the graph: An exception must be raised
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        f = lambda: g.add_edge('Execution', 'PositionUpdate', 'trade', 'a')
        self.assertRaises(dfpe.DuplicatedEdgeError, f)

    def test_raise_UnlinkedInputError_call(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        f = lambda: g.start()
        self.assertRaises(dfpe.UnlinkedInputError, f)
        # f = lambda : g()
        # self.assertRaises(dfpe.UnlinkedInputAtRunTimeError, f)

    def test_raise_UnlinkedInputError_graph_call_args(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.set_call_args('obs')
        # g.add_edge_call_args('Decision', 'obs', 'observation')
        g.add_edge('Execution', 'Decision', 'observation')
        f = lambda: g.start()
        self.assertRaises(dfpe.UnlinkedInputError, f)

    def test_ok_call_args_and_rets_connected(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.set_call_args('obs')
        g.set_call_rets('pos')
        g.add_edge_call_args('Decision', 'obs', 'observation')
        g.add_edge_call_rets('LaggedPosition')
        g.start()
        data = g(10)
        assert 0 == data.pos
        data = g(10)
        assert 9.9 == data.pos

    def test_raise_UnlinkedInputError_call_with_wrong_arguments(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.set_call_args('obs')
        g.add_edge_call_args('Decision', 'obs', 'observation')
        g.start()
        f = lambda: g()
        self.assertRaises(dfpe.NotSpecifiedCallArgsError, f)

    def test_raise_UnlinkedInputError_call_with_not_specified__argument(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.set_call_args('obs')
        g.add_edge_call_args('Decision', 'obs', 'observation')
        g.start()
        f = lambda: g(ar=10)
        self.assertRaises(dfpe.NotSpecifiedCallArgsError, f)
        # correct call

    def test_raise_UnlinkedInputError_call_with_one_additional_wrong_argument(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.set_call_args('obs')
        g.add_edge_call_args('Decision', 'obs', 'observation')
        g.start()
        f = lambda: g(obs=10, ar=20)
        self.assertRaises(dfpe.WrongSpecifiedCallArgsError, f)

    def test_too_many_args_in_call(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.set_call_args('obs')
        g.add_edge_call_args('Decision', 'obs', 'observation')
        g.start()
        g(10)
        f = lambda: g(10, 3)
        self.assertRaises(dfpe.TooManyArgsError, f)

    def test_use_graph_as_callable(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.set_call_args('obs')
        g.set_call_rets('position')
        g.add_edge_call_args('Decision', 'obs', 'observation')
        g.add_edge_call_rets('LaggedPosition', 'position')
        g.start()

        datas = []
        for __ in range(10):
            data = g(obs=10)
            datas.append(data.position)

        assert datas[-1] == 89.10000000000001

    def test_use_graph_as_callable_and_retsSpec_specified(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.set_call_rets('position')
        g.set_call_args('obs')
        g.add_edge_call_args('Decision', 'obs', 'observation')
        g.add_edge_call_rets('LaggedPosition', 'position')
        g.start()

        datas = []
        for __ in range(10):
            data = g(obs=10)
            datas.append(data.position)

        assert datas[-1] == 89.10000000000001

    def test_iter_edges_and_nodes(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.set_call_args('obs')
        g.set_call_rets('position')
        g.add_edge_call_args('Decision', 'obs', 'observation')
        g.add_edge_call_rets('LaggedPosition', 'position')
        g.start()

        for u, u_out, v, v_in, edge_attr in g._iter_pred_edges():
            print u, u_out, v, v_in, edge_attr

        for u, u_out, v, v_in, edge_attr in g._iter_succ_edges():
            print u, u_out, v, v_in, edge_attr

        for node_id, node_attr in g.iter_nodes():
            print node_id, node_attr


class TestDafpy_based_api(unittest.TestCase):
    def test_use_graph_as_callable(self):
        g = dfp.DataflowEnvironment()
        g.add_node('Decision', decision)
        g.add_node('Execution', execution)
        g.add_node('PositionUpdate', add)
        g.add_node('LaggedPosition', dfp.Lag(0))

        g.add_edge('LaggedPosition', 'Decision', 'position')
        g.add_edge('Decision', 'Execution', 'trade', 'trade')
        g.add_edge('Execution', 'PositionUpdate', 'trade', 'a')
        g.add_edge('LaggedPosition', 'PositionUpdate', 'b')
        g.add_edge('PositionUpdate', 'LaggedPosition')

        g.set_call_args('obs')
        g.set_call_rets('position')
        g.add_edge_call_args('Decision', 'obs', 'observation')
        g.add_edge_call_rets('LaggedPosition', 'position')
        g.start()

        datas = []
        for __ in range(10):
            data = g(obs=10)
            datas.append(data.position)

        assert datas[-1] == 89.10000000000001

    def test_use_graph_as_callable_test_args(self):
        g = dfp.DataflowEnvironment()
        g.add_node('Decision', decision, args='test_obs,test_pos')
        g.add_node('LaggedPosition', dfp.Lag(0))
        f = lambda: g.add_edge('LaggedPosition', 'Decision', 'position')
        self.assertRaises(dfpe.WrongEdgeArgsError, f)

    def test_use_graph_as_callable_test_rets(self):
        g = dfp.DataflowEnvironment()
        g.add_node('Decision', decision, rets='test_trade')
        g.add_node('Execution', execution)

        f = lambda: g.add_edge('Decision', 'Execution', 'trade', 'trade')
        self.assertRaises(dfpe.WrongEdgeArgsError, f)

    def test_use_graph_as_callable_automatic_edges_3_args(self):
        g = dfp.DataflowEnvironment()
        g.add_node('Decision', decision, rets='trade')
        g.add_node('Execution', execution)
        g.add_node('PositionUpdate', add)
        g.add_node('LaggedPosition', dfp.Lag(0))

        g.add_edge('LaggedPosition', 'Decision', 'position')

        ###############################
        # call add_edge with 3 arguments
        ###############################
        g.add_edge('Decision', 'Execution', 'trade')
        ###############################


        g.add_edge('Execution', 'PositionUpdate', 'trade', 'a')
        g.add_edge('LaggedPosition', 'PositionUpdate', 'b')
        g.add_edge('PositionUpdate', 'LaggedPosition')

        g.set_call_args('obs')
        g.set_call_rets('position')
        g.add_edge_call_args('Decision', 'obs', 'observation')
        g.add_edge_call_rets('LaggedPosition', 'position')
        g.start()

        datas = []
        for __ in range(10):
            data = g(obs=10)
            datas.append(data.position)

        assert datas[-1] == 89.10000000000001

    def test_use_graph_as_callable_automatic_edges_2_args(self):
        g = dfp.DataflowEnvironment()
        g.add_node('Decision', decision, rets='trade')
        g.add_node('Execution', execution, args='trade', rets='trade')
        g.add_node('PositionUpdate', add)
        g.add_node('LaggedPosition', dfp.Lag(0))

        g.add_edge('LaggedPosition', 'Decision', 'position')

        ###############################
        # call add_edge with 2 arguments
        ###############################
        g.add_edge('Decision', 'Execution')
        ###############################


        g.add_edge('Execution', 'PositionUpdate', 'trade', 'a')
        g.add_edge('LaggedPosition', 'PositionUpdate', 'b')
        g.add_edge('PositionUpdate', 'LaggedPosition')

        g.set_call_args('obs')
        g.set_call_rets('position')
        g.add_edge_call_args('Decision', 'obs', 'observation')
        g.add_edge_call_rets('LaggedPosition', 'position')
        g.start()

        datas = []
        for __ in range(10):
            data = g(obs=10)
            datas.append(data.position)

        assert datas[-1] == 89.10000000000001


class TestDAFPY_Sub_DataflowEnvironment(unittest.TestCase):
    def setUp(self):
        self.nodes = {
            'Decision': decision,
            'Execution': execution,
            'PositionUpdate': add,
            'LaggedPosition': dfp.Lag(0),
        }

        self.edges = [
            ('LaggedPosition', 'Decision', 'position'),
            ('Decision', 'Execution', 'trade', 'trade'),
            ('Execution', 'PositionUpdate', 'trade', 'a'),
            ('LaggedPosition', 'PositionUpdate', 'b'),
            ('PositionUpdate', 'LaggedPosition'),
        ]

    def test_graph_as_node_in_another_graph_not_develop(self):
        # Create The first Open Dataflow
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.set_call_rets('position')
        g.set_call_args('obs')
        g.add_edge_call_args('Decision', 'obs', 'observation')
        g.add_edge_call_rets('LaggedPosition', 'position')
        # We need to specify the CallSpec of the graph in order to be able to find its args when it will be used in an other graph
        g.start()

        # Create the second graph
        terminal_node = DataAppender()
        self.nodes = {
            'Data': DataGenerator(),
            'Simulator': g,
            'Terminal Node': terminal_node,
        }

        self.edges = [
            ('Data', 'Simulator', 'observations', 'obs'),
            ('Simulator', 'Terminal Node', 'position', 'data'),
        ]
        self.terminal_node = terminal_node

        g2 = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g2.start(develop=False)

        # The len is 3 because the subgraph has not been developed
        assert len(g2) == 3

        l = [g2() for __ in range(10)]
        assert l == [None] * 10
        assert self.terminal_node.data[-1] == 89.10000000000001

    def test_graph_as_node_in_another_graph_develop(self):
        # Create The first Open Dataflow
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.set_call_rets('position')
        g.set_call_args('obs')
        g.add_edge_call_args('Decision', 'obs', 'observation')
        g.add_edge_call_rets('LaggedPosition', 'position')
        # We need to specify the CallSpec of the graph in order to be able to find its args when it will be used in an other graph
        g.start()

        # Create the second graph
        terminal_node = DataAppender()
        self.nodes = {
            'Data': DataGenerator(),
            'Simulator': g,
            'Terminal Node': terminal_node,
        }

        self.edges = [
            ('Data', 'Simulator', 'observations', 'obs'),
            ('Simulator', 'Terminal Node', 'position', 'data'),
        ]
        self.terminal_node = terminal_node

        g2 = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g2.start(develop=True)

        # The len is 6 because the subgraph has been developed
        assert len(g2) == 6

        l = [g2() for __ in range(10)]
        assert l == [None] * 10
        assert self.terminal_node.data[-1] == 89.10000000000001


class TestDAFPY_Feddback(unittest.TestCase):
    def setUp(self):
        pass

    def test_graph_feedback(self):

        SimulExec = dfp.DataflowEnvironment()
        SimulExec.set_call_rets('position')
        SimulExec.set_call_args('trade')
        SimulExec.add_node('Execution', execution)
        SimulExec.add_node('PositionUpdate', add)
        SimulExec.add_node('LaggedPosition', dfp.Lag(0))
        SimulExec.add_edge('Execution', 'PositionUpdate', 'trade', 'a')
        SimulExec.add_edge('LaggedPosition', 'PositionUpdate', 'b')
        SimulExec.add_edge('PositionUpdate', 'LaggedPosition')
        SimulExec.add_edge_call_args('Execution', 'trade')
        SimulExec.add_edge_call_rets('LaggedPosition', 'position')
        SimulExec.start()

        g = dfp.DataflowEnvironment()
        g.add_node('Decision', decision)
        g.add_node('SimulExec', SimulExec)
        g.add_edge('Decision', 'SimulExec', 'trade', 'trade')
        g.add_edge('SimulExec', 'Decision', 'position', 'position')
        g.set_call_args('obs')
        g.set_call_rets('position')
        g.add_edge_call_args('Decision', 'observation')
        g.add_edge_call_rets('SimulExec', 'position')
        g.start(develop=True)

        datas = [g(obs=10) for __ in range(10)]
        assert datas[-1].position == 89.10000000000001

    def test_graph_feedback_do_not_develop_sub_graph(self):

        SimulExec = dfp.DataflowEnvironment()
        SimulExec.set_call_rets('position')
        SimulExec.set_call_args('trade')
        SimulExec.add_node('Execution', execution)
        SimulExec.add_node('PositionUpdate', add)
        SimulExec.add_node('LaggedPosition', dfp.Lag(0))
        SimulExec.add_edge('Execution', 'PositionUpdate', 'trade', 'a')
        SimulExec.add_edge('LaggedPosition', 'PositionUpdate', 'b')
        SimulExec.add_edge('PositionUpdate', 'LaggedPosition')
        SimulExec.add_edge_call_args('Execution', 'trade')
        SimulExec.add_edge_call_rets('LaggedPosition', 'position')
        SimulExec.start()

        g = dfp.DataflowEnvironment()
        g.add_node('Decision', decision)
        g.add_node('SimulExec', SimulExec)
        g.add_edge('Decision', 'SimulExec', 'trade', 'trade')
        g.add_edge('SimulExec', 'Decision', 'position', 'position')
        g.set_call_args('obs')
        g.set_call_rets('position')
        g.add_edge_call_args('Decision', 'observation')
        g.add_edge_call_rets('SimulExec', 'position', 'position')

        f = lambda: g.start(develop=False)
        self.assertRaises(dfpe.StalledDataflowCallError, f)

    def test_graph_lqg_feedback(self):
        """
        epsilonp variable is set in order to have coverage on the "self.add_edge(*edge)" part of the code.
        """

        # Make SimulExec
        SimulExec = dfp.DataflowEnvironment(name='SimulExec')
        SimulExec.set_call_rets('position')
        SimulExec.set_call_args('trade')
        SimulExec.add_node('Execution', execution)
        SimulExec.add_node('PositionUpdate', add)
        SimulExec.add_node('LaggedPosition', dfp.Lag(0))
        SimulExec.add_edge('Execution', 'PositionUpdate', 'trade', 'a')
        SimulExec.add_edge('LaggedPosition', 'PositionUpdate', 'b')
        SimulExec.add_edge('PositionUpdate', 'LaggedPosition')
        SimulExec.add_edge_call_args('Execution', 'trade')
        SimulExec.add_edge_call_rets('LaggedPosition', 'position')
        SimulExec.start()

        # Make Market Plant

        MarketPlant = dfp.DataflowEnvironment(name='MarketPlant')
        MarketPlant.set_call_rets('position,observation, epsilonp')
        MarketPlant.set_call_args('trade,observation, epsilonp')
        MarketPlant.add_edge_call_args_rets('epsilonp')
        MarketPlant.add_node('SimulExec', SimulExec)
        MarketPlant.add_edge_call_args_rets('observation')
        MarketPlant.add_edge_call_rets('SimulExec', 'position', 'position')
        MarketPlant.add_edge_call_args('SimulExec', 'trade', 'trade')
        MarketPlant.start()

        # Make final Dataflow
        def decision(epsilonp, position, observation):
            return {'trade': 10, 'espilonp': epsilonp}

        LQG = dfp.DataflowEnvironment(name='LQG')
        LQG.add_node('Decision', decision)
        LQG.add_node('MarketPlant', MarketPlant)
        LQG.add_node('Data', DataGenerator())
        LQG.add_edge('Decision', 'MarketPlant', 'trade', 'trade')
        LQG.add_edge('MarketPlant', 'Decision')
        LQG.add_edge('Data', 'MarketPlant', 'observations', 'observation')
        LQG.set_call_args('epsilonp')
        LQG.add_edge_call_args('MarketPlant')
        LQG.set_call_rets('position, epsilonp')
        LQG.add_edge_call_rets('MarketPlant')
        LQG.start(develop=True, level=2)

        datas = [LQG(0.1) for __ in range(10)]
        assert datas[-1].position == 89.10000000000001

    def test_graph_lqg_feedback2(self):

        # Make SimulExec
        SimulExec = dfp.DataflowEnvironment(name='SimulExec')
        SimulExec.set_call_rets('position')
        SimulExec.set_call_args('trade')
        SimulExec.add_node('Execution', execution)
        SimulExec.add_node('PositionUpdate', add)
        SimulExec.add_node('LaggedPosition', dfp.Lag(0))
        SimulExec.add_edge('Execution', 'PositionUpdate', 'trade', 'a')
        SimulExec.add_edge('LaggedPosition', 'PositionUpdate', 'b')
        SimulExec.add_edge('PositionUpdate', 'LaggedPosition')
        SimulExec.add_edge_call_args('Execution', 'trade')
        SimulExec.add_edge_call_rets('LaggedPosition', 'position')
        SimulExec.start(develop=False)

        # Make Market Plant

        MarketPlant = dfp.DataflowEnvironment(name='MarketPlant')
        MarketPlant.add_node('SimulExec', SimulExec)
        MarketPlant.set_call_rets('position,observation')
        MarketPlant.set_call_args('trade,observation')
        MarketPlant.add_edge_call_args_rets('observation')
        MarketPlant.add_edge_call_rets('SimulExec', 'position', 'position')
        MarketPlant.add_edge_call_args('SimulExec', 'trade', 'trade')
        MarketPlant.start(develop=False)

        # Make Decision Graph
        DecisionGraph = dfp.DataflowEnvironment(name='DecisionGraph')
        DecisionGraph.add_node('Decision', decision)
        DecisionGraph.set_call_rets('trade')
        DecisionGraph.set_call_args('observation,position')
        DecisionGraph.add_edge_call_rets('Decision', 'trade', 'trade')
        DecisionGraph.add_edge_call_args('Decision', 'position', 'position')
        DecisionGraph.add_edge_call_args('Decision', 'observation', 'observation')
        DecisionGraph.start(develop=False)

        # Make final Dataflow

        LQG = dfp.DataflowEnvironment(name='LQG')
        LQG.add_node('Decision', DecisionGraph)
        LQG.add_node('MarketPlant', MarketPlant)
        LQG.add_node('Data', DataGenerator())
        LQG.add_edge('Decision', 'MarketPlant', 'trade', 'trade')
        LQG.add_edge('MarketPlant', 'Decision', 'position', 'position')
        LQG.add_edge('MarketPlant', 'Decision', 'observation')
        LQG.add_edge('Data', 'MarketPlant', 'observations', 'observation')
        LQG.set_call_rets('position')
        LQG.add_edge_call_rets('MarketPlant', 'position', 'position')

        # First the graph is develop only at the first level
        assert len(LQG) == 3
        f = lambda: LQG.start(develop=True)
        self.assertRaises(dfpe.StalledDataflowCallError, f)

        # We develop the second level
        LQG.start(develop=True, level=2)
        assert len(LQG) == 5
        datas = [LQG() for __ in range(10)]
        assert datas[-1].position == 89.10000000000001

    def test_graph_self_loop_feedback(self):

        SimulExec = dfp.DataflowEnvironment()
        SimulExec.set_call_rets('position')
        SimulExec.set_call_args('trade')
        SimulExec.add_node('Execution', execution)
        SimulExec.add_node('PositionUpdate', add)
        SimulExec.add_node('LaggedPosition', dfp.Lag(0))
        SimulExec.add_edge('Execution', 'PositionUpdate', 'trade', 'a')
        SimulExec.add_edge('LaggedPosition', 'PositionUpdate', 'b')
        SimulExec.add_edge('PositionUpdate', 'LaggedPosition')
        SimulExec.add_edge_call_args('Execution', 'trade')
        SimulExec.add_edge_call_rets('LaggedPosition', 'position')
        SimulExec.start()

        g = dfp.DataflowEnvironment()
        g.add_node('SimulExec', SimulExec)
        g.add_edge('SimulExec', 'SimulExec', 'position', 'trade')
        g.set_call_rets('position')
        g.add_edge_call_rets('SimulExec', 'position', 'position')
        g.start(develop=True)

        datas = [g() for __ in range(10)]
        assert datas[-1].position == 0.0

    def test_graph_incoherent_return_for_intern_call(self):

        g = dfp.DataflowEnvironment()
        g.add_node('data_appender', DataAppender())
        g.add_node('data_generator', DataGenerator())
        g.add_edge('data_generator', 'data_appender', 'observations_wrong', 'data')
        g.start()
        f = lambda: g()
        self.assertRaises(KeyError, f)

    def test_graph_with_no_returns_specified(self):
        class DataGenerator2(object):
            def __call__(self):
                return np.ones(10)

        g = dfp.DataflowEnvironment()
        data_appender = DataAppender()
        g.add_node('data_appender', data_appender)
        g.add_node('data_generator', DataGenerator2())
        g.add_edge('data_generator', 'data_appender')
        g.start()
        g()
        g()

        datas = data_appender.data

        # g has been called two times
        assert len(datas) == 2
        np.testing.assert_equal(datas[0], np.ones(10))
        np.testing.assert_equal(datas[1], np.ones(10))

    def test_graph_with_no_returns_specified_call_name_specified(self):
        class DataGenerator2(object):
            def __call__(self):
                return np.ones(10)

        g = dfp.DataflowEnvironment()
        data_appender = DataAppender()
        g.add_node('data_appender', data_appender)
        g.add_node('data_generator', DataGenerator2())
        g.add_edge('data_generator', 'data_appender', 'data')
        g.start()
        g()
        g()

        datas = data_appender.data

        # g has been called two times
        assert len(datas) == 2
        np.testing.assert_equal(datas[0], np.ones(10))
        np.testing.assert_equal(datas[1], np.ones(10))

    def test_graph_with_no_returns_specified_call_name_specified_arbitrary_rets_name(self):
        class DataGenerator2(object):
            def __call__(self):
                return np.ones(10)

        g = dfp.DataflowEnvironment()
        data_appender = DataAppender()
        g.add_node('data_appender', data_appender)
        g.add_node('data_generator', DataGenerator2())
        g.add_edge('data_generator', 'data_appender', 'arbitrary_name', 'data')
        g.start()
        f = lambda: g()
        # self.assertRaises(ValueError, f)
        self.assertRaises(IndexError, f)
        return
        g()

        datas = data_appender.data

        # g has been called two times
        assert len(datas) == 2
        np.testing.assert_equal(datas[0], np.ones(10))
        np.testing.assert_equal(datas[1], np.ones(10))

    def test_graph_with_no_returns_specified_multiple_return_one_args_in_call(self):

        class DataAppender2(object):
            def __init__(self):
                self.data = []

            def __call__(self, data):
                self.data.append(data)

        class DataGenerator2(object):
            def __call__(self):
                return np.ones(10), 30, 'a'

        g = dfp.DataflowEnvironment()
        data_appender = DataAppender2()
        g.add_node('data_appender', data_appender)
        g.add_node('data_generator', DataGenerator2())
        g.add_edge('data_generator', 'data_appender')
        g.start()
        g()
        g()

        datas = data_appender.data

        # g has been called two times
        assert len(datas) == 2

        for i in [0, 1]:
            assert len(datas[i]) == 3
            a, b, c = datas[i]
            np.testing.assert_equal(a, np.ones(10))
            assert b == 30
            assert c == 'a'

    def test_graph_with_no_returns_specified_call_type_is_args(self):

        class DataGenerator2(object):
            def __call__(self):
                return np.ones(10), 30, 'a'

        class DataAppender2(object):
            def __init__(self):
                self.data = []

            def __call__(self, a, b, c):
                self.data.append([a, b, c])

        g = dfp.DataflowEnvironment()
        data_appender = DataAppender2()
        g.add_node('data_generator', DataGenerator2(), rets='a,b,c')
        g.add_node('data_appender', data_appender)
        g.add_edge('data_generator', 'data_appender')
        g.start()
        g()
        g()

        datas = data_appender.data

        # g has been called two times
        assert len(datas) == 2

        for i in [0, 1]:
            assert len(datas[i]) == 3
            a, b, c = datas[i]
            np.testing.assert_equal(a, np.ones(10))
            assert b == 30
            assert c == 'a'

    def test_multiple_in_out_edge_not_specified(self):

        class DataAppender2(object):
            def __init__(self):
                self.data = []

            def __call__(self, a, b, c):
                self.data.append([a, b, c])

        def DataGenerator2():
            return {'a': 'a_val', 'b': 'b_val', 'c': 'c_val'}

        g = dfp.DataflowEnvironment()
        data_appender = DataAppender2()
        g.add_node('data_appender', data_appender)
        g.add_node('data_generator', DataGenerator2, rets='a,b,c')
        g.add_edge('data_generator', 'data_appender')
        g.start()
        g()
        g()

        datas = data_appender.data
        # g has been called two times
        assert len(datas) == 2

        for i in [0, 1]:
            assert len(datas[i]) == 3
            a, b, c = datas[i]
            assert a == 'a_val'
            assert b == 'b_val'
            assert c == 'c_val'

    def test_multiple_in_out_edge_specified_one_by_one(self):

        class DataAppender2(object):
            def __init__(self):
                self.data = []

            def __call__(self, a, b, c):
                self.data.append([a, b, c])

        def DataGenerator2():
            return {'a': 'a_val', 'b': 'b_val', 'c': 'c_val'}

        g = dfp.DataflowEnvironment()
        data_appender = DataAppender2()
        g.add_node('data_appender', data_appender)
        g.add_node('data_generator', DataGenerator2, rets='a,b,c')
        g.add_edge('data_generator', 'data_appender', 'a')
        g.add_edge('data_generator', 'data_appender', 'b')
        g.add_edge('data_generator', 'data_appender', 'c')
        g.start()
        g()
        g()

        datas = data_appender.data
        # g has been called two times
        assert len(datas) == 2

        for i in [0, 1]:
            assert len(datas[i]) == 3
            a, b, c = datas[i]
            assert a == 'a_val'
            assert b == 'b_val'
            assert c == 'c_val'

    def test_multiple_in_one_out(self):

        class DataAppender2(object):
            def __init__(self):
                self.data = []

            def __call__(self, data_in):
                self.data.append([data_in])

        def DataGenerator2():
            return {'a': 'a_val', 'b': 'b_val', 'c': 'c_val'}

        g = dfp.DataflowEnvironment()
        data_appender_aa = DataAppender2()
        data_appender_bb = DataAppender2()
        data_appender_cc = DataAppender2()
        g.add_node('data_appender_aa', data_appender_aa)
        g.add_node('data_appender_bb', data_appender_bb)
        g.add_node('data_appender_cc', data_appender_cc)
        g.add_node('data_generator', DataGenerator2, rets='a,b,c')
        g.add_edge('data_generator', 'data_appender_aa', 'a')
        g.add_edge('data_generator', 'data_appender_bb', 'b')
        g.add_edge('data_generator', 'data_appender_cc', 'c')
        g.start()
        g()
        g()

        datas_aa = data_appender_aa.data
        datas_bb = data_appender_bb.data
        datas_cc = data_appender_cc.data
        # g has been called two times
        assert len(datas_aa) == 2
        assert len(datas_bb) == 2
        assert len(datas_cc) == 2

        for i in [0, 1]:
            assert len(datas_aa[i]) == 1
            assert len(datas_bb[i]) == 1
            assert len(datas_cc[i]) == 1
            a = datas_aa[i][0]
            b = datas_bb[i][0]
            c = datas_cc[i][0]
            assert a == 'a_val'
            assert b == 'b_val'
            assert c == 'c_val'
