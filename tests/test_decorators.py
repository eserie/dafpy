import unittest
import collections

import dafpy as dfp


######################################
# Some definitions for the tests
######################################

class DataGenerator(object):
    def __call__(self):
        return {'observations': 10}


@dfp.set_call_rets_decorator('trade')
def decision(position, observation):
    return 10


@dfp.set_call_rets_decorator('trade')
def execution(trade):
    return trade * 0.99


@dfp.set_call_rets_decorator('sum')
def add(a, b):
    return a + b


class DataAppender(object):
    def __init__(self):
        self.data = []

    def __call__(self, data):
        self.data.append(data)


class TestDAFPYDec(unittest.TestCase):
    def setUp(self):
        self.terminal_node = DataAppender()
        self.nodes = {
            'Data': DataGenerator(),
            'Decision': decision,
            'Execution': execution,
            'Position Update': add,
            'Lagged Position': dfp.Lag(0),
            'Terminal Node': self.terminal_node,
        }

        self.edges = [
            ('Data', 'Decision', 'observations', 'observation'),
            ('Lagged Position', 'Decision', 'position'),
            ('Decision', 'Execution', 'trade', 'trade'),
            ('Execution', 'Position Update', 'trade', 'a'),
            ('Lagged Position', 'Position Update', 'b'),
            ('Position Update', 'Lagged Position'),
            ('Lagged Position', 'Terminal Node', 'data'),
        ]

    def test_terminal_node_data(self):
        g = dfp.DataflowEnvironment.from_items(self.nodes, self.edges)
        g.start()

        l = [g() for __ in range(10)]

        assert l == [None] * 10
        assert self.terminal_node.data[-1] == 89.10000000000001
