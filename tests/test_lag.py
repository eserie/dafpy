import unittest
import dafpy as dfp

import collections
import numpy as np
import copy

import dafpy.exceptions as dfpe
import dafpy.defs as dfpd


def test_lag():
    lag = dfp.Lag(10)

    assert lag(11) == 10
    assert lag('a') == 11
    assert lag.reset() == 'a'
    assert lag('b') is None
    assert lag.reset() == 'b'


def test_lag_state():
    lag = dfp.DataflowEnvironment()

    lag.args = 'lag_in'
    lag.add_task('lag', dfp.Lag(None))
    lag.rets = 'lag_out'
    lag.add_edge_call_rets('lag')
    lag.start()
    assert lag(11).lag_out == None
    assert lag('a').lag_out == 11
    assert lag(None).lag_out == 'a'
    assert lag('b').lag_out is None
    assert lag(None).lag_out == 'b'


def test_add_lag():
    lag = dfp.DataflowEnvironment()
    lag.args = 'lag_in'
    lag.add_lag('lag', None)
    lag.rets = 'lag_out'
    lag.add_edge_call_args('lag')
    lag.add_edge_call_rets('lag')
    lag.start()
    assert lag(11).lag_out == None
    assert lag('a').lag_out == 11
    assert lag(None).lag_out == 'a'
    assert lag('b').lag_out is None
    assert lag(None).lag_out == 'b'
