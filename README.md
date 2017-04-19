Description
===========

Dafpy is a very experimental package which play with dataflow programming in python.
It aims to define an API inspired by networkx where nodes of a graph are python callables, generators or coroutines.
The graphs can be directed acyclic graphs (DAG) or directed graphs (with feedback).
In the later case, some nodes must have an initial state.
A graph can itself be used as a callable, generator or coroutine and can be reused as nodes of another graph.

Two api are proposed:

- DataflowGraphBase: an API very similar to networkx's DiGraph class.
- DataflowEnvironment: another API with a method 'add_task'


Dafpy is a very experimental and now certainly outdated package which I wrote few years ago (around 2012) when I wanted to explore the concept of dataflow in python.
At this time, I also discovered the concept of coroutines and played with it in this library, but certainly in a very naive way.
(I used David Beazley's documentation about coroutines: http://www.dabeaz.com/coroutines).
Since, there has been the library asyncio which should be a better choice for implementation of coroutines.

Also, since this time, the project [dask](http://dask.readthedocs.io/en/latest/) emerged and has been actively developped.
The project dask implements in a much more elegant and pythonic way the concept of dataflow and is certainly much more usefull than dafpy.
However, I share this package in case some other aspects developed here (generators, coroutines, feedbacks, graph API) interest someone.


Related Projects
--------------------

- Networkx: https://networkx.github.io/
- dask: http://dask.readthedocs.io/en/latest/
- Ptolemy: http://ptolemy.eecs.berkeley.edu/ptolemyII/index.htm
- Simpy: https://simpy.readthedocs.org/en/latest/


Usage Example
================

```python
import dafpy as dfp
g = dfp.DataflowEnvironment(name='test', verbose=False)
g.set_call_args('u')
g.set_call_rets('y')
g.add_node('f', lambda x: x)
g.add_node('g', lambda x: x)
g.add_edge('f', 'g', apply=lambda x: -.9 * x)
g.add_edge_call_args('f')
g.add_edge_call_rets('g')
g.start()
g(10).y == -0.9 * 10
```


See tests/ and notebooks/ directories for other, more advanced, code examples.

To run the test, execute:
```
pip install -e .
make test
```


Historical discussions
========================

Functional requirements
--------------------------

Here are a list of funcionalities that has been targetted:

- Design python callables with the Dataflow programming paradigm
- This callable could be used alone in a standard program or in another graph.
- Possibility to specify graphs with feedback
- Detect unfeasible graphs

 
Dataflow programming
------------------------

Dataflow programming is a programming paradigm, described on the page http://en.wikipedia.org/wiki/Dataflow_programming.

The idea of this project is to propose some simple tools in python in order to be able to easily specify a progam or "algorithm" in the Dataflow programming paradigm.


Contexts
----------

The Dataflow paradigm appears naturally in design of Control systems, where a system is often specified by a set of blocks interacting by links.

This kind of programming paradigm is also quite natural is the design of simulators.

Basic concepts of the implementation
--------------------------------------

A program is simply a directed graph which has two methods init() and __call__().

A graph is composed of nodes and links interconnecting nodes.

during a call of the graph, we try to execute every nodes as soon as data are available.

