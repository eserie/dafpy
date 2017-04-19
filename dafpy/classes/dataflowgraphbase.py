import copy
from pprint import pprint
import inspect
import collections
from dafpy.util.cogen_func import coroutine
import dafpy.util.filters as dfpf

import dafpy as dfp
import dafpy.exceptions as dfpe
import dafpy.defs as dfpd
import dafpy.util.func as dfpfunc


##################
# Dataflow Class
##################

class DataflowGraphBase(object):
    def __init__(self, **attr):
        """Initialize a Dataflow with name, Dataflow attributes.

        Parameters
        ----------
        :param name: string, optional (default='')
            An optional name for the Dataflow.
        :param attr: keyword arguments, optional (default= no attributes)
            Attributes to add to Dataflow as key=value pairs.


        Examples
        --------
        >>> g = dfp.DataflowGraphBase()
        >>> g = dfp.DataflowGraphBase(name='test')
        >>> assert g.name == 'test'
        """

        self.graph = {}  # dictionary for dataflow attributes
        self.node = collections.OrderedDict()  # dictionary for node attributes, in particular callable objects used by the Dataflow
        self._call_nodes = None  # execution plan: list containing the nodes to call in the correct order
        # We store two adjacency lists:
        # the  predecessors of node n are stored in the dict self.pred
        # the successors of node n are stored in the dict self.succ=self.adj
        self.adj = {}  # empty adjacency dictionary
        self.pred = {}  # predecessor
        self.succ = self.adj  # successor
        self._nodeoutdata = {}  # dictionary to store the output data of nodes
        # self.indata = {} #dictionary to store the output data of nodes

        # Deprecated members
        self._is_locked = False

        # load graph attributes
        self.graph.update(attr)

    ###############################
    # Properties
    ###############################

    @property
    def name(self):
        return self.graph.get('name', 'NotNamedGraph')

    @name.setter
    def name(self, s):
        self.graph['name'] = s

    @property
    def verbose(self):
        return self.graph.get('verbose', False)

    @verbose.setter
    def verbose(self, s):
        self.graph['verbose'] = s

    @property
    def args(self):
        return self.graph.get('args', [])

    @args.setter
    def args(self, s):
        self.set_call_args(s)

    @property
    def rets(self):
        return self.graph.get('rets', [])

    @rets.setter
    def rets(self, s):
        self.set_call_rets(s)

    ###############################
    # Copies
    ###############################


    def set_from(self, g):
        self.node = g.node
        self.pred = g.pred
        self.succ = g.succ
        self._nodeoutdata = g._nodeoutdata
        self.reset_task = g.reset_task

    ###############################
    # Builtins
    ###############################    

    def __repr__(self):
        if self.name != 'NotNamedGraph':
            return '{!r} at {!r}'.format(self.__class__.__name__, self.name)
        return '{!r}'.format(self.__class__.__name__)

    def __str__(self):
        """Return the graph name.

        Returns
        -------
        name : string
            The name of the graph.

        Examples
        --------
        >>> G = dfp.DataflowGraphBase(name='foo')
        >>> str(G)
        'foo'
        """
        return self.name

    def __iter__(self):
        """Iterate over the nodes. Use the expression 'for n in G'.

        Returns
        -------
        niter : iterator
            An iterator over all nodes in the graph.

        Examples
        --------
        >>> G = dfp.DataflowGraphBase()   # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> G.add_path([0,1,2,3], [lambda x : x] * 4)
        """
        for n, n_attr in self.node.iteritems():
            yield n, n_attr

    def __contains__(self, n):
        """Return True if n is a node, False otherwise. Use the expression
        'n in G'.

        Examples
        --------
        >>> G = dfp.DataflowGraphBase()   # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> G.add_path([0,1,2,3], [lambda x : x] * 4)
        >>> 1 in G
        True
        """
        try:
            return n in self.node
        except TypeError:
            return False

    def __len__(self):
        """Return the number of callable nodes. Use the expression 'len(G)'.

        Returns
        -------
        nnodes : int
            The number of nodes in the graph.

        Examples
        --------
        >>> G = dfp.DataflowGraphBase()   # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> G.add_path([0,1,2,3], [lambda x : x] * 4)
        >>> len(G)
        4

        """
        return len(self.callable_nodes())

    def __getitem__(self, n):
        """Return a dict of neighbors of node n.  Use the expression 'G[n]'.

        Parameters
        ----------
        n : node
           A node in the graph.

        Returns
        -------
        adj_dict : dictionary
           The adjacency dictionary for nodes connected to n.

        Notes
        -----
        G[n] is similar to G.neighbors(n) but the internal data dictionary
        is returned instead of a list.

        Assigning G[n] will corrupt the internal graph data structure.
        Use G[n] for reading data only.

        Examples
        --------
        >>> G = dfp.DataflowGraphBase()   # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> G.add_path([0,1,2,3], [lambda x : x] * 4)
        >>> assert G[0] == {dfpd.OBJ_CALL_RET : {1: {'x': {}}}}
        """
        return self.adj[n]

    ###############################
    # Iteration
    ###############################

    def internal_nodes(self):
        return list(self.iter_internal_nodes())

    def iter_internal_nodes(self):
        for node in self.nodes():
            if node not in [dfpd.CALL_ARGS_NODE_ID, dfpd.CALL_RETS_NODE_ID]:
                yield node

    def iter_nodes(self):
        for node_id, node_attr in self.node.iteritems():
            yield node_id, node_attr

    def nodes(self):
        return [node_id for node_id, _ in self.iter_nodes()]

    def iter_edges(self):
        for u, v, u_out, v_in, edge_attr in self._iter_pred_edges():
            yield u, v, u_out, v_in, edge_attr

    def edges(self):
        return [(u, v, u_out, v_in, edge_attr) for u, v, u_out, v_in, edge_attr in self.iter_edges()]

    def _iter_pred_edges(self):
        for v, v_dict in self.pred.iteritems():
            for v_in, v_in_dict in v_dict.iteritems():
                for u, u_dict in v_in_dict.iteritems():
                    for u_out, edge_attr in u_dict.iteritems():
                        if u_out == (dfpd.OBJ_CALL_RET,):
                            u_out = dfpd.OBJ_CALL_RET
                        yield u, v, u_out, v_in, edge_attr

    def _iter_succ_edges(self):
        for u, u_dict in self.succ.iteritems():
            for u_out, u_out_dict in u_dict.iteritems():
                for v, v_dict in u_out_dict.iteritems():
                    for v_in, edge_attr in v_dict.iteritems():
                        yield u, v, u_out, v_in, edge_attr

    def callable_nodes(self):
        return [n for n, attr in self.node.iteritems() if attr.get('func') is not None]

    def iter_callalbe_nodes(self):
        for node_id in self.callable_nodes():
            yield node_id, self.node[node_id]

    def iter_developable_nodes(self):
        for node_id, node_attr in self.iter_callalbe_nodes():
            node_call = node_attr['func']
            if isinstance(node_call, DataflowGraphBase):
                yield node_id, node_attr

    def developable_nodes(self):
        return [node_id for node_id, _ in self.iter_developable_nodes()]

    ##############################
    # Access Data
    ##############################

    def _indata(self, node_id):
        if node_id not in self.pred or not self.pred[node_id]:
            return
        data = {}
        for pred_node_id, pred_node_dict in self.pred[node_id].iteritems():
            # Get the id of the related outputs
            assert len(pred_node_dict) == 1
            u, u_dict = pred_node_dict.items()[0]
            assert len(u_dict) == 1
            u_out, edge_attr = u_dict.items()[0]

            assert u in self._nodeoutdata
            pred_nodeoutdata = self._nodeoutdata[u]
            u_rets = self.node[u]['rets']
            if len(u_out) == 1:
                # TODO: check that this is safe...
                # It seems to correct the bug encounter in the bellow reported error at run time

                u_out = u_out[0]
            if tuple(u_rets) == dfpd.OBJ_CALL_RETS:
                if u_out == dfpd.OBJ_CALL_RET:
                    pred_nodeoutdata = pred_nodeoutdata[dfpd.OBJ_CALL_RET]['value']
                else:
                    # connections are specified with names but field 'rets' of the pred_node_id is generic (not specified)
                    # In this case we can have an error at run time
                    pred_nodeoutdata = pred_nodeoutdata[dfpd.OBJ_CALL_RET]['value'][u_out]
            else:
                if tuple(pred_nodeoutdata.keys()) == dfpd.OBJ_CALL_RETS:
                    pred_nodeoutdata = pred_nodeoutdata[dfpd.OBJ_CALL_RET]['value']
                    if isinstance(pred_nodeoutdata, tuple) and hasattr(pred_nodeoutdata, '_asdict'):
                        pred_nodeoutdata = pred_nodeoutdata._asdict()
                    if isinstance(pred_nodeoutdata, dict):
                        pred_nodeoutdata = pred_nodeoutdata[u_out]
                    elif isinstance(pred_nodeoutdata, list) or isinstance(pred_nodeoutdata, tuple):
                        pred_nodeoutdata = pred_nodeoutdata[u_rets.index(u_out)]

                else:
                    pred_nodeoutdata = pred_nodeoutdata[u_out]['value']
            data[pred_node_id] = pred_nodeoutdata

            if edge_attr.get('apply'):
                data[pred_node_id] = edge_attr['apply'](data[pred_node_id])
        return data

    def _parse_call_args(self, args, kwargs):
        if dfpd.CALL_ARGS_NODE_ID not in self._nodeoutdata:
            return

        call_args = {}
        if args:
            # assert hasattr(self, dfpd.CALL_SPEC_ATTR)
            if len(args) > len(self.args):
                raise (dfpe.TooManyArgsError(
                    "Too many arguments specified {!r}. Expected arguments are {!r}".format(args, self.args)))

            if len(args) < len(self.args):
                i_missing = len(self.args) - len(args)
                # We check if args are given by kwargs
                if set(self.args[i_missing:]) - set(kwargs.keys()):
                    raise (dfpe.TooManyArgsError(
                        'Not Enough arguments specified {!r}. Expected arguments are {!r}'.format(args, self.args)))

            assert len(args) <= len(self.args)
            for key, value in zip(self.args, args):
                call_args[key] = value

        if kwargs:
            for key, value in kwargs.iteritems():
                call_args[key] = value

        if tuple(self.args) != dfpd.OBJ_CALL_RETS:
            # we do not check anything

            user_args_keys_ = set(call_args.keys())
            specified_args_ = set(self.args)
            # assert specified_args_ == set(self._nodeoutdata[dfpd.CALL_ARGS_NODE_ID].keys())

            diff_ = specified_args_ - user_args_keys_
            if diff_:
                raise (dfpe.NotSpecifiedCallArgsError(diff_, user_args_keys_))

            diff_ = user_args_keys_ - specified_args_
            if diff_:
                raise (dfpe.WrongSpecifiedCallArgsError(diff_, specified_args_))

        if not call_args:
            self._nodeoutdata[dfpd.CALL_ARGS_NODE_ID].clear()
        else:
            for key, value in call_args.iteritems():
                if key not in self._nodeoutdata[dfpd.CALL_ARGS_NODE_ID]:
                    self._nodeoutdata[dfpd.CALL_ARGS_NODE_ID][key] = dict()
                self._nodeoutdata[dfpd.CALL_ARGS_NODE_ID][key]['value'] = value
        if self.args and not call_args:
            self._nodeoutdata[dfpd.CALL_ARGS_NODE_ID][dfpd.OBJ_CALL_RET] = dict(value=None)

    ###############################
    # Helper Constructor
    ###############################

    @classmethod
    def from_items(cls, nodes, edges, **attr):
        g = cls(**attr)
        for node_id, node_call in nodes.iteritems():
            g.add_node(node_id, node_call)

        for edge in edges:
            g.add_edge(*edge)
        return g

    ###############################
    # add_node, add_edge
    ###############################
    @staticmethod
    def _convert_args_rets_to_tuple(attr_dict):
        for key in ['args', 'rets']:
            if key in attr_dict:
                if isinstance(attr_dict[key], list):
                    attr_dict[key] = tuple(attr_dict[key])
                elif not isinstance(attr_dict[key], tuple):
                    attr_dict[key] = getattr(collections.namedtuple('test', attr_dict[key]), '_fields')

    def add_node(self, n, func=None, attr_dict=None, detect_func=True, **attr):
        """Add a single node n, attach a callable object and update node attributes.

        Parameters
        ----------
        :param n : node
            A node can be any hashable Python object except None.

        :param func:
            Any callable object
        :param attr_dict: dictionary, optional (default= no attributes)
            Dictionary of node attributes.  Key/value pairs will            
        :param attr: keyword arguments, optional
            Set attributes using key=value.

        Examples
        --------
        >>> g = dfp.DataflowGraphBase()   
        >>> g.add_node(1, lambda : 1)
        >>> g.add_node('Hello', lambda : "Hello")

        Use keywords set/change node attributes:

        >>> g = dfp.DataflowGraphBase()
        >>> g.add_node(1, lambda : 1, size=10)

        Notes
        -----
        A hashable object is one that can be used as a key in a Python
        dictionary. This includes strings, numbers, tuples of strings
        and numbers, etc.

        On many platforms hashable items also include mutables such as
        Dafpy Dataflows, though one should be careful that the hash
        doesn't change on mutables.
        """
        if self._is_locked:
            raise (dfpe.DataflowLockedError())

        # set up attribute dict
        if attr_dict is None:
            attr_dict = attr
        else:
            try:
                attr_dict.update(attr)
            except AttributeError:
                raise dfpe.DafpyError("The attr_dict argument must be a dictionary.")

        if n not in [dfpd.CALL_ARGS_NODE_ID, dfpd.CALL_RETS_NODE_ID]:
            # assert 'func' in attr_dict
            if detect_func and func is None:
                if 'func' in attr_dict:
                    func = attr_dict['func']
                elif callable(n):
                    func = n
            if func:
                if 'rets' not in attr_dict:
                    call_rets = dfp.get_call_rets(func)
                    if call_rets is not None:
                        attr_dict['rets'] = call_rets
                    else:
                        attr_dict['rets'] = dfpd.OBJ_CALL_RET

                if 'args' not in attr_dict:
                    call_args = dfp.get_call_args(func)
                    attr_dict['args'] = call_args

                    # convert 'args' and 'rets' items in tuple
                self._convert_args_rets_to_tuple(attr_dict)

        # add argument 'func' to attr
        if func is not None and 'func' not in attr_dict:
            attr_dict['func'] = func

        if n not in self.node:
            self.succ[n] = {}
            self.pred[n] = {}
            self.node[n] = attr_dict
        else:
            self.node[n].update(attr_dict)

    def add_edge(self, u, v, *args, **attr):
        """

        Parameters
        ----------
        :param u: node
            A node can be any hashable Python object except None.

        :param v: node
            A node can be any hashable Python object except None.

        Options
        -------
        
        :param u_out:
            An output / 'call return' of the 'func' attribute of the node u

        :param v_in:
            An input / 'call args' of the 'func' attribute of node v

        
        :param attr_dict: dictionary, optional (default= no attributes)
            Dictionary of the edge attributes.  Key/value pairs will
        
        :param attr: keyword arguments, optional
            Set attributes using key=value.


        Examples
        --------
        
        Notes
        -----
        For hashable object is the Notes of add_nodes method.
        """

        largs = list(args)
        assert len(largs) <= 3

        if len(largs) > 0 and isinstance(largs[-1], dict):
            attr_dict = largs.pop(-1)
        else:
            if 'attr_dict' in attr:
                attr_dict = attr['attr_dict']
                del attr['attr_dict']
            else:
                attr_dict = None

        assert len(largs) <= 2

        if u not in self.node:
            raise (dfpe.UnknownNodeError(u))
        if v not in self.node:
            raise (dfpe.UnknownNodeError(v))

        v_args = self.node[v]['args']
        u_rets = self.node[u]['rets']

        if len(largs) == 2:
            u_out, v_in = largs
        elif len(largs) == 1:
            if tuple(u_rets) == dfpd.OBJ_CALL_RETS or len(u_rets) == 1:
                if tuple(u_rets) == dfpd.OBJ_CALL_RETS:
                    u_out = dfpd.OBJ_CALL_RET
                else:
                    assert len(u_rets) == 1
                    u_out = u_rets[0]
                v_in = largs[0]
                # assert v_in in v_args
                # self.add_edge(u, v, u_out, v_in, attr_dict = attr_dict, **attr)
                # return
            else:
                if len(v_args) == 1:
                    # v_args has 1-dimension
                    # l_args[0] is used to select entries in u_rets
                    u_out = largs[0]
                    assert u_out in u_rets
                    v_in = v_args[0]
                else:
                    # u_rets and v_args are multidimentional
                    # largs[0] is used to identify one entry in the intersection of u_rets and v_args
                    assert len(u_rets) > 1
                    assert len(v_args) > 1
                    common_args = set(u_rets).intersection(v_args).intersection(largs)
                    for arg in common_args:
                        self.add_edge(u, v, arg, arg, attr_dict=attr_dict, **attr)
                    return
                    # raise(dfpe.DafpyError('We do not know if {!r} is to select u_rets or v_args'.format(largs)))
        else:
            assert len(largs) == 0
            # nothing is specified
            # we try to connect all common output/input
            if tuple(u_rets) == dfpd.OBJ_CALL_RETS:
                # no intersection is searched
                u_out = dfpd.OBJ_CALL_RET
                v_args = self.node[v]['args']
                # assert len(v_args) == 1, "We don't know how to do for more than one argument"
                for v_in in v_args:
                    self.add_edge(u, v, u_out, v_in, attr_dict=attr_dict, **attr)
            else:
                if len(u_rets) == 1 and len(v_args) == 1:
                    u_out = u_rets[0]
                    v_in = v_args[0]
                    self.add_edge(u, v, u_out, v_in, attr_dict=attr_dict, **attr)
                else:
                    common_args = set(u_rets).intersection(v_args)
                    for arg in common_args:
                        self.add_edge(u, v, arg, attr_dict=attr_dict, **attr)
            return

        if self._is_locked:
            raise (dfpe.DataflowLockedError())

        if v_in not in self.node[v]['args']:
            raise (dfpe.WrongEdgeArgsError(v_in, v, v_args))

        u_rets = self.node[u]['rets']
        if tuple(u_rets) != dfpd.OBJ_CALL_RETS:
            if u_out not in u_rets:
                raise (dfpe.WrongEdgeArgsError(u_out, u, u_rets))

        assert u in self
        assert v in self
        assert v_in in self.node[v]['args']

        # set up attribute dictionary
        if attr_dict is None:
            attr_dict = attr
        else:
            try:
                attr_dict.update(attr)
            except AttributeError:
                raise dfpe.DafpyError("The attr_dict argument must be a dictionary.")

        if self.verbose:
            print "(node {!r} : arg {!r}  ) <-- (node {!r} : ret {!r} )".format(v, v_in, u, u_out)
            # add the edge
        datadict = {}
        datadict.update(attr_dict)

        if u_out not in self.succ[u]:
            self.succ[u][u_out] = {}
        if v not in self.succ[u][u_out]:
            self.succ[u][u_out][v] = {}
        if v_in in self.succ[u][u_out][v]:
            raise (dfpe.DuplicatedEdgeError("""{!r} {!r} {!r} {!r} """.format(u, v, u_out, v_in)))
        self.succ[u][u_out][v][v_in] = datadict

        if v_in not in self.pred[v]:
            self.pred[v][v_in] = {}
        if u not in self.pred[v][v_in]:
            self.pred[v][v_in][u] = {}
        assert u_out not in self.pred[v][v_in][u]
        # This Exception can not be raised by construction
        # raise(dfpe.DuplicatedEdgeError(''))
        assert len(self.pred[v][v_in][u]) == 0, "input ({!r}, {!r}) is already connected to output ({!r}, {!r})".format(
            v, v_in, u, self.pred[v][v_in][u].keys())
        self.pred[v][v_in][u][u_out] = datadict

        assert len(
            self.pred[v][v_in]) == 1, "input {!r} of node {!r} is has to too many predecessor nodes: {!r}".format(v_in,
                                                                                                                  v,
                                                                                                                  self.pred[
                                                                                                                      v][
                                                                                                                      v_in].keys())
        assert len(self.pred[v][v_in][
                       u]) == 1, "input {!r} of node {!r} is has to too many predecessor outputs {!r} of node {!r} ".format(
            v_in, v, self.pred[v][v_in][u].keys(), u)

        # add data to the edge (facilitate the acces to data later)
        if u not in self._nodeoutdata:
            self._nodeoutdata[u] = {}
        if u_out not in self._nodeoutdata[u]:
            self._nodeoutdata[u][u_out] = {}
            # assert 'data' not in datadict
            # datadict['_nodeoutdata'] = self._nodeoutdata[u][u_out]

            # if v not in self._indata:
            #    self._indata[v] = {}
            # self._indata[v][v_in] = self._nodeoutdata[u][u_out]

    def add_edge_call_args(self, v, *args):
        """
        Add a edge to the Dataflow instance that relates its __call__ arguments to a node of the graph.

        :param arg_id: argument of the __call__ method
        :param node_id: node_id at the end of the edge
        :param input_id: argument_id of the node **node_id**

        No attribute can be specified for this edges (need for developement of graphs)
        
        """
        if dfpd.CALL_ARGS_NODE_ID not in self.node:
            raise (dfpe.DafpyError('Specify call args with Dataflow.set_call_args() method or property self.args'))
        self.add_edge(dfpd.CALL_ARGS_NODE_ID, v, *args)

    def add_edge_call_rets(self, u, *args):
        """
        Add a edge to the Dataflow instance that relates a node of the graph to its __call__ return arguments.

        :param node_id: node_id at the start of the edge
        :param output_id: return argument of the node **node_id**
        :param ret_id: id ot the __call__ returned data

        No attribute can be specified for this edges (need for developement of graphs)
        """
        if dfpd.CALL_RETS_NODE_ID not in self.node:
            self.set_call_rets(dfpd.OBJ_CALL_RET)
            # raise(dfpe.DafpyError('Specify call rets with Dataflow.set_call_rets() method'))
        self.add_edge(u, dfpd.CALL_RETS_NODE_ID, *args)

    def add_edge_call_args_rets(self, *args):
        assert dfpd.CALL_ARGS_NODE_ID in self.node
        assert dfpd.CALL_RETS_NODE_ID in self.node
        self.add_edge(dfpd.CALL_ARGS_NODE_ID, dfpd.CALL_RETS_NODE_ID, *args)

    def set_call_args(self, call_spec=None):
        if dfpd.CALL_ARGS_NODE_ID not in self:
            self.add_node(dfpd.CALL_ARGS_NODE_ID)
        if call_spec is None:
            self.graph['args'] = dfpd.OBJ_CALL_RETS
            attr = self.node[dfpd.CALL_ARGS_NODE_ID]
            # attr['args'] = dfpd.OBJ_CALL_RETS
            attr['rets'] = dfpd.OBJ_CALL_RETS  # TODO: check if it is the right thing to do
        else:
            # TODO: do not use dfpfunc.set_call_args and  dfp.get_call_args
            # Do the conversion directly
            dfpfunc.set_call_args(self, call_spec)
            formated_call_args = dfp.get_call_args(self)

            self.graph['args'] = formated_call_args

            attr = self.node[dfpd.CALL_ARGS_NODE_ID]
            attr['args'] = formated_call_args
            attr['rets'] = formated_call_args

    def set_call_rets(self, ret_spec):
        dfpfunc.set_call_rets(self, ret_spec)
        formated_call_rets = dfp.get_call_rets(self)

        if dfpd.CALL_RETS_NODE_ID not in self:
            self.add_node(dfpd.CALL_RETS_NODE_ID)

        self.graph['rets'] = formated_call_rets
        attr = self.node[dfpd.CALL_RETS_NODE_ID]
        attr['args'] = formated_call_rets

    #######################################
    # Helper function add_node_..., add_edge_...
    #######################################

    def add_nodes_from(self, *args, **attr):
        """
        Add multiple nodes.

        Parameters
        ----------
        nodes : iterable container
            A container of nodes (list, dict, set, etc.).
            OR
            A container of (node, attribute dict) tuples.
            Node attributes are updated using the attribute dict.
        attr : keyword arguments, optional (default= no attributes)
            Update attributes for all nodes in nodes.
            Node attributes specified in nodes as a tuple
            take precedence over attributes specified generally.

        See Also
        --------
        add_node

        Examples
        --------
        >>> G = dfp.DataflowGraphBase()   # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> G.add_nodes_from('Helo', func = lambda x : x)
        >>> K3 = dfp.DataflowGraphBase()
        >>> K3.add_path([0,1, 2], func = lambda x : x)
        >>> G.add_nodes_from(K3)
        >>> sorted(G.nodes(),key=str)
        [0, 1, 2, 'H', 'e', 'l', 'o']


        Use keywords to update specific node attributes for every node.

        >>> G.add_nodes_from([3,4], func = lambda x : x, size=10)
        >>> G.add_nodes_from([5,6], func = lambda x : x, weight=0.4)

        Use (node, attrdict) tuples to update attributes for specific
        nodes.

        >>> G.add_nodes_from([(7,dict(size=11)), (8,{'color':'blue'})], func = lambda x : x)
        >>> G.node[7]['size']
        11
        >>> H = dfp.DataflowGraphBase()
        >>> H.add_nodes_from(G)
        >>> H.node[7]['size']
        11

        """
        assert len(args) <= 2
        assert len(args) >= 1

        if len(args) == 2:
            nodes, calls = args
            nlist = list(nodes)
            ncall = list(calls)
            for n, c in zip(nlist, ncall):
                self.add_node(n, c, **attr)
        else:
            nlist = args[0]
            if isinstance(nlist, dict):
                assert False, "Not covered by dafpy unittests, please add a test from the running example"
                for n, n_attr in nlist.iteritems():
                    node_attr = {}
                    node_attr.update(attr)
                    node_attr.update(n_attr)
                    self.add_node(n, attr_dict=node_attr)
            elif isinstance(nlist, DataflowGraphBase):
                for n, n_attr in nlist:
                    node_attr = {}
                    node_attr.update(attr)
                    node_attr.update(n_attr)
                    self.add_node(n, attr_dict=node_attr)
            else:
                for n in nlist:
                    try:
                        self.add_node(n, **attr)
                    except TypeError:
                        assert len(n) == 2
                        node_attr = {}
                        node_attr.update(attr)
                        node_attr.update(n[1])
                        self.add_node(n[0], attr_dict=node_attr)

    def add_edges_from(self, edges, **attr):
        for edge in edges:
            self.add_edge(*edge, **attr)

    def add_path(self, *args, **attr):
        """Add a path.

        Parameters
        ----------
        nodes : iterable container
            A container of nodes.  A path will be constructed from
            the nodes (in order) and added to the graph.
        calls : iterable container
            A container of callables associated to nodes.
        attr : keyword arguments, optional (default= no attributes)
            Attributes to add to every edge in path.

        See Also
        --------
        add_star, add_cycle

        Examples
        --------
        >>> G=dfp.DataflowGraphBase()   # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> G.add_path([0,1,2,3], [lambda x : x] * 4)
        >>> G.add_path([10,11,12], [lambda x : x] * 3, weight=7)


        The same call can be used for all the nodes:
        >>> G=dfp.DataflowGraphBase()   # or DiGraph, MultiGraph, MultiDiGraph, etc
        >>> G.add_path([0,1,2,3], func = lambda x : x)
        """

        self.add_nodes_from(*args, **attr)
        nlist = args[0]
        edges = zip(nlist[:-1], nlist[1:])
        self.add_edges_from(edges)

    ###############################
    # Lock of the Graph, checks...
    ###############################

    def lock(self, develop=False, level=1):
        """
        Lock the Dataflow instance        
        """
        self._is_locked = True
        self._check()

        if develop:
            self.develop_graph(level)

    def _ckeck_rets(self, call_rets, succ_rets, node_id):
        diff_ = call_rets - succ_rets
        if diff_:
            # this case correspond to outputs not used.
            pass
        diff_ = succ_rets - call_rets
        if diff_:
            pass

    def _check_args(self, call_args, pred_args, node_id):
        diff_ = call_args - pred_args
        if diff_:
            assert node_id != dfpd.CALL_RETS_NODE_ID, "already tested at the top of _check() method"
            raise (dfpe.UnlinkedInputError(
                'Graph {!r} : node_id {!r} has unlinked args: {!r}'.format(self.name, node_id, list(diff_))))
        diff_ = pred_args - call_args
        if diff_:
            assert False, "Not covered by dafpy unittests, please add a test from the running example"
            raise (dfpe.UnlinkedInputError(
                'Graph {!r} : node_id {!r} has no args in: {!r}'.format(self.name, node_id, list(diff_))))

    def _check(self):

        if self.args:
            diff_ = set(self.args) - set(self.succ[dfpd.CALL_ARGS_NODE_ID].keys())
            if diff_:
                message = 'Graph {!r} : has unlinked call_args: {!r}'.format(self.name, list(diff_))
                raise (dfpe.UnlinkedInputError(message))

        # check callables args with edge structure
        for node_id in self.callable_nodes():
            call_args = set(self.node[node_id]['args'])
            if call_args:
                if node_id not in self.pred:
                    assert False, "Not covered by dafpy unittests, please add a test from the running example"
                if not self.pred[node_id]:
                    raise (dfpe.UnlinkedInputError(
                        'Graph {!r} : node_id {!r} has no connection to its input'.format(self.name, node_id)))
                pred_args = set(self.pred[node_id].keys())
                if tuple(pred_args) != dfpd.OBJ_CALL_ARGS:
                    self._check_args(call_args, pred_args, node_id)

        for node_id in self.callable_nodes():
            call_rets = self.node[node_id]['rets']
            if call_rets:
                if tuple(call_rets) != dfpd.OBJ_CALL_RETS:
                    call_rets = set(call_rets)
                    if node_id not in self.succ:
                        assert False, "Not covered by dafpy unittests, please add a test from the running example"
                    succ_rets = set(self.succ[node_id].keys())
                    self._ckeck_rets(call_rets, succ_rets, node_id)

    #########################################
    # Graph development
    #########################################

    @staticmethod
    def _get_new_node_id(node_id, sub_node_id):
        if sub_node_id is not node_id:
            return (node_id, sub_node_id)
        else:
            return node_id

    def _add_nodes_from_graph(self, parent_node_id, g_dev):
        assert isinstance(g_dev, DataflowGraphBase)

        for node_id, node_attr in g_dev.node.iteritems():
            if 'func' in node_attr:
                nested_name = self._get_new_node_id(parent_node_id, node_id)
                self.add_node(nested_name, attr_dict=node_attr)
                # if node_id in self.reset_task:
                #    assert False, "check if it is correct"
                #    g.reset_task[nested_name] = g.task[nested_name]

    def _add_edges_from_graph_internals(self, parent_node_id, g_dev):
        for u, v, u_out, v_in, edge_attr in g_dev._iter_pred_edges():
            if u not in [dfpd.CALL_ARGS_NODE_ID, dfpd.CALL_RETS_NODE_ID] and v not in [dfpd.CALL_ARGS_NODE_ID,
                                                                                       dfpd.CALL_RETS_NODE_ID]:
                self.add_edge(self._get_new_node_id(parent_node_id, u),
                              self._get_new_node_id(parent_node_id, v),
                              u_out, v_in,
                              attr_dict=edge_attr)

    def _add_edges_from_graph_out(self, parent_u, parent_v, parent_u_out, parent_v_in, parent_edge_attr, g_dev_u_out,
                                  parent_graph):
        """
        u edges connect to v will be developped

        """
        for u, u_dict in g_dev_u_out.pred[dfpd.CALL_RETS_NODE_ID][parent_u_out].iteritems():
            for u_out, edge_attr_out in u_dict.iteritems():
                if u != dfpd.CALL_ARGS_NODE_ID:
                    edge_attr = {}
                    edge_attr.update(edge_attr_out)
                    edge_attr.update(parent_edge_attr)
                    self.add_edge(self._get_new_node_id(parent_u, u),
                                  parent_v,
                                  u_out,
                                  parent_v_in, attr_dict=edge_attr)
                else:
                    dict2 = parent_graph.pred[parent_u][u_out]
                    for u2, u2_dict in dict2.iteritems():
                        for u2_out, edge_attr_u2 in u2_dict.iteritems():
                            edge_attr = {}
                            edge_attr.update(edge_attr_out)
                            edge_attr.update(edge_attr_u2)
                            edge_attr.update(parent_edge_attr)
                            edge = u2, parent_v, u2_out, parent_v_in, edge_attr
                            if edge not in self.edges():
                                self.add_edge(*edge)

    def add_edges_from_graph_in(self, parent_u, parent_v, parent_u_out, parent_v_in, parent_edge_attr, gdev_v_in,
                                parent_graph):
        for v, v_dict in gdev_v_in.succ[dfpd.CALL_ARGS_NODE_ID][parent_v_in].iteritems():
            for v_in, edge_attr_in in v_dict.iteritems():
                if v != dfpd.CALL_RETS_NODE_ID:
                    edge_attr = {}
                    edge_attr.update(edge_attr_in)
                    edge_attr.update(parent_edge_attr)
                    self.add_edge(parent_u, self._get_new_node_id(parent_v, v),
                                  parent_u_out, v_in, attr_dict=edge_attr)
                else:
                    dict2 = parent_graph.succ[parent_v][v_in]
                    for v2, v2_dict in dict2.iteritems():
                        for v2_in, edge_attr_v2 in v2_dict.iteritems():
                            edge_attr = {}
                            edge_attr.update(edge_attr_in)
                            edge_attr.update(edge_attr_v2)
                            edge_attr.update(parent_edge_attr)
                            edge = parent_u, v2, parent_u_out, v2_in, edge_attr
                            if edge not in self.edges():
                                self.add_edge(*edge)

    def add_edges_from_graph_in_and_out(self, parent_u, parent_v, parent_u_out, parent_v_in, parent_edge_attr,
                                        g_dev_u_out, g_dev_v_in):
        for v, v_dict in g_dev_v_in.succ[dfpd.CALL_ARGS_NODE_ID][parent_v_in].iteritems():
            for v_in, edge_attr_in in v_dict.iteritems():
                for u, u_dict in g_dev_u_out.pred[dfpd.CALL_RETS_NODE_ID][parent_u_out].iteritems():
                    for u_out, edge_attr_out in u_dict.iteritems():
                        if u != dfpd.CALL_ARGS_NODE_ID and v != dfpd.CALL_RETS_NODE_ID:
                            edge_attr = {}
                            edge_attr.update(edge_attr_in)
                            edge_attr.update(edge_attr_out)
                            edge_attr.update(parent_edge_attr)
                            self.add_edge(self._get_new_node_id(parent_u, u),
                                          self._get_new_node_id(parent_v, v),
                                          u_out, v_in, attr_dict=edge_attr)

    def develop_graph(self, level):
        """
        Develop the nodes of the graph that are Dataflow callables

        """
        if not self._is_locked:
            raise (dfpe.DataflowNotLockedError())

        if len(self.developable_nodes()) == 0:
            return

        nodes_to_develop = copy.copy(self.developable_nodes())
        developed_node = []

        while len(nodes_to_develop) > 0:
            node_to_develop = nodes_to_develop.pop(0)
            g = dfp.DataflowEnvironment(name=self.name + '_develop_node_{!r}'.format(node_to_develop))

            # set callable nodes which are not being developed
            for node_id, node_attr in self.iter_nodes():
                if node_id != node_to_develop:
                    g.add_node(node_id, attr_dict=node_attr)
                    # if node_id in self.reset_task:
                    #    assert False, "check if it is correct"
                    #    g.reset_task[node_id] =  g.task[node_id]

            # develop nodes and developable nodes's internal edges
            node_id = node_to_develop
            node_attr = self.node[node_id]

            g._add_nodes_from_graph(node_id, node_attr['func'])
            g._add_edges_from_graph_internals(node_id, node_attr['func'])

            # develop developable nodes's edges related to call args and ret
            for u, v, u_out, v_in, edge_attr in self.iter_edges():
                if u == node_id:
                    if v == node_id:
                        g.add_edges_from_graph_in_and_out(u, v, u_out, v_in, edge_attr, self.node[u]['func'],
                                                          self.node[v]['func'])
                    else:
                        g._add_edges_from_graph_out(u, v, u_out, v_in, edge_attr, self.node[u]['func'], self)
                elif v == node_id:
                    g.add_edges_from_graph_in(u, v, u_out, v_in, edge_attr, self.node[v]['func'], self)
                else:
                    g.add_edge(u, v, u_out, v_in, attr_dict=edge_attr)

            g.lock(develop=False)  # will not develop subgraph of g
            self.set_from(g)
            developed_node.append(node_id)

        self._check()

        if level > 1:
            self.develop_graph(level - 1)
