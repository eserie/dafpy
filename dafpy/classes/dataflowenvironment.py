"""
This code attepmpt to construct a Dataflow  environement based on python coroutines, generators and callables

It is inspired by the code in:
http://www.dabeaz.com/coroutines/

"""
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

from dafpy.classes.dataflowgraphbase import DataflowGraphBase

__all__ = ['DataflowEnvironment', 'DataflowGraphBase', 'JobDataflow', 'dask_to_dataflow', 'AsyncDataFlowEnv']


class DataflowEnvironment(DataflowGraphBase):
    _reserved_task_ids = (dfpd.CALL_ARGS_NODE_ID,)

    def set_from(self, g):
        DataflowGraphBase.set_from(self, g)

        self.task = self.node
        self._taskoutdata = self._nodeoutdata

    def __init__(self, **attr):
        """
        :param name: string, optional (default='')
            An optional name for the Dataflow.
        :param attr: keyword arguments, optional (default= no attributes)
            Attributes to add to Dataflow as key=value pairs.

        """
        DataflowGraphBase.__init__(self, **attr)
        self.env_attr = self.graph

        self.task = self.node
        self.reset_task = collections.OrderedDict()
        self._taskoutdata = self._nodeoutdata

        self._queue = []
        self._executed = []
        self._ask_data = True  # use to pass the ask_data flag to _gen_env

        # started generator and coroutine internal environnements
        self._co_env_started = self._co_env()
        self._gen_env_started = self._gen_env()

        # started coroutine corresponding to the coroutine self.co()
        self._co_started = None

        # update graph attributes
        self.env_attr.update(attr)

    @property
    def data(self):
        # import pandas as pd
        data = dict()
        for tid in self.internal_nodes():
            data[tid] = self._taskoutdata[tid][dfpd.OBJ_CALL_RET]['value']
            # data = pd.Series(data)
        return data

    def add_lag(self, n, initial_state, attr_dict=None, **attr):
        """
        Add a Lag node to the DataflowEnvironment
        
        Parameters
        ----------        
        :param n : node
            A node can be any hashable Python object except None.
        :param initial_state:
            The initial state of the Lag node
        :param attr_dict: dictionary, optional (default= no attributes)
            Dictionary of node attributes.  Key/value pairs will            
        :param attr: keyword arguments, optional
            Set attributes using key=value.
        
        """
        func = dfp.Lag(initial_state)
        self.add_node(n, func=func, attr_dict=attr_dict, detect_func=False, **attr)

    def add_node(self, n, func=None, attr_dict=None, detect_func=True, **attr):
        """
        same as DataflowGraphBase.add_node
        """
        DataflowGraphBase.add_node(self, n, func, attr_dict, detect_func, **attr)
        if 'func' in self.node[n]:
            # TODO remove this functionality and set lag with colag or genlag in an API add_lag
            func = self.node[n]['func']
            if hasattr(func, "reset"):
                self.reset_task[n] = dict(func=func.reset)

    def pprint(self):

        print '-' * 30
        print '{:^30}'.format('env')
        pprint(self.graph)

        print '-' * 30
        print '{:^30}'.format('reset_task')
        pprint(self.reset_task)

        print '-' * 30
        print '{:^30}'.format('task')
        for item in self.task.iteritems():
            pprint(item)

        print '-' * 30
        print '{:^30}'.format('succ')
        for u, v, u_out, v_in, edge_attr in self._iter_succ_edges():
            print "(node {!r} : ret {!r}  ) --> (node {!r} : arg {!r} )".format(u, u_out, v, v_in),
            # print "(node {!r} : arg {!r}  ) <-- (node {!r} : ret {!r} )".format(v, v_in, u, u_out),
            if edge_attr:
                print ",attribute :  {!r:>10}".format(edge_attr)
            else:
                print ""

        print '-' * 30
        print '{:^30}'.format('pred')
        for u, v, u_out, v_in, edge_attr in self._iter_pred_edges():
            # print "(node {!r} : ret {!r}  ) --> (node {!r} : arg {!r} )".format(u, u_out, v, v_in),
            print "(node {!r} : arg {!r}  ) <-- (node {!r} : ret {!r} )".format(v, v_in, u, u_out),
            if edge_attr:
                print ",attribute :  {!r:>10}".format(edge_attr)
            else:
                print ""

        print ''

        print '-' * 30
        print '{:^30}'.format('taskoutdata')
        pprint(self._taskoutdata)
        # for item in self._taskoutdata.iteritems():
        #    pprint(item) 

    ###################
    # Tasks declaration
    ###################
    def _check_task_id(self, task):
        assert task not in DataflowEnvironment._reserved_task_ids, "The task id {!r} is a reserved named for DataflowEnvironment implementation".format(
            task)

    @staticmethod
    def _complete_filters(filters=None, depends_on=None):
        if depends_on is not None:
            if filters is None:
                filters = dict()
            filters['args'] = filters.get('args', [])
            filters['args'] += depends_on
        return filters

    def add_task(self, task, func=None, filters=None, depends_on=None, **attr):
        """
        :param task: node
             A node can be any hashable Python object except None.           
        :param f_gen: function
             The task is defined by a function or any callable object
        :param attr: keyword arguments, optional
            Set attributes using key=value.        
        """
        self._check_task_id(task)
        if func is None and callable(task):
            func = task

        filters = self._complete_filters(filters, depends_on)
        self._add_node_from_task(task, func, filters, attr)

        attr['func'] = func

        self.task[task].update(attr)

        if hasattr(func, 'reset'):
            self.reset_task[task] = dict(func=func.reset)

    def add_cotask(self, task, co=None, filters=None, reset=False, depends_on=None, **attr):
        """
        :param task: node
             A node can be any hashable Python object except None.           
        :param f_gen: coroutine
             The task is defined by a coroutine
        :param attr: keyword arguments, optional
            Set attributes using key=value.


        The generator task is assumed to take as first argument another generator

        Its other arguments can be passed with the option:
        co_args = {'args' : [], 'kwargs' : {}}        
        """
        self._check_task_id(task)
        if co is None and callable(task):
            co = task
        attr['co'] = co

        filters = self._complete_filters(filters, depends_on)
        self._add_node_from_cogentask(task, filters, attr)

        if 'co_args' not in attr:
            attr['co_args'] = dict(args=[self._co_env_started])
        else:
            attr['co_args']['args'] = [self._co_env_started] + attr['co_args']['args']

        self.task[task].update(attr)

        if reset:
            self.reset_task[task] = attr

    def add_gentask(self, task, gen=None, filters=None, initial=False, reset=False, depends_on=None, **attr):
        """
        :param task: node
             A node can be any hashable Python object except None.           
        :param gen: generator
             The task is defined by a generator with **no argument**
        :param attr: keyword arguments, optional
             Set attributes using key=value.

        The coroutine task is assumed to take as first argument another coroutine task
        Its other arguments can be passed with the option:
        gen_args = {'args' : [], 'kwargs' : {}}
            
        """
        self._check_task_id(task)
        if gen is None and callable(task):
            gen = task
        attr['gen'] = gen

        filters = self._complete_filters(filters, depends_on)
        self._add_node_from_cogentask(task, filters, attr, initial)

        if not initial:
            if 'gen_args' not in attr:
                attr['gen_args'] = dict(args=[self._gen_env_started])
            else:
                attr['gen_args']['args'] = [self._gen_env_started] + attr['gen_args']['args']

        self.task[task].update(attr)

        if reset:
            self.reset_task[task] = attr

    ##################################
    # Mapping with Dataflowgraph
    ##################################

    def _parse_filter(self, fil):
        if not isinstance(fil, list) and not isinstance(fil, tuple):
            u = fil
            if u in self and tuple(self.task[u]['rets']) != dfpd.OBJ_CALL_RETS and len(self.task[u]['rets']) == 1:
                u_out = self.task[u]['rets'][0]
            else:
                u_out = dfpd.OBJ_CALL_RET

            apply_filter = None
        else:
            assert len(fil) > 0
            fil = list(fil)
            if callable(fil[-1]):
                apply_filter = fil.pop(-1)
            else:
                apply_filter = None
            if len(fil) == 1:
                u, = fil
                u_out = dfpd.OBJ_CALL_RET
            elif len(fil) == 2:
                u, u_out = fil

        if u == 'call_args':
            assert u == dfpd.CALL_ARGS_NODE_ID
            u = dfpd.CALL_ARGS_NODE_ID
        return u, u_out, apply_filter

    def _add_node_from_task(self, task, func, filters, attr):
        #####################
        # check if it is usefull
        if 'args' not in attr:
            attr['args'] = dfpfunc.get_call_args(func)
        if 'rets' not in attr:
            attr['rets'] = dfpd.OBJ_CALL_RETS
        self._convert_args_rets_to_tuple(attr)

        v = task
        if v in self:
            # Attributes of the setted task will be replaced by attr
            # We change keys in already setted connections if the already setted connection is one dimensional
            # TODO: move this code in add_node
            # TODO: factorize the replacement part of self.pred and self.succ
            new_ret = attr['rets'][0]
            if new_ret != dfpd.OBJ_CALL_RET:
                if len(self.succ[v]) == 1:
                    old_key, dict_k = self.succ[v].items()[0]
                    if new_ret != old_key:
                        self.succ[v][new_ret] = dict_k
                        del self.succ[v][old_key]
                for u, u_dict in self.pred.iteritems():
                    for u_out, u_out_dict in u_dict.iteritems():
                        if v in u_out_dict:
                            if len(u_out_dict[v]) == 1:
                                old_key, dict_k = u_out_dict[v].items()[0]
                                if new_ret != old_key:
                                    u_out_dict[v][new_ret] = dict_k
                                    del u_out_dict[v][old_key]

        if filters is not None:
            args_filters = filters.get('args', [])
            kwargs_filters = filters.get('kwargs', {})
            if args_filters and not attr['args']:
                # TODO: check that it is ok...
                attr['args'] = args_filters
            if attr.get('args'):
                assert len(attr['args']) >= len(args_filters)
            if isinstance(filters, dict):
                # transform filters args into kwargs
                assert isinstance(args_filters, list) or isinstance(args_filters, tuple)
                for arg, fil in zip(attr['args'], args_filters):
                    assert arg not in kwargs_filters, 'arg specify in args and kwargs filters'
                    kwargs_filters[arg] = fil
                filters = dict(kwargs=kwargs_filters)

            for arg, fil in kwargs_filters.iteritems():
                u, _, _ = self._parse_filter(fil)
                if u not in self:
                    self.add_node(u, rets=dfpd.OBJ_CALL_RETS, detect_func=False)
                    if u == dfpd.CALL_ARGS_NODE_ID:
                        self.set_call_args(None)

            self.add_node(task, detect_func=False, attr_dict=attr)
            for arg, fil in kwargs_filters.iteritems():
                u, u_out, apply_filter = self._parse_filter(fil)
                v = task
                v_in = arg
                if u in self and tuple(self.task[u]['rets']) == dfpd.OBJ_CALL_RETS:
                    u_out = dfpd.OBJ_CALL_RET
                self.add_edge(u, v, u_out, v_in, apply=apply_filter)
        else:
            self.add_node(task, detect_func=False, attr_dict=attr)
            if attr['args']:
                v = task
                u = self.task.keys()[-1]
                if u is v:
                    u = self.task.keys()[-2]
                self.add_edge(u, v)

    def _add_node_from_cogentask(self, task, filters, attr, initial=False):
        args = dfpd.OBJ_CALL_ARGS

        self.add_node(task, detect_func=False, args=args, rets=dfpd.OBJ_CALL_RETS, attr_dict=attr)
        if filters:
            u, u_out, apply_filter = self._parse_filter(filters)
            if u not in self:
                self.add_node(u, detect_func=False, rets=dfpd.OBJ_CALL_RETS)
                if u == dfpd.CALL_ARGS_NODE_ID:
                    self.set_call_args(None)
            v = task
            v_in = dfpd.OBJ_CALL_ARG
            self.add_edge(u, v, u_out, v_in, apply=apply_filter)

    ####################################
    # Execution and Environement methods
    ####################################

    def _gen_env(self):
        """
        generator environnement used to execute generator tasks
        """
        while True:
            assert self._executed
            # tid = self._executed.pop(-1)
            tid = self._executed[-1]
            if self._ask_data:
                data_ = self._indata(tid)
                if data_ is not None:
                    data_ = data_[dfpd.OBJ_CALL_ARG]
                yield data_
            else:
                yield None

    @coroutine
    def _co_env(self):
        """
        coroutine environnement used to execute coroutine tasks
        """
        while True:
            res_ = yield
            if self._queue:
                tid = self._queue.pop(0)
                self._executed.append(tid)
                self._fill_result_data(tid, res_, reset_call=not self._ask_data)

    def run(self):
        """
        The run method should be called to start a simulation
        """
        print "start run"
        self._check_env_started()

        while True:
            try:
                self._co_started.next()
            except StopIteration:
                break
        print "end run"

    def _eliminate_OBJ_CALL_RET_level(self, ret_data_):
        if ret_data_ is not None and tuple(ret_data_.keys()) == dfpd.OBJ_CALL_RETS:
            assert len(ret_data_) == 1
            ret_data_ = ret_data_.items()[0][1]
        if isinstance(ret_data_, dict):
            ret_data_ = collections.namedtuple(self.name, self.rets)(**ret_data_)

        return ret_data_

    def __call__(self, *args, **kwargs):
        """
        call method of the instances of DataflowEnvironment
        """
        self._check_env_started()
        self._parse_call_args(args, kwargs)
        self._co_started.next()

        if self.rets:
            ret_data_ = self._indata(dfpd.CALL_RETS_NODE_ID)
            return self._eliminate_OBJ_CALL_RET_level(ret_data_)

    def gen(self, gen_in=None):
        """
        generator associated to the DataflowEnvironment instance

        :param gen_in: optional chained generator
        """
        self._check_env_started()

        while True:
            try:
                if gen_in is not None:
                    data_ = gen_in.next()
                    self._taskoutdata[dfpd.CALL_ARGS_NODE_ID][dfpd.OBJ_CALL_RET] = dict(value=data_)
                else:
                    data_ = None
                self._co_started.send(data_)
            except StopIteration:
                break
            if self.rets:
                ret_data_ = self._indata(dfpd.CALL_RETS_NODE_ID)
                yield self._eliminate_OBJ_CALL_RET_level(ret_data_)
            else:
                assert False, "Not covered by dafpy unittests, please add a test from the running example"
                yield None

    @coroutine
    def co(self, co_out=None):
        """
        coroutine associated to the DataflowEnvironment instance

        :param :co_out optional chained coroutine
        """

        ###########################
        # Execute tasks
        ###########################

        while True:
            data_ = yield
            if data_ is not None:
                self._taskoutdata[dfpd.CALL_ARGS_NODE_ID][dfpd.OBJ_CALL_RET] = dict(value=data_)
            else:
                if self.args:
                    assert dfpd.CALL_ARGS_NODE_ID in self._taskoutdata

            try:
                self._queue = self.reset_task.keys()
                self._executed = []
                for tid, t_attr in self.reset_task.iteritems():
                    assert self._queue[0] == tid
                    if not t_attr.get('co_started'):
                        self._queue.pop(0)
                        self._executed.append(tid)
                    self._execute_task(tid, t_attr, ask_data=False)
                self._reset_queue = self._queue
                self._reset_executed = self._executed

                self._queue = self.task.keys()
                self._executed = []
                for tid, t_attr in self.task.iteritems():
                    assert self._queue[0] == tid
                    if not t_attr.get('co_started'):
                        self._queue.pop(0)
                        self._executed.append(tid)
                    self._execute_task(tid, t_attr)
                if co_out is not None:
                    if self.rets:
                        ret_data_ = self._indata(dfpd.CALL_RETS_NODE_ID)
                        co_out.send(self._eliminate_OBJ_CALL_RET_level(ret_data_))
                    else:
                        assert False, "Not covered by dafpy unittests, please add a test from the running example"
                        co_out.send(None)
            except StopIteration:
                break

    def _execute_task(self, tid, t_attr, ask_data=True):
        """
        Execute the task corresponding to tid and declared in t_attr
        """
        if t_attr.get('func'):
            if ask_data:
                data_ = self._indata(tid)
                if data_ is None:
                    data_ = {}
            else:
                data_ = {}

            if 'args' in t_attr:
                args = [data_[key] for key in t_attr['args']]
                res_ = t_attr['func'](*args)
            else:
                res_ = t_attr['func'](**data_)

            if not ask_data or res_ is not None:
                # The not ask_data condition is here to populate the taskoutdata with None values for reset tasks => see test_lag_state()
                u_rets = self.task[tid]['rets']
                if tuple(u_rets) == dfpd.OBJ_CALL_RETS:
                    self._taskoutdata[tid][dfpd.OBJ_CALL_RET] = dict(value=res_)
                else:
                    if isinstance(res_, tuple) and hasattr(res_, '_asdict'):
                        res_ = res_._asdict()
                    if isinstance(res_, dict):
                        for u_out in u_rets:
                            self._taskoutdata[tid][u_out] = dict(value=res_[u_out])
                    elif isinstance(res_, list) or isinstance(res_, tuple):
                        assert len(res_) == len(u_rets)
                        for i, u_out in enumerate(u_rets):
                            self._taskoutdata[tid][u_out] = dict(value=res_[i])
                    else:
                        assert len(u_rets) == 1
                        u_out = u_rets[0]
                        self._taskoutdata[tid][u_out] = dict(value=res_)

        if t_attr.get('co_started'):
            data_ = None
            if ask_data:
                data_ = self._indata(tid)
                if data_ is not None:
                    data_ = data_[dfpd.OBJ_CALL_ARG]

            # prepare self._ask_dta for the .next() call
            # self._ask_data is used in _co_env to detect if it is an reset call or not
            orig_ask_data_ = self._ask_data
            self._ask_data = ask_data
            # execute the task and fill results
            t_attr['co_started'].send(data_)
            # restore the value
            self._ask_data = orig_ask_data_

        if t_attr.get('gen_started'):
            # prepare self._ask_dta for the .next() call
            orig_ask_data_ = self._ask_data
            self._ask_data = ask_data
            # execute the task
            res_ = t_attr['gen_started'].next()
            # fill results
            self._fill_result_data(tid, res_, reset_call=not self._ask_data)
            # restor the value
            self._ask_data = orig_ask_data_

    def _fill_result_data(self, tid, res_, reset_call=False):
        if tid in self.reset_task and not reset_call:
            # This situation correspond to the case where the data has already been filled during the "reset_task", so during this second evaluation, the result should be None otherwie we have two returned values for 1 call
            assert res_ is None
            return
        if dfpd.OBJ_CALL_RET not in self._taskoutdata[tid]:
            self._taskoutdata[tid][dfpd.OBJ_CALL_RET] = dict(value=res_)
        else:
            self._taskoutdata[tid][dfpd.OBJ_CALL_RET]['value'] = res_

    def _clean_outdata(self):
        for tid in self.task.keys():
            self._taskoutdata[tid].clear()

    #############################################################################################
    # Methods to start and initialize the environnement (execution plan, graph developement,...)
    #############################################################################################

    def _start_env(self):
        if self._co_started is None:
            self._co_started = self.co()

        ###########################
        # Initialize the coroutine
        ###########################

        for tid, t_attr in self.reset_task.iteritems():
            if 'co' in t_attr:
                args, kwargs = dfpf.data_to_args_kwargs(t_attr['co_args'])
                t_attr['co_started'] = t_attr['co'](*args, **kwargs)
            if 'gen' in t_attr:
                args, kwargs = dfpf.data_to_args_kwargs(t_attr['gen_args'])
                t_attr['gen_started'] = t_attr['gen'](*args, **kwargs)

        for tid, t_attr in self.task.iteritems():
            if 'co' in t_attr:
                args, kwargs = dfpf.data_to_args_kwargs(t_attr.get('co_args'))
                t_attr['co_started'] = t_attr['co'](*args, **kwargs)
            if 'gen' in t_attr:
                args, kwargs = dfpf.data_to_args_kwargs(t_attr.get('gen_args'))
                t_attr['gen_started'] = t_attr['gen'](*args, **kwargs)

    def _check_env_started(self):
        if not self._co_started:
            raise (dfpe.DataflowNotLockedError())
        assert self._co_started is not None, "The environnement is not started. Use self.start() method to start it"

    def _populate_datadict(self, tid):
        if tuple(self.task[tid]['rets']) == dfpd.OBJ_CALL_RETS:
            rets = dfpd.OBJ_CALL_RETS
        else:
            rets = self.task[tid]['rets']
        for out_id in rets:
            if tid not in self._taskoutdata:
                self._taskoutdata[tid] = {}
            self._taskoutdata[tid][out_id] = dict(value=True)

    def _filter(self, tid):
        def fil(pred_tid):
            if pred_tid in self._taskoutdata:
                filtered_taskoutdata = {out_id: out_dict for out_id, out_dict in self._taskoutdata[pred_tid].iteritems()
                                        if 'value' in out_dict}
                if tuple(self.task[pred_tid]['rets']) == dfpd.OBJ_CALL_RETS:
                    rets = dfpd.OBJ_CALL_RETS
                else:
                    rets = self.task[pred_tid]['rets']
                if tuple(rets) != dfpd.OBJ_CALL_RETS and tuple(filtered_taskoutdata.keys()) == dfpd.OBJ_CALL_RETS:
                    filtered_taskoutdata = filtered_taskoutdata[dfpd.OBJ_CALL_RET]['value']
                if isinstance(filtered_taskoutdata, tuple) and hasattr(filtered_taskoutdata, '_asdict'):
                    filtered_taskoutdata = filtered_taskoutdata._asdict()
                if isinstance(filtered_taskoutdata, dict):
                    if set(rets) == set(filtered_taskoutdata.keys()):
                        return True
                elif isinstance(filtered_taskoutdata, list) or isinstance(filtered_taskoutdata, tuple):
                    if len(rets) == len(filtered_taskoutdata):
                        return True

        return fil

    def _premisse(self, tid):
        if not self.pred[tid]:
            # the task do not need data
            return True
        else:
            connected_u = list(
                set([u for pred_tid, pred_tid_dict in self.pred[tid].iteritems() for u in pred_tid_dict]))
            u_with_data_ = filter(self._filter(tid), connected_u)
            if set(u_with_data_) == set(connected_u):
                return True
        return False

    def _indata(self, tid):
        """
        TODO: when the merge of api is finished. move the implementation of _indata in Dataflowenv

        """
        assert self._premisse(tid)
        return DataflowGraphBase._indata(self, tid)

    def _determine_exection_plan(self):
        """
        determine the execution plan of the DataflowEnvironment instance
        #TODO adapt Execution plan computation to dataflows with named outputs
        """

        self._queue = set(self.task.keys()) - set((dfpd.CALL_RETS_NODE_ID,))
        assert len(self._queue) == len(set(self._queue))
        self._executed = []

        # We do not update the queue for this tasks because they have also to be executed as normal tasks
        for tid in self.reset_task.keys():
            self._populate_datadict(tid)

        # 'call_args' is executed as a normal task
        if self.args:
            tid = dfpd.CALL_ARGS_NODE_ID
            self._queue.remove(tid)
            self._executed.append(tid)
            self._populate_datadict(tid)
        else:
            assert dfpd.CALL_ARGS_NODE_ID not in self
        while True:
            runned_task = False
            for tid in copy.copy(self._queue):
                if self._premisse(tid):
                    self._queue.remove(tid)
                    self._executed.append(tid)
                    self._populate_datadict(tid)
                    runned_task = True
            if not self._queue:
                assert set(self._executed) == set(self.task.keys()) - set((dfpd.CALL_RETS_NODE_ID,))
                if dfpd.CALL_RETS_NODE_ID in self:
                    assert self._premisse(
                        dfpd.CALL_RETS_NODE_ID), "The premisse of 'call_rets' should always be true by contsruction"
                break
            if not runned_task:
                raise (dfpe.StalledDataflowCallError("Stalled Graph"))
            assert runned_task, "Stalled Graph"
        # reorder task
        self.task = collections.OrderedDict([(tid, self.task[tid]) for tid in self._executed])
        assert set(self._taskoutdata.keys()) == set(self.task.keys())

        self._clean_outdata()

    def start(self, develop=False, level=1):
        """
        A DataflowEnvironment must be started in order to:
        - determine the execution plan
        - develop the sub Dataflows in case of feedbacks (not developed for DataflowEnvironment -> see DataflowGraphBase object for that)
        - start the internal coroutine self.co()
        """
        self._check()
        self.lock(develop, level)
        self._taskoutdata.clear()
        self._determine_exection_plan()

        self._start_env()

    def to_dask(self):
        dsk = dict()
        for v in self.callable_nodes():
            if 'func' in self.node[v]:
                v_dict = self.pred.get(v)
                if v_dict:
                    depends_on = list()
                    for v_in, v_in_dict in v_dict.iteritems():
                        for u, u_dict in v_in_dict.iteritems():
                            depends_on.append(u)
                            assert len(
                                u_dict) == 1, "jobs must have only one output to be convertible in dask dataflows"
                            # for u_out, edge_attr in u_dict.iteritems():
                    dsk[v] = self.node[v]['func'], depends_on
                else:
                    dsk[v] = self.node[v]['func'],
        return dsk


class JobDataflow(DataflowEnvironment):
    """
    This class add some special nodes "sum_alldata"
    This node is used to sum all the outputs of the graph.
    We assume that all nodes return job_ids.

    """

    def return_alldata(self):
        def sum_list(*args):
            sum_l = []
            for l in args:
                sum_l += l
            return sum_l

        internal_nodes = self.internal_nodes()
        self.add_node('sum_alldata', sum_list, args=['data_from_{}'.format(nodeid) for nodeid in self.internal_nodes()])
        for i, nodeid in enumerate(internal_nodes):
            self.add_edge(nodeid, 'sum_alldata', self.node[nodeid]['rets'], 'data_from_{}'.format(nodeid))
        self.add_edge_call_rets('sum_alldata')

    def start(self, develop=True):
        if 'sum_alldata' not in self:
            self.return_alldata()
        super(JobDataflow, self).start(develop=develop)

    def _check_env_started(self):
        '''
        Automatically start
        '''
        if not self._co_started:
            self.start()
        super(JobDataflow, self)._check_env_started()


def name(x):
    try:
        return str(hash(x))
    except TypeError:
        return str(hash(str(x)))


def dask_to_dataflow(dsk):
    from dask.core import istask, get_dependencies
    g = DataflowEnvironment()
    for k, v in dsk.items():
        k_name = k
        g.add_node(k_name, v[0])
    for k, v in dsk.items():
        k_name = k
        if callable(v[0]):
            deps = list(get_dependencies(dsk, k))
            print k_name, v[0], deps
            g.add_node(k_name, v[0], args=['args_{}'.format(i) for i, dep in enumerate(deps)])
            for i, dep in enumerate(deps):
                g.add_edge(dep, k_name, dfpd.OBJ_CALL_RETS, 'args_{}'.format(i))
    if 'sum_alldata' in g:
        g.add_edge_call_rets('sum_alldata')
    g.start()
    return g


###################################################
# Test of another API
# This not seems to be a good one
# This class will certainly be removed
###################################################

class AsyncDataFlowEnv(JobDataflow):
    _reserved_task_ids = (dfpd.CALL_ARGS_NODE_ID,)

    @staticmethod
    def _async_delayed(delayed_func, runner, **runner_args):
        def func(*job_lists):
            # ids = None
            # if job_lists:
            #    ids = list(zip(*job_lists)[0])
            runner.append(delayed_func, depends_on=job_lists, **runner_args)
            jobids = runner.run()
            return jobids

        return func

    def __init__(self, runner=None, **attr):
        """
        """
        self.runner = runner
        super(AsyncDataFlowEnv, self).__init__(**attr)

    ###################
    # Tasks declaration
    ###################

    def add_task(self, task, delayed_func, depends_on=None, **runner_args):
        """
        :param task: node
             A node can be any hashable Python object except None.           
        :param f_gen: function
             The task is defined by a function or any callable object
        :param attr: keyword arguments, optional
            Set attributes using key=value.        
        """
        func = self._async_delayed(delayed_func, self.runner)
        super(AsyncDataFlowEnv, self).add_task(task, func, depends_on=depends_on, **runner_args)
