import unittest
import collections

import dafpy as dfp

###############################
# TESTS
###############################

from dafpy.testing import FakeRunner, GlobalFakeRunner, FakePendingException, FakeErrorException, TestBaseFunctions


class TestDaskWorkflow(TestBaseFunctions):
    def test_async_dataflowenv(self):
        runner = FakeRunner()
        adfe = dfp.AsyncDataFlowEnv(runner=runner)

        pool = 'test'
        adfe.add_task('pricedata', dfp.delayed(self.generate_pricedata)(pool))
        adfe.add_task('fundata', dfp.delayed(self.generate_fundata)(pool))
        adfe.add_task('riskdata', dfp.delayed(self.generate_riskdata)(pool, 'risk'), depends_on=['pricedata'])
        adfe.add_task('pred', dfp.delayed(self.generate_predictors)(pool, 'risk'),
                      depends_on=['pricedata', 'fundata', 'riskdata'])
        # adfe.start()
        adfe()
        # adfe.pprint()
        print adfe.data
        assert adfe.data == {'riskdata': [3], 'fundata': [1], 'sum_alldata': [2, 1, 3, 4], 'pricedata': [2],
                             'pred': [4]}

    def test_job_delayed(self):
        runner = FakeRunner()
        pool = 'test'
        func = dfp.job_delayed(runner)(self.generate_pricedata)(pool)
        func()

    def test_job_delayed_with_dataflowenvironment(self):
        runner = FakeRunner()
        dfe = dfp.JobDataflow()
        pool = 'test'

        # decorate functions...

        generate_pricedata = dfp.job_delayed(runner)(self.generate_pricedata)
        generate_fundata = dfp.job_delayed(runner)(self.generate_fundata)
        generate_riskdata = dfp.job_delayed(runner)(self.generate_riskdata)
        generate_predictors = dfp.job_delayed(runner)(self.generate_predictors)

        # declare the dataflow
        dfe.add_task('pricedata', generate_pricedata(pool))
        dfe.add_task('fundata', generate_fundata(pool))
        dfe.add_task('riskdata', generate_riskdata(pool, 'risk'),
                     depends_on=['pricedata'])
        dfe.add_task('pred', generate_predictors(pool, 'risk'),
                     depends_on=['pricedata', 'fundata', 'riskdata'])
        dfe.start()
        dfe()
        dfe.pprint()
        assert dfe.data == {'riskdata': [3], 'fundata': [1], 'sum_alldata': [2, 1, 3, 4], 'pricedata': [2], 'pred': [4]}

    def test_job_delayed_with_dataflowenvironment_parallel(self):
        runner = FakeRunner()
        pool = 'test'

        # decorate functions...

        generate_pricedata = dfp.job_delayed(runner)(self.generate_pricedata)
        generate_fundata = dfp.job_delayed(runner)(self.generate_fundata)
        generate_riskdata = dfp.job_delayed(runner)(self.generate_riskdata)
        generate_predictors = dfp.job_delayed(runner)(self.generate_predictors)

        # declare the dataflow
        env_dict = dict()
        pools = ['pool1', 'pool2']
        for pool in pools:
            dfe = dfp.JobDataflow()
            dfe.add_task('pricedata', generate_pricedata(pool))
            dfe.add_task('fundata', generate_fundata(pool))
            dfe.add_task('riskdata', generate_riskdata(pool, 'risk'),
                         depends_on=['pricedata'])
            dfe.add_task('pred', generate_predictors(pool, 'risk'),
                         depends_on=['pricedata', 'fundata', 'riskdata'])

            # dfe.return_alldata()
            # dfe.start()
            # dfe()
            # sdfsdfsd
            env_dict[pool] = dfe
        dfe = dfp.JobDataflow()
        for pool in pools:
            dfe.add_task(pool, env_dict[pool])
        # dfe.return_alldata()
        dfe.start(develop=False)
        jobs = dfe()
        assert set(jobs) == set([0, 1, 2, 3, 5, 6, -1])
        # dfe.pprint()
        print dfe.data
        assert dfe.data == {'pool2': [2, 1, 3, -1], 'pool1': [6, 5, 0, 0], 'sum_alldata': [6, 5, 0, 0, 2, 1, 3, -1]}

    def test_job_delayed_with_dataflowenvironment_parallel_develop(self):
        runner = FakeRunner()
        pool = 'test'

        # decorate functions...

        generate_pricedata = dfp.job_delayed(runner)(self.generate_pricedata)
        generate_fundata = dfp.job_delayed(runner)(self.generate_fundata)
        generate_riskdata = dfp.job_delayed(runner)(self.generate_riskdata)
        generate_predictors = dfp.job_delayed(runner)(self.generate_predictors)

        # declare the dataflow
        dfe_dict = dict()
        pools = ['pool1', 'pool2']
        for pool in pools:
            dfe = dfp.JobDataflow()
            dfe.add_task('pricedata', generate_pricedata(pool))
            dfe.add_task('fundata', generate_fundata(pool))
            dfe.add_task('riskdata', generate_riskdata(pool, 'risk'),
                         depends_on=['pricedata'])
            dfe.add_task('pred', generate_predictors(pool, 'risk'),
                         depends_on=['pricedata', 'fundata', 'riskdata'])

            # dfe.return_alldata()
            dfe.start()
            # dfe()
            # sdfsdfsd
            dfe_dict[pool] = dfe
        dfe = dfp.JobDataflow()
        for pool in pools:
            dfe.add_task(pool, dfe_dict[pool])
        # dfe.return_alldata()
        # dfe.start(develop=True)
        jobs = dfe()
        assert set(jobs) == set([0, 1, 2, 3, 4, 5, -1])
        # dfe.pprint()
        print dfe.data
        assert dfe.data == {('pool1', 'sum_alldata'): [3, 4, 0, 0], ('pool2', 'riskdata'): [5],
                            ('pool2', 'fundata'): [2], ('pool2', 'pricedata'): [1], ('pool1', 'pred'): [0],
                            ('pool2', 'pred'): [-1], ('pool2', 'sum_alldata'): [1, 2, 5, -1],
                            'sum_alldata': [3, 4, 0, 0, 1, 2, 5, -1], ('pool1', 'riskdata'): [0],
                            ('pool1', 'pricedata'): [3], ('pool1', 'fundata'): [4]}

        # Try to convert the graph to a dask graph
        dsk = dfe.to_dask()
        from dask.async import get_sync as get
        runner.JOBID = 0
        res = dict(zip(dsk.keys(), get(dsk, dsk.keys())))

        assert set(res.keys()) == set(dfe.data.keys())
        assert len(res.keys()) == len(dfe.data.keys())

        set(res.keys()) - set(dfe.data.keys())
        set(dfe.data.keys()) - set(res.keys())

        # import pandas as pd
        # res_dfe = pd.DataFrame(dfe.data).ix[0]
        # res_dask = pd.DataFrame(res).ix[0]
        # df_comp = pd.concat([res_dfe, res_dask], keys=['dfe','dask'], axis=1)

        g = dfp.dask_to_dataflow(dsk)
        assert len(g) == len(dfe)
        assert set(g.nodes()) == set(dfe.nodes())
        assert len(g()) == len(dfe())

    def test_simple_dict_workflow(self):
        runner = FakeRunner()

        # decorate functions...

        generate_pricedata = dfp.job_delayed(runner)(self.generate_pricedata)
        generate_fundata = dfp.job_delayed(runner)(self.generate_fundata)
        generate_riskdata = dfp.job_delayed(runner)(self.generate_riskdata)
        generate_predictors = dfp.job_delayed(runner)(self.generate_predictors)

        # declare the dataflow
        dfd = dict()
        pools = ['pool1', 'pool2']
        for pool in pools:
            dfd[pool] = dict()
            dfd[pool]['pricedata'] = generate_pricedata(pool)()
            dfd[pool]['fundata'] = generate_fundata(pool)()
            dfd[pool]['riskdata'] = generate_riskdata(pool, 'risk')(dfd[pool]['pricedata'])
            dfd[pool]['pred'] = generate_predictors(pool, 'risk')(
                dfd[pool]['pricedata'] + dfd[pool]['fundata'] + dfd[pool]['riskdata'])

        assert dfd == {'pool2': {'riskdata': [7], 'fundata': [6], 'pricedata': [5], 'pred': [-1]},
                       'pool1': {'riskdata': [0], 'fundata': [2], 'pricedata': [1], 'pred': [0]}}

    def test_dask_workflow(self):
        import dask
        # runner = GlobalFakeRunner()
        runner = FakeRunner()

        # decorate functions...

        generate_pricedata = dfp.job_delayed(runner)(self.generate_pricedata)
        generate_fundata = dfp.job_delayed(runner)(self.generate_fundata)
        generate_riskdata = dfp.job_delayed(runner)(self.generate_riskdata)
        generate_predictors = dfp.job_delayed(runner)(self.generate_predictors)

        # declare the dataflow
        dsk = dict()
        pools = ['pool1', 'pool2']
        for pool in pools:
            dsk[(pool, 'pricedata')] = generate_pricedata(pool),
            dsk[(pool, 'fundata')] = generate_fundata(pool),
            dsk[(pool, 'riskdata')] = generate_riskdata(pool, 'risk'), (pool, 'pricedata')
            dsk[(pool, 'pred')] = generate_predictors(pool, 'risk'), [(pool, t) for t in
                                                                      ['pricedata', 'fundata', 'riskdata']]
        # from dask.multiprocessing import get
        # from dask.threaded import get
        from dask.async import get_sync as get
        # get(dsk, [(pool,'pred') for pool in pools])  # executes in parallel
        # results = get(dsk, dsk.keys())

        import pandas as pd
        jobids = pd.DataFrame(dict(zip(dsk.keys(), get(dsk, dsk.keys()))))
        assert jobids.shape == (1, 8)
        assert set(jobids.values.flatten().tolist()) == set([0, 1, 3, 5, 6, 7, -1])
        assert set(jobids.loc[slice(None), (slice(None), 'fundata')].values.flatten().tolist()) == set([7, 3])

    def test_dask_workflow_and_paramenter_sweeping(self):
        """
        We test a workflow with dask
        """
        import dask
        # runner = GlobalFakeRunner()
        runner = FakeRunner()

        # decorate functions...
        generate_pricedata = dfp.job_delayed(runner)(self.generate_pricedata)
        generate_fundata = dfp.job_delayed(runner)(self.generate_fundata)
        generate_riskdata = dfp.job_delayed(runner)(self.generate_riskdata)
        generate_predictors = dfp.job_delayed(runner)(self.generate_predictors)
        generate_positions = dfp.job_delayed(runner)(self.generate_positions)
        # declare the dataflow
        dsk = dict()
        pools = ['pool1', 'pool2', 'pool3']
        for pool in pools:
            dsk[(pool, 'pricedata')] = generate_pricedata(pool),
            dsk[(pool, 'fundata')] = generate_fundata(pool),
            dsk[(pool, 'riskdata')] = generate_riskdata(pool, 'risk'), (pool, 'pricedata')
            dsk[(pool, 'pred')] = generate_predictors(pool, 'risk'), [(pool, t) for t in
                                                                      ['pricedata', 'fundata', 'riskdata']]
            for max_risk in range(3):
                dsk[(pool, 'positions', ('max_risk', max_risk))] = generate_positions(pool, 'risk', 'momentum',
                                                                                      'markowitz_aversion',
                                                                                      max_risk=max_risk), (pool, 'pred')
        # from dask.multiprocessing import get
        # from dask.threaded import get
        from dask.async import get_sync as get
        # get(dsk, [(pool,'pred') for pool in pools])  # executes in parallel
        # results = get(dsk, dsk.keys())
        # Execute (to convert in other formats): dot mydask.dot -Teps > mydask.eps
        import pandas as pd
        jobids = dict(zip(dsk.keys(), get(dsk, dsk.keys())))
        jobids_s = pd.DataFrame(jobids).ix[0]
        assert len(jobids) == 21
        status = runner.get_status(jobids)
        assert status == {('pool3', 'pred'): 'valid', ('pool2', 'positions', ('max_risk', 2)): 'invalid',
                          ('pool2', 'riskdata'): 'valid', ('pool3', 'riskdata'): 'valid',
                          ('pool3', 'pricedata'): 'valid', ('pool3', 'fundata'): 'valid',
                          ('pool2', 'positions', ('max_risk', 0)): 'invalid', ('pool1', 'pred'): 'pending',
                          ('pool2', 'pred'): 'invalid', ('pool1', 'positions', ('max_risk', 1)): 'pending',
                          ('pool3', 'positions', ('max_risk', 0)): 'valid',
                          ('pool3', 'positions', ('max_risk', 2)): 'valid', ('pool2', 'fundata'): 'valid',
                          ('pool1', 'positions', ('max_risk', 2)): 'pending',
                          ('pool1', 'positions', ('max_risk', 0)): 'pending', ('pool1', 'riskdata'): 'pending',
                          ('pool2', 'pricedata'): 'valid', ('pool1', 'pricedata'): 'valid',
                          ('pool1', 'fundata'): 'valid', ('pool3', 'positions', ('max_risk', 1)): 'valid',
                          ('pool2', 'positions', ('max_risk', 1)): 'invalid'}
        # Plot the graph with color corresponding to the status of jobs
        from dask.dot import dot_graph
        # dot_graph(dsk)
        # sdfsdf
        def get_status_dot_attributes(v):
            if v == 'valid':
                return dict(style='filled', color='lightgreen')
            if v == 'invalid':
                return dict(style='filled', color='red')
            if v == 'pending':
                return dict(style='filled', color='lightgrey')

        dot_status = {k: get_status_dot_attributes(v) for k, v in status.iteritems()}
        dot_graph(dsk, filename='dask_graph', format='dot', data_attributes=dot_status, function_attributes=dot_status)
        # dot_graph(dsk, filename='dask_graph', format='png', data_attributes=dot_status, function_attributes=dot_status)
        # dot_graph(dsk, filename='dask_graph', format='pdf', data_attributes=dot_status, function_attributes=dot_status)
        dot_graph(dsk, filename='dask_graph', format='svg', data_attributes=dot_status, function_attributes=dot_status)
        # g = dask.dot.to_graphviz(dsk, data_attributes=status)

    def test_dask_workflow_with_explicit_parallel_sub_tasks(self):
        """
        We do explicit Parrallel call for some tasks
        """
        import dask
        # runner = GlobalFakeRunner()
        runner = FakeRunner()
        # decorate functions...

        generate_pricedata = dfp.job_delayed(runner)(self.generate_pricedata)
        generate_fundata = dfp.job_delayed(runner)(self.generate_fundata)
        generate_riskdata = dfp.job_delayed(runner)(self.generate_riskdata)
        generate_predictors = dfp.job_delayed(runner)(self.generate_predictors)
        generate_positions = dfp.delayed(self.generate_positions)

        # from dask.multiprocessing import get
        # from dask.threaded import get
        from dask.async import get_sync as get

        # declare the dataflow
        dsk = dict()
        pools = ['pool1', 'pool2']
        for pool in pools:
            dsk[(pool, 'pricedata')] = generate_pricedata(pool),
            dsk[(pool, 'fundata')] = generate_fundata(pool),
            dsk[(pool, 'riskdata')] = generate_riskdata(pool, 'risk'), (pool, 'pricedata')
            dsk[(pool, 'pred')] = generate_predictors(pool, 'risk'), [(pool, t) for t in
                                                                      ['pricedata', 'fundata', 'riskdata']]
            dsk[(pool, 'positions')] = dfp.ParallelJobs(runner)(
                [generate_positions(pool, 'risk', 'momentum', 'markowitz_aversion', max_risk=max_risk) for max_risk in
                 range(10)]), (pool, 'pred')
        # get(dsk, [(pool,'pred') for pool in pools])  # executes in parallel
        # results = get(dsk, dsk.keys())
        jobids = dict(zip(dsk.keys(), get(dsk, dsk.keys())))
        assert len(jobids) == 10
        assert jobids[('pool2', 'positions')] == [19, 20, 21, 22, 23, 24, 25, 26, 27, 28]
        get(dsk, ('pool2', 'positions'))
        get(dsk, ('pool2', 'positions'))


def test_dask():
    import dask.array as da
    x = da.ones((5, 15), chunks=(5, 5))
    d = (x + 1).dask
    from dask.dot import dot_graph
    dot_graph(d, format='svg')


if __name__ == '__main__':
    # test_async_dataflowenv()
    # test_job_delayed_with_dataflowenvironment()
    # test_job_delayed_with_dataflowenvironment_parallel()
    # test_job_delayed_dict_env()
    test_dask_workflow_and_paramenter_sweeping()
    # test_dask()
