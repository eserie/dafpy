import unittest
from time import sleep

import dafpy as dfp

__all__ = ['FakeRunner', 'GlobalFakeRunner',
           'FakePendingException', 'FakeErrorException',
           'TestBaseFunctions']


###############################
# DEFINITIONS
###############################

###############################
# function for fake runner
###############################

class FakePendingException(Exception):
    pass


class FakeErrorException(Exception):
    pass


class FakeRunner(object):
    """
    jobid convention for this fake runner:
    -1 : failed
    0 : pending
    >0 : success

    """

    def get_jobids(self):
        self.JOBID += 1
        return [self.JOBID]

    def __init__(self):
        self.JOBID = 0
        self.jobs = []

    def append(self, delayed_func, **runner_args):
        self.jobs.append((delayed_func, runner_args))

    def run(self):
        # assert len(self.jobs) == 1
        jobids = list()
        while self.jobs:
            delayed_func, runner_args = self.jobs.pop(0)
            depends_on = runner_args.get('depends_on')
            func_joblist = self.get_jobids()
            func, args, kwargs = delayed_func
            if depends_on and len(depends_on) > 1:
                depends_on = dfp.concat_joblists(*depends_on)
            # if args[0] == 'pool1' and func.__name__ == 'generate_predictors':
            #    sdfsdf

            # propagate status
            if depends_on and min(depends_on) < 0:
                # error in predecessors
                jobids += [-1]
                continue
            elif depends_on and min(depends_on) == 0:
                # running job in predecessors: this job is pending
                jobids += [0]
                continue
            else:
                try:
                    func(*args, **kwargs)
                    jobids += func_joblist
                except FakeErrorException:
                    # error in execution
                    jobids += [-1]
                    continue
                except FakePendingException:
                    # error in execution
                    jobids += [0]
                    continue
            print 'run task {} with id {}, runner_args = {}'.format(func, jobids, runner_args)
        # if args[0] == 'pool1' and func.__name__ == 'generate_riskdata':
        #    sdfsdf
        return jobids

    def get_status(self, jobids):
        status = dict()
        for k, v in jobids.iteritems():
            if min(v) < 0:
                status[k] = 'invalid'
            elif min(v) == 0:
                status[k] = 'pending'
            else:
                status[k] = 'valid'
        return status


import pickle
import os


class GlobalFakeRunner(object):
    def get_jobids(self):
        if not os.path.exists('JOBID.pkl'):
            JOBID = 0
            with open('JOBID.pkl', 'w') as file_:
                pickle.dump(JOBID, file_)

        with open('JOBID.pkl', 'r') as file_:
            JOBID = pickle.load(file_)
        JOBID += 1
        with open('JOBID.pkl', 'w') as file_:
            pickle.dump(JOBID, file_)
        return [JOBID]

    def __init__(self):
        self.jobs = []

    def append(self, delayed_func, **runner_args):
        self.jobs.append((delayed_func, runner_args))

    def run(self):
        assert len(self.jobs) == 1
        while True:
            delayed_func, runner_args = self.jobs.pop(0)
            jobids = self.get_jobids()
            func, args, kwargs = delayed_func
            func(*args, **kwargs)
            print 'run task {} with id {}, runner_args = {}'.format(func, jobids, runner_args)
            return jobids


class TestBaseFunctions(unittest.TestCase):
    @staticmethod
    def generate_pricedata(pool):
        print 'computing pricedata ...'
        return pool

    @staticmethod
    def generate_pricedata_sleep(pool):
        print 'computing pricedata ...'
        sleep(3)
        return pool

    @staticmethod
    def generate_fundata(pool):
        print 'computing fundata ...'
        return pool

    @staticmethod
    def generate_riskdata(pool, riskname):
        print 'computing riskdata ...'
        if pool == 'pool1':
            raise (FakePendingException('error in computation'))
        return pool, riskname

    @staticmethod
    def generate_predictors(pool, riskname):
        print 'computing predictor ...'
        if pool == 'pool2':
            raise (FakeErrorException('error in computation'))
        return pool, riskname

    @staticmethod
    def generate_positions(pool, riskname, predname, algoname, **params):
        print "generate positions on {}{}{}{} with param {}".format(pool, riskname, predname, algoname, params)
        return (pool, riskname, predname, algoname)


generate_pricedata = TestBaseFunctions.generate_pricedata
generate_pricedata_sleep = TestBaseFunctions.generate_pricedata_sleep
generate_riskdata = TestBaseFunctions.generate_riskdata
generate_fundata = TestBaseFunctions.generate_fundata
generate_predictors = TestBaseFunctions.generate_predictors
generate_positions = TestBaseFunctions.generate_positions
