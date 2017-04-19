
import dafpy as dfp


class Series(dfp.DataflowEnvironment):

    def copy(self):
        ctasks = [t.copy() for t in self.tasks]
        g = Series(*ctasks,
                    **self.env_attr)
        return g
        
    def __init__(self, *tasks,
                 **attr):
        self.tasks = tasks
        dfp.DataflowEnvironment.__init__(self, **attr)
        self.add_path(tasks)
        self.set_call_args(tasks[0].args)
        self.add_edge_call_args(tasks[0])
        self.set_call_rets(tasks[-1].rets)
        self.add_edge_call_rets(tasks[-1])
        self.start()    
