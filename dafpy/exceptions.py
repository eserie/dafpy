##################
# Exceptions
##################

class DafpyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class DuplicatedReturnedDataError(DafpyError):
    pass


class UnlinkedInputError(DafpyError):
    pass


class UnlinkedOutputRunTimeError(DafpyError):
    pass


class UnlinkedInputAtRunTimeError(DafpyError):
    pass


class DuplicatedEdgeError(DafpyError):
    pass


class DuplicatedNodeError(DafpyError):
    pass


class UnknownNodeError(DafpyError):
    def __init__(self, node_id):
        self.value = "node {!r} does not exist in nodes of the graph".format(node_id)


class NoCallArgsSpecificationError(DafpyError):
    pass


class StalledDataflowCallError(DafpyError):
    pass


class DataflowNotLockedError(DafpyError):
    def __init__(self):
        self.value = 'The Dataflow is not locked. The operation being done is not permitted if the graph is not locked. Use the method lock() to lock the Dataflow'


class DataflowLockedError(DafpyError):
    def __init__(self):
        self.value = 'The Dataflow is locked. The operation being done is not permitted if the graph is locked'


class NotSpecifiedCallArgsError(DafpyError):
    def __init__(self, diff, call_args):
        self.value = '[Wrong Call Args] : The argument  {!r} of the callable could not be matched with the given arguments in the current call: {!r} '.format(
            diff, call_args)


class WrongSpecifiedCallArgsError(DafpyError):
    def __init__(self, diff, call_args):
        self.value = 'The arguments {!r} specified in arguments do not correspond to the declared arguments of the graph (with the set_call_args() method), which are {!r} '.format(
            diff, call_args)


class WrongEdgeArgsError(DafpyError):
    def __init__(self, u_out, u, u_rets):
        self.value = 'The ret id {!r} specified is not coherent with the rets {!r} specified for node {!r} '.format(
            u_out, u_rets, u)


class TooManyArgsError(DafpyError):
    pass
