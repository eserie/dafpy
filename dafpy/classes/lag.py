from dafpy import coroutine


class Lag(object):
    """
    Lag is a callable class wich implement a lag.
    """

    def __init__(self, initial_state):
        self.state = initial_state
        self._reseted = False

    @property
    def __name__(self):
        return 'Lag'

    def reset(self):
        """
        The reset method return the non-consumed data
        """
        # Data can be consumed only one time
        if not self._reseted:
            self._reseted = True
            return self.state

    def __call__(self, lag_in):
        """
        The Lab call return the non-consumed data if it has not been already consumed (with the Lag.reset metho for example)
        """
        old_state = None
        if not self._reseted:
            old_state = self.reset()
        self.state = lag_in
        self._reseted = False
        return old_state


def gen_lag(env, initial_state=None):
    state_ = initial_state
    while True:
        data_ = env.next()
        yield state_
        state_ = data_


@coroutine
def co_lag(env, initial_state=None):
    state_ = initial_state
    while True:
        data_ = yield
        env.send(state_)
        state_ = data_
