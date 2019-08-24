# abstract scheduler

class SchedNode(object):
    """
    Abstract nodes representing tasks that can be scheduled.

    nodes start in 'waiting' state.
    waiting nodes with no dependencies become 'ready'.

    failing a node that is submitted just logs the failure and places the
    node back in ready state (this allows retries). it needs to be explitly
    cancelled to be considered fatal.
    """

    def __init__(self, uid):
        self.uid = uid
        self.rdeps = [] # cascade dependencies
        self.state = 'waiting'
        self.failures = []

    def submitted(self):
        assert self.state == 'ready'
        self.state = 'submitted'

    def failed(self, reason="failed"):
        assert self.state == 'submitted'
        self.failures.append(reason)
        self.state = 'ready'

    def done(self):
        assert self.state == "ready"
        self.state = 'done'

    def cancelled(self):
        self.state = 'cancelled'

    def depends_on(self, dep):
        dep.rdeps.append(self)

def Scheduler(object):
    def __init__(self):
        self.nodes = []

    def schedule(self, node):
        
