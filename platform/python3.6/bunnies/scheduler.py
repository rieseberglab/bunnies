# abstract scheduler

from collections import OrderedDict


class SchedNode(object):
    """
    Abstract nodes representing tasks that can be scheduled.

    nodes start in 'waiting' state.

    they become 'ready' when their dependencies are satisfied
    failing a node that is submitted just logs the failure and places the
    node back in ready state (this allows retries). it needs to be explitly
    cancelled to be considered fatal.
    """
    __slots__ = ("uid", "sched", "state", "failures", "deps", "rdeps")

    def __init__(self, uid, sched):
        self.uid = uid
        self.sched = sched
        self.state = 'waiting'  # waiting, ready, done, cancelled
        self.failures = []
        self.deps = {}
        self.rdeps = {}

    def is_active(self):
        return self.state not in ('done', 'cancelled')

    def cascade(self):
        self.sched.dequeue(self)

        if not self.is_active():
            # update dependents
            rdeps = [d for d in self.rdeps.values()]
            for rdep in rdeps:
                if rdep.is_active():
                    rdep.cascade()
            return

        # node is either: ready, waiting, submitted

        if self.state == "submitted":
            # we're not going to change that state. we need to wait for
            # the user to change state explicitly.
            return

        deps = [d for d in self.deps.values()]
        dep_states = {}
        for d in deps:
            dep_states.setdefault(d.state, []).append(d)

        if len([d for d in deps if d.state == 'done']) == len(deps):
            # active nodes with all dependencies satisfied (or no dependencies) become ready
            if self.state == 'waiting':
                self.state = 'ready'
                self.sched.enqueue(self)
            return

        if 'cancelled' in dep_states:
            # one or more jobs we depend on are cancelled

            # cancel this node
            if self.state not in ('submitted',):
                self.cancel()

            # if the node is already submitted, user is expected to
            # cancel explicitly (we let the job finish)
            return

        if 'waiting' in dep_states or 'submitted' in dep_states:
            assert self.state != "submitted"
            self.state = 'waiting'
            return

        if self.state == 'ready':
            self.sched.enqueue(self)
            return

    def depends_on(self, dep_node):
        assert self.state in ('waiting', 'ready')
        self.deps[dep_node.uid] = dep_node
        dep_node.rdeps[dep_node.uid] = self

    def failed(self, reason="failed"):
        assert self.state == 'submitted'
        self.failures.append(reason)
        self.state = 'waiting'
        self.cascade()

    def submit(self):
        assert self.state == 'ready'
        self.state = 'submitted'
        self.cascade()

    def done(self):
        assert self.state in ("ready", "submitted")
        self.state = 'done'
        self.cascade()

    def cancel(self):
        assert self.state in ('waiting', 'ready', 'submitted')
        self.state = 'cancelled'
        self.cascade()


def get_leaves(n, visited=None):
    if n.uid in visited:
        return

    visited[n.uid] = True
    if len(n.deps) == 0:
        yield n
    else:
        for dep in n.deps:
            for leaf in get_leaves(dep, visited=visited):
                yield leaf


def Scheduler(object):
    """the design of this scheduler is that it should be invoked
       iteratively to obtain a list of nodes that are "ready" to process.

       each node that is ready should either:
          - be submitted: node.submitted()
          - be marked as done: node.done()
          - be cancelled: node.cancelled()
    """

    def __init__(self):
        self.nodes = OrderedDict()
        self.ready = OrderedDict()

    def initialize(self):
        visited = {}
        for node in self.nodes:
            for leaf in get_leaves(node, visited):
                leaf.cascade()

    def add_node(self, uid):
        if uid not in self.nodes:
            self.nodes[uid] = SchedNode(uid, self)
        return self.nodes[uid]

    def enqueue(self, node):
        if node.uid in self.ready:
            return
        self.ready[node.uid] = node

    def dequeue(self, node):
        self.ready.pop(node.uid, None)

    def status(self):
        """
        get a list of nodes that are ready for submission
        {
          'ready': [...]
          'done': [...]
          'cancelled': [...]
          'waiting': [...]
          'submitted': [...]
        }

        For updates, users should:

          - process ready nodes:
               - inspect failures
               - submit or cancel
        """
        status = {
            'ready': [],
            'done': [],
            'cancelled': [],
            'waiting': [],
            'submitted': []
        }
        for node in self.nodes:
            status[node.state].append(node)
        return status
