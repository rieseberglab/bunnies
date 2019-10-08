# abstract scheduler

from collections import OrderedDict


class SchedError(Exception):
    pass


class SchedStateError(SchedError):
    pass


class SchedNode(object):
    """
    Abstract nodes representing tasks that can be scheduled.

    nodes start in 'waiting' state.

    they become 'ready' when their dependencies are satisfied
    failing a node that is submitted just logs the failure and places the
    node back in ready state (this allows retries). it needs to be explitly
    cancelled to be considered fatal.
    """
    __slots__ = ("uid", "sched", "state", "failures", "deps", "rdeps", "data")

    def __init__(self, uid, sched, data):
        self.uid = uid
        self.sched = sched
        self.state = 'waiting'  # waiting, ready, done, cancelled, submitted
        self.failures = []
        self.deps = {}
        self.rdeps = {}
        self.data = data

    def __str__(self):
        return "N(%s)" % (self.uid,)

    def __repr__(self):
        return "SchedNode(%s)" % (self.uid,)

    def is_active(self):
        return self.state not in ('done', 'cancelled')

    def __expect_state(self, op, valid):
        if self.state not in valid:
            raise SchedStateError("%s: invalid state transition. node %s is in state %s but expected one of %s" % (
                                  repr(op), self, repr(self.state), repr(valid)))

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
        self.__expect_state("depends_on", ('waiting', 'ready'))
        self.deps[dep_node.uid] = dep_node
        dep_node.rdeps[self.uid] = self

    def failed(self, reason="failed"):
        self.__expect_state("failed", ('submitted',))
        self.failures.append(reason)
        self.state = 'waiting'
        self.cascade()

    def submit(self):
        self.__expect_state("submit", ("ready",))
        self.state = 'submitted'
        self.cascade()

    def done(self):
        self.__expect_state("done", ("ready", "submitted"))
        self.state = 'done'
        self.cascade()

    def cancel(self):
        self.__expect_state("cancel", ("waiting", "ready", "submitted"))
        self.state = 'cancelled'
        self.cascade()


    # def __cmp__(self, other):
    #     if not other:
    #         return -1
    #     if not isinstance(other, SchedNode):
    #         return -1
    #     if self.uid < other.uid:
    #         return -1
    #     if self.uid > other.uid:
    #         return 1
    #     return 0


def get_leaves(n, visited=None):
    if n.uid in visited:
        return

    visited[n.uid] = True
    if len(n.deps) == 0:
        yield n
    else:
        for dep in n.deps.values():
            for leaf in get_leaves(dep, visited=visited):
                yield leaf


class Scheduler(object):
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
        for node in self.nodes.values():
            for leaf in get_leaves(node, visited):
                leaf.cascade()

    def add_node(self, uid, data=None):
        if uid not in self.nodes:
            self.nodes[uid] = SchedNode(uid, self, data)
        return self.nodes[uid]

    def get_node(self, uid):
        return self.nodes.get(uid, None)

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
               - inspect failures (len(node.failures) != 0)
               - submit() or cancel()

        node.submit(), node.done(), and node.cancel() will propagate
        state to nodes that depend on them.
        """
        status = {
            'ready': [],
            'done': [],
            'cancelled': [],
            'waiting': [],
            'submitted': []
        }
        for node in self.nodes.values():
            status[node.state].append(node)
        return status
