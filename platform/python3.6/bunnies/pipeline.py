"""
  Tools to generate build pipelines.
  The utilities here take a pipeline specification and convert
  them into a list of tasks to execute.
"""
from .graph import Cacheable, Transform
from .utils import canonical_hash

class PipelineException(Exception):
    pass

class BuildNode(object):
    __slots__ == ("data", "deps", "_uid")
    def __init__(self, data):
        self.data = data
        self.deps = [] if not deps else [d for d in deps]

    @property
    def uid(self):
        if self._uid: return self._uid
        if isinstance(self.data, Cacheable):
            canon = self.data.canonical()
            str_canon = canonical_hash(canon)
            self._uid = str_canon
        else:
            self._uid = id(self.data)

        return self._uid

class BuildGraph(object):
    """Traverse the pipeline graph and obtain a graph of data dependencies"""

    def __init__(self):

        # cache of build nodes, by uid
        self.by_uid = {}

        # roots to build
        self.targets = []

    def _dealias(self, obj, path=None):
        """
           walk the graph, making sure there are no cycles,
           and dealias any Cacheable object along the way.
           The result is a BuildNode overlay on top of the data graph.
        """
        if path is None:
            path = set()

        if isinstance(list, obj):
            return [self._dealias(o, path=path) for o in obj]
        if isinstance(dict, obj):
            return {k:self._dealias(v, path=path) for k,v in obj.items()}
        if isinstance(tuple, obj):
            return tuple([self._dealias(o, path=path) for o in obj])

        if not isinstance(obj, Cacheable):
            raise PipelineException("pipeline targets and their dependencies should be cacheable: %s" % (repr(obj)))

        if obj in path:
            # produce ordered proof of cycle
            raise PipelineException("Cycle in dependency graph detected: %s", path)

        path.add(obj)
        if isinstance(obj, Transform):
            # recurse
            dealiased_deps = [self._dealias(obj.inputs[k], path=path) for k in obj.inputs]
        path.remove(obj)

        build_node = BuildNode(obj)
        dealiased = self.by_id.get(build_node.uid, None)

        if not dealiased:
            # first instance
            dealiased = build_node
            dealiased.deps = dealiased_deps
            self.by_id[dealiased.uid] = dealiased

        return dealiased

    def add_targets(self, targets):
        if not isinstance(targets, (list, tuple)):
            targets = list([targets])

        all_targets = self.targets + self._dealias(targets)
        self.targets[:] = [x for x in set(all_targets)]

def build_target(roots):
    """
    Construct a pipeline execution schedule from a graph of
    data dependencies.
    """

    if not isinstance(roots, list):
        roots = [roots]

    build_graph = BuildGraph()
    build_graph.add_targets(roots)

    return build_graph

