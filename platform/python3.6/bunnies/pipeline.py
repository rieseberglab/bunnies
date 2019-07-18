"""
  Tools to generate build pipelines.
  The utilities here take a pipeline specification and convert
  them into a list of tasks to execute.
"""
from .graph import Cacheable, Transform, Target
from .utils import canonical_hash
import json


class PipelineException(Exception):
    pass


class BuildNode(object):
    """graph of buildable things with dependencies"""
    __slots__ = ("data", "deps", "_uid")

    def __init__(self, data):
        self.data = data  # Cacheable
        self.deps = []    # BuildNodes
        self._uid = None

    @property
    def uid(self):
        if not self._uid:
            if isinstance(self.data, Cacheable):
                self._uid = self.data.canonical_id
            else:
                self._uid = id(self.data)

        return self._uid

    def postorder(self, prune_fn=lambda x: False):
        """yield each node in DFS post order.
           use prune_fn() to prune out this node and its depencies

            - stop if prune_fn(self) == True
            - recurse into dependencies, yielding in turn
            - yield self
        """
        if prune_fn(self):
            return

        for dep in self.deps:
            for node in dep.postorder(prune_fn=prune_fn):
                yield node

        yield self

    def output_ready(self):
        """determines if the buildnode's output is readily available for consumption by downstream analyses"""
        if not isinstance(self.node, Target):
            raise TypeError("only valid on Targets")
        return self.node.exists()

    def execution_transfer_script(self):
        """
        Create a self-standing script that executes just the one node
        """

        # fixme -- inefficient pickle.
        #          - slow
        #          - nodes will need to be dealiased again.
        #
        manifest_s = repr(json.dumps(self.node.manifest()))
        return """#!/usr/bin/env python3
import bunnies.runtime
import from bunnies.unmarshall import unmarshall

## FIXME - START USER HOOK
import snpcalling
## FIXME - END USER HOOK
manifest = %(manifest_s)s
transform = unmarshall(manifest)
output = transform.run()
# fixme upload files encountered
""" % {
    'manifest_s': manifest_s
    
}




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

        # recurse in basic structures
        if isinstance(obj, list):
            return [self._dealias(o, path=path) for o in obj]
        if isinstance(obj, dict):
            return {k: self._dealias(v, path=path) for k, v in obj.items()}
        if isinstance(obj, tuple):
            return tuple([self._dealias(o, path=path) for o in obj])

        if not isinstance(obj, Cacheable):
            raise PipelineException("pipeline targets and their dependencies should be cacheable: %s" % (repr(obj)))

        # dealias object based on its uid

        if obj in path:
            # produce ordered proof of cycle
            raise PipelineException("Cycle in dependency graph detected: %s", path)

        path.add(obj)

        # fixme modularity -- need a "getDeps" interface
        if isinstance(obj, Transform):
            # recurse
            dealiased_deps = [self._dealias(obj.inputs[k].node, path=path) for k in obj.inputs]
        else:
            dealiased_deps = []

        path.remove(obj)

        build_node = BuildNode(obj)
        dealiased = self.by_uid.get(build_node.uid, None)

        if not dealiased:
            # first instance
            dealiased = build_node
            dealiased.deps = dealiased_deps
            self.by_uid[dealiased.uid] = dealiased

        return dealiased

    def add_targets(self, targets):
        if not isinstance(targets, (list, tuple)):
            targets = list([targets])

        all_targets = self.targets + self._dealias(targets)
        self.targets[:] = [x for x in set(all_targets)]

    def dependency_order(self):
        """generate the nodes of the graph in dependency order (if A depends on B, then B is yielded first).

           Nodes of the graph are visited once only.
        """
        seen = set()

        def _prune_visited(node):
            if node in seen:
                return True
            seen.add(node)
            return False

        for target in self.targets:
            if target in seen:
                continue
            for node in target.postorder(prune_fn=_prune_visited):
                yield node.data

    def build_order(self):
        """generate nodes in the graph that need to be built.
           if A depends on B, B is yielded first

           nodes that have already been built are skipped
        """
        seen = set()

        def _already_built(node):
            # visit only once
            if node in seen:
                return True
            seen.add(node)

            # prune if the result is already computed
            if node.output_ready():
                return True
            return False

        for target in self.targets:
            if target in seen:
                continue
            for node in target.preorder(prune_fn=_already_built):
                yield node


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
