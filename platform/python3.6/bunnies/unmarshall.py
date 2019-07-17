import logging

from . import exc

logger = logging.getLogger(__package__)

_registry = {}

def _get_kind(obj):
    if isinstance(obj, str):
        return obj
    try:
        return getattr(obj, "kind")
    except AttributeError:
        pass

    raise ValueError("cannot extract kind from obj: %s", obj)

def _get_parser(obj):
    if callable(obj):
        return obj
    try:
        return getattr(obj, "from_manifest")
    except AttributeError:
        pass

    raise ValueError("expected a callable or a from_manifest method: %s",
                     obj)

def register_kind(pickled_class, unmarshaller=None):
    """
    Register a method to use to convert a manifest back into
    an object representation.
    """
    kind = _get_kind(pickled_class)

    if unmarshaller is None:
        unmarshaller = _get_parser(pickled_class)

    if not callable(unmarshaller):
        raise ValueError("expected a callable, got: %s", unmarshaller)

    if kind in _registry:
        logger.warn("overwriting unmarshaller for kind %s", kind)
    _registry[kind] = unmarshaller


def _unmarshall(obj, path):
    def _pathstr():
        return ".".join(path)

    if isinstance(obj, list):
        return [self._unmarshall(o, path=path + ("[%d]" % (i,),)) for i, o in enumerate(obj)]
    if isinstance(obj, tuple):
        return tuple([self._dealias(o, path=path) for o in obj])
    if isinstance(obj, dict):
        if '_kind' not in obj:
            # regular dict
            unwrap = None
        else:
            unwrap = _registry.get(obj['_kind'])
            if unwrap is None:
                msg = "error at document path %s: %s is not a registered object kind" % (_pathstr, obj['_kind'])
                logger.error("%s", msg)
                raise exc.UnmarshallException(msg)
        recursed = {k: _unmarshall(v, path=path + (k,)) for k, v in obj.items()}

        if unwrap:
            try:
                # custom object
                return unwrap(recursed)
            
        return {k: self._dealias(v, path=path) for k, v in obj.items()}

def unmarshall(obj):
    """
    reconstruct the dependency graph based on basic objects
    """
    # recurse in basic structures
    return _unmarshall(obj, path=("",))
