import logging

from . import exc
from . import constants as C

logger = logging.getLogger(__package__)

_registry = {}


def register_kind(pickled_class, unmarshaller=None):
    """
    Register a method to use to convert a manifest back into
    an object representation.

    pickled_class.kind is used as the name to register.
    pickled_class.from_manifest is used as the unmarshalling constructor.

    Alternately, you can pass (str, callable) as a pair.
    """
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

        raise ValueError("expected a callable or a from_manifest method: %s", obj)

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
        if C.MANIFEST_KIND_ATTR not in obj:
            # regular dict
            unwrap = None
        else:
            kind = obj[C.MANIFEST_KIND_ATTR]
            unwrap = _registry.get(kind)
            if unwrap is None:
                msg = "error at document path %s: %s is not a registered object kind" % (_pathstr(), kind)
                logger.error("%s", msg)
                raise exc.UnmarshallException(msg)

        # recurse into each key of the dict
        with_converted_deps = {k: _unmarshall(v, path=path + (k,)) for k, v in obj.items()}

        try:
            # custom object unmarshall
            return unwrap(with_converted_deps) if callable(unwrap) else with_converted_deps
        except exc.UnmarshalledException:
            raise
        except Exception as _exc:
            logger.error("error at document path %s (kind=%s): %s", _pathstr(), kind, _exc, exc_info=True)
            msg = "error at document path %s (kind=%s): %s" % (_pathstr(), kind, str(_exc))
            raise exc.UnmarshallException(msg)


def unmarshall(obj):
    """
    reconstruct the graph of objects based on basic objects
    """
    # recurse in basic structures
    return _unmarshall(obj, path=("",))
