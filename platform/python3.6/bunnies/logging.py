import logging
import sys


def setup_logging(loglevel=logging.INFO):
    """configure custom logging for the platform"""
    root = logging.getLogger(__package__)
    root.setLevel(loglevel)
    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(loglevel)
    formatter = logging.Formatter('[%(asctime)s] %(name)s %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)
