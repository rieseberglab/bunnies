import sys
import logging

from . import setup_logging

log = logging.getLogger(__package__)

def main():
    log.info("bunnies is setup.")
    pass

if __name__ == "__main__":
    setup_logging(logging.DEBUG)
    sys.exit(main() or 0)
