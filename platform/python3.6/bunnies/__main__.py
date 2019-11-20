import sys
import logging
import argparse
from . import setup_logging
import bunnies.jobs
import bunnies.environment
import bunnies.kvstore

log = logging.getLogger(__package__)


def main():
    setup_logging(logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.set_defaults(func=None)

    subparsers = parser.add_subparsers(help="sub-command", dest="command", description="bunnies modules offering a CLI")

    bunnies.jobs.configure_parser(subparsers)
    bunnies.environment.configure_parser(subparsers)
    bunnies.kvstore.configure_parser(subparsers)

    args = parser.parse_args(sys.argv[1:])

    if args.command is None:
        sys.stderr.write("No subcommand specified.\n")
        sys.stderr.write(parser.format_usage() + "\n")
        sys.exit(1)

    if not args.func:
        sys.stderr.write("No command specified.\n")
        sys.stderr.write(parser.format_usage() + "\n")
        sys.exit(1)

    retcode = args.func(**vars(args))
    sys.exit(int(retcode) if retcode is not None else 0)


if __name__ == "__main__":
    main()

    pass

if __name__ == "__main__":
    sys.exit(main() or 0)
