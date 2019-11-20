#!/usr/bin/env python3
# -*- charset: utf-8; -*-

"""
An example noop pipeline which consists of launching a single job
"""

# framework
import bunnies
import bunnies.runtime
import os
import logging

# experiment specific
from snpcalling import Noop

bunnies.setup_logging(logging.DEBUG)

bunnies.runtime.add_user_deps(".", "snpcalling", excludes=("__pycache__"))
bunnies.runtime.add_user_hook("import snpcalling")
bunnies.runtime.add_user_hook("snpcalling.setup_logging()")

noop = Noop(wait_time=10*60, exit_code=1)

pipeline = bunnies.build_pipeline(noop)

#
# Tag all entities with the name of the program
#
pipeline.build(os.path.basename(__file__))

for target in pipeline.targets:
    print(target.data.exists())
