"""
misc utilities
"""

import os, os.path
import errno

def find_config_file(startdir, filname):
    """recurse in folder and parents to find filname and open it"""
    parent = os.path.dirname(startdir)
    try:
        return open(os.path.join(startdir, filname), "r")
    except IOError as ioe:
        if ioe.errno != errno.ENOENT:
            raise
    # reached /
    if parent == startdir:
        return None

    return find_config_file(parent, filname)
