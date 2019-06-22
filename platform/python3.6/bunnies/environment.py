#!/usr/bin/env python3


"""
  Tools for managing a bunnies compute environment
"""
from .config import config

def get_key_name():
    return config["KeyName"]

def get_subnet_id():
    return config["subnet_id"]

