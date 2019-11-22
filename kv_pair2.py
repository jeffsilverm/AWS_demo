#! /usr/bin/env python3
#
# This script implements a simple key-value store using a variety of backends.
# The intent is that this file will be imported into other programs that will
# invoke it to do things, and that it will use a variety of backing stores.
# Input is a key and the return is a value.  Operations include
# (These verbs are from RFC 2616 section 9)
# http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html
# GET - given a key, return the corresponding value, error if the key does
# not already exist
# POST - insert a key-value pair in the database, error if the key already
# exists
# PUT - update a key-value pair in the database, error if the key does not
# already exist
# DELETE - deletes a key-value pair from the database
# OPTION - change the configuration to use different back ends
# TRACE - report the configuration

from enum import Enum, unique, auto
from json import load


@unique
class Backends(Enum):
    SQLLIGHT = auto
    DYNAMO_DB = auto()
    MYSQL = auto()
    SHELVES = auto()
    TEXTFILE = auto()
    MONGODB = auto()


class Platforms(Enum):
    LOCAL = auto()
    AWS = auto()
    COMMERCIALVENTVAC = auto()


class Configuration(object):

    def __init__(self, filename: str):
        """
        :param filename: str: The name of the file that holds the
        conifugration or None if the file should be created
        """
        self.filename = filename
        self.configuration['backend'] = Backends.MONGODB

    def load_configuration(self, filename: str):
        if filename is not None:
            self.filename = filename
        if self.filename is not None:
            self.configuration = load(self.filename)
        else:
            # Load the configuration with default values
            self.configuration = {"filename": "kv_pair2_configuration.json",
                                  "backend": "local"}





class Backend(object):
    """This class calls all of the backends """

    pass
