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

import importlib
import json
import os
import random
import string
import sys
import uuid
from enum import Enum, unique, auto


@unique
class Backends(Enum):
    DICT = auto()
    DYNAMO_DB = auto()
    MONGODB = auto()
    MYSQL = auto()
    SHELVES = auto()
    SQLLIGHT = auto
    TEXTFILE = auto()



class Platforms(Enum):
    LOCAL = auto()
    AWS = auto()
    COMMERCIALVENTVAC = auto()


class Configuration(object):

    def __init__(self, configuration_filename: str):
        """
        :param configuration_filename: str The name of the file that holds the
        configuration or None if the file should be created
        :return None
        """
        self.filename = configuration_filename
        if configuration_filename is None:
            # This is the file we're going to write to on exit.
            self.filename = os.path.join(os.curdir,
                                         "kv_pair2_configuration.json")
            self.backend: Backends = Backends.MONGODB
        else:
            configuration_dict = json.load(self.filename)
            self.backend = configuration_dict['backend_']


def random_generator(size=6, chars=string.ascii_uppercase + string.digits):
    """Generate a random string of size characters long
    :size   int:    The length of the random string to generate
    :chars  str:    A string of characters which is the set of characters from
                    the random characters will be drawn
    """

    # To see the list of inspections, goto
    # https://gist.github.com/pylover/7870c235867cf22817ac5b096defb768
    # noinspection PyUnusedLocal
    return ''.join(random.choice(chars) for x in range(size))


class Backend(object):
    """This class handles all of the interfacing to all of the
    backends.  Thus, it is an implementation of an implementation
     agnostic Key/Value server"""

    def __init__(self, backend_: Backends = Backends.SQLLIGHT):
        """

        :type backend_: Backends Which backend to use
        """
        self.backend = backend_
        if self.backend == Backends.DICT:
            db_con = dict()
        elif self.backend == Backends.SQLLIGHT:
            default_path = os.path.join(os.path.dirname(__file__),
                                        'database.sqlite3')
            db_con = self.initialize_sqllite(db_path=default_path)

        elif self.backend == Backends.MONGODB:
            db_con = self.initialize_mongodb()
            print(
                f"In kv_pair2.py: The type of db_con is {type(db_con)} "
                f"or {str(type(db_con))}", file=sys.stderr)
        else:
            raise NotImplementedError(f"backend type {str(self.backend)}.")

        self.db_con = db_con
        self.gold = dict()

    # CRUD - Create, Read, Update, Delete
    def create(self, ckey, cvalue):
        """
        :param ckey: str: the key to search for
        :param cvalue: str: the value to find
        :return: None
        Note: if trying to create a key/value pair on a key that already
        exists, then just do it
        """
        if self.backend == Backends.DICT:
            self.db_con[ckey] = cvalue
        elif self.backend == Backends.DYNAMO_DB:
            raise NotImplementedError("create Backends.DYNAMO_DB")
        elif self.backend == Backends.MONGODB:
            # Mongo stores "documents" which are implemented as python dictionaries
            # If this fails, then start the daemon:
            # sudo systemctl status mongod)
            document = { 'key' : ckey,
                         'value' : cvalue }
            self.db_con.insert_one(document)
        elif self.backend == Backends.MYSQL:
            raise NotImplementedError("create Backends.MYSQL")
        elif self.backend == Backends.SHELVES:
            raise NotImplementedError("create Backends.SHELVES")
        elif self.backend == Backends.SQLLIGHT:
            raise NotImplementedError("create Backends.SQLIGHT")
        elif self.backend == Backends.TEXTFILE:
            raise NotImplementedError("create Backends.TEXTFILE")
        else:
            raise ValueError(
                f"Backend type {self.backend} is not implemented or just wrong")

    def read(self, r_key):
        """

        :param r_key: str: the key to search for
        :return: str: the value that the key sought
        """

        if self.backend == Backends.DICT:
            return self.db_con[r_key]
        elif self.backend == Backends.DYNAMO_DB:
            raise NotImplementedError("read Backends.DYNAMO_DB")
        elif self.backend == Backends.MONGODB:
            value = self.db_con.find_one({'key': r_key })
            return value
        elif self.backend == Backends.MYSQL:
            raise NotImplementedError("read Backends.MYSQL")
        elif self.backend == Backends.SHELVES:
            raise NotImplementedError("read Backends.SHELVES")
        elif self.backend == Backends.SQLLIGHT:
            raise NotImplementedError("read Backends.SQLIGHT")
        elif self.backend == Backends.TEXTFILE:
            raise NotImplementedError("read Backends.TEXTFILE")
        else:
            raise ValueError(
                f"Backend type {self.backend} is not implemented or just wrong")

    def update(self, u_key, u_value):
        """

        :param u_key: str: the key to search for
        :param u_value: str: the value to replace the original value with
        :return: str: the original value, or None if None
        Does not raise an exception if there is no key/value pair,
        just returns None.  If the caller
        has a use case where it needs to raise an exception if trying to
        update a non-existant key/value pair,
        then it can call read and let it raise an assertion
        """

        if self.backend == Backends.DICT:
            original = self.db_con[u_key]
            self.db_con[u_key] = u_value
        elif self.backend == Backends.DYNAMO_DB:
            raise NotImplementedError
        elif self.backend == Backends.MONGODB:
            # Mongo stores "documents" which are implemented as python
            # dictionaries
            document = {'key': u_key,
                        'value': u_value}
        elif self.backend == Backends.MYSQL:
            raise NotImplementedError
        elif self.backend == Backends.SHELVES:
            raise NotImplementedError
        elif self.backend == Backends.SQLLIGHT:
            raise NotImplementedError
        elif self.backend == Backends.TEXTFILE:
            raise NotImplementedError
        else:
            raise ValueError(
                f"Backend type {self.backend} is not implemented or just wrong")
        return original

    def delete(self, d_key):
        """
        Delete a key/value pair
        :rtype: None
        :param d_key: str: the key to delete
        :return: None
        Note: if trying to delete a key/value pair on a key that does not
        already exist, then just return
        """

        if self.backend == Backends.DICT:
            del self.db_con[d_key]
        elif self.backend == Backends.DYNAMO_DB:
            raise NotImplementedError
        elif self.backend == Backends.MONGODB:
            self.db_con.clear()
            raise NotImplementedError
        elif self.backend == Backends.MYSQL:
            raise NotImplementedError
        elif self.backend == Backends.SHELVES:
            raise NotImplementedError
        elif self.backend == Backends.SQLLIGHT:
            raise NotImplementedError
        elif self.backend == Backends.TEXTFILE:
            raise NotImplementedError
        else:
            raise ValueError(
                f"Backend type {self.backend} is not implemented or just wrong")

    @staticmethod
    def initialize_sqllite(db_path: str):
        """
        :param db_path: str The path to the SQL lite database.  This is a file
        :return: an sqlite3 connection object
        """
        sqlite3 = importlib.import_module(name="sqlite3", package=None)
        con = sqlite3.connect(db_path)
        return con

    @staticmethod
    def initialize_mongodb():
        """

        :rtype: pymongo.mongo_client.MongoClient    A handle to a Mongo database
        """
        pymongo = importlib.import_module(name="pymongo", package="mongo")
        dba = pymongo.admin
        try:
            ss = dba.command("serverStatus")
        except pymongo.errors.ServerSelectionTimeoutError as p:
            print("Unable to connect to the mongodb.  Try the 'sudo systemctl start mongodb' command", file=sys.stderr)
        if ss['ok'] != 1.0:
            print(f"The mongodb status is {ss['ok']}.  It should be 1.0.  I don't know what that means", file=sys.stderr)
        mc = pymongo.MongoClient()
        con = mc.kv_test
        return con


def verify_db(backend_obj: Backend):
    """
    :param backend_obj: Backend  Verify the database against the gold standard, 
    the internal dictionary
    :return: Number of bad entries
    """
    bad_cntr = 0
    for k in backend_obj.gold.keys():
        readback = backend_obj.read(k)
        if readback != backend_obj.gold[k]:
            print(
                f"Key {k} is {readback} but should be "
                f"{backend_obj.gold[k]}. ")
            bad_cntr += 1
    return bad_cntr


if "__main__" == __name__:

    COUNT = 100  # How many K/V pairs to generate

    def run_crud_verify( vrfy_backend: Backends = Backends.DICT):
        """

        :param vrfy_backend:
        :return: True if 100% pass on all tests
        """
        print(f"Running CRUD verify on backend {str(vrfy_backend)}")
        results = True

        bcknd = Backend(vrfy_backend)
        for i in range(COUNT):
            key = uuid.uuid4()
            value = random_generator(4)
            bcknd.gold[key] = value
            bcknd.create(key, value)
        print("Completed create", file=sys.stderr)

        if verify_db(bcknd) > 0:
            print(
                "SOMETHING WENT WRONG DURING THE INITIAL READBACK - PAY "
                "ATTENTION!!!!!!")
            results = False
        print("Completed verify after create test", file=sys.stderr)

        for key in bcknd.gold.keys():
            value = random_generator(4)
            bcknd.gold[key] = value
            bcknd.update(key, value)
        print("Completed update", file=sys.stderr)

        if verify_db(bcknd) > 0:
            print("SOMETHING WENT WRONG DURING THE UPDATE - PAY ATTENTION!!!!!!")
            results = False
        print("Completed verify after update test", file=sys.stderr)

        for key in bcknd.gold.keys():
            bcknd.delete(key)
        print("Completed delete", file=sys.stderr)

        for key in bcknd.gold.keys():
            try:
                v = bcknd.read(key)
            except KeyError:
                # We expect a KeyError exception, because the key/value pair
                # should have been deleted.
                pass
            else:
                print(
                    "SOMETHING WENT WRONG DURING THE DELETE - PAY ATTENTION!!!!!!")
                results = False
                print(f"Reading from key {key} ")

        print("Completed verify after delete test", file=sys.stderr)
        return results


    run_crud_verify(vrfy_backend=Backends.DICT)
    # run_crud_verify(vrfy_backend=Backends.DYNAMO_DB)
    run_crud_verify(vrfy_backend=Backends.MONGODB)
    run_crud_verify(vrfy_backend=Backends.MYSQL)
    run_crud_verify(vrfy_backend=Backends.SHELVES)
    run_crud_verify(vrfy_backend=Backends.SQLLIGHT)
    run_crud_verify(vrfy_backend=Backends.TEXTFILE)

