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
import shelve
import string
import sys
import uuid
from enum import Enum, unique, auto
from string import ascii_letters
from traceback import print_stack

import pymongo
from pymongo import errors


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


class BackendsAbstract(object):
    """This is an abstract class that defines the methods that a backend
    class has to implement.  If a backend class does not implement one
    of these methods, then the abstract class will raise a NonImplemented
    exception.
    This class has a method for all of the database options.  There is
    CRUD - Create, Read, Update, and Delete.  But these are also connect,
    disconnect, and settings methods.  Creating a Backend object is essentially
    a connect, so I didn't create a connect method.
     """
    # It is inappropriate to put defaults here - they should go in a real
    # not an abstract class, because they will be different for each
    # real class
    """
    def __init__(self, db_server: str = "localhost", db_port: int = 27017,
                 db_name: str = "mongo_db",
                 table_name: str = "my_collection") -> None:
    """

    def __init__(self, db_name: str,
                 db_server: str = None, db_port: int = -1,
                 table_name: str = None) -> None:
        """
        Create a database object.  Generally, this will be called by the
        connect method.

        :param db_server:   str The name of the server or IP address
        :param db_port:     int The TCP port number the server is listening to
        :param db_name:     str The name of the database
        :param table_name:  str The name of the collection.  The reason it's
                                called table_name is for compatibility with SQL
        """
        #
        # This is here to stop variable unused errors in pycharm
        print(db_server, db_port, db_name, table_name, file=sys.stderr)
        print(f"The type of self is {type(self)}", file=sys.stderr)
        # Actually, now that I have the following statement, I can get rid of
        # those upper 2 statements, but I am leaving them in for now.
        self.connected = True

    def create(self, key, value) -> None:  # class BackendsAbstract
        """Create a record or a document.  It is an error to create a record
        that already exists.  Enforcement of this rule is left to the
        backend and testing this rule is a requirement for the test software """
        print(key, value, file=sys.stderr)
        raise NotImplementedError

    def read(self, key) -> None:  # class BackendsAbstract
        """Read a record or a document"""
        print(key, file=sys.stderr)
        raise NotImplementedError

    def update(self, key, value) -> None:  # class BackendsAbstract
        """
        Change a record at key with a new value.  It is an error to update a
        record that doesn't exist - this is more or less to differentiate
        update from create.  However, there is a use case: if the caller
        thinks they are updating an existing record, and it doesn't actually
        exist, then that might be a symptom of a logic error in the caller
        :param key:
        :param value:
        :return:
        """
        print(key, value, file=sys.stderr)
        raise NotImplementedError

    def delete(self, key) -> None:  # class BackendsAbstract
        print(key, file=sys.stderr)
        raise NotImplementedError

    def connect(self, db_name) -> None:  # class BackendsAbstract
        print(db_name, file=sys.stderr)
        raise NotImplementedError

    def disconnect(self) -> None:  # class BackendsAbstract
        self.connected = False
        raise NotImplementedError

    def get_key_list(self) -> None:
        raise NotImplementedError

    def settings(self) -> None:
        raise NotImplementedError


class BackendDict(BackendsAbstract):
    """
    Connect the KV server to a dictionary by loading a shelf file
    """

    def settings(self) -> None:
        pass

    def __init__(self, db_name: str = "dict_db",
                 ) -> None:
        BackendsAbstract.__init__(self, db_name=db_name)
        self.db_name = db_name
        if os.path.exists(db_name) and os.path.isfile(db_name):
            # The persistent store of a dictionary is a pickle file
            with shelve.open(db_name, 'c') as shelf:
                self.dict = shelf.copy()
        else:
            self.dict = dict()
        self.connected = True

    def create(self, key, value) -> None:  # class BackendDict
        """Create a record or a document"""
        if key in self.dict:
            raise ValueError(f"key {key} is already in dictionary.  "
                             f"Value is {self.dict[key]}.")

    def read(self, key) -> None:  # class BackendDict
        """Read a record or a document"""
        raise NotImplementedError

    def update(self, key, value) -> None:  # class BackendDict
        """
        Change a record at key with a new value.  It is an error to update a
        record that doesn't exist - this is more or less to differentiate
        update from create.  However, there is a use case: if the caller
        thinks they are updating an existing record, and it doesn't actually
        exist, then that might be a symptom of a logic error in the caller
        :param key:
        :param value:
        :return:
        """
        raise NotImplementedError  # class BackendDict

    def delete(self, key) -> None:  # class BackendDict
        raise NotImplementedError

    def connect(self, db_name) -> None:  # class BackendDict
        raise NotImplementedError

    def disconnect(self) -> None:  # class BackendDict
        self.connected = False
        raise NotImplementedError

    def get_key_list(self) -> None:  # class BackendDict
        raise NotImplementedError


class BackendMongo(BackendsAbstract):

    def settings(self) -> None:
        pass

    # This is necessary because one of the generic operations
    # you do on a database is connect to it
    def connect(self, db_server: str = "localhost", db_port: int = 27017,
                # BackendMongo
                db_name: str = "mongo_db",
                table_name: str = "my_collection") -> None:

        # Creating a BackendMongo object is *not* generic.  However, the
        # nature of the object is hidden inside the BackendMongo class
        connection = self.__init__(db_server=db_server, db_port=db_port,
                                   db_name=db_name,
                                   table_name=table_name)

        return connection

    def __init__(self, db_server, db_port, db_name, table_name) -> None:
        """
        Create a database object for Mongo

        :type db_port: int
        :param db_server:   str The name of the server or IP address
        :param db_port:     int The TCP port number the server is listening to
        :param db_name:     str The name of the database
        :param table_name:  str The name of the collection.  The reason it's
                                called table_name is for compatibility with SQL
        """
        # This call is a bit future proofing
        BackendsAbstract.__init__(self, db_server=db_server, db_port=db_port,
                                  db_name=db_name, table_name=table_name)
        # Method BackendMongo.__init__ is called twice, should only be called
        # once (I think).  Where is it getting called from?
        print_stack()

        # I ran into this problem, db_port was a string, raised a _topography
        # problem.
        assert isinstance(db_port, int), "db_port has to be an int"

        # There is a very lengthy comment on how Mongo works at the bottom of
        # this file
        try:
            self.client = pymongo.MongoClient(host=db_server, port=db_port)
        except Exception as e:
            print(
                f"pymongo.MongoClient raised exception {str(e)}.  Is the "
                f"server running?",
                file=sys.stderr)
            raise
        # This is to verify my understanding of pymongo
        assert isinstance(self.client, pymongo.mongo_client.MongoClient), \
            f"client is {type(self.client)}, not " \
            "pymongo.mongo_client.MongoClient"
        try:
            # The ismaster command is cheap and does not require auth.
            self.client.admin.command('ismaster')
        except pymongo.errors.ConnectionFailure as c:
            print("client.admin.command('ismaster') raised a ConnectionFailure "
                  "exception\nIs the server running?\nUse " +
                  "the command 'sudo systemctl start mongodb'to start\n" +
                  str(c), file=sys.stderr)
            raise
        else:
            print(f"Have a good connection to the database server ",
                  f"{self.client.address} port {self.client.PORT}", \
                  file=sys.stderr)
        # db = conn.database
        # This was suggested by
        # https://stackoverflow.com/questions/18371351/python-pymongo-insert
        # -and-update-documents
        try:
            self.db_obj = self.client.get_database(name=db_name)
        except Exception as e:
            print(f"self.client.get_database({db_name} raised an "
                  f"{str(e)} exception", file=sys.stderr)
            raise
        # noinspection PyUnresolvedReferences
        assert isinstance(self.db_obj, pymongo.database.Database), \
            f"db_obj is type {type(self.db_obj)} not pymongo.database.Database"
        print(f"Have database {self.db_obj.name}", file=sys.stderr)
        try:
            collection_list = self.db_obj.list_collection_names()
            if table_name in collection_list:
                self.coll = self.db_obj.get_collection(name=table_name)
            else:
                # the collection name is table_name for compatibility with SQL
                self.coll = self.db_obj.create_collection(name=table_name)
        except pymongo.errors.CollectionInvalid as c:
            print(f"db_obj.create_collection({db_name} raised an"
                  f"{str(c)} exception",
                  f"The collection_list is {collection_list}",
                  file=sys.stderr)
            raise
            # These asserts are here to test my understanding
        assert self.db_obj.name == db_name, \
            f"self.db_obj.name is {self.db_obj.name} and db_name is {db_name}"
        assert self.coll.name == table_name, \
            f"self.coll.name is {self.coll.name} and table_name is {table_name}"
        assert hasattr(self.coll, 'insert_one'), \
            f"self.coll should have an insert_one attribute and doesn't"
        print(f"Using collection {self.coll.name}", file=sys.stderr)
        return

    def create(self, key, value) -> None:
        """Insert a document into the database.  Mongodb looks for a special
        key, _id, which must be unique across a collection
        (this is a test case)"""
        # From
        # https://api.mongodb.com/python/current/api/pymongo/collection.html
        # #pymongo.collection.Collection.insert_one
        document = {"_id": key, "value": value}
        print(f"Created a document object {document}", file=sys.stderr)
        self.coll.insert_one(document=document)
        return None

    def read(self, key) -> str:
        """fetch a document from the database
        :param  key str The key to look up in the database
        """
        print(f"Searching for _id: {key}. ", file=sys.stderr, end='')
        results = self.coll.find({"_id": key})
        assert results is not None, "find returned a None object."
        print("found it. ", file=sys.stderr, end='')
        r = ""
        while True:
            try:
                nx = results.next()
            except StopIteration:
                break
            else:
                r += nx["value"]
        print(r, file=sys.stderr)
        return r

    def update(self, key, value) -> None:
        """
        Change a record at key with a new value.  It is an error to update a
        record that doesn't exist - this is more or less to differentiate
        update from create.  However, there is a use case: if the caller
        thinks they are updating an existing record, and it doesn't actually
        exist, then that might be a symptom of a logic error in the caller
        :param key: str         The key to update
        :param value:   str     the value to update
        :return:
        """
        print("!!!!!! Called update in kv_pair2 in class BackendMongo!!!!!!",
              file=sys.stderr)
        sys.stderr.flush()
        #  value = random_generator(13)  #######  ARGGGG!  How could you be
        #  so stupid!!!!
        print(
            f"In kv_pair2.BackendMongo.update. Updating _id: {key} with "
            f"{value}.",
            file=sys.stderr)
        upd_filter = {"_id": key}
        update = {"value": value}
        # I got this answer, sort of, from
        # https://github.com/mongodb-labs/mongo-rust-driver-prototype/issues/264
        results = self.coll.update_one(filter=upd_filter,
                                       update={"$set": update}, upsert=False)
        assert results.matched_count == 1, \
            f"results.matched_count should be 1, was {results.matched_count}"
        assert results.modified_count == 1, \
            f"results.modified_count should be 1, was {results.matched_count}"
        return

    def delete(self, key) -> None:
        """
        Delete the document pointed to by key
        :param key: str the key of the document to delete
        :return:
        """
        self.coll.delete_one(filter={"_id": key})
        return

    def disconnect(self) -> None:
        """
        Do a clean disconnect from the database
        :return:
        """
        if hasattr(self.db_obj, "close"):
            print("self.db_obj has a close attribute", file=sys.stderr)
            if callable(self.db_obj.close):
                print("self.db_obj.close is callable", file=sys.stderr)
                self.db_obj.close()
                self.connected = False
                return
        if hasattr(self.coll, "close"):
            print("self.coll.close() a close attribute", file=sys.stderr)
            self.coll.close()
        else:
            print("self.client.close() has the close method", file=sys.stderr)
            self.client.close()
        return

    def get_key_list(self) -> None:
        raise NotImplementedError


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
            db_con = BackendMongo(db_server="localhost", db_port=27017,
                                  db_name="Marvin", table_name="Richard")

            print(
                f"In kv_pair2.py: The type of db_con is {type(db_con)} "
                f"or {str(type(db_con))}", file=sys.stderr)

        else:
            raise NotImplementedError(f"backend type {str(self.backend)}.")

        self.db_con = db_con
        self.gold = dict()
        self.connected = True

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
            # Mongo stores "documents" which are implemented as python
            # dictionaries
            # If this fails, then start the daemon:
            # sudo systemctl status mongod)
            document = {'key': ckey,
                        'value': cvalue}
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
            value = self.db_con.find_one({'key': r_key})
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

    def update1(self, u_key, u_value):
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
        print("+++++ in update1 not sure how or why ++++", file=sys.stderr)
        if self.backend == Backends.DICT:
            # original = self.db_con[u_key]
            self.db_con[u_key] = u_value
        elif self.backend == Backends.DYNAMO_DB:
            raise NotImplementedError
        elif self.backend == Backends.MONGODB:
            # Mongo stores "documents" which are implemented as python
            # dictionaries
            #            document = {'key': u_key,
            #                        'value': u_value}
            pass
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
        return

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

    '''
    def initialize_mongodb(self, db_name: str = "test_db"):
        """

        :return:
        :rtype: pymongo.mongo_client.MongoClient    A handle to a Mongo database
        """
        # Getting guidance from
        # https://api.mongodb.com/python/current/tutorial.html
        pymongo_mod = importlib.import_module(name="pymongo", package="mongo")
        client = pymongo.MongoClient()
        self.db = client[db_name]
        self.dba = pymongo_mod.admin
        try:
            ss = self.dba.command("serverStatus")
        except pymongo_mod.errors.ServerSelectionTimeoutError as p:
            print(
                "Unable to connect to the mongodb.  Try the 'sudo systemctl "
                "start mongodb' command.  Error message is " + str(p),
                file=sys.stderr)
            raise SystemError
        else:
            if ss['ok'] != 1.0:
                print(
                    f"The mongodb status is {ss['ok']}.  It should be 1.0.  I "
                    f"don't know what that means",
                    file=sys.stderr)
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
'''


if "__main__" == __name__:

    """This is operational, not test, software """

    COUNT = 100  # How many K/V pairs to generate


    def random_string(length) -> str:
        # A generator
        return ''.join(random.choice(ascii_letters)
                       for _ in range(length))  # _ is a variable is not used


    def verify_crud(db):

        kv_pairs = {}
        for m in range(4):
            key = random_string(5)
            value = random_string(8)
            kv_pairs[key] = value
            db.create(key=key, value=value)
        for k in kv_pairs.keys():
            value = db.read(key=k)
            assert value == kv_pairs[k], \
                f"db.read should have returned {kv_pairs[k]}, " \
                f"but actually returned {value}"
        # Keep going, we're not done yet with verify_crud


    db_mongo = BackendMongo(db_server="localhost", db_port=27017,
                            db_name="Eric", table_name="tab34")
    verify_crud(db_mongo)
    db_mongo.disconnect()
    del db_mongo

    db_dict = BackendDict()
    verify_crud(db_dict)
    db_dict.disconnect()

    sys.exit(0)


    def run_crud_verify(vrfy_backend: Backends = Backends.DICT):
        """

        :param vrfy_backend:
        :return: True if 100% pass on all tests
        """

        def verify_db(bcknd_) -> int:
            # This is to protect against variable unused errors
            assert bcknd_
            return 0

        assert isinstance(vrfy_backend, Backends), \
            print(f"vrfy_backend is type {type(vrfy_backend)},"
                  f"should be an instance of Backends", file=sys.stderr)

        print(f"Running CRUD verify on backend {str(vrfy_backend)}")
        results = True

        bcknd = Backend(vrfy_backend)
        for _ in range(COUNT):
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
            bcknd.update1(key, value)
        print("Completed update", file=sys.stderr)

        if verify_db(bcknd) > 0:
            print(
                "SOMETHING WENT WRONG DURING THE UPDATE - PAY ATTENTION!!!!!!")
            results = False
        print("Completed verify after update test", file=sys.stderr)

        for key in bcknd.gold.keys():
            bcknd.delete(key)
        print("Completed delete", file=sys.stderr)

        for key in bcknd.gold.keys():
            try:
                bcknd.read(key)
            except KeyError:
                # We expect a KeyError exception, because the key/value pair
                # should have been deleted.
                pass
            else:
                print(
                    "SOMETHING WENT WRONG DURING THE DELETE - PAY "
                    "ATTENTION!!!!!!")
                results = False
                print(f"Reading from key {key} ")

        print("Completed verify after delete test", file=sys.stderr)
        return results


    #    run_crud_verify(vrfy_backend=Backends.DICT)
    #    run_crud_verify(vrfy_backend=Backends.DYNAMO_DB)
    run_crud_verify(vrfy_backend=Backends.MONGODB)
    #    run_crud_verify(vrfy_backend=Backends.MYSQL)
    #    run_crud_verify(vrfy_backend=Backends.SHELVES)
    #    run_crud_verify(vrfy_backend=Backends.SQLLIGHT)
    #    run_crud_verify(vrfy_backend=Backends.TEXTFILE)

    # =================================================
    """
     A very lengthy comment on how Mongo works

     I got this insight from Geeks for Geeks: MongoDB python | insert(), 
     replace_one(), replace_many().

>>> from pymongo import MongoClient
>>> conn = MongoClient() 
>>> db = conn.database 
>>> collection = db.my_gfg_collection
>>> dir(conn)
>>> type(conn.database)
<class 'pymongo.database.Database'>
>>> coll=db.XyZZy
>>> dir(coll)
['_BaseObject__codec_options', '_BaseObject__read_concern', 
'_BaseObject__read_preference', '_BaseObject__write_concern', 
'_Collection__create', '_Collection__create_index', '_Collection__database', 
'_Collection__find_and_modify', '_Collection__full_name', 
'_Collection__name', '_Collection__write_response_codec_options', '__call__', 
'__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', 
'__format__', '__ge__', '__getattr__', '__getattribute__', '__getitem__', 
'__gt__', '__hash__', '__init__', '__init_subclass__', '__iter__', '__le__', 
'__lt__', '__module__', '__ne__', '__new__', '__next__', '__reduce__', 
'__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', 
'__subclasshook__', '__weakref__', '_aggregate', '_aggregate_one_result', 
'_command', '_count', '_delete', '_delete_retryable', '_insert', 
'_insert_one', '_legacy_write', '_map_reduce', '_read_preference_for', 
'_socket_for_reads', '_socket_for_writes', '_update', '_update_retryable', 
'_write_concern_for', '_write_concern_for_cmd', 'aggregate', 
'aggregate_raw_batches', 'bulk_write', 'codec_options', 'count', 
'count_documents', 'create_index', 'create_indexes', 'database', 
'delete_many', 'delete_one', 'distinct', 'drop', 'drop_index', 
'drop_indexes', 'ensure_index', 'estimated_document_count', 'find', 
'find_and_modify', 'find_one', 'find_one_and_delete', 'find_one_and_replace', 
'find_one_and_update', 'find_raw_batches', 'full_name', 'group', 
'index_information', 'initialize_ordered_bulk_op', 
'initialize_unordered_bulk_op', 'inline_map_reduce', 'insert', 'insert_many', 
'insert_one', 'list_indexes', 'map_reduce', 'name', 'next', 'options', 
'parallel_scan', 'read_concern', 'read_preference', 'reindex', 'remove', 
'rename', 'replace_one', 'save', 'update', 'update_many', 'update_one', 
'watch', 'with_options', 'write_concern']
>>> coll.name
'XyZZy'
>>> 
>>> coll=db.get_collection(name="Q")
>>> coll.name
'Q'
>>> coll.insert_one({"name":"Eric", "num":4 })
<pymongo.results.InsertOneResult object at 0x7f7234a4a6e0>
>>> coll.insert_one({"name":"Fred", "num":6 })
<pymongo.results.InsertOneResult object at 0x7f7234a4a8c0>
>>> dir(coll)
['_BaseObject__codec_options', '_BaseObject__read_concern', 
'_BaseObject__read_preference', '_BaseObject__write_concern', 
'_Collection__create', '_Collection__create_index', '_Collection__database', 
'_Collection__find_and_modify', '_Collection__full_name', 
'_Collection__name', '_Collection__write_response_codec_options', '__call__', 
'__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', 
'__format__', '__ge__', '__getattr__', '__getattribute__', '__getitem__', 
'__gt__', '__hash__', '__init__', '__init_subclass__', '__iter__', '__le__', 
'__lt__', '__module__', '__ne__', '__new__', '__next__', '__reduce__', 
'__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', 
'__subclasshook__', '__weakref__', '_aggregate', '_aggregate_one_result', 
'_command', '_count', '_delete', '_delete_retryable', '_insert', 
'_insert_one', '_legacy_write', '_map_reduce', '_read_preference_for', 
'_socket_for_reads', '_socket_for_writes', '_update', '_update_retryable', 
'_write_concern_for', '_write_concern_for_cmd', 'aggregate', 
'aggregate_raw_batches', 'bulk_write', 'codec_options', 'count', 
'count_documents', 'create_index', 'create_indexes', 'database', 
'delete_many', 'delete_one', 'distinct', 'drop', 'drop_index', 
'drop_indexes', 'ensure_index', 'estimated_document_count', 'find', 
'find_and_modify', 'find_one', 'find_one_and_delete', 'find_one_and_replace', 
'find_one_and_update', 'find_raw_batches', 'full_name', 'group', 
'index_information', 'initialize_ordered_bulk_op', 
'initialize_unordered_bulk_op', 'inline_map_reduce', 'insert', 'insert_many', 
'insert_one', 'list_indexes', 'map_reduce', 'name', 'next', 'options', 
'parallel_scan', 'read_concern', 'read_preference', 'reindex', 'remove', 
'rename', 'replace_one', 'save', 'update', 'update_many', 'update_one', 
'watch', 'with_options', 'write_concern']
>>> coll.find()
<pymongo.cursor.Cursor object at 0x7f7234a26bd0>
>>> c=coll.find()
>>> c.next()
{'_id': ObjectId('5e16ffa62f6d187ff431b7e0'), 'name': 'Eric', 'num': 4}
>>> c.next()
{'_id': ObjectId('5e16ffba2f6d187ff431b7e1'), 'name': 'Fred', 'num': 6}
>>> c.next()
Traceback (most recent call last):
File "<stdin>", line 1, in <module>
File "/usr/local/lib/python3.7/dist-packages/pymongo/cursor.py", line 1164, 
in next
 raise StopIteration
StopIteration
>>> 
     """
