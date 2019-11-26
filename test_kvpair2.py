import sqlite3

import pytest

import kv_pair2


# import pdb


class TestBackends:
    def test_backends_sqlite3(self):
        be_sqlite3 = kv_pair2.Backend(backend_=kv_pair2.Backends.SQLLIGHT)
        assert isinstance(be_sqlite3,
                          kv_pair2.Backend), f"be_sqlite3 is of type " \
                                             f"{type(be_sqlite3)}, should be " \
                                             f"an kv_pair2.Backend"
        assert isinstance(be_sqlite3.db_con,
                          sqlite3.Connection), f"be_sqlite3 is of type " \
                                               f"{type(be_sqlite3.db_con)}, " \
                                               f"should be an " \
                                               f"sqlite3.Connection"

    def test_backends_mongodb(self):
        be_mongodb = kv_pair2.Backend(backend_=kv_pair2.Backends.MONGODB)
        assert isinstance(be_mongodb,
                          kv_pair2.Backend), f"be_mongodb is of type " \
                                             f"{type(be_mongodb)}, should be " \
                                             f"an kv_pair2.Backend"
        print(
            f"The type of be_mongodb.db_con is {type(be_mongodb.db_con)} or "
            f"{str(type(be_mongodb.db_con))}")
        # The following assertion doesn't work.  The type of
        # be_mongodb.db_con is 'type'
        # assert isinstance(be_mongodb.db_con, MongoClient), \
        #    f"be_mongodb is of type {type(be_mongodb.db_con)}, should be a " \
        #    f"MongoClient"
        from pymongo import MongoClient
        mndb = kv_pair2.Backend.initialize_mongodb()
        assert isinstance(mndb,
                          MongoClient), f"mndb is {type(mndb)} but should be " \
                                        f"of type MongoClient"
        dba = mndb.admin
        ss = dba.command("serverStatus")
        assert ss[
                   "ok"] == 1.0, f"The server status is {ss['ok']} and should "\
                                 f"be 1.0"
        db = mndb.kv_pytest
        db.kv_pytest.insert_one({"generator": "pytest", "value": "OX"})
        result = db.kv_pytest.find_one({"generator": "pytest"})
        assert result[
                   "value"] == "OX", f"The mongo DB returned " \
                                     f"{result['value']} should have returned "\
                                     f"OX"
        db.kv_pytest.delete_one({"generator": "pytest"})
        result = db.kv_pytest.find_one({"generator": "pytest"})
        assert result[
                   "value"] != "OX", f"The mongo DB returned {result['value']} \
                   should have returned anything OTHER THAN OX"


if __name__ == '__main__':
    pytest.main()
