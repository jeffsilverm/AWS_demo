import random
import sys
from string import ascii_letters

import pytest

import kv_pair2


def random_string(length) -> str:
    # A generator
    return ''.join(random.choice(ascii_letters)
                   for _ in range(length))  # _ is a variable is not used


class TestKvPair2:

    backend = None

    def __init__(self, backend: kv_pair2.Backends):
        self.backend = backend

    @classmethod
    def setup_class(cls):
        """"""
        print("In setup_class", file=sys.stderr)
        backend = cls.backend
        assert isinstance(backend, kv_pair2.Backends), \
            f"backend is type {type(backend)}, " \
            f"but should really be an instance of kv_pair2.Backends"
        if backend == kv_pair2.Backends.MONGODB:
            cls.be_dbms = kv_pair2.BackendMongo(db_name="M5",
                                                db_server="localhost",
                                                db_port=27017, table_name="TN")
        elif backend == kv_pair2.Backends.DICT:
            cls.be_dbms = kv_pair2.BackendDict(db_name="D5")
        elif backend == kv_pair2.Backends.MYSQL:
            raise NotImplementedError(str(backend))
        else:
            raise NotImplementedError(str(backend))
        assert callable(cls.be_dbms.connect), \
            "cls.be_dbms.connect is not callable and should be. " \
            f"backend is {str(backend)} and " \
            f"the attributes of cls.be_dbms are {dir(cls.be_dbms)}."
        cls.be_dbms = cls.be_dbms.connect
        assert callable(cls.be_dbms.disconnect), \
            "cls.be_dbms.disconnect is not callable and should be. " \
            f"backend is {str(backend)} and " \
            f"the attributes of cls.be_dbms are {dir(cls.be_dbms)}."
        assert cls.be_dbms.connected, "cls.be_dbms.connected is False, " \
                                      "should be true"

    @classmethod
    def teardown_class(cls):
        """Teardown the DB connection"""

        print("Calling backend_obj.disconnect")
        cls.be_dbms.disconnect()
        assert not cls.be_dbms.connected, "cls.be_dbms.connected is True " \
                                          "after " \
                                          "disconnect, should be False "


    def test_create_and_recover_key(self, key, value):
        """
        :param self:
        :param key:
        :param value:
        :return:
        test create - can I create a key / value pair and get it back?"""
        # Step into create ======================== step into create
        print("In test_create_and_recover_key", file=sys.stderr)
        assert self.hasattr("create"), "self does not have a create attribute "\
            f"and should.\n{dir(self)}\n"
        self.create(key, value)
        returned_value = self.read(key)
        assert (value == returned_value), \
            f"value {value} was not returned.  {returned_value} was, instead"

    def test_duplicate_keys(self):
        """This test case verifies that entering two records with
        duplicate keys raises an exception"""
        print("In test_duplicate_keys", file=sys.stderr)
        key = random_string(6)
        value = random_string(11)
        self.create(key, value)
        # Cannot create the same key twice.  That the value is the same makes no
        # difference, it should still raise a ValueError Exception
        with pytest.raises(ValueError):
            self.create(key, value)


test_dict = TestKvPair2(kv_pair2.Backends.DICT)
test_mongo = TestKvPair2(kv_pair2.Backends.MONGODB)

"""
if __name__ == '__main__':
    pytest.main()
"""
