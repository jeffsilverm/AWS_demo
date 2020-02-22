import random
import sys
from string import ascii_letters

import pytest

import kv_pair2


####################
# Look at [200~How do I correctly setup and teardown my pytest class with
# tests?
# https://pythontesting.net/framework/pytest/pytest-xunit-style-fixtures/
#
#


def random_string(length) -> str:
    # A generator
    return ''.join(random.choice(ascii_letters)
                   for _ in range(length))  # _ is a variable is not used


class TestKvPair2(object):
    # def __init__(self, backend: kv_pair2.Backends):
    #    self.backend = backend
    print("In class TestKvPair2", file=sys.stderr)

    @classmethod
    def setup_class(cls, backend: kv_pair2.Backends):
        """"""
        print("In setup_class", file=sys.stderr)
        # backend = cls.backend
        if backend == kv_pair2.Backends.MONGODB:
            cls.be_dbms = kv_pair2.BackendMongo(db_name="M5",
                                                db_server="localhost",
                                                db_port=27017, table_name="TN")
            assert isinstance(cls.be_dbms, kv_pair2.BackendMongo), \
                f"cls.be_dbms is NOT an instance of class " \
                f"kv_pair2.BackendMongo\n" \
                f"It's of type {type(cls.be_dbms)}"

        elif backend == kv_pair2.Backends.DICT:
            cls.be_dbms = kv_pair2.BackendDict(db_name="D5")
        elif backend == kv_pair2.Backends.MYSQL:
            raise NotImplementedError(str(backend))
        else:
            raise NotImplementedError(str(backend))
        assert hasattr(cls.be_dbms, "connect"), \
            "cls.be_dbms does not have a *connect* attribute\n" \
            f"The attributes of cls.be_dbms are {dir(cls.be_dbms)}\n" \
            f"The type of cls.be_dbms is {type(cls.be_dbms)}"
        assert callable(cls.be_dbms.connect), \
            "cls.be_dbms.connect is not callable and should be. " \
            f"backend is {str(backend)} and " \
            f"the attributes of cls.be_dbms are {dir(cls.be_dbms)}."
        cls.be_dbms.connection = cls.be_dbms.connect()
        assert cls.be_dbms is not None, \
            "cls.dbms is None and should be something else"
        assert cls.be_dbms.connection is not None, \
            "cls.be_dbms.connection is None and should be something else"
        assert hasattr(cls.be_dbms, "disconnect"), \
            "cls.be_dbms does not have a disconnect attribute\n" \
            f"The attributes of cls.be_dbms are {dir(cls.be_dbms)}\n" \
            f"The type of cls.be_dbms is {type(cls.be_dbms)}"
        assert callable(cls.be_dbms.disconnect), \
            "cls.be_dbms.disconnect is not callable and should be. " \
            f"backend is {str(backend)} and " \
            f"the attributes of cls.be_dbms are {dir(cls.be_dbms)}."
        assert cls.be_dbms.connected, "cls.be_dbms.connected is False, " \
                                      "should be true"

    @classmethod
    def teardown_class(cls):
        """Teardown the DB connection"""
        # This happened during development - not sure I understand why
        assert hasattr(cls.be_dbms, "disconnect"), \
            "cls.be_dbms does not have a disconnect attribute\n" \
            f"The attributes of cls.be_dbms are {dir(cls.be_dbms)}\n" \
            f"The type of cls.be_dbms is {type(cls.be_dbms)}"
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
        assert self.hasattr("create"), "self does not have a create attribute " \
                                       "" \
                                       "" \
                                       "" \
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


test_mongo = TestKvPair2()
test_mongo.setup_class(kv_pair2.Backends.MONGODB)

test_dict = TestKvPair2()
test_dict.setup_class(kv_pair2.Backends.DICT)

"""
if __name__ == '__main__':
    pytest.main()
"""
