import random
import sys
from string import ascii_letters

import pytest

import kv_pair2

be_obj = None
key_list = []


@pytest.fixture(params=[kv_pair2.Backends.MONGODB])    #, kv_pair2.Backends.DICT])
def select_backend(request: kv_pair2.Backends):
    """ This method tests a backend given by be
    :param  request kv_pair2.Backends    the backend to test
    """
    assert hasattr(request, "param"), \
        f"request has no attribute param.  Why?\n{type(request)}\n" \
        f"{dir(request)}\n"
    assert isinstance(request.param, kv_pair2.Backends), \
        f"request.param is type {type(request.param)}, " \
        f"but should really be an instance of kv_pair2.Backends"

    if request.param == kv_pair2.Backends.MONGODB:
        print("Using mongodb ", end='', file=sys.stderr)
        backend_obj = kv_pair2.BackendMongo(db_server="localhost",
                                            db_port=27017, db_name="mongo_db",
                                            table_name="my_collection")
    elif request.param == kv_pair2.Backends.DICT:
        backend_obj = kv_pair2.BackendDict()
    elif request.param == kv_pair2.Backends.MYSQL:
        backend_obj = kv_pair2.BackendMySql(db_server="localhost",
                                            db_port=3306, db_name="mysql_db",
                                            table_name="my_table")
    else:
        raise NotImplementedError(str(request.parm))
    # There ought to be a way to populate this list programmatically
    for op_name in ["connect", "create", "read", "update", "delete",
                    "disconnect"]:
        assert hasattr(backend_obj, op_name), \
            f"backend_obj has no {op_name} attribute.  It has: " \
            f"{dir(backend_obj)}."
        assert callable(backend_obj.__getattribute__(op_name)), \
            f"backend.{op_name} is *not* callable"
    return backend_obj


def random_string(length) -> str:
    return ''.join(random.choice(ascii_letters)
                   for _ in range(length))  # _ is a variable is not used


def test_duplicate_keys(select_backend):
    """This test case verifies that entering two records with
    duplicate keys raises an exception"""
    key = random_string(6)
    value = random_string(11)
    if not hasattr(select_backend, "create"):
        print(
            f"select_backend does not have a create method.  It has "
            f"{dir(select_backend)}",
            file=sys.stderr)
        for d in dir(select_backend):
            print(d, "callable" if d.__callable__ else str(type(d)),
                  file=sys.stderr)
    select_backend.create(key, value)
    with pytest.raises(Exception):
        select_backend.create(key, value)


def test_create_and_recover_key(be_dbms, key, value):
    # Step into create ======================== step into create
    be_dbms.create(key, value)
    returned_value = be_dbms.read(key)
    assert (value == returned_value), \
        f"value {value} was not returned.  {returned_value} was, instead"


def test_read_non_existent_keys(be_dbms) -> None:
    # This should throw an exception
    key = random_string(23)

    # I don't know what exception will be thrown
    with pytest.raises(expected_exception=Exception) as excinfo:
        be_dbms.read(key=key)
    print(
        "Attempting to find a record by a key which does not exist " + str(
            excinfo), file=sys.stderr)
    return None


def test_update(be_dbms):
    key = random_string(6)
    original_value = random_string(11)
    be_dbms.create(key=key, value=original_value)
    print("In test_kvpair2.TestMyKvPair.test_update.\n"
          f"Just created a document with key {key} "
          f"and value {original_value}.", file=sys.stderr)
    new_value = random_string(11)
    be_dbms.update(key=key, value=new_value)
    print(f"Just updated {key} with {new_value}.", file=sys.stderr)
    answer = be_dbms.read(key=key)
    if answer == new_value:
        print("Updated successfully", file=sys.stderr)
    elif answer == original_value:
        print("Did not update - the original value is still there.",
              file=sys.stderr)
    else:
        print(f"The answer is {answer}.", file=sys.stderr)
    assert new_value == answer, \
        f"After updating key {key} with new_value {new_value}, read back " \
        f"{answer}. The original value was {original_value}."
    return


def test_delete(be_dbms):
    """
    Verify that I can delete a record.
    Create a record with a known key.
    Delete it
    Try to read that record.  Should raise an exception
    :return:
    """
    key = random_string(6)
    original_value = random_string(11)
    be_dbms.create(key=key, value=original_value)
    answer = be_dbms.read(key=key)
    assert answer == original_value, \
        "Tried to create a record, the value was wrong on readback" \
        f"Key is {key}, original value was {original_value}, " \
        f"readback value is {answer}"
    be_dbms.delete(key=key)
    with pytest.raises(expected_exception=Exception) as e:
        be_dbms.read(key=key)
    print(f"read raised the {str(e)} exception as expected",
          file=sys.stderr)


def test_update_non_existent_keys(be_dbms) -> None:
    key = random_string(6)
    value = random_string(11)
    with pytest.raises(expected_exception=Exception) as e:
        be_dbms.update(key=key, value=value)
    print("After trying to update a non-existance key, update raised a "
          f"a {str(e)} exception, as expected", file=sys.stderr)
    return


def test_get_key_list(be_dbms, key_list_):
    if be_dbms == key_list_:
        pass
    pass


def test_read_after_disconnect(be_dbms) -> None:
    if be_dbms is not None:
        pass
    pass


def main(select_backend):
    """
    Test the selected back end
    :param select_backend:
    :return:
    """
    global be_obj, key_list
    print(f"The type of select_backend is {type(select_backend)}.",
          file=sys.stderr)
    print(f"select_backend has attributes {dir(select_backend)}",
          file=sys.stderr)

    be_obj = select_backend
    print(f"The type of the backend object (be_obj) is {type(be_obj)}."
          f"The value of the backend object is {str(be_obj)}.", file=sys.stderr)

    key_list = list()

    if not hasattr(be_obj, "connect"):
        print("!!!!!be_obj does not have a connect!!!!", file=sys.stderr)
        print(dir(be_obj), file=sys.stderr)
        raise AssertionError

    be_dbms = be_obj.connect()

    # CRUD - Create and Read
    for _ in range(10):
        key = random_string(13)
        key_list.append(key)
        value = random_string(4)
        test_create_and_recover_key(be_dbms=be_dbms, key=key, value=value)

    # CRUD - update
    test_update(be_dbms=be_dbms)

    # CRUD - delete
    test_delete(be_dbms=be_dbms)

    # Get a list of keys
    test_get_key_list(be_dbms=be_dbms, key_list_=key_list)

    # Test pathological cases
    test_duplicate_keys(be_dbms=be_dbms)
    test_read_non_existent_keys(be_dbms=be_dbms)
    test_update_non_existent_keys(be_dbms=be_dbms)
    test_read_after_disconnect(be_dbms=be_dbms)


if __name__ == '__main__':
    print("Starting main")
    pytest.main()
