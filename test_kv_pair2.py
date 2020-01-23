import random
import sys
from string import ascii_letters

import pytest

import kv_pair2

def random_string(length) -> str:
    # A generator
    return ''.join(random.choice(ascii_letters)
                   for _ in range(length))  # _ is a variable is not used

@pytest.fixture(params=[kv_pair2.Backends.MONGODB, kv_pair2.Backends.DICT],
                scope="module")
def select_backend(request: kv_pair2.Backends):
    """ This method tests a backend given by be
    :param  request kv_pair2.Backends    the backend to test
    """

    # Define a finalizer and let pytest know that it is a finalizer
    def fin():
        print("Calling backend_obj.disconnect")
        backend_obj.disconnect()
        assert not backend_obj.connected, "backend_obj is True after " \
                                          "disconnect "

    request.addfinalizer(fin)

    assert isinstance(request.param, kv_pair2.Backends), \
        f"request.param is type {type(request.param)}, " \
        f"but should really be an instance of kv_pair2.Backends"
    if request.parm == kv_pair2.Backends.MONGODB:
        backend_obj = kv_pair2.BackendMongo(db_name="M5", db_server="localhost",
                                            db_port=27017, table_name="TN")
    elif request.parm == kv_pair2.Backends.DICT:
        backend_obj = kv_pair2.BackendDict(db_name="D5")
    elif request.parm == kv_pair2.Backends.MYSQL:
        raise NotImplementedError(str(request.parm))
    else:
        raise NotImplementedError(str(request.parm))
    # These asserts are here because I don't trust my knowledge of pytest
    assert callable(backend_obj.connect), \
        "backend_obj.connect is not callable and should be. "\
        f"request.parm is {str(request.parm)} and "\
        f"the attributes of backend_obj are {dir(backend_obj)}."
    assert backend_obj.connected, "backend_obj.connected is False"
    return backend_obj




@pytest.mark.dependency(depends=['select_backend', 'test_create_and_recover_key'])
def test_duplicate_keys(select_backend):
    """This test case verifies that entering two records with
    duplicate keys raises an exception"""
    key = random_string(6)
    value = random_string(11)
    select_backend.create(key, value)
    # Cannot create the same key twice.  That the value is the same makes no
    # difference, it should still raise a ValueError Exception
    with pytest.raises(ValueError):
        select_backend.create(key, value)

@pytest.mark.usefixtures('select_backend')
def test_create_and_recover_key(select_backend, key, value):
    # Step into create ======================== step into create
    select_backend.create(key, value)
    returned_value = select_backend.be_dbms.read(key)
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

@pytest.mark.dependency(depends=['select_backend', 'test_create_and_recover_key'])
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


def test_get_key_list(be_dbms):
    assert hasattr(be_dbms, 'get_key_list')
    pass


def test_read_after_disconnect(be_dbms) -> None:
    assert not be_dbms.connected, "be_dbms.connected is True at " \
                                  "test_read_after_disconnect.  The DBMS " \
                                  "should have been disconnected already "
    key = random_string(4)
    with pytest.raises(Exception) as e:
        be_dbms.read(key)
        print(str(e))





def test_backend(select_backend):
    print(f"The type of select_backend is {type(select_backend)}.",
          file=sys.stderr)
    #    print(f"select_backend() returns type {type(select_backend())}.",
    #          file=sys.stderr)

    be_obj = select_backend
    print(f"The type of the backend object (be_obj) is {type(be_obj)}."
          f"The value of the backend object is {str(be_obj)}.", file=sys.stderr)

    key_list = list()

    # CRUD - Create and Read
    for _ in range(10):
        key = random_string(13)
        key_list.append(key)
        value = random_string(4)
        be_obj.test_create_and_recover_key(key=key, value=value)

    # CRUD - update
    be_obj.test_update()

    # CRUD - delete
    be_obj.test_delete()

    # Get a list of keys
    be_obj.test_get_key_list(key_list)

    # Test pathological cases
    be_obj.test_duplicate_keys()
    be_obj.test_read_non_existent_keys()
    be_obj.test_update_non_existent_keys()
    be_obj.test_read_after_disconnect()


"""
if __name__ == '__main__':
    pytest.main()
"""
