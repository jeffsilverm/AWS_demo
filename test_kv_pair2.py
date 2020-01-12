import random
import sys
from string import ascii_letters

import pytest

import kv_pair2


def random_string(length) -> str:
    return ''.join(random.choice(ascii_letters)
                   for _ in range(length))  # _ is a variable is not used


def test_duplicate_keys(self):
    """This test case verifies that entering two records with
    duplicate keys raises an exception"""
    key = random_string(6)
    value = random_string(11)
    self.be_dbms.create(key, value)
    with pytest.raises(Exception):
        self.be_dbms.create(key, value)

def test_create_and_recover_key(self, key, value):
    # Step into create ======================== step into create
    self.be_dbms.create(key, value)
    returned_value = self.be_dbms.read(key)
    assert (value == returned_value), \
        f"value {value} was not returned.  {returned_value} was, instead"

def test_read_non_existent_keys(self) -> None:
    # This should throw an exception
    key = random_string(23)

    # I don't know what exception will be thrown
    with pytest.raises(expected_exception=Exception) as excinfo:
        self.be_dbms.read(key=key)
    print(
        "Attempting to find a record by a key which does not exist " + str(
            excinfo), file=sys.stderr)
    return None

def test_update(self):
    key = random_string(6)
    original_value = random_string(11)
    self.be_dbms.create(key=key, value=original_value)
    print("In test_kvpair2.TestMyKvPair.test_update.\n"
          f"Just created a document with key {key} "
          f"and value {original_value}.", file=sys.stderr)
    new_value = random_string(11)
    self.be_dbms.update(key=key, value=new_value)
    print(f"Just updated {key} with {new_value}.", file=sys.stderr)
    answer = self.be_dbms.read(key=key)
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

def test_delete(self):
    """
    Verify that I can delete a record.
    Create a record with a known key.
    Delete it
    Try to read that record.  Should raise an exception
    :return:
    """
    key = random_string(6)
    original_value = random_string(11)
    self.be_dbms.create(key=key, value=original_value)
    answer = self.be_dbms.read(key=key)
    assert answer == original_value, \
        "Tried to create a record, the value was wrong on readback" \
        f"Key is {key}, original value was {original_value}, "\
        f"readback value is {answer}"
    self.be_dbms.delete(key=key)
    with pytest.raises(expected_exception=Exception) as e:
        self.be_dbms.read(key=key)
    print(f"read raised the {str(e)} exception as expected",
          file=sys.stderr)

def test_update_non_existent_keys(self) -> None:
    key = random_string(6)
    value = random_string(11)
    with pytest.raises(expected_exception=Exception) as e:
        self.be_dbms.update(key=key, value=value)
    print("After trying to update a non-existance key, update raised a "
          f"a {str(e)} exception, as expected", file=sys.stderr)
    return

def test_get_key_list(self, key_list):
    pass

def test_read_after_disconnect(self) -> None:
    pass



@pytest.fixture(params=[kv_pair2.Backends.MONGODB])
def select_backend(request: kv_pair2.Backends):
    """ This method tests a backend given by be
    :param  request kv_pair2.Backends    the backend to test
    """
    assert isinstance(request.param, kv_pair2.Backends), \
        f"request.param is type {type(request.param)}, " \
        f"but should really be an instance of kv_pair2.Backends"
    if request.parm == kv_pair2.Backends.MONGODB:
        backend_obj = kv_pair2.

    elif request.parm == kv_pair2.Backends.DICT:

    elif request.parm == kv_pair2.Backends.MYSQL:

    else:
        raise NotImplementedError(str(request.parm))
    return backend_obj


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
