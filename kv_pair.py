#! /usr/bin/env python
#
# This script implements a simple key-value store using DynamoDB.  Input
# is a key and the return is a value.  Operations include
# (These verbs are from RFC 2616 section 9) 
# http://www.w3.org/Protocols/rfc2616/rfc2616-sec9.html
# GET - given a key, return the corresponding value, error if the key does not already exist
# POST - insert a key-value pair in the database, error if the key already exists
# PUT - update a key-value pair in the database, error if the key does not already exist
# DELETE - deletes a key-value pair from the database
# GET, HEAD, PUT and DELETE are idempotent
#
# This assumes that access credentials are stored in the file ~/.boto

import boto.dynamodb2
from boto.dynamodb2.table import Table
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.fields import HashKey
from boto.exception import JSONResponseError
import time

DEBUG=False

TABLE_NAME = 'kv_pairs'

print "Making the connection to the database"
conn = DynamoDBConnection()
print "Getting the list of tables"
table_list = conn.list_tables()
# The table_list is a dictionary with a single key, the value of which is
# a list of tables associated with this account.  If TABLE_NAME is not in
# that list, then create the table, otherwise, just connect to it.
if TABLE_NAME in table_list[u'TableNames'] :
# Make sure that the database is new, otherwise leftovers from previous runs
# may cause some of the tests to fail.
  kv_pairs = Table(TABLE_NAME)

#  kv_pairs = Table(TABLE_NAME)
#  print "Table %s already exists: connecting to it" % TABLE_NAME


def get ( key ):
  """This function returns the value associated with the key, and an HTTP
status code.  Traps the
exception boto.dynamodb2.exceptions.ItemNotFound if the key isn't present
which would return a 403 error (looking at RFC 2616, that seems to be the
status code that fits the best, but recognize that it is not the best fit).
However, sometimes, get doesn't throw the ItemNotFound exception.  In that
case, the value of value['value'] is None"""
  try:
    value = kv_pairs.get_item(key=key)
  except boto.dynamodb2.exceptions.ItemNotFound:
    return ( None, 403 )
  if value['value'] == None :
    return ( None, 403)
  else :
    return ( value['value'], 200 )


def post ( key, value ):
  """ This subroutine adds the key-value pair to the database, and returns 
200. If the key is already present, then it throws a 
boto.dynamodb2.exceptions.ConditionalCheckFailedException and returns 403"""
  try :
    kv_pairs.put_item(data={'key':key,'value':value})
    return 200
  except boto.dynamodb2.exceptions.ConditionalCheckFailedException:
    return 403

def delete ( key ) :
  """This deletes the key-value pair from the database.  If the key is not
present, then it returns 403, otherwise, it returns 200"""
  if len(key) > 0:
     try:
       kv_pairs.delete_item(key=key)
       return 200
     except boto.dynamodb2.exceptions.ConditionalCheckFailedException:
       return 403
   return 400

def put ( key, new_value ):
  """This updates a key-value pair in the database.  If the key is not
present, then it returns 403, otherwise, it returns 200"""
  try:
    old_value = kv_pairs.get_item(key=key)
  except boto.dynamodb2.exceptions.ItemNotFound:
    return 403
  if old_value['value'] == None :
    return 403
  old_value['value'] = new_value
  try:
    old_value.save(overwrite=True)
# Sometimes, save raises a ValidationException, I don't know why
# According to https://sourcegraph.com/github.com/boto/boto/symbols/python/boto/opsworks/exceptions/ValidationException
# ValidationException inherits from JSONResponseError
  except JSONResponseError:
    print "Something went wrong updating the database.  ValidationcwException was\
raised."
    print "The type of old_value is "+str(type(old_value))
    return 500		# I hate to do this
  return 200

################################### Beginning of test code ******
if __name__ == "__main__" :

  def test_get( key ):
    print "Getting key %s from database" % key
    value = get ( key )
    if key in check_dict :
      print "The value for key %s is %s" % ( key, value )
      assert value[0] == check_dict[key], \
             "Database returned wrong value: %s should have returned %s" % \
             (value[0], check_dict[key])
      assert value[1] == 200, "Database call did not return 200"
    else :
      print "key %s does not exist in database" % key
      assert value[0] == None, \
             "Database should have returned None but returned %s" % value[0]
      assert value[1] == 403, \
             "Database should have returned 403 but returned %d" % value[1]


  def test_post( key, value ):
    print "Inserting key %s with value %s" % ( key, value )
    status = post ( key, value )
    if key not in check_dict:
      if DEBUG :
        assert status == 200, \
          "Key %s not in check_dict and should be because DEBUG is True." + \
          "Status is %d" % (key, status)
      else :
        assert status == 403 or status == 200, \
          "key % is not in check_dict, and we don't know if it should be because"+\
          "it's not in check_dict.  Status is %d" % ( key, status )
      check_dict[key] = value
# Verify that the key really inserted the value into the database
      assert value == get ( key )[0]
    else:
      print "Tried to insert a key that was already in the database." +\
      "Should have used put instead of post"
      assert status == 403
      
  def test_delete ( key ):
    print "Testing deleting key %s from the database" % key
    status = delete ( key )
    if key in check_dict :
      print "key-value pair was deleted"
      assert status == 200, \
          "Status was %d should have been 200 when removing a key that exists" % status
      del check_dict[key]
# Verify that the key is really gone.  This should fail
      status = get ( key )
      assert status == (None, 403),"Status was %s should have been a tuple (None,403)"%\
             str( status )
    else:
      print "The key was not in the database"
      assert status == 200,"Status was %d should have been 200 after attempting to "+\
             "delete a key from the database that should not have been there"
  def test_put ( key, value ):
    print "Testing updating key %s with value %s" % ( key, value )
    status = put ( key, value )
    if key in check_dict :
      assert status == 200
      check_dict[key] = value
      value = get ( key )
      assert value[0] == check_dict[key]
      assert value[1] == 200
    else :
      print "Key %s is not in check_dict" % key
# A little consistency check to make sure that the database and the dictionary
# really are in sync.
      value = get ( key )
      assert value[0] == None
      assert value[1] == 403 

########################################################## 
  check_dict = {}   # This dictionary is used to verify that the database
			# is working correctly
  if DEBUG :
    print "Deleting the table %s" % TABLE_NAME
    kv_pairs.delete()
  # now that the table is gone, recreate it
    while True :
      time.sleep(10)    # it takes some time for the table to delete
      try:
        kv_pairs = Table.create(TABLE_NAME, schema=[ HashKey('key')],
       throughput={ 'read': 5, 'write': 15, })
      except JSONResponseError:
        print "The table %s still isn't deleted.... waiting" % TABLE_NAME
      else:
        break
    print "Created table %s" % TABLE_NAME
    time.sleep(10)
    while True:
      try:
        test_post("Dillon", 17)
      except JSONResponseError:
        print "Trying to insert a key-value pair failed.  Perhaps the database isn't ready yet"
        time.sleep(5)
      else:
        print "The database is ready now"
        break
  else :
    print "Not deleting the table"
    test_delete("Dillon")    # He may already be in the table
    test_post("Dillon", "Didn't delete the table")
  test_get("Dillon")
  if not DEBUG :
    test_delete("Devin")
  test_post("Devin", 20)
  test_get("Devin")
  test_put("Devin", 22)
  test_get("Devin")
  if not DEBUG :
    test_delete("Janie")
  test_post("Janie", 12)
  test_get("Janie")
  test_put("Janie", -3)
  test_get("Janie")
  if not DEBUG :
    delete("Randall")
  try:
    check_dict["Randall"] = -12.3
    test_get("Randall")   # This should throw an error because Randall is in the check_dict
  except AssertionError:
    print "Threw an expected Assertion error getting a non-existant key - all is well"
  else :
    assert True,"Did *not* throw an expected Assertion Error"
  try:
    test_put("Randall", 3421)
  except AssertionError:
    print "Threw an expected Assertion error - all is well.  Tried to update a non-existant key"
  else :
    assert True,"Did *not* throw an expected Assertion Error"
# GET, HEAD, PUT
  test_delete("Janie")
  test_delete("Janie")  # Test that delete is idempotent
  test_put("Devin", "Green")
  test_get("Devin")
  test_put("Devin", "Green")  # Test that put is idempotent
  test_get("Devin")
  test_get("Devin")
  while True:
    op = raw_input("Enter I to insert, U to update, D to delete, or G to get ")
    op = op.upper()
    key = raw_input("Enter a key ")
    if op == "I" :
      value = raw_input("Enter a value for key %s " % key )
      try :
        post ( key, value )
      except boto.dynamodb2.exceptions.ConditionalCheckFailedException:
        print "Probably the key %s already has a value.  let's see" % key
        value = get ( key )
        print "Yes, the value is %s" % value
    elif op == "U" :
      value = raw_input("Enter a value for key %s" % key )
      try :
        put ( key, value )
      except boto.dynamodb2.exceptions.ConditionalCheckFailedException:
        print "Probably the key %s already doesn't a value.  let's see" % key
        try :
          value_1 = get( key )
        except boto.dynamodb2.exceptions.ConditionalCheckFailedException:
          print "I was right - there is no value for key %s" % key
        else :
          print "Something else must be wrong.  Key %s has value %s" % \
                (key, value_1)
    elif op == "D" :
      delete ( key )
    elif op == "G" :
      value = get ( key )
      print "The value of %s is %s" % ( key, value )
    else :
      print "You didn't enter I, U, D, or G!"

    
