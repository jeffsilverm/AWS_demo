#! /usr/bin/exec python3
#
#
import datetime
import sys


class ClassUUT(object):
    """The class under test """

    def __init__(self, parameter):
        log_it(f"In class ClassUUT.__init__ with parameter={parameter}")
        self.parameter = parameter

    def function_uut_one(self, para):
        """This is the first function unit under test (UUT)"""
        # Note that this will raise a value exception if para is most
        # non-numeric
        log_it(f"In function_uut_one: para is {para}")
        return para * self.parameter

    def function_uut_two(self, para):
        """This is the second function unit under test (UUT) """
        log_it(f"In function_uut_two: para is {para}")
        return para / self.parameter


def log_it(message):
    """Invoke this method when you want to log something """
    print(f"{str(datetime.datetime.now())}: {message}", file=sys.stderr)


if "__main__" == __name__:
    my_instance_uut = ClassUUT(4.0)
    log_it("Instantiated ClassUUT with 4.0")
    assert my_instance_uut.function_uut_one(3.0) == 12.0
    assert my_instance_uut.function_uut_two(36) == 9.0
    try:
        my_instance_uut.function_uut_one("blue")
    except TypeError as v:
        log_it("Threw ValueError exception as expected" + str(v))
    else:
        log_it("Did *not* raise a ValueError exception.  Oops")
        raise AssertionError("An exception did not occur")