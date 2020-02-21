#! /usr/bin/env python3
#
import pytest

import pytest_uut  # Program under test


# https://www.patricksoftwareblog.com/monkeypatching-with-pytest/
#

class TestPytestUut(pytest_uut.ClassUUT):

    def __init__(self, p):
        # super.__init__(parameter=p)
        super().__init__(parameter=p)
        assert self.parameter == p, \
            "Call to super.__init__ doesn't work as " \
            f"you expected. p={p} and self.parameter=" \
            f"{self.parameter}"

    def test_function_uut_one(self, para, nominal):
        # function_uut_one => para * self.parameter
        ans = self.function_uut_one(para=para)
        if isinstance(para, float) or isinstance(self.parameter, float):
            equal = abs(ans - nominal) < 0.0001
        else:
            equal = ans == nominal
        assert equal, f"function_uut_one Should be {nominal} is {ans} "

    def test_function_uut_two(self, para, nominal):
        # function_uut_two => para / self.parameter
        ans = self.function_uut_one(para=para)
        equal = (abs(ans - nominal) < 0.0001 if
                 isinstance(para, float) or
                 isinstance(self.parameter, float) else ans == nominal)
        assert equal, f"function_uut_two Should be {nominal} is {ans} "


tc1 = TestPytestUut(7)
tc1.test_function_uut_one(7, 49)
tc1.test_function_uut_one(7.0, 49.0)
tc1.test_function_uut_one(7.1, 49.7)

tc2 = TestPytestUut("7")
tc2.test_function_uut_one(7, "7777777")
with pytest.raises(TypeError):
    tc2.test_function_uut_one(7, "abstract")
