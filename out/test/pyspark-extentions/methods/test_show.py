from tests.conftest import *
import pyspark_extensions

def test_show(test_set_1):
    assert test_set_1.eShow() == None
