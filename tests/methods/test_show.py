from tests.conftest import *
import pyspark_extensions

def test_show(test_set_1):
    assert test_set_1.filter('false').eShow() == None
