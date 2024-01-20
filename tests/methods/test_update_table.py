from tests.conftest import *
import pyspark_extensions
import pyspark.sql.functions as F

def test_update_table(spark, test_set_1):
    test_set_1.eUpdateTable('test_update', 'line_id')

    add proper test, check location