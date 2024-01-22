from tests.conftest import *
import pyspark_extensions
import pyspark.sql.functions as F

def test_update_table(spark, test_set_1):
    table = 'default.test_update'

    spark.sql(f'drop table if exists {table}')

    test_set_1.filter('customer_id in ("5001", "5321")').eUpdateTable(table, 'line_id')
    assert spark.table(table).count() == 6

    test_set_1.filter('customer_id in ("5001", "7233")').eUpdateTable(table, 'line_id')
    assert spark.table(table).count() == 12

    assert spark.table(table).select('created_dtime').distinct().count() == 2
    # assert \
    spark.table(table).select('customer_id', 'created_dtime').distinct().show(truncate=False)

# 'created_dtime', 'updated_dtime'
