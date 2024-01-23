from tests.conftest import *
import pyspark_extensions

def test_update_table(spark, test_set_1):
    table = 'default.test_update'

    spark.sql(f'drop table if exists {table}')

    test_set_1.filter('customer_id in ("5001", "5321")').eUpdateTable(table, 'line_id')
    assert spark.table(table).count() == 6

    test_set_1.filter('customer_id in ("5001", "7233")').eUpdateTable(table, 'line_id')
    assert spark.table(table).count() == 12

    assert spark.table(table).select('created_dtime').distinct().count() == 2

    # check that customer 5001 was not updated in the second update
    assert (
        spark.table(table)
        .select('customer_id', 'updated_dtime')
        .distinct()
        .groupBy('updated_dtime')
        .count()
        .orderBy('updated_dtime')
        .eToList('C', columns='count')
    ) == [2, 1]
