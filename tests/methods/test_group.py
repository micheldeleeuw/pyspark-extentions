from tests.conftest import *
import pyspark_extensions
import pyspark.sql.functions as F

import datetime

def test_group(test_set_1):

    grouped_1 = (
        test_set_1
        .withColumn('non standard', F.lit(1))
        .eGroup()
        .agg()
    )
    assert grouped_1.count() == 1
    assert grouped_1.columns == ['count']
    assert grouped_1.first()[0] == 19

    grouped_2 = (
        test_set_1
        .eGroup('order_date')
        .agg()
    )
    assert grouped_2.count() == 5
    assert grouped_2.columns == ['order_date', 'count']
    assert grouped_2.first()[0] == datetime.date(2024, 1, 1)

    grouped_3 = (
        test_set_1
        .withColumn('non standard', F.lit(1))
        .eGroup('non standard')
        .agg('max(sales_amount)')
    )
    assert grouped_3.columns == ['non standard', 'max_sales_amount']

