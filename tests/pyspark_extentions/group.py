from tests.conftest import *
import pyspark_extentions
import pyspark.sql.functions as F

import datetime

def test_group(spark, test_set_1):

    grouped_1 = (
        test_set_1
        .withColumn('non standard', F.lit(1))
        .eGroup()
        .agg()
    )
    assert grouped_1.count() == 1
    assert grouped_1.columns == ['count']
    assert grouped_1.first()[0] == 14

    grouped_2 = (
        test_set_1
        .eGroup('order_date')
        .agg()
    )
    assert grouped_2.count() == 3
    assert grouped_2.columns == ['order_date', 'count']
    assert grouped_2.first()[0] == datetime.date(2024, 1, 1)

    # (
    #     test_set_1
    #     .withColumn('non standard', F.lit(1))
    #     # .eGroup()
    #     # .eGroup('order_date')
    #     .eGroup('order_date', 'customer_id')
    #     # .pivot('customer_id', prefix='customer=')
    #     .totalsBy('order_date', label='')
    #     .totals(keep_group_column=True)
    #     # .agg()
    #     # .agg(F.expr('sum(1) as bla'), alias=False)
    #     .agg('sum', F.expr('sum(1) as bla'), alias=False)
    #     .show(truncate=False)
    # )
