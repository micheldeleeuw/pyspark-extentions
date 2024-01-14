from tests.conftest import *
import pyspark_extentions
import pyspark.sql.functions as F

def test_group(spark, test_set_1):
    (
        test_set_1
        .withColumn('non standard', F.lit(1))
        # .eGroup()
        # .eGroup('order_date')
        .eGroup('order_date', 'customer_id')
        # .pivot('customer_id', prefix='customer=')
        .totalsBy('order_date', label='')
        .totals(keep_group_column=True)
        # .agg()
        # .agg(F.expr('sum(1) as bla'), alias=False)
        .agg('sum', F.expr('sum(1) as bla'), alias=False)
        .show(truncate=False)
    )
