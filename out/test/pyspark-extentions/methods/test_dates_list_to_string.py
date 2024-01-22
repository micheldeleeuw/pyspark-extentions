from tests.conftest import *
import pyspark_extensions
from pyspark.sql import Row
import pyspark.sql.functions as F

def test_dates_list_to_string(test_set_1):

    dates_transform = (
        test_set_1
        .eGroup('customer_id')
        .agg('collect_set(order_date) as order_dates')
        .eDatesListToString('order_dates')
    )

    assert (dates_transform.collect()) == [
        Row(customer_id='5001', order_dates='2024-01-01'),
        Row(customer_id='5321', order_dates='2024-01-01, 2024-01-03'),
        Row(customer_id='7232', order_dates='2024-01-01 to 2024-01-03, 2024-01-05 to 2024-01-06'),
        Row(customer_id='7233', order_dates='2024-01-02')
    ]

    dates_transform2 = (
        test_set_1
        # .withColumn('order_date', F.expr('if(customer_id = "5321", to_date(null), order_date)'))
        .eGroup()
        .agg('collect_list(order_date) as order_dates')
        .withColumn('order_dates', F.expr('transform(order_dates, x -> if(x = "2024-01-02", to_date(null), x))'))
        .eDatesListToString('order_dates')
    )

    assert (dates_transform2.collect()) == [
        Row(order_dates='null, 2024-01-01, 2024-01-03, 2024-01-05 to 2024-01-06')
    ]
