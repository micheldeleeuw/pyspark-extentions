from tests.conftest import *
import pyspark_extentions
from pyspark.sql import Row
from pyspark_extentions import *

def test_dates_list_to_string(spark, test_set_1):

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

