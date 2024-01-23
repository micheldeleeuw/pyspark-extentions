from tests.conftest import *
import pyspark_extensions

def test_to_string(test_set_1):

    data1 = (
        test_set_1
        .groupBy('customer_id')
        .sum()
    )

    data2 = (
        test_set_1
        .eGroup('customer_id', 'line_id', 'article_description')
        .totalsBy('customer_id', keep_group_column=True)
        .sum()
    )

    data3 = (
        test_set_1
        .eGroup('customer_id', 'line_id', 'article_description')
        .totals(keep_group_column=True)
        .sum()
    )
    # data.ePrintable().eShow(by='customer_id')
    data1.eShow()
    data2.eShow()
    data2.eShow(by='customer_id')
    data3.eShow()
