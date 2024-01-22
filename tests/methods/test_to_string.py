from tests.conftest import *
import pyspark_extensions

def test_to_string(test_set_1):

    data = (
        test_set_1
        .groupBy('customer_id')
        .sum()
    )

    # print(test_set_1.eToString())
    # assert test_set_1.eToString() == 'x'

    data = (
        test_set_1
        .eGroup('customer_id', 'line_id')
        .totalsBy('customer_id', keep_group_column=True)
        .sum()
    )

    data.ePrintable().eShow(by='customer_id')
    # print(data.toString())
