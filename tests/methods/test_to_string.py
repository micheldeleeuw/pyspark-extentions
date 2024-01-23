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
        .totals(keep_group_column=True)
        .sum()
    )

    data3 = (
        test_set_1
        .eGroup('customer_id', 'line_id', 'article_description')
        .totals(keep_group_column=True)
        .sum()
    )

    data4 = (
        test_set_1
        .eGroup()
        .agg('max')
    )

    format=None

    assert isinstance(data1.eToString(format=format, title='\nShow 1:'), str)
    assert isinstance(data2.eToString(format=format, title='\nShow 2:'), str)
    assert isinstance(data2.eToString(
        by='customer_id', format=format, title='\nShow 2:'), str)
    assert isinstance(data3.eToString(format=format, title='\nShow 4:'), str)
    assert isinstance(data3.eToString(
        columns_widths=[None, None, 10, None], format='plain', title='\nShow 5:'),
        str
    )
    assert isinstance(data4.eToString(title='\nShow 6:', format='simple_outline'), str)