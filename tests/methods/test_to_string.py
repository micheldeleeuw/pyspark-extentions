from tests.conftest import *
import pyspark_extensions
from pyspark_extensions.methods.to_string import ToString

def test_to_string(test_set_1):

    data1 = (
        test_set_1
        .groupBy('customer_id')
        .sum()
    )

    data2 = (
        test_set_1
        .eGroup('customer_id', 'line_id', 'article_description')
        .totalsBy('customer_id', keep_group_column=True, label='')
        .totals(keep_group_column=True, label='')
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

    assert isinstance(data1.eToString(table_format='default', title='Title'), str)
    assert isinstance(data2.eToString(table_format='compact'), str)
    assert isinstance(data2.eToString(by='customer_id'), str)
    assert isinstance(data3.eToString(), str)
    assert isinstance(data3.eToString(columns_widths=[None, None, 10, None]), str)
    assert isinstance(data4.eToString(), str)
