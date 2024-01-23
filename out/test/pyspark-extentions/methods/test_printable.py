from tests.conftest import *
import pyspark_extensions
from datetime import date, datetime


def test_printable(test_set_1):
    data = (test_set_1  # .filter('customer_id = "5321"')
            .eGroup('customer_id').agg('max(line_id)', 'max(order_date)', 'max(order_timestamp)', 'sum(sales_amount)',
                                       '11111.111 * sum(sales_amount) as big_sales_amount',
                                       '100*sum(sales_amount)/600 as sales_amount_perc',
                                       'sum(sales_amount)/600 as sales_amount_rate', 'max(returned_indicator)', ))

    assert data.ePrintable().eToList('D') == [
        ('5001', '10.004', date(2024, 1, 1), datetime(2024, 1, 1, 10, 15, 53), '576,97', '6.410.777,71', '96,2', '1,0', True),
        ('5321', '10.014', date(2024, 1, 3), datetime(2024, 1, 3, 15, 0), '115,09', '1.278.777,76', '19,2', '0,2', False),
        ('7232', '10.019', date(2024, 1, 6), datetime(2024, 1, 6, 11, 59, 59), '609,16', '6.768.444,38', '101,5', '1,0', False),
        ('7233', '10.013', date(2024, 1, 2), datetime(2024, 1, 2, 9, 1, 1), '230,16', '2.557.333,31', '38,4', '0,4', True)
    ]

    assert data.ePrintable(k=True).eToList('D') == [
        ('5001', '10', date(2024, 1, 1), datetime(2024, 1, 1, 10, 15, 53), '1', '6.411', '96,2', '1,0', True),
        ('5321', '10', date(2024, 1, 3), datetime(2024, 1, 3, 15, 0), '0', '1.279', '19,2', '0,2', False),
        ('7232', '10', date(2024, 1, 6), datetime(2024, 1, 6, 11, 59, 59), '1', '6.768', '101,5', '1,0', False),
        ('7233', '10', date(2024, 1, 2), datetime(2024, 1, 2, 9, 1, 1), '0', '2.557', '38,4', '0,4', True)
    ]

    assert data.ePrintable(k=True, k_label='k').eToList('D') == [
        ('5001', '10k', date(2024, 1, 1), datetime(2024, 1, 1, 10, 15, 53), '1k', '6.411k', '96,2', '1,0', True),
        ('5321', '10k', date(2024, 1, 3), datetime(2024, 1, 3, 15, 0), '0k', '1.279k', '19,2', '0,2', False),
        ('7232', '10k', date(2024, 1, 6), datetime(2024, 1, 6, 11, 59, 59), '1k', '6.768k', '101,5', '1,0', False),
        ('7233', '10k', date(2024, 1, 2), datetime(2024, 1, 2, 9, 1, 1), '0k', '2.557k', '38,4', '0,4', True)
    ]

    assert data.select('sum_sales_amount', 'big_sales_amount').ePrintable(
        precision=-1).eToList('D') == [
            ('577', '6.411k'), ('115', '1.279k'), ('609', '6.768k'),
            ('230', '2.557k')
    ]
