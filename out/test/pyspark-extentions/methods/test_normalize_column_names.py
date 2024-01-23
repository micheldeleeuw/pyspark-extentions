from tests.conftest import *
import pyspark_extensions

def test_dates_list_to_string(test_set_1):

    data1 = (
        test_set_1
        .selectExpr(
            'line_id as `line id`',
            'article_id',
            'sales_amount as `max(sales_amount)`',
        )
    )

    data2 = (
        data1
        .selectExpr('*', '1 as `line id`')
    )

    assert data1.eNormalizeColumnNames().columns == ['line_id', 'article_id', 'max_sales_amount']
    assert data1.eNormalizeColumnNames('max(sales_amount)').columns == ['line id', 'article_id', 'max_sales_amount']
    assert data1.eNormalizeColumnNames().columns == ['line_id', 'article_id', 'max_sales_amount']
    assert data2.eNormalizeColumnNames().columns == ['line_id', 'article_id', 'max_sales_amount', 'line_id_1']
