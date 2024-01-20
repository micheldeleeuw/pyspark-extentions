from tests.conftest import *
import pyspark_extensions
import pyspark.sql.functions as F
from pyspark_extensions import *

def test_transpose(spark, test_set_1):

    transposed = (
        test_set_1
        .selectExpr('line_id as `line id`', '*')
        .filter('order_id = "1001"')
        .eTranspose(first_col_gives_column_name=True, column_prefix='line_id=')
    )

    assert transposed.count() == 10
    assert transposed.columns == [
        'column_id', 'column', 'line_id=10001', 'line_id=10002',
        'line_id=10003', 'line_id=10004'
    ]
