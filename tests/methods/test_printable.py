from tests.conftest import *
import pyspark_extensions
import pyspark.sql.functions as F
from pyspark_extensions import *

def test_printable(spark, test_set_1):

    data = (
        test_set_1
        .filter('customer_id = "5321"')
        .select('order_id', 'order_date', 'order_timestamp', 'article_description', 'sales_amount', 'returned_indicator')
    )

    # data.show(truncate=False)
    # data.ePrintable().show(truncate=False)