from tests.conftest import *
import pyspark_extentions
import pyspark.sql.functions as F
from pyspark_extentions import *
from pyspark.sql import Row

def test_transpose(spark, test_set_1):

    data = (
        test_set_1
        .filter('sales_amount in (29.49, 18.0, 51.21)')
        .selectExpr('sales_amount', 'sales_amount as sales_amount2')
        .distinct()
    )

    assert data.eRound().eToList('D') == [(18.0, 18.0), (29.49, 29.49), (51.21, 51.21)]
    assert data.eRound(1, columns='sales_amount2').eToList('D') == [(18.0, 18.0), (29.49, 29.5), (51.21, 51.2)]
