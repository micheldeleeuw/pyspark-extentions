from tests.conftest import *
import pyspark_extensions
import pyspark.sql.functions as F


def test_columns_integer(test_set_1):
    assert test_set_1.eColumnsInteger == ['line_id']


def test_columns_nummeric(test_set_1):
    assert test_set_1.eColumnsNummeric == ['line_id', 'sales_amount']


def test_columns_percentage(test_set_1):
    assert (
        test_set_1
        .withColumnRenamed('line_id', 'some_perc')
        .withColumn('some_rate', F.lit(1))
        .eColumnsPercentage
    ) == ['some_perc', 'some_rate']


def test_columns_nummeric_not_percentage(test_set_1):
    assert (
        test_set_1
        .withColumnRenamed('line_id', 'some_perc')
        .eColumnsNummericNotPercentage
    ) == ['sales_amount']
