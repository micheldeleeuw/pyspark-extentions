import pytest
import logging
import os

from pyspark.sql import SparkSession

logging.getLogger('tests').setLevel('DEBUG')
log = logging.getLogger(__name__)

@pytest.fixture(scope='session')
def spark(tmp_path_factory) -> SparkSession:
    """
    Prepares the testing environment for Spark using the temporary directory created for the pytest session
    :param tmp_path: str
    :return: SparkSession
    """

    tmp_path = tmp_path_factory.mktemp("spark_temp")
    log.info(f'Spark temp path : {tmp_path}')

    return (
        SparkSession.builder
        .config('spark.sql.warehouse.dir', str(tmp_path))
        # .config('spark.jars.packages', 'io.delta:delta-core_2.12:2.4.0')
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


@pytest.fixture(scope='session')
def test_set_1(spark):
    file = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'fixtures',
        'test_set_1.csv',
    )

    return (
        spark.read.format('csv')
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS")
        .option('header', 'true')
        .option('delimiter', ',')
        .schema('''
            order_id string,
            line_id string,
            order_date date,
            order_timestamp timestamp,
            customer_id string,
            article_id string,
            article_description string,
            sales_amount double,
            returned_indicator boolean,
            registration_timestamp timestamp
        ''')
        .load(file)
    )

