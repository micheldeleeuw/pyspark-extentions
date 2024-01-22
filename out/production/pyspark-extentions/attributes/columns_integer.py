import logging
from typing import List
from pyspark.sql import DataFrame

log = logging.getLogger(__name__)


def columns_integer(df: DataFrame) -> List[str]:

    return [dtype[0] for dtype in df.dtypes if
            dtype[1] in ('integer', 'bigint', 'int')]

