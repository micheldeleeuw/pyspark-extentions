import logging
from typing import List
from pyspark.sql import DataFrame

log = logging.getLogger(__name__)


def columns_nummeric(df: DataFrame) -> List[str]:

    return [dtype[0] for dtype in df.dtypes if
            dtype[1] in ('double', 'integer', 'int', 'short', 'long', 'float', 'bigint',) or dtype[1].find(
                'decimal') > -1]
