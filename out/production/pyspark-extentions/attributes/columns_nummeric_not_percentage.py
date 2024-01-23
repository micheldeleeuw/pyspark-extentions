import logging
from typing import List
from pyspark.sql import DataFrame

log = logging.getLogger(__name__)


def columns_nummeric_not_percentage(df: DataFrame) -> List[str]:
    columns_percentage = df.eColumnsPercentage
    return [
        col for col in df.eColumnsNummeric
        if col not in columns_percentage
    ]
