import logging
from typing import List
from pyspark.sql import DataFrame
from pyspark_extensions.config import ENVARS
import re

log = logging.getLogger(__name__)


def columns_percentage(df: DataFrame) -> List[str]:

    return [
        col for col in df.eColumnsNummeric
        if re.search(fr"{ENVARS['percentage_column_regex']}", col)
    ]
