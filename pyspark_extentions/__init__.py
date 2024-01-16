global DataFrame
from pyspark.sql import DataFrame

from .group import Group
from .normalize_column_name import normalize_column_names
from .transpose import transpose
from .dates_list_to_string import dates_list_to_string

DataFrame.eGroup = Group.group
DataFrame.eNormalizeColumnNames = normalize_column_names
DataFrame.eTranspose = transpose
DataFrame.eDatesListToString = dates_list_to_string
