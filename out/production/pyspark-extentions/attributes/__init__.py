global DataFrame
from pyspark.sql import DataFrame

from .columns_integer import columns_integer
from .columns_nummeric import columns_nummeric
from .columns_nummeric_not_percentage import columns_nummeric_not_percentage
from .columns_percentage import columns_percentage

DataFrame.eColumnsInteger = property(columns_integer)
DataFrame.eColumnsNummeric = property(columns_nummeric)
DataFrame.eColumnsNummericNotPercentage = property(columns_nummeric_not_percentage)
DataFrame.eColumnsPercentage = property(columns_percentage)

