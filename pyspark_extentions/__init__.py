global DataFrame
from pyspark.sql import DataFrame

from .group import Group
from .transpose import transpose

DataFrame.eGroup = Group.group
DataFrame.eTranspose = transpose
