global DataFrame
from pyspark.sql import DataFrame

from .group import Group
DataFrame.eGroup = Group.group
