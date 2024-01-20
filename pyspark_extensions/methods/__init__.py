global DataFrame
from pyspark.sql import DataFrame

from methods.dates_list_to_string import dates_list_to_string
from methods.group import Group
from methods.normalize_column_names import normalize_column_names
from methods.printable import Printable
from methods.round import round
from methods.to_list import to_list
from methods.transpose import transpose
from methods.update_table import update_table

DataFrame.eDatesListToString = dates_list_to_string
DataFrame.eGroup = Group.group
DataFrame.eNormalizeColumnNames = normalize_column_names
DataFrame.ePrintable = Printable.printable
DataFrame.eRound = round
DataFrame.eToList = to_list
DataFrame.eTranspose = transpose
DataFrame.eUpdateTable = update_table

