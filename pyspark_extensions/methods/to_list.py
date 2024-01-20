from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def to_list(df: DataFrame, type: str = 'C'):

    assert type in ('C', 'R', 'D'), 'Type must be C (first column), R (first row) or D (total dataframe).'

    if type == 'R':
        return list(df.limit(1)
                    .select(F.struct(df.columns).alias("dataCol"))
                    .collect()[0].dataCol
                    .asDict().values())

    elif type == "C":
        col = df.columns[0]
        return [row[0] for row in df.select(col).collect()]

    else:
        return [tuple(row.dataCol.asDict().values())
                for row in df.select(F.struct([f'`{col}`' for col in df.columns]).alias("dataCol")).collect()]

