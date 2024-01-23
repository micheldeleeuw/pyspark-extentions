from pyspark.sql import DataFrame

def show(
    df: DataFrame,
    *args,
    **kwargs,
):

    print(df.eToString(*args, **kwargs))