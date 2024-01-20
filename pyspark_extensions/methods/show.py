from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def show(
    df: DataFrame,
    precision: int = 2,
    percentage_precision: int = None,
    padding_character:str = '',
    n: int = 1000,
    truncate: bool = False,
    vertical: bool = False,
    by: str = None,
    output:str = 'screen'
):
    assert output in ('screen', 'file', 'return'), "output must be 'screen', 'file' or 'return')"

    if output == 'file':
        raise Exception('output = file is not suppported at this time.')

    if isinstance(truncate, bool) and truncate:
        truncate = 20

    if by:
        df.cache().count()
        by_values = [col[0] for col in df.select(by).distinct().orderBy(by).collect()]
        cols = [col for col in df.columns if col != by]
    else:
        by_values = [None]
        cols = df.columns

    to_output = ''

    for by_value in by_values:
        if by:
            _output = (f'{by}: {by_value}')
            if output == 'screen':
                print('\n' + _output)
            else:
                to_output += '\n' + _output
        _output = (
            df
            .filter('true' if not by else F.col(by)==by_value)
            .drop(by if by else 'zzxzxzxzxz')
            # .muPrintable(precision=precision, percentage_precision=percentage_precision)

            # add spaces to the data
            .selectExpr([f'concat("{padding_character}", cast(`{col}` as string), "{padding_character}") as `{col}`' for col in cols])

            ._jdf.showString(n, int(truncate), vertical)
        )
        if output == 'screen':
            print(_output)
        else:
            to_output += '\n' + _output

    df.unpersist()

    if output == 'return':
        return to_output
    elif output == 'file':
        downloadLink(to_output)


