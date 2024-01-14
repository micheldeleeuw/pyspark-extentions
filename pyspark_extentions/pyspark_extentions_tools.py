import logging

log = logging.getLogger(__name__)

class PysparkExtentionsTools():
    def add_dataframe_properties_to_self(self, df):
        self.columns = df.columns
        self.columns_nummeric = [
            dtype[0] for dtype in df.dtypes
            if dtype[1] in (
                'double', 'integer', 'int', 'short', 'long',
                'float', 'bigint',
            )
            or dtype[1].find('decimal')>-1
        ]

    @staticmethod
    def validate_columns_parameters(var):
        var = [] if not var else var
        var = [var] if isinstance(var, str) else var
        return var