from DateTimeTransformer import DateTimeTransformer
from MissingValue import MissingValue
from duplicates import duplicates
from outlier import outlier
from scaler import scaler
from data_format import data_format


class Greenday:
    def __init__(self, df):
        self._df = df
        self.DateTimeTransformer = DateTimeTransformer(df)
        self.MissingValue = MissingValue(df)
        self.data_format = data_format(df)
        self.duplicates = duplicates(df)
        self.outlier = outlier(df)
        self.scaler = scaler(df)
