from DateTimeTransformer import DateTimeTransformer
from MissingValue import MissingValue
from duplicates import duplicates
from outlier import outlier
from scaler import scaler
from data_format import data_format


class Greenday:
    def __init__(self, df):
        self._df = df
        self.DateTimeTransformer = DateTimeTransformer(self._df)
        self.MissingValue = MissingValue(self._df)
        self.data_format = data_format(self._df)
        self.duplicates = duplicates(self._df)
        self.outlier = outlier(self._df)
        self.scaler = scaler(self._df)
    # def update_df:
    #     self._df = df
