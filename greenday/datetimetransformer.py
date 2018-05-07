import pyspark.sql.dataframe
from pyspark.ml.feature import Imputer
from pyspark.sql.types import FloatType, DateType, IntegerType
from pyspark.sql.functions import unix_timestamp, from_unixtime, current_date, datediff, col, dayofmonth, year, month
import pandas as pd



class datetimetransformer:

    #constructor
    def __init__(self, df):

        assert (isinstance(df, pyspark.sql.dataframe.DataFrame)), "input type should be 'pyspark.sql.dataframe.DataFrame'"

        self._df = df

    ###########################################################################################################
    #embedding these three basic operations of pyspark.sql.DataFrame so that our GreenDay object can also use
    def show(self, n=20):
        return self._df.show(n)

    def select(self, columns):
        self._df = self._df.select(columns)
        return self

    def collect(self):
        return self._df.collect()
    ###########################################################################################################

    def str_to_Date(self, columns, new_col, stripTime=False, Inplace=False):

        """
        @columns: a list or a string of the column(s) name in original dataframe, the columns needs to be converted
        @new_col: a list or a string of the new column(s) name after converting
        @stripTime: a boolean to indicate whether to keep time, e.g.'00:00:00'
        @Inplace:  a boolean to indicate whether to replace/drop the original columns 
        """

        if isinstance(columns, str):
            columns = [columns]
        else:
            assert isinstance(columns, list), "Error: columns argument must be a string or a list!"

        if isinstance(new_col, str):
            new_col = [new_col]
        else:
            assert isinstance(new_col, list), "Error: columns argument must be a string or a list!"

        assert len(columns) == len (new_col), "Error: inconsistent lengths for argument of columns list and new columns list"

        for i in range(len(columns)):
            self._df = self._df.select(columns[i], from_unixtime(unix_timestamp(columns[i], 'MM/dd/yyy')).alias('with time'))
            if stripTime:
                if Inplace: #strip time and in place change
                    self._df = self._df.withColumn('without time', self._df['with time'].cast(DateType()))
                    self._df = self._df.drop(columns[i]).drop('with time').withColumnRenamed('without time', new_col[i])
                else: #strip time and keep the old column
                    self._df = self._df.withColumn('without time', self._df['with time'].cast(DateType()))
                    self._df = self._df.drop('with time').withColumnRenamed('without time', new_col[i])
            else:
                if Inplace:
                    self._df = self._df.drop(columns[i]).withColumnRenamed("with time", new_col[i])
                else:
                    self._df = self._df.withColumnRenamed("with time", new_col[i])

        return self._df


    def age_calculator(self, columns, age_colname):
        """
        @columns: a string of column name
        @age_colname: a string of the new column of age, e.g.'age'
        """
        assert isinstance(columns, str), "Error: columns argument must be a string!"
        assert isinstance(age_colname, str), "Error: the name for age column argument must be a string!"

        self._df = (self.str_to_Date(columns, columns, stripTime=True, Inplace=True))._df
        self._df = self._df.withColumn(age_colname, ((datediff(current_date(), col(columns)).cast(FloatType()))/365).cast(IntegerType()))
        
        return self._df


    def Date_spliter(self, columns, y_col, m_col, d_col):

        """
        @columns: a string of the date column
        @y_col: a string of the new name of year column
        @m_col: a string of the new name of month column
        @d_col: a string of the new name of day column
        """

        assert isinstance(columns, str), "Error: columns argument must be a string!"
        assert isinstance(y_col, str), "Error: name for year column argument must be a string!"
        assert isinstance(m_col, str), "Error: name for month column argument must be a string!"
        assert isinstance(d_col, str), "Error: name for day column argument must be a string!"

        self._df = (self.str_to_Date(columns, columns, stripTime=True, Inplace=True))._df
        self._df = self._df.withColumn(y_col, year(self._df[columns])).withColumn(m_col,month(self._df[columns])).withColumn(d_col,dayofmonth(self._df[columns]))

        return self._df





