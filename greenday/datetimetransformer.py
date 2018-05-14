import pyspark.sql.dataframe
from pyspark.ml.feature import Imputer
from pyspark.sql.types import FloatType, DateType, IntegerType
from pyspark.sql.functions import unix_timestamp, from_unixtime, current_date, datediff, col, dayofmonth, year, month
import pandas as pd

class DateTimeTransformer:

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

            df = self._df.select(columns[i], from_unixtime(unix_timestamp(columns[i], 'MM/dd/yyy')).alias('with time'))
            df=df.withColumn('without time', df['with time'].cast(DateType()))
            df=df.withColumnRenamed(columns[i], 'tmp')
            self._df = self._df.join(df, col(columns[i])==col('tmp'), 'leftouter').drop('tmp')    

            if stripTime:
                if Inplace: #strip time and convert in place    
                    self._df = self._df.withColumnRenamed('without time', new_col[i]).drop('with time').drop(columns[i])
                else: #strip time and keep the old column
                    self._df = self._df.withColumnRenamed('without time', new_col[i]).drop('with time')
            else:
                if Inplace:
                    self._df = self._df.withColumnRenamed('with time', new_col[i]).drop('without time').drop(columns[i])
                else:
                    self._df = self._df.withColumnRenamed('with time', new_col[i]).drop('without time')

        return self


    def age_calculator(self, columns, age_colname):
        """
        @columns: a string of column name
        @age_colname: a string of the new column of age, e.g.'age'
        """
        assert isinstance(columns, str), "Error: columns argument must be a string!"
        assert isinstance(age_colname, str), "Error: the name for age column argument must be a string!"

        df = self._df.select(columns, from_unixtime(unix_timestamp(columns, 'MM/dd/yyy')).alias('with time'))
        df=df.withColumn('without time', df['with time'].cast(DateType()))
        df=df.withColumnRenamed(columns, 'tmp')
        df=df.withColumn(age_colname, ((datediff(current_date(), col('without time')).cast(FloatType()))/365).cast(IntegerType()))
        self._df = self._df.join(df, col(columns)==col('tmp'), 'leftouter').drop('tmp').drop('with time').drop('without time')

        return self


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


        df = self._df.select(columns, from_unixtime(unix_timestamp(columns, 'MM/dd/yyy')).alias('with time'))
        df=df.withColumn('without time', df['with time'].cast(DateType()))
        df=df.withColumnRenamed(columns, 'tmp')
        self._df = self._df.join(df, col(columns)==col('tmp'), 'leftouter').drop('tmp')   
         

        return self