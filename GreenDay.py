import pyspark.sql.dataframe
from pyspark.ml.feature import Imputer
from pyspark.sql.types import FloatType, DateType, IntegerType
from pyspark.sql.functions import unix_timestamp, from_unixtime, current_date, datediff, col, dayofmonth, year, month
import pandas as pd


####################################################################################################################################
# create an object for dealing with missing values             
####################################################################################################################################

class MissingValue:

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

    def na_percent(self, columns="*"):

        """
        show a summary of the percentage 
        of missing value in specified columns by pd.DataFrame
        """
        
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"
    
        percentage = []
        for col in columns:
            na_num = self._df.filter(self._df[col].isNull()).count()
            non_na_num = self._df.filter(self._df[col].isNotNull()).count()
            percentage.append(na_num/(na_num+non_na_num))
            #print("{} has {} missing values".format(col,na_num))

        percentage = [format(x, '.0%') for x in percentage]

        na_percent_df = {'column name': columns, 'missing value percent': percentage}
        na_percent_df = pd.DataFrame(na_percent_df)
        print(na_percent_df)

        return self


    def drop_row(self, columns="*"):

        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"

        self._df = self._df.dropna(subset=columns)

        return self


    def drop_col(self, columns="*"):

        if columns == "*":
            columns = self._df.schema.names
        elif isinstance(columns, str):
            columns = [columns]
        else:
            assert isinstance(columns, list), "Error: columns argument must be a string or a list!"

        for col in columns:
            self._df = self._df.drop(col)

        return self


    def replace_na_value(self, value, columns="*"):

        if columns == "*":
            columns = self._df.schema.names
        elif isinstance(columns, str):
            columns = [columns]
        else:
            assert isinstance(columns, list), "Error: columns argument must be a string or a list!"

        if isinstance(value, str) or isinstance(value, float) or isinstance(value, int):
            value=[value]
        else:
            assert isinstance(value, list), "Error: value argument must be a list!"
        
        assert len(columns) == len (value), "Error: inconsistent lengths for argument of columns list and value list"

        for item in value:
            assert isinstance(item, (int, float, str, dict)), "Error: items in value argument can only be an int, long, float, string!"

        col_val_dic = {}
        for i in range(len(columns)):
            col_val_dic[columns[i]] = value[i]

        self._df = self._df.na.fill(col_val_dic)

        return self


    
    def replace_value(self, na_value, replace_value, columns="*"):

        """
        @na_value: missing value equivalent, e.g. "999", "99"
        @replace_value: value used to replace the na_value
        """

        if columns == "*":
            columns = self._df.schema.names
        elif isinstance(columns, str):
            columns = [columns]
        else:
            assert isinstance(columns, list), "Error: columns argument must be a string or a list!"

        assert isinstance(replace_value, str) or isinstance(replace_value, float) or isinstance(replace_value, int), "Error: Only scalar and string can be used to replace old values!"
        assert type(replace_value) == type(na_value), "Error: inconsistent datatype of na_value and replace_value parameters!"      

        self._df = self._df.replace(na_value, replace_value, subset=columns)

        return self



    def na_imputer(self, strategy, out_columns="*", na=None, columns="*"):

        """
        replace missing value with mean or median according to users' choice
        user can also customize the definition of missing value, e.g. 999
        the default missing value is 'nan' or null
        the default setting of out_columns is just columns, so the original columns will be overrided if not specially defined
        """

        #check columns
        if columns == "*":
            columns = self._df.schema.names
        elif isinstance(columns, str):
            columns = [columns]
        else:
            assert isinstance(columns, list), "Error: columns argument must be a string or a list!"

        if out_columns == "*":
            out_columns = self._df.schema.names

        #check output columns
        if isinstance(out_columns, str):
            out_columns = [out_columns]
        else:
            assert isinstance(out_columns, list), "Error: output columns argument must be a string or a list!"

        #check input and output columns have consistent lengths
        assert len(columns) == len (out_columns), "Error: inconsistent lengths for argument of columns list and output columns list"

        #check strategy argument
        assert (strategy == "mean" or strategy == "median"), "Error: strategy can only be 'mean' or 'median'."

        #firstly convert the type in input columns to FloatType for Imputer
        for col in columns:
            self._df = self._df.withColumn(col, self._df[col].cast(FloatType()))

        #fit the model
        imputer = Imputer(inputCols=columns, outputCols=out_columns)

        if na is None:
            model = imputer.setStrategy(strategy).fit(self._df)
        else:
            model = imputer.setStrategy(strategy).setMissingValue(na).fit(self._df)
        
        self._df = model.transform(self._df)

        return self


####################################################################################################################################
# create another object for dealing with date and time                       
####################################################################################################################################

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

        return self


    def age_calculator(self, columns, age_colname):
        """
        @columns: a string of column name
        @age_colname: a string of the new column of age, e.g.'age'
        """
        assert isinstance(columns, str), "Error: columns argument must be a string!"
        assert isinstance(age_colname, str), "Error: the name for age column argument must be a string!"

        self._df = (self.str_to_Date(columns, columns, stripTime=True, Inplace=True))._df
        self._df = self._df.withColumn(age_colname, ((datediff(current_date(), col(columns)).cast(FloatType()))/365).cast(IntegerType()))
        
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

        return self





