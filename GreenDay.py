import pyspark.sql.dataframe
from pyspark.ml.feature import Imputer
from pyspark.sql.types import FloatType
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

    def 





















    






