from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import mean
from pyspark.sql.functions import abs as abs_spark
import pyspark.sql.dataframe

def median(df, column):
    return df.approxQuantile(column, [0.5], 0.01)[0]

class outlier:
    '''
    find and delete outliers for pyspark dataframes.
    '''
    def __init__(self,df):
        '''
        df: pyspark dataframe to analyze
        '''
        assert (isinstance(df, pyspark.sql.dataframe.DataFrame)), "input type should be 'pyspark.sql.dataframe.DataFrame'"
        self._df = df
        
         
    def mad(self,columns='*'):
        '''
        return the median absolute deviation of the column
        column: column in dataframe to analyze
        '''
        df = self._df
        
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!," + str(type(columns))
        
        mad = []
        for column in columns:
        
            # compute absolute deviation  
            abs_dev = (df.select(column).orderBy(column)
                       .withColumn(column, abs_spark(col(column) - median(df,column))).cache())
        
            # compute median absolute deviation
            mad.append(median(abs_dev,column))
        
        return mad
    
    def is_outlier(self,columns='*',method='mad',threshold=3.5,a=-100,b=100):
        '''
        get the outlier values which are out of our desired range.
        method: method used to detect outliers: mad-based test or user's specific requirement
        threshold: z-score threshold when we use mad-based test for outliers
        a: lower bound of the interval user requires
        b: upper bound of the interval user requires
        '''
        df = self._df
        
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"
        
        assert isinstance(a,(float,int)), "Error: a must be numerical"
        assert isinstance(b,(float,int)), "Error: b must be numerical"
        assert isinstance(threshold,(float,int)),"Error: threshold must be numerical"
        
        outliers = []
        intervals = []
        mad_value = self.mad(columns)
            
        for i in range(len(columns)):
            # compute the lower and upper bound of valid interval when method = 'mad'
            mad_a = median(df,columns[i]) - threshold * mad_value[i] / 0.6745
            mad_b = median(df,columns[i]) + threshold * mad_value[i] / 0.6745
        
            if method == 'mad':
                interval = [mad_a, mad_b]
            elif method == 'range':
                interval = [a,b]
            else:
                raise ValueError("method must be 'mad' or 'range'")
        
            # if value is not in valid interval, it's an outlier
            outliers.append(list(df.rdd.map(lambda x:x[columns[i]]).filter(lambda x:x<interval[0] or x>interval[1]).collect()))
            intervals.append(interval)
            
        return outliers, intervals
    
    def delete_outlier(self,columns="*",method='mad',threshold=3.5,a=-100,b=100):
        '''
        delete all the rows with outliers
        '''
        df = self._df
        
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"
        
        # valid interval
        intervals = self.is_outlier(columns,method,threshold,a,b)[1]
        
        for i in range(len(columns)):
            # delete values not in the interval
            f = lambda x:(x>=intervals[i][0]) & (x<=intervals[i][1])
            df = df.filter(f(col(columns[i])))
        self._df = df
        return self._df
    
    def replace_outlier(self,columns,method='mad',replacement='median',threshold=3.5,a=-100,b=100):
        '''
        replace all the outliers with mean/median value of the remain values
        replacement: value used to replace the outliers: mean/median value
        '''
        df = self._df
        
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"
        
        # obtain a list of outlier value
        values = self.is_outlier(columns,method,threshold,a,b)[0]
        
        # obtain a new dataframe without outlier and compute the mean of column
        df_new = self.delete_outlier(columns)
        
        for i in range(len(columns)):
            average = df_new.agg(mean(df_new[columns[i]]).alias("mean")).collect()[0]["mean"]
            # repalce outlier with different values
            if replacement == 'median':
                df = df.replace(values[i],[median(df_new,columns[i])]*len(values[i]),columns[i])
            elif replacement == 'mean':
                df = df.replace(values[i],[average]*len(values[i]),columns[i])
            else:
                raise ValueError("replacement must be 'median' or 'mean'")
                
        self._df = df
        return self._df      