from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import mean
from pyspark.sql.functions import abs as abs_spark
from pyspark.sql import SQLContext

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
        
         
    def mad(df,column):
        '''
        return the median absolute deviation of the column
        column: column in dataframe to analyze
        '''
        # compute absolute deviation  
        abs_dev = (df.select(column).orderBy(column)
                   .withColumn(column, abs_spark(col(column) - median(df,column))).cache())
        
        # compute median absolute deviation
        mad = median(abs_dev,column)
        
        return mad
    
    def is_outlier(df,column,method='mad',threshold=3.5,a=-100,b=100):
        '''
        get the outlier values which are out of our desired range.
        method: method used to detect outliers: mad-based test or user's specific requirement
        threshold: z-score threshold when we use mad-based test for outliers
        a: lower bound of the interval user requires
        b: upper bound of the interval user requires
        '''
            
        assert isinstance(a,(float,int)), "Error: a must be numerical"
        assert isinstance(b,(float,int)), "Error: b must be numerical"
        assert isinstance(threshold,(float,int)),"Error: threshold must be numerical"
        
        df = self._df
        
        mad_value = mad(df,column)
        
        # compute the lower and upper bound of valid interval when method = 'mad'
        mad_a = median(df,column) - threshold * mad_value / 0.6745
        mad_b = median(df,column) + threshold * mad_value / 0.6745
        
        if method == 'mad':
            interval = [mad_a, mad_b]
        elif method == 'range':
            interval = [a,b]
        else:
            raise ValueError("method must be 'mad' or 'range'")
        
        # if value is not in valid interval, it's an outlier
        outliers = list(df.rdd.map(lambda x:x[column]).filter(lambda x:x<interval[0] or x>interval[1]).collect())
        return outliers, interval
    
    def delete_outlier(df,column,method='mad',threshold=3.5,a=-100,b=100):
        '''
        delete all the rows with outliers
        '''
        df = self._df
        
        # valid interval
        interval = is_outlier(df,column,method,threshold,a,b)[1]
        
        # delete values not in the interval
        f = lambda x:(x>=interval[0]) & (x<=interval[1])
        df = df.filter(f(col(column)))
        
        return df
    
    def replace_outlier(df,column,method='mad',replacement='median',threshold=3.5,a=-100,b=100):
        '''
        replace all the outliers with mean/median value of the remain values
        replacement: value used to replace the outliers: mean/median value
        '''
        
        # obtain a list of outlier value
        value = is_outlier(df,column,method,threshold,a,b)[0]
        
        # obtain a new dataframe without outlier and compute the mean of column
        df_new = delete_outlier(df,column)
        average = df_new.agg(mean(df_new[column]).alias("mean")).collect()[0]["mean"]
        
        # repalce outlier with different values
        if replacement == 'median':
            df = df.replace(value,[median(df_new,column)]*len(value),column)
        elif replacement == 'mean':
            df = df.replace(value,[average]*len(value),column)
        else:
            raise ValueError("replacement must be 'median' or 'mean'")
        return df      