from pyspark.ml.feature import MaxAbsScaler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import Normalizer
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf
from pyspark.ml.linalg import DenseVector
import pyspark.sql.dataframe

class scaler():
    
    def __init__(self,df):
        '''
        df: pyspark dataframe to analyze
        '''
        assert (isinstance(df, pyspark.sql.dataframe.DataFrame)), "input type should be 'pyspark.sql.dataframe.DataFrame'"
        self._df = df
        
    def maxabs_scale(self,columns='*'):
        '''
        rescale the columns by dividing by the max absolute value
        '''
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"
            
        for column in columns:
            outputcol = column + '_scaled'
            assembler = VectorAssembler(inputCols=[column],outputCol='features')
            df = assembler.transform(self._df)
            scaler = MaxAbsScaler(inputCol='features',outputCol=outputcol)
            df = scaler.fit(df).transform(df).drop('features')
            to_float = udf(lambda x:float(x[0]))
            self._df = df.withColumn(outputcol,to_float(outputcol))
        return self._df
    
    def minmax_scale(self,columns='*',Min=0.0,Max=1.0):
        '''
        rescale the columns to range [min,max]
        Rescaled(e_i) = (e_i - E_min) / (E_max - E_min) * (max - min) + min
        '''
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"
            
        assert isinstance(Min,(float,int)), "Error: Min must be numerical"
        assert isinstance(Max,(float,int)), "Error: Max must be numerical"
            
        for column in columns:
            outputcol = column + '_scaled'
            assembler = VectorAssembler(inputCols=[column],outputCol='features')
            df = assembler.transform(self._df)
            scaler = MinMaxScaler(inputCol='features',outputCol=outputcol)
            scaler.setMax(Max).setMin(Min)
            df = scaler.fit(df).transform(df).drop('features')
            to_float = udf(lambda x:float(x[0]))
            self._df = df.withColumn(outputcol,to_float(outputcol))
        return self._df
    
    def standard_scale(self,columns='*'):
        '''
        rescale the columns by removing the mean and scaling to unit variance 
        '''
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"
            
        for column in columns:
            outputcol = column + '_scaled'
            assembler = VectorAssembler(inputCols=[column],outputCol='features')
            df = assembler.transform(self._df)
            scaler = StandardScaler(inputCol='features',outputCol=outputcol)
            df = scaler.fit(df).transform(df).drop('features')
            to_float = udf(lambda x:float(x[0]))
            self._df = df.withColumn(outputcol,to_float(outputcol))
        return self._df