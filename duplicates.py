from itertools import combinations
import pyspark.sql.dataframe

class duplicates:
    
    def __init__(self,df):
        '''
        df: pyspark dataframe to analyze
        '''
        assert (isinstance(df, pyspark.sql.dataframe.DataFrame)), "input type should be 'pyspark.sql.dataframe.DataFrame'"
        self._df = df
    
    def read_col(self,columns):
        '''
        read columns of the dataframe as list
        '''
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"
            
        return [list((self._df.rdd.map(lambda x: x[column]).collect())) for column in columns]

    def remove_duplicates_rows(self):
        '''
        remove duplicated rows if they are identical in every column
        '''
        self._df = self._df.drop_duplicates()
        return self._df

    def remove_duplicates_cols(self):
        '''
        remove duplicated columns if they are identical in every row
        '''
        df = self._df
        permutations = list(combinations(df.columns, 2))
        columns_to_drop = set()
        for permutation in permutations:
            if df.filter(df[list(permutation)[0]] != df[list(permutation)[1]]).count()==0:
                columns_to_drop.add(permutation[1])
        df = df.select([c for c in df.columns if c not in columns_to_drop])
        self.df = df
        return self._df