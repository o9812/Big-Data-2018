from itertools import combinations

def read_col(self,column):
    '''
    read columns of the dataframe as list
    '''
    return list((self._df.rdd.map(lambda x: x['col1']).collect()))

def remove_duplicates_rows(self):
    '''
    remove duplicated rows if they are identical in every column
    '''
    self._df.drop_duplicates(inplace=True)
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
    return df