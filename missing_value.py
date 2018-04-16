def na_cleaner(df, columns="*", percent=False, replace=False, value=None, drop_row=False, drop_col=False):

    if columns == "*":
        columns = df.schema.names
    else:
        assert isinstance(columns, list), "Error: columns argument must be a list!"

    #show a summary of the percentage of missing value in specified columns in pd.DataFrame
    if percent is True:
        percentage = []
        for col in columns:
            na_num = df.filter(df[col].isNull()).count()
            non_na_num = df.filter(df[col].isNotNull()).count()
            percentage.append(na_num/(na_num+non_na_num))
            #print("{} has {} missing values".format(col,na_num))

        percentage = [format(x, '.0%') for x in percentage]
        
        import pandas as pd
        na_percent_df = {'column name': columns, 'missing value percent': percentage}
        na_percent_df = pd.DataFrame(na_percent_df)
        print(na_percent_df)

    #replace missing value with specified value, deal with multi-columns
    if replace is True:
        assert isinstance(value, list), "Error: value argument must be a list!"
        assert len(columns) != len (value), "Error: inconsistent lengths for argument of columns list and value list"

        for item in value:
            assert isinstance(item, (int, float, str, dict)), "Error: items in value argument can only be an int, long, float, string!"

        col_val_dic = {}
        for i in range(len(columns)):
            col_val_dic[columns[i]] = value[i]

        df = df.na.fill(col_val_dic)

    #drop the row according to 
    if drop_row is True:
        df = df.dropna(subset=columns)

    if drop_col is True:
        for col in columns:
            df = df.drop(col)
        #df = df.drop('PIN','SectionName')

    return df    


#["EventCity","EventStateCode","EventZipCode"]
















