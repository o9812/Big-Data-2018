import pyspark.sql.dataframe
import pyspark.sql.functions
import unicodedata
import re
from pyspark.sql.functions import col, udf, trim, lit, format_number, months_between, date_format, unix_timestamp, \
    current_date, abs as mag
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, ArrayType
import string
##################################################
from pyspark.ml.clustering import LDA
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import udf
from pyspark.ml.feature import NGram
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.clustering import KMeans
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
##################################################

# this work is based on open resoruce optimus. Please see: https://github.com/ironmussa/Optimus


class DataClean:

    def __init__(self, df):
        assert (isinstance(df, pyspark.sql.dataframe.DataFrame)), "input type should be 'pyspark.sql.dataframe.DataFrame'"

        self._df = df
##################################################

    def show(self):
        self._df.show()

    def clean_accent(self, columns='*'):

        # if isinstance(columns, str):
        #     columns = [columns]
        valid_cols = [col for (col, typ) in filter(lambda typ: typ[1] == 'string', self._df.dtypes)]

        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"

        # col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

        # assert (col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' % col_not_valids

        # Receives  a string as an argument
        def remove_accents(input_str):
            # first, normalize strings:
            nfkd_str = unicodedata.normalize('NFKD', input_str)
            # Keep chars that has no other char combined (i.e. accents chars)
            with_out_accents = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])
            return with_out_accents

        function = udf(lambda x: remove_accents(x) if x is not None else x, StringType())
        exprs = [function(col(c)).alias(c) if (c in columns) and (c in valid_cols) else c for c in self._df.columns]
        self._df = self._df.select(*exprs)

        # Returning the transformer object for able chaining operations
        return self

##################################################
    def remove_special_chars(self, columns='*'):
        """This function remove special chars in string columns, such as: .!"#$%&/()
        :param columns      list of names columns to be processed.
        columns argument can be a string or a list of strings."""

        # Check if columns argument must be a string or list datatype:
        # self._assert_type_str_or_list(columns, "columns")

        # Filters all string columns in dataFrame
        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"

        # If columns is string, make a list:
        if isinstance(columns, str):
            columns = [columns]

        # Check if columns to be process are in dataframe
        # self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        # col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

        # assert (
        #     col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' \
        #                               % col_not_valids

        def rm_spec_chars(input_str):
            # Remove all punctuation and control characters
            for punct in (set(input_str) & set(string.punctuation)):
                input_str = input_str.replace(punct, "")
                input_str = re.sub('<span class=love>.*?</span>', '', input_str)
            return input_str

        # User define function that does operation in cells
        function = udf(lambda cell: rm_spec_chars(cell) if cell is not None else cell, StringType())

        exprs = [function(c).alias(c) if (c in columns) and (c in valid_cols) else c for c in self._df.columns]

        self._df = self._df.select(*exprs)

        # self._add_transformation()  # checkpoint in case
        return self
##################################################

    def remove_special_chars_regex(self, regex, columns='*'):
        """This function remove special chars in string columns using a regex, such as: .!"#$%&/()
        :param columns      list of names columns to be processed.
        :param regex        string that contains the regular expression
        columns argument can be a string or a list of strings."""

        # Check if columns argument must be a string or list datatype:
        # self._assert_type_str_or_list(columns, "columns")

        # Filters all string columns in dataFrame
        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"

        # Check if columns to be process are in dataframe
        self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

        assert (
            col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' \
                                      % col_not_valids

        def rm_spec_chars_regex(input_str, regex):
            for _ in set(input_str):
                input_str = re.sub(regex, '', input_str)
            return input_str
            # User define function that does operation in cells

        function = udf(lambda cell: rm_spec_chars_regex(cell, regex) if cell is not None else cell, StringType())

        exprs = [function(c).alias(c) if (c in columns) and (c in valid_cols) else c for c in self._df.columns]

        self._df = self._df.select(*exprs)

        # self._add_transformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self
##################################################

    def remove_duplicates(self, cols='*'):
        """
        :param cols: List of columns to make the comparison, this only  will consider this subset of columns,
        for dropping duplicates. The default behavior will only drop the identical rows.
        :return: Return a new DataFrame with duplicate rows removed
        """
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"

        self._df = self._df.drop_duplicates(cols)

        return self
##################################################

    def length_cut(self, columns='*'):

            # if isinstance(columns, str):
            #     columns = [columns]
        valid_cols = [col for (col, typ) in filter(lambda typ: typ[1] == 'string', self._df.dtypes)]

        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"

        # col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

        # assert (col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' % col_not_valids

        # Receives  a string as an argument
        def remove_accents(input_str):
            # first, normalize strings:
            nfkd_str = unicodedata.normalize('NFKD', input_str)
            # Keep chars that has no other char combined (i.e. accents chars)
            with_out_accents = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])
            return with_out_accents

        function = udf(lambda x: remove_accents(x) if x is not None else x, StringType())
        exprs = [function(col(c)).alias(c) if (c in columns) and (c in valid_cols) else c for c in self._df.columns]
        self._df = self._df.select(*exprs)

        # Returning the transformer object for able chaining operations
        return self


##################################################
# Clustering Part:

    def clustering(self, columns='*', num_cluster=2):
        # input data type should be string
        # let text data as string with blank space

        data_frame_1 = self._df
        # check data type, if not string turn it into string
        valid_cols = [col for (col, typ) in filter(lambda typ: typ[1] == 'string', self._df.dtypes)]
        if columns not in valid_cols:
            data_frame_1 = data_frame_1.withColumn(columns + '_', data_frame_1[columns].cast("string"))
            data_frame_1 = data_frame_1.drop(columns)
            data_frame_1 = data_frame_1.withColumnRenamed(columns + '_', columns)
        # make the string toknizable
        udf_space = udf(lambda z: " ".join(z))
        data_frame_1 = data_frame_1.withColumn(columns + '_split', udf_space(columns)).orderBy(columns)
        # Token the word and do the bi-gram
        tokenizer = Tokenizer(inputCol=columns + '_split', outputCol=columns + "_token")
        data_frame_2 = tokenizer.transform(data_frame_1)
        ngram = NGram(n=2, inputCol=columns + "_token", outputCol=columns + "_ngram")
        ngramDataFrame = ngram.transform(data_frame_2)
        # vectorization: text map to vector
        cv = CountVectorizer(inputCol=columns + "_ngram", outputCol="features", vocabSize=10, minDF=2.0)
        model = cv.fit(ngramDataFrame)
        result = model.transform(ngramDataFrame)
        # setup kmeans
        kmeans = KMeans().setK(num_cluster).setSeed(1)
        model_kmean = kmeans.fit(result)
        predictions_kmean = model_kmean.transform(result)

        df = predictions_kmean.orderBy('prediction', ascending=True).select(self._df.schema.names + ['prediction'])
        # reshape the table, user are easy to read it
        print('show each count numbers in this row')
        temp = df.groupBy(columns, 'prediction').count()
        temp = temp.withColumnRenamed('prediction', 'cluster')
        df = df.withColumnRenamed('prediction', 'cluster')
        temp = temp.withColumnRenamed('count', 'count in cluster')
        temp.show()
        # show the cluster number
        window = Window.partitionBy("cluster").orderBy(col("count in cluster").desc())
        test = (temp.withColumn('row_num', F.row_number().over(window)).where(F.col('row_num') == 1).select(columns, 'cluster'))
        print('Defult replace: replace the mode of a instance each cluster')
        test.orderBy('cluster', ascending=True).show()

        # turn the modest number to list
        test_list = test.select(columns).orderBy('cluster').collect()

        # name_list = [i.columns for i in test_list]
        name_list = [i[columns] for i in test_list]

        list_setting = input("Type 'yes' to enter customized replace words or press any key for default replace setting: \n")
        # let the usr defined the replaced word
        count = 0
        if list_setting == 'yes':
            count = 0
            while(count < num_cluster):
                usr_replace = input('Enter the cluster {0} shoud be, or press enter to skip: \n'.format(count))
                if usr_replace != '':
                    name_list[count] = usr_replace
                else:
                    name_list[count] = name_list[count]
                count += 1

        # replace the words
        udf_place_name = udf(lambda z: name_list[z])
        data_frame_replace = df.withColumn('replace_' + columns, udf_place_name('cluster'))

        replace_input = input('type yes to replace origin column, press any key to keep the origin column:\n')
        # change the origin dataframe
        if replace_input == 'yes':
            data_frame_replace = data_frame_replace.drop(columns)
            data_frame_replace = data_frame_replace.withColumnRenamed("replace_" + columns, columns)

        data_frame_replace = data_frame_replace.drop('cluster')
        self._df = data_frame_replace
        self._df.show()
        return self

##################################################
