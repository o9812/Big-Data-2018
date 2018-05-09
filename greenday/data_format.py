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


class data_format:
    """
    initial datafram to data_format class
    """

    def __init__(self, df):
        # initial dataframe
        self._df = df
##################################################

    def show(self):
        # incase user would like to see the dataframe
        self._df.show()
##################################################

    def clean_latin(self, columns='*'):
        """
        input:
            @columns: a list or a string of the column(s) name in original dataframe, the columns needs to be converted

        output:
            @sefl.dataframe: a clean dataframe without latin words
        # this function is based on open resoruce optimus. Please see: https://github.com/ironmussa/Optimus
        """

        # input a list of column
        valid_cols = [col for (col, typ) in filter(lambda typ: typ[1] == 'string', self._df.dtypes)]

        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"

        def remove_latin(input_str):
            # first, normalize strings:
            nfkd_str = unicodedata.normalize('NFKD', input_str)
            # join the list and remove the latin words
            with_out_latin = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])
            return with_out_latin
        # def the function to remove latin words
        function = udf(lambda x: remove_latin(x) if x is not None else x, StringType())
        # apply function on column, if it is not a validated column, just return its self
        exprs = [function(col(c)).alias(c) if (c in columns) and (c in valid_cols) else c for c in self._df.columns]

        self._df = self._df.select(*exprs)
        return self

##################################################
    def clean_sp_char(self, columns='*'):
        """
        input:
            @columns: a list or a string of the column(s) name in original dataframe, the columns needs to be converted
        output:
            @sefl.dataframe: a clean dataframe without punctuations
        # this function is based on open resoruce optimus. Please see: https://github.com/ironmussa/Optimus
        """

        # input a list of column, remove specila char each column

        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"

        def remove_sp_char(input_str):
            # Remove all punctuation and control characters
            for punct in (set(input_str) & set(string.punctuation)):
                input_str = input_str.replace(punct, "")
                input_str = re.sub('<span class=love>.*?</span>', '', input_str)
            return input_str

        # User define function that does operation in cells
        function = udf(lambda x: remove_sp_char(x) if x is not None else x, StringType())
        # apply function on column, if it is not a validated column, just return its self
        exprs = [function(c).alias(c) if (c in columns) and (c in valid_cols) else c for c in self._df.columns]

        self._df = self._df.select(*exprs)

        return self
##################################################

    def clean_html(self, columns='*'):
         """
        input:
            @columns: a list or a string of the column(s) name in original dataframe, the columns needs to be converted
        output:
            @sefl.dataframe: a clean dataframe without html tag
        """

        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"

        def remove_html(input_str):
            # Remove all punctuation and control characters
            for punct in set(input_str):
                input_str = re.sub("'<[^<]+?>'", '', input_str)
            return input_str

        # User define function that does operation in cells
        function = udf(lambda x: remove_html(x) if x is not None else x, StringType())
        # apply function on column, if it is not a validated column, just return its self
        exprs = [function(c).alias(c) if (c in columns) and (c in valid_cols) else c for c in self._df.columns]

        self._df = self._df.select(*exprs)

        return self
    ################################################

    def clean_rex(self, rex, columns='*'):
        """
        input
            @ a column list and remove special char column
            @ reg expression which is going to be removed
        this function is based on open resoruce optimus. Please see: https://github.com/ironmussa/Optimus
        """
        valid_cols = [c for (c, t) in filter(lambda t: t[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*":
            columns = self._df.schema.names
        else:
            assert isinstance(columns, list), "Error: columns argument must be a list!"
        # store the wrong type of column
        col_not_valids = (set([column for column in columns]).difference(set([column for column in valid_cols])))

        assert (
            col_not_valids == set()), 'Error: The following columns do not have same datatype argument provided: %s' \
                                      % col_not_valids

        def remove_rex(input_str, rex):
            for _ in set(input_str):
                input_str = re.sub(rex, '', input_str)
            return input_str
            # User define function that does operation in cells

        function = udf(lambda x: remove_rex(x, rex) if x is not None else x, StringType())

        exprs = [function(c).alias(c) if (c in columns) and (c in valid_cols) else c for c in self._df.columns]

        self._df = self._df.select(*exprs)

        return self

##################################################
# Clustering Part:
    def clustering(self, columns='*', num_cluster=2, n_g=2):
        """
        input
            @ a column list and remove special char column
            @ number of cluster
            @ number of n_gram
        return:
            @ a data frame with clustering column
        """

        # let text data as string with blank space
        n_gr = n_g
        data_frame_1 = self._df
        # check data type, if not string turn it into string
        valid_cols = [col for (col, typ) in filter(lambda typ: typ[1] == 'string', self._df.dtypes)]
        # turn it into string
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
        # make features like n-gram
        ngram = NGram(n=n_gr, inputCol=columns + "_token", outputCol=columns + "_ngram")
        ngramDataFrame = ngram.transform(data_frame_2)
        # vectorization: text map to vector
        cv = CountVectorizer(inputCol=columns + "_ngram", outputCol="features", vocabSize=10, minDF=1.0)
        # fit the vectorization
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
        #  replace the origin dataframe
        self._df = data_frame_replace
        #  show result to users
        self._df.show()
        return self

##################################################
