import pyspark.sql.dataframe
import pyspark.sql.functions
import unicodedata
import re
from pyspark.sql.functions import col, udf, trim, lit, format_number, months_between, date_format, unix_timestamp, \
    current_date, abs as mag
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, ArrayType
import string
# this work is based on open resoruce optimus. Please see: https://github.com/ironmussa/Optimus

class DataClean:

    def __init__(self, df):
        assert (isinstance(df, pyspark.sql.dataframe.DataFrame)), "input type should be 'pyspark.sql.dataframe.DataFrame'"

        self._df = df
##################################################

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
