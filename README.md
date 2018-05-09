# Green Day
Green Day is a data cleaning tookit for Spark. It's developed via Python and takes pyspark DataFrame as input. 

## Features
* Missing Value
* Date Format
* Text Cleaning
* Clustering
* Outlier Detection
* Duplicative Removing
* Normalization & Scaling

## Prerequisites
```
PySpark 2.3.0
Python 3.6.5
```
## The way to use it

1. Upload the `greenday` folder to the hdfs and the directory you're working on. 
2. Import the package as `from greenday import *`
   Then you can use all the modules under Green Day toolkit.
1. The input must be a `pyspark DataFrame`
1. The default setting is the whole dataframe. Users can choose just a subset of all columns as input argument. 

***
### Missing Value
represent an overview of the percentage of missing values
```
>>> df.na_percent()
                     column name missing value percent
0                      RequestID                    0%
1                      StartDate                    0%
2                        EndDate                    0%
3                     AgencyName                    0%
4        TypeOfNoticeDescription                   81%
5            CategoryDescription                   84%
6                     ShortTitle                    0%
7     SelectionMethodDescription                   84%
8                    SectionName                    0%
9   SpecialCaseReasonDescription                   97%
10                           PIN                   84%
11                       DueDate                   91%
12              AddressToRequest                   91%
13                   ContactName                   91%
14                  ContactPhone                   91%
15                         Email                   95%
16                ContractAmount                   91%
17                    ContactFax                   97%
18        AdditionalDescription1                   15%
19       AdditionalDesctription2                  100%
20        AdditionalDescription3                  100%
```

impute missing values or its equivalent by mean or median
```
>>> df1.show()
+-------+----+------+------+-----+
|   Name| Age|Height|Weight|State|
+-------+----+------+------+-----+
|  Aaron|  18|   170|    77|   NY|
|    Bob|  45|   175|    59|   NJ|
|  Chris|null|   189|    88|   IL|
|    Dan|  28|   168|    99|   NY|
|  Evans|  17|  null|    99|  999|
|Francis|  33|  null|    65|  999|
| George|null|   190|    90|   NY|
+-------+----+------+------+-----+

>>> df1.na_imputer('median','Weight','99','Weight',Inplace=True).na_imputer('mean',['Age','Height'],None,['Age','Height'],True).show()
+-------+----+------+------+-----+
|   Name| Age|Height|Weight|State|
+-------+----+------+------+-----+
|  Aaron|  18|   170|    77|   NY|
|    Bob|  45|   175|    59|   NJ|
|  Chris|28.2|   189|    88|   IL|
|    Dan|  28|   168|    77|   NY|
|  Evans|  17| 178.4|    77|  999|
|Francis|  33| 178.4|    65|  999|
| George|28.2|   190|    90|   NY|
+-------+----+------+------+-----+
```
replace user-defined missing values, e.g.null or '999', etc.
```
>>> df2.show()
+-------+----+-----+
|   Name| Age|State|
+-------+----+-----+
|  Aaron|  18|   NY|
|    Bob|  45|   NJ|
|  Chris|null|   IL|
|    Dan|  28|   NY|
|  Evans|  17|  999|
|Francis|  33|  999|
| George|null|   NY|
+-------+----+-----+

>>> df2.replace_na_value(28.4, columns="Age").replace_value('999','NY',"State").show()
+-------+----+-----+
|   Name| Age|State|
+-------+----+-----+
|  Aaron|  18|   NY|
|    Bob|  45|   NJ|
|  Chris|28.4|   IL|
|    Dan|  28|   NY|
|  Evans|  17|   NY|
|Francis|  33|   NY|
| George|28.4|   NY|
+-------+----+-----+
```

### Date Format

convert String type to Date type as customized
```
>>> df3.show()
+----------+
|  date_str|
+----------+
|03/25/1991|
|11/24/2008|
|09/30/1989|
+----------+


>>> df3.str_to_Date('date_str', 'date', stripTime=False, Inplace=False).show()
+----------+-------------------+
|  date_str|               date|
+----------+-------------------+
|03/25/1991|1991-03-25 00:00:00|
|11/24/2008|2008-11-24 00:00:00|
|09/30/1989|1989-09-30 00:00:00|
+----------+-------------------+


>>> df3.str_to_Date('date_str', 'date', stripTime=True, Inplace=False).show()
+----------+----------+
|  date_str|      date|
+----------+----------+
|03/25/1991|1991-03-25|
|11/24/2008|2008-11-24|
|09/30/1989|1989-09-30|
+----------+----------+


>>> df3.str_to_Date('date_str', 'date', stripTime=True, Inplace=True).show()
+----------+
|      date|
+----------+
|1991-03-25|
|2008-11-24|
|1989-09-30|
+----------+


>>> df3.str_to_Date('date_str', 'date', stripTime=False, Inplace=True).show()
+-------------------+
|               date|
+-------------------+
|1991-03-25 00:00:00|
|2008-11-24 00:00:00|
|1989-09-30 00:00:00|
+-------------------+
```

Calculate the years until now
```
>>> df3.age_calculator('date_str', 'age').show()
+----------+---+
|  date_str|age|
+----------+---+
|1990-03-25| 28|
|2008-02-24| 10|
|1988-01-31| 30|
+----------+---+
```

Split the Date column into year/month/day (Int type)
```
>>> df1.Date_Spliter("date_str", "year", "month", "day").show()
+----------+----+-----+---+
|  date_str|year|month|day|
+----------+----+-----+---+
|1990-03-25|1990|    3| 25|
|2008-02-24|2008|    2| 24|
|1988-01-31|1988|    1| 31|
|2017-09-25|2017|    9| 25|
|1994-12-25|1994|   12| 25|
|2020-11-30|2020|   11| 30|
+----------+----+-----+---+
```
## Text Cleaning
- To using the text cleaning function, import from `Greenday` package
 > `from Greenday import data_format` . 
 > `tc = data_format(df)` . 

 The following table containing special characters and punctuations
```
+---------+-------+
|   cities|friends|
+---------+-------+
|   Bogotá|John##!|
| New York|  M@ãrk|
|São~Paulo|  Mãrry|
|  ~Madrid|  4$Tám|
+---------+-------+
```
- To clean all latin words through out the table, we done need to put any argument.
 > `tc.clean_clean_latin()`
 > `tc._df.show()`
```
+--------+-------+
|  cities|friends|
+--------+-------+
|  Bogota|John##!|
|New York|  M@ark|
|SaoPaulo| Marr!y|
|  Madrid|  4$Tam|
+--------+-------+
```
- To clean punctuations for certain column: 
> `tc.clean_clean_sp_char(["friends"])`
> `tc._df.show()`

We would get a clean table as following
```
+--------+-------+
|  cities|friends|
+--------+-------+
|  Bogota|   John|
|New York|   Mark|
|SaoPaulo|  Marry|
|  Madrid|   4Tam|
+--------+-------+
```

## Clustering
Here is the example of clustering, the following is origin table
```
+---+------+
|age|  name|
+---+------+
| 15|  John|
| 17|  John|
| 12|  Jahn|
| 16|Johnny|
| 15| Alice|
| 32| Alice|
| 32|  Alux|
| 32|  Alex|
| 39|  Lily|
+---+------+
```
Predict the group
```
+---+------+----------+
|age|  name|prediction|
+---+------+----------+
| 32|  Alux|         0|
| 32|  Alex|         0|
| 15|  John|         1|
| 17|  John|         1|
| 12|  Jahn|         1|
| 16|Johnny|         1|
| 32| Alice|         2|
| 15| Alice|         2|
| 39|  Lily|         3|
+---+------+----------+
```
Show the count number each group to user, so user can determine which words should be replaced
```
+------+----------+-----+
|  name|prediction|count|
+------+----------+-----+
|  Alex|         0|    1|
|  Alex|         0|    1|
|  John|         1|    2|
|  Jahn|         1|    1|
|Johnny|         1|    1|
| Alice|         2|    2|
|  Lily|         3|    1|
+------+----------+-----+
```
The result of replace the words
```
replace the name
+---+------+----------+------------+
|age|  name|prediction|replace_name|
+---+------+----------+------------+
| 32|  Alux|         0|        Alux|
| 32|  Alex|         0|        Alux|
| 15|  John|         1|        John|
| 17|  John|         1|        John|
| 12|  Jahn|         1|        John|
| 16|Johnny|         1|        John|
| 32| Alice|         2|       Alice|
| 15| Alice|         2|       Alice|
| 39|  Lily|         3|        Lily|
+---+------+----------+------------+
```
