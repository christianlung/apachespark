import pyspark
import re

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType 
from pyspark.sql.functions import col, desc

# Required for spark-submit
sc = SparkContext("local", "TitlePairs")
spark = SparkSession(sc)

# A regular expression for parsing data from part1.dat
TUPLE = re.compile(r'\(\((\d+), (\d+)\), (\d+)\)')

titles_schema = StructType() \
    .add("book_id", IntegerType(), True) \
    .add("title", StringType(), True)

# A function that creates DataFrame rows from a line of text from
# part1.dat.
def parse_tuple(t: str) -> pyspark.sql.Row:
    book1, book2, frequency = TUPLE.search(t).groups()
    return pyspark.sql.Row(book1=int(book1),
              book2=int(book2),
              frequency=int(frequency))

# Read in the tab-delimited goodreads_titles.dat file.
titles_df = spark.read.format("csv") \
        .option("delimiter", "\t") \
        .option("header", "true") \
        .schema(titles_schema) \
        .load("/home/cs143/data/goodreads_titles.dat")


# Read in the file into an RDD and apply parse_tuple over the entire
# RDD.
part1_df = spark.createDataFrame(sc.textFile("/home/cs143/data/part1.dat").map(parse_tuple))
partial_join_df = part1_df.join(titles_df, col('book1') == col('book_id')) \
                            .select('title', 'book2', 'frequency') \
                            .withColumnRenamed('title', 'book1_title')
title_pairs_df = partial_join_df.join(titles_df, col('book2') == col('book_id')) \
                            .select('book1_title', 'title', 'frequency') \
                            .withColumnRenamed('title', 'book2_title')

title_pairs = title_pairs_df.sort(desc('frequency'), 'book1_title', 'book2_title')
title_pairs.toDF("book1_title", "book2_title", "frequency").write.csv("/home/cs143/output2", sep="\t")