import pyspark

from pyspark import SparkContext, SparkConf
sc = SparkContext(master = 'local[*]')

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Student Pyspark Example").getOrCreate()

df = spark.read.option("header",True).csv('/Users/deepak/Downloads/sample.csv')

df.show()