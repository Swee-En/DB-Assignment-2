import sys
from pyspark.sql import SparkSession

# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW
df = spark.read.option("header", True)\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

df.createOrReplaceTempView("Temp_Table1")
df.createOrReplaceTempView("Temp_Table2")

Temp_Table1.filter(col("Price Range").isNotNull())\
    .groupBy("Price Range")\
        .agg({"Rating": "max"})

Temp_Table2.filter(col("Price Range").isNotNull())\
    .groupBy("Price Range")\
        .agg({"Rating": "min"})

t1.join(t2, ['Name'])\
    .groupBy("Price Range")\
    .write.csv("Q2_answer.csv")

