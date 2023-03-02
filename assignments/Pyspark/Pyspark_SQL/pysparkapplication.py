from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
from pyspark.sql.functions import *

Bank_DF = spark.read.format("csv").load("/mnt/c/Users/miles.MILE-BL-4409-LA/futurense_hadoop-pyspark/labs/dataset/bankmarket/bankmarketdata.csv", header=True, sep = ";", escape = ",", inferSchema = True )

output=spark.sql("Select age,count(age) from Bank where y='yes' group by age")

output.write.parquet("hdfs://localhost:9000/user/training/bankmarketing/out/parquet")

data=spark.read.parquet("hdfs://localhost:9000/user/training/bankmarketing/out/parquet/part-00000-9b27639c-8190-405c-a9f3-79fc17fcc482-c000.snappy.parquet")

data.show()