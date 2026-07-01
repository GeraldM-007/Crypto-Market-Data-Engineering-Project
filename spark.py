from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("crypto").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0").getOrCreate()

#readstream tell spark to use structured streaming instead of onetime batch read
ohlc_stream_df = (
    spark.readStream
    .format("kafka") #defining the type of source spark is reading from
    .option("kafka.bootstrap.servers", "3.144.131.176:9092") #tells spark how to connect to kafka i.e(Bootstrap-service, server address)
    .option("subscribe", "test.public.kraken_ohlc") #tells spark which topic to consume (can be multiple)
    .option("startingOffsets", "earliest") #tells spark to read from begining of the topic
    .load() #tells spark to create a streaming dataframe with all the options provided
)

#At this point, nothing is spark only builds the executing plan and reading begins only when a streaming query is executed

#To see the data, convert the kafka messages from binary to string
ohlc_df = ohlc_stream_df.select(col("value").cast("string").alias("message"))

#Use a streaming query
query = (ohlc_df.writeStream.format("console").outputMode("append").option("truncate", False).start())

query.awaitTermination()