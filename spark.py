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

'''
An example of the json being received from Kafka
|{"before":null,"after":{"id":1,"timestamp":"2026-06-27T13:58:00.152960Z","data":"{\"error\": [], \"result\": {\"last\": 1782518400, \"0GUSD\": [[1782518400, \"0.217\", \"0.217\", \"0.212\", \"0.215\", \"0.214\", \"14475.54376\", 59], [1782518400, \"0.217\", \"0.217\", \"0.212\", \"0.215\", \"0.214\", \"14475.54376\", 59]]}}"},"source":{"version":"3.1.3.Final","connector":"postgresql","name":"test","ts_ms":1782942470192,"snapshot":"first_in_data_collection","db":"postgres_db","sequence":"[null,\"83222960\"]","ts_us":1782942470192970,"ts_ns":1782942470192970000,"schema":"public","table":"kraken_ohlc","txId":95951,"lsn":83222960,"xmin":null},"transaction":null,"op":"r","ts_ms":1782942470445,"ts_us":1782942470445730,"ts_ns":1782942470445730928}             |
The Kafka topic contains Debezium CDC events and not the actual OHLC records.
They are having two levels of JSON (Outer Debezium JSON and Inner JSON stored as a string after.data)
We will need to parse the JSON twice
Below is the schema for the json
'''
raw_ohlc_json_schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('timestamp', StringType(), True),
    StructField('data', StringType(), True)
])

debezium_json_schema = StructType([
    StructField('before', StringType(), True),
    StructType('after', StringType(), True),
    StructField('op', StringType(), True)
])

#Parse the kafka JSON inside the received events from Kafka
parsed_ohlc_df = (
    ohlc_df
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col('json'), debezium_json_schema).alias('record'))
    .select('record.after.*')
)


#Use a streaming query
query = (ohlc_df.writeStream.format("console").outputMode("append").option("truncate", False).start())

query.awaitTermination()