from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from load import write_stream_to_cassandra

#create a spark application named crypto and load the kafka-connector
spark = SparkSession.builder.appName("crypto").getOrCreate()

#readstream tell spark to use structured streaming instead of onetime batch read
ohlc_stream_df = (
    spark.readStream
    .format("kafka") #defining the type of source spark is reading from
    .option("kafka.bootstrap.servers", "kafka:9092") #tells spark how to connect to kafka i.e(Bootstrap-service, server address)
    .option("subscribe", "test.public.kraken_ohlc") #tells spark which topic to consume (can be multiple)
    .option("startingOffsets", "earliest") #tells spark to read from begining of the topic
    .load() #tells spark to create a streaming dataframe with all the options provided
)

#At this point, nothing is created yet, spark only builds the executing plan and reading begins only when a streaming query is executed

#To see the data, convert the kafka messages from binary to string
#ohlc_df = ohlc_stream_df.select(col("value").cast("string").alias("message"))

'''
An example of the json being received from Kafka
|{"before":null,"after":{"id":1,"timestamp":"2026-06-27T13:58:00.152960Z","data":"{\"error\": [], \"result\": {\"last\": 1782518400, \"0GUSD\": [[1782518400, \"0.217\", \"0.217\", \"0.212\", \"0.215\", \"0.214\", \"14475.54376\", 59], [1782518400, \"0.217\", \"0.217\", \"0.212\", \"0.215\", \"0.214\", \"14475.54376\", 59]]}}"},"source":{"version":"3.1.3.Final","connector":"postgresql","name":"test","ts_ms":1782942470192,"snapshot":"first_in_data_collection","db":"postgres_db","sequence":"[null,\"83222960\"]","ts_us":1782942470192970,"ts_ns":1782942470192970000,"schema":"public","table":"kraken_ohlc","txId":95951,"lsn":83222960,"xmin":null},"transaction":null,"op":"r","ts_ms":1782942470445,"ts_us":1782942470445730,"ts_ns":1782942470445730928}             |
The Kafka topic contains Debezium CDC events and not the actual OHLC records.
They are having two levels of JSON (Outer Debezium JSON and Inner JSON stored as a string after.data)
We will need to parse the JSON twice
Below is the schema for the json
'''
#'after' keyword in events schema
after_schema = StructType([
    StructField("id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("data", StringType())
])

#the outer debezium event
debezium_schema = StructType([
    StructField("before", StringType()),
    StructField("after", after_schema),
    StructField("op", StringType())
])

#parse the debezium json
debezium_df = (
    ohlc_stream_df
    #kafka stores messages as bytes but spark cannot parse bytes as JSON. Convert the bytes to String and rename resulting column to json
    .selectExpr("CAST(value AS STRING) AS json")
    #convert the created JSON string into a spark struct (spark sql column) called record
    .select(from_json(col("json"), debezium_schema).alias("record"))
    #ignore delete events because they have 'after:null'
    .filter(col("record.after").isNotNull())
    #expands id, timestamp and data into individual columns
    .select("record.after.*")
)

# Extract the trading pair name using a regular expression to find the pair name
''' Example output:
"result": {"last": 1782518400, "0GUSD": [[1782518400, "0.217", "0.217", "0.212", "0.215", "0.214", "14475.54376", 59], 0.217", "0.212", "0.215", "0.214", "14475.54376", 59]]}
'''
pair_df = debezium_df.withColumn(
    "symbol",
    regexp_extract(
        col("data"),
        r'"result"\s*:\s*\{"last"\s*:\s*[^,]+,\s*"([^"]+)"',
        1
    )
)

# spark builds $.result.symbol and extracts only the OHLC array for that symbol 
pair_df = pair_df.withColumn(
    "ohlc_json",
    expr("get_json_object(data, concat('$.result.', symbol))")
)

# Parse the OHLC JSON. The JSON contains Array, Array and then Values
''' Example: [[1782518400, "0.217", "0.217", "0.212", "0.215", "0.214", "14475.54376", 59], 0.217", "0.212", "0.215", "0.214", "14475.54376", 59]] '''

ohlc_schema = ArrayType(ArrayType(StringType()))

#convert the json string into a spark array
pair_df = pair_df.withColumn(
    "ohlc",
    from_json(col("ohlc_json"), ohlc_schema)
)

# Flatten first candle
#For each symbol get the OHLC values from the first event from the array

final_ohlc_df = pair_df.select(
    col("symbol"),
    to_timestamp(from_unixtime(col("ohlc")[0][0].cast("long"))).alias("candle_time"), #converts the candle time to timestamp from unix format
    col("ohlc")[0][1].cast("double").alias("open"),
    col("ohlc")[0][2].cast("double").alias("high"),
    col("ohlc")[0][3].cast("double").alias("low"),
    col("ohlc")[0][4].cast("double").alias("close"),
    col("ohlc")[0][5].cast("double").alias("vwap"),
    col("ohlc")[0][6].cast("double").alias("volume"),
    col("ohlc")[0][7].cast("int").alias("trades"),
    to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").alias("ingestion_time") #convert timestamp to data type timestamp
)

# Write Stream

console_query = (
    final_ohlc_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query = write_stream_to_cassandra(final_ohlc_df)

query.awaitTermination()