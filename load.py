from pyspark.sql import DataFrame

#batch_df is a dataframe automatically created by spark and has a batch_id
def write_to_cassandra(batch_df: DataFrame, batch_id: int):
    (
        batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(keyspace = "cryptoproject", table = 'ohlc')
        .save()
    )

#write realtime events into cassandra realtime_table
def write_to_cassandra_realtime(batch_df: DataFrame, batch_id: int):
    (
        batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(keyspace = "cryptoproject", table = "realtime")
        .save()
    )

#df will be passed from spark.py
def write_stream_to_cassandra(df: DataFrame):
    query = (
        df.writeStream
        .foreachbatch(write_to_cassandra) #tells spark that everytime it finishes processing a micro-batch to call the function "write to cassandra"
        .outputMode("append")
        .start()
    )
    
    return query