#This file makes tell spark submit to wait for 60 seconds after confirming that spark master is up.
# The it executes the command to submit spark.py that creates a consumer, subscribes and start listening to messages on the defined kafka topic

#!/bin/sh

echo "Waiting for Spark Master..."

until curl -s http://spark-master:8080/json >/dev/null
do
    sleep 60
done

echo "Spark Master is ready."

exec /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.jars.ivy=/tmp/ivy \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 \
    /opt/spark/work-dir/spark.py
