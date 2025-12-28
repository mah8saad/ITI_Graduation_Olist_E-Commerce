from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, count as _count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, IntegerType

# ------------------ SPARK SESSION ------------------
spark = SparkSession.builder \
    .appName("ShipmentEventsConsumer") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# ------------------ KAFKA SCHEMA ------------------
payload_after_schema = StructType([
    StructField("shipment_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_country", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("order_amount", DecimalType(18, 2), True),
    StructField("shipment_status", StringType(), True),
    StructField("estimated_delivery_ts", TimestampType(), True),
    StructField("actual_delivery_ts", TimestampType(), True),
    StructField("event_ts", TimestampType(), True)
])

schema = StructType([
    StructField("payload", StructType([
        StructField("after", payload_after_schema, True)
    ]), True)
])



# ------------------ READ STREAM FROM KAFKA ------------------
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "cdc.public.shipment_events") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert value from binary to string and parse JSON  ## CAST(value AS STRING) as value
events_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.payload.after.*")

# ------------------ SIMPLE AGGREGATION ------------------
agg_df = events_df.groupBy("city") \
    .agg(
        _sum("amount").alias("total_amount"),
        _count("transaction_id").alias("total_orders")
    )

# ------------------ WRITE TO CASSANDRA ------------------
query = agg_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "olist_delivery") \
    .option("table", "shipment_agg") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoints/shipment_agg") \
    .start()

query.awaitTermination()
