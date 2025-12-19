## Spark Streaming — Kafka to Spark Pipeline (Introduction Lab)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    """
    Main entry point of the Spark Streaming application.
    """

    # TODO 2: Create a SparkSession
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()

    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    print("✓ Spark Session Created Successfully")
    print(f"Spark Version: {spark.version}")


    # TODO 3: Define Kafka connection parameters
    # Use "kafka:29092" when running inside Docker container
    # Use "localhost:9092" when running from host machine
    kafka_bootstrap_servers = "kafka:29092"
    kafka_topic = "transactions"

    print(f"✓ Kafka Configuration: {kafka_bootstrap_servers}, Topic: {kafka_topic}")


    # TODO 4: Read data from Kafka as a streaming DataFrame
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    print("✓ Connected to Kafka stream")


    # TODO 5: Inspect the schema of the streaming DataFrame
    print("\nKafka DataFrame Schema:")
    kafka_df.printSchema()
    # Expected columns: key, value, topic, partition, offset, timestamp, timestampType


    # TODO 6: Convert the Kafka message value from bytes to string
    # Define the schema for the JSON data
    transaction_schema = StructType([
        StructField("transaction_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Parse the JSON from Kafka value
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), transaction_schema).alias("data")) \
        .select("data.*")

    print("✓ Kafka messages parsed to structured DataFrame")
    print("\nParsed DataFrame Schema:")
    parsed_df.printSchema()


    # TODO 7: Apply a simple transformation
    filtered_df = parsed_df.filter(col("amount") > 300)

    print("✓ Transformation applied: filtering amount > 300")


    # TODO 8: Write the streaming output
    # Option 1: Console output (for debugging)
    console_query = filtered_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    print("✓ Streaming query started - writing to console")

    # Option 2: Memory sink (for testing/inspection)
    # memory_query = filtered_df.writeStream \
    #     .outputMode("append") \
    #     .format("memory") \
    #     .queryName("transactions_table") \
    #     .option("checkpointLocation", "/tmp/checkpoint/memory") \
    #     .start()

    # Option 3: File sink (Parquet format)
    # file_query = filtered_df.writeStream \
    #     .outputMode("append") \
    #     .format("parquet") \
    #     .option("path", "/tmp/output/transactions") \
    #     .option("checkpointLocation", "/tmp/checkpoint/file") \
    #     .start()


    # TODO 9: Keep the streaming query running
    print("\n🚀 Streaming application is running...")
    print("Press Ctrl+C to stop\n")

    console_query.awaitTermination()
    #memory_query.awaitTermination()
    #file_query.awaitTermination()
    # If using multiple queries:
    # spark.streams.awaitAnyTermination()


    # TODO 10: Gracefully stop the Spark session
    # This will be called when you interrupt the program
    # spark.stop()
    # print("✓ Spark session stopped")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n✓ Streaming application stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
