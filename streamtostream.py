from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, col, expr
from pyspark.sql.types import StructType, StructField, StringType


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Stream Stream Example") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()


    impressionSchema = StructType([
        StructField("InventoryID", StringType()),
        StructField("CreatedTime", StringType()),
        StructField("Campaigner", StringType())
    ])

    clickSchema = StructType([
        StructField("InventoryID", StringType()),
        StructField("CreatedTime", StringType())
    ])

    kafka_impression_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "impressions") \
        .option("startingOffsets", "earliest") \
        .load()

    impressions_df = kafka_impression_df \
        .select(from_json(col("value").cast("string"), impressionSchema).alias("value")) \
        .selectExpr("value.InventoryID as ImpressionID", "value.CreatedTime", "value.Campaigner") \
        .withColumn("ImpressionTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("CreatedTime")

    kafka_click_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "clicks") \
        .option("startingOffsets", "earliest") \
        .load()

    clicks_df = kafka_click_df.select(
        from_json(col("value").cast("string"), clickSchema).alias("value")) \
        .selectExpr("value.InventoryID as ClickID", "value.CreatedTime") \
        .withColumn("ClickTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .drop("CreatedTime")

    join_expr = "ImpressionID == ClickID"
    join_type = "inner"

    joined_df = impressions_df.join(clicks_df, expr(join_expr), join_type)

    output_query = joined_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    print("Waiting....")
    output_query.awaitTermination()
