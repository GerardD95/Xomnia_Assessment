import sys
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *

from utils.utils import log_new_file
from data_processor import process_data
from write_strategies import ConsoleWriteStrategy, DuckDBWriteStrategy, GoogleCloudSQLWriteStrategy, WriteContext
from schemas.pyspark_schemas import raw_message_schema


def get_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
        # .master("spark://localhost:7077") \
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def read_stream_data(spark: SparkSession, schema, path: str):
    return spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .option("path", path) \
        .load()

def normalize_and_write(batch_df: DataFrame, batch_id: int, context: WriteContext):
    # # Devices Table
    # df_devices = batch_df.select("device_id", "address_ip", "address_port").distinct()
    # context.execute_strategy(df_devices, batch_id, "devices", "device_id")

    # # Messages Table
    # df_messages = batch_df.select("original_message_id", "device_id", "datetime", "status").distinct()
    # context.execute_strategy(df_messages, batch_id, "messages", "original_message_id")

    # # Locations Table
    # df_locations = batch_df.select("original_message_id", "lat", "long", "lat_dir", "long_dir").distinct()
    # context.execute_strategy(df_locations, batch_id, "locations", "original_message_id")

    # # Navigation Table
    # df_navigation = batch_df.select("original_message_id", "spd_over_grnd", "true_course", "mag_variaton", "mag_var_dir").distinct()
    # context.execute_strategy(df_navigation, batch_id, "navigation", "original_message_id")

    context.execute_strategy(batch_df, batch_id, "raw_messages", ["original_message_id", "device_id"])

def get_stream_writer(df: DataFrame, context: WriteContext, checkpnt_enabled: bool = True) -> DataFrame.writeStream:
    query = df.writeStream \
        .outputMode("append") \
        .foreachBatch(log_new_file) \
        .foreachBatch(lambda batch_df, batch_id: normalize_and_write(batch_df, batch_id, context))
    if checkpnt_enabled:
        checkpoint_path = "s3a://test/checkpoints"
        query = query.option("checkpointLocation", checkpoint_path) 

    return query

def main(context: WriteContext):
    spark = get_spark_session("ShippingDataProcessor")
    df = read_stream_data(spark, raw_message_schema, "s3a://test/shipping_data")
    df = process_data(df)
    df.printSchema()
    query = get_stream_writer(df, context, checkpnt_enabled=False)
    query = query.start()
    query.awaitTermination()

if __name__ == "__main__":
    write_strategy_map = {
        "console": ConsoleWriteStrategy,
        "localdb": DuckDBWriteStrategy,
        "cloud": GoogleCloudSQLWriteStrategy
    }

    if len(sys.argv) < 2 or sys.argv[1].lower() not in write_strategy_map:
        logging.error("Usage: spark_streaming_job.py <console|localdb|cloud>")
        sys.exit(1)
    
    strategy_key = sys.argv[1].lower()
    write_strategy = write_strategy_map[strategy_key]()
    write_strategy.create_tables()
    main(WriteContext(write_strategy))