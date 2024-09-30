from pyspark.sql.types import StructType, StructField, StringType, IntegerType

raw_message_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("datetime", IntegerType(), True),
    StructField("address_ip", StringType(), True),
    StructField("address_port", IntegerType(), True),
    StructField("original_message_id", StringType(), True),
    StructField("raw_message", StringType(), True)
])