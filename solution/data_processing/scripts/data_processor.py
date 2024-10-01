import logging
from typing import Union
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, col, to_timestamp, from_unixtime
from utils.cleaning_functions import clean_numerical_col, clean_alphabetical_col

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def flatten_df(df: DataFrame) -> DataFrame:
    # Split the 'raw_message' column
    df_split = df.withColumn("split_col", split(col("raw_message"), ","))

    # Define the column names
    column_names = [
        'Data status',
        'Latitude',
        'Latitude Direction',
        'Longitude',
        'Longitude Direction',
        'Speed over ground in knots',
        'Track made good in degrees True',
        'UT date',
        'Magnetic variation degrees',
        'Magnetic variation direction'
    ]

    # Assign the split columns to new columns with the specified names
    for i, name in enumerate(column_names):
        df_split = df_split.withColumn(name, col("split_col").getItem(i))

    # Drop the 'raw_message' and 'split_col' columns
    df_split = df_split.drop("raw_message", "split_col")

    return df_split

def clean_df(df: DataFrame) -> DataFrame:
    def apply_mappings(df: DataFrame, mappings: dict, clean_func: Union[clean_alphabetical_col, clean_numerical_col]) -> DataFrame:
        for old_col, new_col in mappings.items():
            logging.info(f"Cleaning column: {old_col}")
            df = df.withColumnRenamed(old_col, new_col)
            df = df.withColumn(new_col, clean_func(df[new_col]))
        return df

    df = df.withColumn("datetime", from_unixtime(col("datetime"), "yyyy-MM-dd'T'HH:mm:ss"))
    df = df.withColumn("datetime", to_timestamp(col("datetime"), "yyyy-MM-dd'T'HH:mm:ss"))
    df = (
        df.withColumnRenamed("UT date", "datestamp")
            .withColumn("datestamp", col("datestamp").cast("int"))
            .withColumnRenamed("Data status", "status")
            .filter(col("status") == "A")
    )

    # Clean numerical and alphabetical columns
    num_mappings = {
        'Latitude': 'lat',
        'Longitude': 'lon',
        'Speed over ground in knots': 'spd_over_grnd',
        'Track made good in degrees True': 'true_course',
        'Magnetic variation degrees': 'mag_variation'
    }
    char_mappings = {
        'Latitude Direction': 'lat_dir',
        'Longitude Direction': 'lon_dir',
        'Magnetic variation direction': 'mag_var_dir'
    }
    df = apply_mappings(df, num_mappings, clean_numerical_col)
    df = apply_mappings(df, char_mappings, clean_alphabetical_col)

    df = df.dropDuplicates(["device_id", "original_message_id"])
   
    return df

def process_data(df: DataFrame) -> DataFrame:
    df = flatten_df(df)
    df = clean_df(df)
    return df
