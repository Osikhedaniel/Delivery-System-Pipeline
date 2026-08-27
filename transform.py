from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, from_json, initcap, to_date, to_timestamp, trim, upper, when


def parse_json_stream(df:DataFrame,schema) -> DataFrame:
    parsed_df = df.select(
        from_json(col("value").cast("string"),
                  schema).alias("Data")
    )

    return parsed_df.select("Data.*") 

def add_delay_flag(df:DataFrame) -> DataFrame:
    return df.withColumn("delayed_flag",
                         when(col("speed") < 15,
                              "delayed")
                              .otherwise("on schedule")
                              )

def filter_invalid_coordinates(df:DataFrame) -> DataFrame:
    return df.filter(
        (col("longitude").isNotNull())&
       (col("latitude").isNotNull())
    )

def filter_invalid_speed(df:DataFrame) -> DataFrame:
    return df.filter(
        (col("speed") >= 0) & (col("speed") <= 180) & (col("speed").isNotNull())
    )

def timestamp_conversion(df:DataFrame) -> DataFrame:
    return df.withColumn("timestamp",
                         to_timestamp(col("timestamp"))
                         )

def standardizing_driver_names(df:DataFrame) -> DataFrame:
    return df.withColumn("driver_name",
                         initcap(trim(col("driver_name")))
                         )

def standardizing_vehicle_status(df:DataFrame) -> DataFrame:
    return df.withColumn("status",
                         upper(trim(col("status")))
                         )

def removing_null_ids(df:DataFrame) -> DataFrame:
    return df.filter(
        (col("vehicle_id").isNotNull()) & (col("delivery_id").isNotNull())
    )

def extracting_date(df:DataFrame) -> DataFrame:
    return df.withColumn("date",
                         to_date(col("timestamp"))
                         )

def ingestion_timestamp(df:DataFrame) -> DataFrame:
    return df.withColumn("ingestion_time",
                         current_timestamp()
                         )


