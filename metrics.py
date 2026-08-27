from datetime import UTC, datetime
import json
from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg, col, count, countDistinct, date_format, dayofweek, hour, lit,
    percentile_approx, stddev, to_timestamp, when, lag, lead, round,
    min, max, sum
)
from pyspark.sql.window import Window 
from pathlib import Path 

def eta_calculation(df:DataFrame,default_distance_km:float=25) -> DataFrame:
    return df.withColumn("estimated_time_of_arrival",
        round((lit(default_distance_km)/col("speed")) * 60,2)
    )

def aggregation_calculations(df:DataFrame):
    return df.agg(
        count("*").alias("total_events"),
        avg("speed").alias("average_speed"),
        min("speed").alias("minimum_speed"),
        max("speed").alias("maximum_speed"),
        stddev("speed").alias("speed_stddev"),

        # longitude aggregation calculations
        avg("latitude").alias("average_latitude"),
        avg("longitude").alias("average_longitude"),
        min("latitude").alias("minimum_latitude"),
        max("latitude").alias("maximum_latitude"),
        min("longitude").alias("minimum_longitude"),
        max("longitude").alias("maximum_longitude"),

        #unique values calculations
        countDistinct("status").alias("unique_statuses"),
        countDistinct("vehicle_id").alias("unique_vehicles"),
        countDistinct("delivery_id").alias("unique_deliveries"),
        countDistinct("driver_name").alias("unique_drivers"),

        # estimated time of arrivel (eta) calculations 
        avg("estimated_time_of_arrival").alias("average_eta_minutes"),
        min("estimated_time_of_arrival").alias("min_eta_minutes"),
        max("estimated_time_of_arrival").alias("max_eta_minutes")
    )

def calculate_status_breakdown(df:DataFrame):
    return ( df.groupBy("status").agg(
        count("*").alias("count"),
        round(avg("speed"),2).alias("speed"),
        round(avg("estimated_time_of_arrival"),2).alias("average_eta_minutes"),
        round(avg("longitude"),4).alias("average_longitude"),
        round(avg("latitude"),4).alias("average_latitude")
    ).orderBy(col("count").desc())
    )

def calculate_delay_metrics(df:DataFrame):
    return (df.groupBy("delayed_flag").agg(
        count("*").alias("count"),
        round(avg("speed"),2).alias("average_speed"),
        round(avg("estimated_time_of_arrival"),2).alias("average_eta"),
        round(percentile_approx("speed",0.5),2).alias("median_speed"),
        round(percentile_approx("estimated_time_of_arrival",0.5),2).alias("median_eta")
    ).orderBy(col("count").desc())
    )

def calculate_vehicle_performance(df:DataFrame):
    return( df.groupBy("vehicle_id","driver_name").agg(
        count("*").alias("total_updates"),
        avg("speed").alias("avg_speed"),
        max("speed").alias("max_speed"),
        min("speed").alias("min_speed"),
        avg("estimated_time_of_arrival").alias("avg_eta"),
        avg("latitude").alias("avg_latitude"),
        avg("longitude").alias("avg_longitude"),
        sum(when(col("delayed_flag") == True, 1).otherwise(0)).alias("delayed_updates"),
        sum(when(col("delayed_flag")== False, 1).otherwise(0)).alias("on_time_updates")
        
    ).withColumn("delay_percentage",
                 round((col("delayed_updates")/col("total_updates")) * 100,2)
                 )
                  .orderBy(col("total_updates").desc())
    )

def calculate_status_transitions(df:DataFrame):
    window_spec = Window.partitionBy("vehicle_id").orderBy("timestamp")

    return(
        df.withColumn("prev_status",lag("status",1).over(window_spec))
        .withColumn("next_status",lead("status",1).over(window_spec))
        .filter(col("prev_status").isNotNull())
        .select("delivery_id","vehicle_id","timestamp","status","prev_status","next_status")
        .groupBy("prev_status","status")
        .agg(count("*").alias("transition_frequency"))
        .orderBy(col("transition_frequency").desc())
    )

def calculate_spatial_metrics(df:DataFrame):
    return (
        df.agg(
            round(avg("latitude"),4).alias("center_latitude"),
            round(avg("longitude"),4).alias("center_longitude"),
            round((max("latitude") - min("latitude")),4).alias("latitude_range"),
            round((max("longitude") - min("longitude")),4).alias("longitude_range"),
            round(stddev("latitude"),4).alias("latitude_spread"),
            round(stddev("longitude"),4).alias("longitude_spread")
        )
    )

def calculate_hourly_metrics(df:DataFrame):

    df_with_hourly = df.withColumn("event_time",to_timestamp("timestamp"))\
                       .withColumn("hour_of_day",hour("event_time"))\
                       .withColumn("day_of_week",dayofweek("event_time"))\
                       .withColumn("date",date_format("event_time","yyyy-MM-dd"))
    
    hourly_metrics =( df_with_hourly.groupBy("hour_of_day").agg(
        count("*").alias("events"),
        round(avg("speed"),2).alias("average_speed"),
        sum(when(col("delayed_flag") == True, 1).otherwise(0)).alias("delays")
    ).orderBy("hour_of_day")

    )
    return hourly_metrics 

def calculate_safety_metrics(df:DataFrame):
    return df.agg(
        sum(when(col("speed") > 80, 1).otherwise(0)).alias("speeding_events"),
        sum(when(col("speed") < 10, 1).otherwise(0)).alias("slow_events"),
        round(avg("speed"),2).alias("overall_average_speed"),
        round(max("speed"),2).alias("max_speed_observed"),
        countDistinct(when(col("speed") > 100,col("vehicle_id"))).alias("vehicles_exceeding_100kmh")
    )

def spark_df_conversion(spark_df:DataFrame) -> list:
    try:
        pandas_df = spark_df.toPandas() 
        if pandas_df.empty:
            return []
        return json.loads(pandas_df.to_json(orient="records",date_format="iso")) 
    except Exception:
        return []

def metrics_report(df:DataFrame) -> dict:
    report = {
        'last_updated': datetime.now(UTC).isoformat(),
        'summary': {},
        'status_breakdown': [],
        'delay_metrics': [],
        'vehicle_performance': [],
        'status_transitions': [],
        'spatial_metrics': {},
        'hourly_metrics': [],
        'safety_metrics': {}
    }

    if df is None:
        return report 

    report['summary'] = (spark_df_conversion(aggregation_calculations(df)) or [{}]) [0]
    report['status_breakdown'] = spark_df_conversion(calculate_status_breakdown(df)) 
    report['delay_metrics'] = spark_df_conversion(calculate_delay_metrics(df))
    report['vehicle_performance'] = spark_df_conversion(calculate_vehicle_performance(df))
    report['status_transitions'] = spark_df_conversion(calculate_status_transitions(df)) 
    report['spatial_metrics'] = (spark_df_conversion(calculate_spatial_metrics(df)) or [{}]) [0] 
    report['hourly_metrics'] = spark_df_conversion(calculate_hourly_metrics(df))
    report['safety_metrics'] = (spark_df_conversion(calculate_safety_metrics(df)) or [{}]) [0]

    print(report)

    return report 

def save_metrics_report(report:dict,filename:str="metrics_report.json") -> Path:
    path = Path(filename)
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    return path


