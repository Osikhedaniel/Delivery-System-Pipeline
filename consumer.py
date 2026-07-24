import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,DoubleType,StringType

def data_schema() -> StructType:
    return StructType([
        StructField('vehicle_id',StringType(),True),
        StructField('delivery_id',StringType(),True),
        StructField('driver_name',StringType(),True),
        StructField('latitude',DoubleType(),True),
        StructField('longitude',DoubleType(),True),
        StructField('speed',DoubleType(),True),
        StructField('status',StringType(),True),
        StructField('timestamp',StringType(),True)
    ])

def creating_spark_session(app_name:str) -> SparkSession:
    spark_version = pyspark.__version__

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}")
        .getOrCreate()
    )

def read_kafka_stream(spark:SparkSession,bootstrap_servers:str,topic:str):
    return(
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",bootstrap_servers)
        .option("subscribe",topic)
        .load()
    )

