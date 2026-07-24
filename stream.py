from loguru import logger 
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, struct, to_json, to_timestamp, window


def delivery_window(df:DataFrame,window_duration:str = "5 minutes") -> DataFrame:
    timeframe_df = df.withColumn("event_time",
                                 to_timestamp("timestamp")
                                 )
    
    return ( timeframe_df.groupBy(window(
        col("event_time"),window_duration
    )).agg(count("*").alias("delivery_count"))
    )

def write_to_console(df:DataFrame,output_mode:str = "append"):
    return(
        df.writeStream
        .format("console")
        .outputMode(output_mode)
        .option("truncate", False)
        .option("numRows", 10)
        .start()
    )

def write_to_postgres(df:DataFrame,
                      host:str,
                      port:int,
                      database:str,
                      user:str,
                      password:str,
                      table_name:str):
    def write_batch(batch_df,batch_id):
        try:
            pdf = batch_df.toPandas()

            if pdf.empty:
                logger.info(f"Batch {batch_id}: empty batch, skipping!")
                return 
            
            logger.info(f"Batch {batch_id}: Batch contains {len(pdf)} records")

            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                table_name=table_name
            )

            cursor = conn.cursor()

            insert_query = f"""
                           insert into {table_name}(
                           vehicle_id,
                           delivery_id,
                           driver_name,
                           latitude,
                           longitude,
                           speed,
                           status,
                           timestamp,
                           delayed_flag,
                           estimated_time_of_arrival,
                           date,
                           ingestion_time
                           )values %s
                            """
            
            data = [(
                     row.vehicle_id if hasattr(row, 'vehicle_id') else None,
                     row.delivery_id if hasattr(row, 'delivery_id') else None,
                     row.driver_name if hasattr(row, 'driver_name') else None,
                     float(row.latitude) if hasattr(row, 'latitude') and row.latitude is not None else None,
                     float(row.longitude) if hasattr(row, 'longitude') and row.longitude is not None else None,
                     float(row.speed) if hasattr(row, 'speed') and row.speed is not None else None,
                     row.status if hasattr(row, 'status') else None,
                     row.timestamp if hasattr(row, 'timestamp') else None,
                     bool(row.delayed_flag) if hasattr(row, 'delayed_flag') and row.delayed_flag is not None else False,
                     float(row.estimated_time_of_arrival) if hasattr(row, 'estimated_time_of_arrival') and row.estimated_time_of_arrival is not None else None,
                     row.date if hasattr(row, 'date') and row.date is not None else None,
                     row.ingestion_time if hasattr(row, 'ingestion_time') and row.ingestion_time is not None else None 
            )
            for _,row in pdf.iterrows()
            ]

            execute_values(cursor,insert_query,data)

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Batch {batch_id}: Successfully inserted {len(pdf)} records into database")

        except Exception as e:
            logger.info(f"Batch {batch_id}: Error - {e}")

    return (
        df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation","./checkpoints/postgres")
        .start()
    )

def write_to_dashboard_topic(df:DataFrame,kafka_server:str,topic:str):
    kafka_df = df.select(
        to_json(struct("*").alias("values"))
    )

    return(
        kafka_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers",kafka_server)
        .option("topic", topic)
        .option("checkpointLocation","./checkpoints/dashboard")
        .outputMode("append")
        .start()
    )
