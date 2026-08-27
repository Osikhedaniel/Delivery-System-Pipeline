import traceback

from loguru import logger 
from consumer import data_schema, creating_spark_session, read_kafka_stream
from generator import generate_synthetic_data
from main import verify_api_key, health_check, generator_function, get_bulk_gps_data
from metrics import eta_calculation, aggregation_calculations, calculate_status_breakdown, calculate_delay_metrics, calculate_vehicle_performance, calculate_status_transitions, calculate_spatial_metrics, calculate_hourly_metrics, calculate_safety_metrics, metrics_report, save_metrics_report
from producer import extract_data
from schema import GPSData
from stream import delivery_window, write_to_console, write_to_postgres, write_to_dashboard_topic
from transform import parse_json_stream, add_delay_flag, filter_invalid_coordinates, filter_invalid_speed, timestamp_conversion, standardizing_driver_names, standardizing_vehicle_status, removing_null_ids, extracting_date, ingestion_timestamp
from dotenv import load_dotenv
import os,sys
from pyspark.sql.functions import col, lit 

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") 
DB_TABLE_NAME = os.getenv("DB_TABLE_NAME")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

required_vars = ['DB_HOST','DB_PORT','DB_NAME','DB_USER','DB_PASSWORD']
missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    logger.error(f"Missing environmental variables found: {missing_vars}")
    logger.error("please check your .env file")
    exit(1)

def build_pipeline():
    logger.info("Starting Logistics Streaming Pipeline")

    # Create spark session
    try:
         spark = creating_spark_session("First_spark_session")
         spark.sparkContext.setLogLevel("WARN")
         logger.info("Successfully created spark session")
    except Exception as e:
        logger.error(f"Failed to create spark session: {e}")
        return 
    
    # Start Kafka Stream 
    try:
        raw_stream = read_kafka_stream(spark=spark,
                                       bootstrap_servers="localhost:9092",
                                       topic="gps_updates")
        logger.info("Kafka stream created successfully") 
    except Exception as e:
        logger.error("Failed to create Kafka Stream")
        spark.stop()
        return 
    
    
    try:
        # Parse json data 
        gps_df = parse_json_stream(raw_stream,data_schema()) 
        
        # Apply Transformations 
        gps_df = add_delay_flag(gps_df)
        gps_df = filter_invalid_coordinates(gps_df)
        gps_df = filter_invalid_speed(gps_df)
        gps_df = timestamp_conversion(gps_df)
        gps_df = standardizing_driver_names(gps_df)
        gps_df = standardizing_vehicle_status(gps_df) 
        gps_df = removing_null_ids(gps_df) 
        gps_df = extracting_date(gps_df) 
        gps_df = ingestion_timestamp(gps_df) 
        gps_df = eta_calculation(gps_df) 

        # 
        gps_df = gps_df.withColumn("data_type", lit("gps_update"))

        gps_df = gps_df.withColumn("latitude", col("latitude").cast("double"))
        gps_df = gps_df.withColumn("longitude", col("longitude").cast("double")) 
        gps_df = gps_df.withColumn("speed", col("speed").cast("double"))
        gps_df = gps_df.withColumn("estimated_time_of_arrival", col("estimated_time_of_arrival").cast("double"))

        logger.info("Finished Data Transformations")

    except Exception as e:
        logger.error(f"Failed to apply transformations: {e}")
        spark.stop()
        return 
    
    queries = []

    # def write_metrics(batch_df,batch_id):
    #     try:
    #         if batch_df.rdd.isEmpty():
    #             logger.info(f"batch {batch_id}: Empty Batch!")
    #             return 

    #         report = metrics_report(batch_df)
    #         saved_report = save_metrics_report(report, filename= f"metrics_report_folder/metrics_report_{batch_id}.json")
    #         logger.info(f"Batch {batch_id}: Successfully saved metrics report at {saved_report}")

    #     except Exception as e:
    #         # logger.error(f"Batch {batch_id}: Failed to generate metrics report - {e}")
    #         logger.exception("Full exception:")
    #         traceback.print_exc()
    #         raise 

    def write_metrics(batch_df,batch_id):
        try:
            if batch_df.limit(1).count() == 0:
                logger.info(f"batch {batch_id}: Empty Batch!")
                return 

            report = metrics_report(batch_df)
            saved_report = save_metrics_report(report, filename= f"metrics_report_folder/metrics_report_{batch_id}.json")
            logger.info(f"Batch {batch_id}: Successfully saved metrics report at {saved_report}")

        except Exception as e:
            # logger.error(f"Batch {batch_id}: Failed to generate metrics report - {e}")
            logger.exception("Full exception:")
            traceback.print_exc()
            raise 

    try:
        logger.info("Starting metrics sink...")
        metrics_query = (
            gps_df.writeStream
            .foreachBatch(write_metrics)
            .outputMode("append")
            .option("checkpointLocation", "/tmp/checkpoints/metrics_report")
            .start()
        ) 
        queries.append(metrics_query)
        logger.info("Successfully started metrics sink")
    except Exception as e:
        logger.error(f"Failed to start metrics sink: {e}")

    try:
        logger.info("Starting Postgres sink...")
        postgres_query = write_to_postgres(gps_df,
                          DB_HOST,
                          DB_PORT,
                          DB_NAME,
                          DB_USER,
                          DB_PASSWORD,
                          DB_TABLE_NAME)
        queries.append(postgres_query)
        logger.info("Successfully started postgres sink")
    except Exception as e:
        logger.error(f"Failed to start postgres sink:{e}") 
    
    try:
        logger.info("Starting Console sink...")
        console_query = write_to_console(gps_df)
        queries.append(console_query)
        logger.info("Successfully started console sink")
    except Exception as e:
        logger.error(f"Failed to start console sink:{e}")

    try:
        logger.info("Starting Dashboard Kafka Sink...")
        Dashboard_query = write_to_dashboard_topic(gps_df,"localhost:9092","dashboard_updates")
        queries.append(Dashboard_query)
        logger.info("Successfully started dashboard sink")
    except Exception as e:
        logger.error(f"Failed to start Dashboard Kafka sink:{e}") 

    # to terminate data sinks 
    try:
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown Signal received, stopping streams...")
        for query in queries:
            try:
                query.stop()
                logger.info(f"Stopped query:{query.name}")
            except Exception as e:
                logger.error(f"Failed to stop query:{e}")
    except Exception as e:
        logger.error(f"Error occurred during stream processing:{e}")
        for query in queries:
            try:
                query.stop()
            except:
                pass 
    finally:
        logger.info("Shutting down spark session...")
        spark.stop()
        logger.info("Pipeline shutdown complete")

if __name__=="__main__":
    build_pipeline()



     
            


        
    
    
