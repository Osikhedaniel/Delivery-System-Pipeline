Real Time Delivery Tracking Pipeline

An end to end streaming data engineering project that simulates a fleet of delivery vehicles, streams their GPS telemetry through Kafka, processes it with Spark Structured Streaming, stores raw and aggregated results in PostgreSQL, and serves a live Power BI dashboard through DirectQuery.

Overview

Logistics teams need to see where their vehicles are, how fast they are moving, and which deliveries are at risk as it happens, not the next morning. This project demonstrates that flow end to end:

A FastAPI service, I built from scratch for this project as the pipeline's data source, generates realistic synthetic GPS events and exposes them behind an API key. Rather than relying on a third party dataset or a static CSV, the pipeline ingests from a live, authenticated HTTP API, the same way it would in a real integration.
A Kafka producer polls the API and publishes each event to the gps_updates topic.
A Spark Structured Streaming job consumes the topic, cleans and enriches the data, then fans out to three sinks.
PostgreSQL holds both the raw enriched events and per batch metric snapshots.
Power BI reads from PostgreSQL (DirectQuery) to render live dashboards.

Tech Stack

Data source (custom-built API) -	Python, FastAPI, Uvicorn, Faker, deployed on Render
Messaging -	Apache Kafka, kafka-python
Stream processing -	Apache Spark (PySpark) Structured Streaming
Storage -	PostgreSQL, psycopg2
Visualisation -	Power BI (DirectQuery)
Utilities -	python-dotenv, loguru, pandas, requests, pydantic

