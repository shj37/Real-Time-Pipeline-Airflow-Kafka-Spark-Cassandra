# Building a Real-Time Data Pipeline with Apache Airflow, Kafka, Spark, and Cassandra

![project overview](https://miro.medium.com/v2/resize:fit:1100/format:webp/1*_lvtt1_2x0gYdklAbdIz1w.jpeg)

## Description:

This repo holds the code for a real-time data pipeline that streams data from an API, processes it with **Kafka** and **Spark**, and stores it in **Cassandra**. **Airflow** automates the workflow, and **Docker** makes it easy to run.

## Key Files:

- docker-compose.yml: Sets up Kafka, Spark, Cassandra, and Airflow.
- dags/kafka_stream.py: Airflow DAG to schedule the pipeline.
- spark/spark_stream.py: Spark script to process Kafka data and write to Cassandra.
- script/entrypoint.sh: initialize Airflow
- requirements.txt: dependencies

## How to Run:

- Clone the repo.
- Run `docker compose up -d`.
- Open Airflow at http://localhost:8080 and trigger the DAG.
- Check Cassandra for results with cqlsh.

##  Tech Used:

- Apache Airflow, Kafka, Spark, Cassandra, Docker, Python.


## Purpose:

It’s a practical project to show how I build and automate data pipelines with real-time tools.

## Learn More

This project is fully documented in a [Medium article series](https://medium.com/@jushijun/building-a-real-time-data-pipeline-with-apache-airflow-kafka-spark-and-cassandra-be4ee5be8843).


## References

- YouTube CodeWithYu https://www.youtube.com/watch?v=GqAcTrqKcrY&ab_channel=CodeWithYu