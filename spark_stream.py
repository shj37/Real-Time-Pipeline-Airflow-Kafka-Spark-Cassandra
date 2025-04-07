import logging
logging.basicConfig(level=logging.INFO)

# authentication provider 
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    # session.execute("""
    #  DROP TABLE IF EXISTS spark_streams.test_new;
    #  """)
    # CREATE TABLE IF NOT EXISTS spark_streams.created_users
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        PRIMARY KEY (last_name, email));
    """)

    print("Table created successfully!")


def create_spark_connection():
    s_conn = None

    try:
        # localhost vs cassandra
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', '192.168.1.6') \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .getOrCreate()

        # Set to WARN by default. You can adjust this with sc.setLogLevel("newLevel") (e.g., "INFO" or "DEBUG") if you need more or less verbosity.
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        # localhost:9092 vs broker:29092
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option("failOnDataLoss", "false") \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info(f"kafka dataframe created successfully {spark_df}")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_cassandra_connection():
    try:
        # localhost, connecting to the cassandra cluster, Docker: cassandra
        cluster = Cluster(['192.168.1.6'])

        cas_session = cluster.connect()

        create_keyspace(cas_session)
        create_table(cas_session)

        return cas_session
    except Exception as e:
        import traceback
        logging.error(f"Could not create cassandra connection due to {e}")
        logging.error(traceback.format_exc())
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("email", StringType(), False),
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    sel = sel.filter(col("last_name").isNotNull() & col("email").isNotNull())
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        session = create_cassandra_connection()

        if session:
            spark_df = connect_to_kafka(spark_conn)

            selection_df = create_selection_df_from_kafka(spark_df)
            
            spark_df.printSchema()

            logging.info("Streaming is being started...")
        

            try:
                streaming_query = selection_df.writeStream \
                                .format("org.apache.spark.sql.cassandra") \
                                .options(table="created_users", keyspace="spark_streams") \
                                .option("checkpointLocation", "/tmp/checkpoint") \
                                .trigger(processingTime="5 seconds") \
                                .start()
    
                print(f"Streaming is being started 2...")

                print(streaming_query.status)
                print(f"Query Status: {streaming_query.status}")
                print(f"Recent Progress: {streaming_query.recentProgress}")

                if streaming_query.isActive:
                    print("Query is running")
                
                streaming_query.awaitTermination(100)

                print("Streaming is being started 3...")

            except Exception as e:
                print(f"Streaming query failed due to: {e}")