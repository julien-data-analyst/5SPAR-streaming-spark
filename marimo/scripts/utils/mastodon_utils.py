#######################################-
# Project : Spark Structured Streaming/Batch utils
# Author : Julien RENOULT
# Date : 05/10/2026
#######################################-

import pyspark
from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP = "kafka:9092"          # depuis le conteneur Jupyter
TOPIC = "mastodon_stream"
DATA_DIR = "/data"
CHECKPOINT_DIR = f"{DATA_DIR}/checkpoints"
MODEL_DIR = f"{DATA_DIR}/models"

JDBC_URL = "jdbc:postgresql://postgres:5432/mastodon"
JDBC_PROPS = {"user": "spark", "password": "spark", "driver": "org.postgresql.Driver"}

def get_spark(app_name: str, shuffle_partitions: int = 8) -> SparkSession:
    """Creates (or retrieves) a local SparkSession configured for Kafka and PostgreSQL."""
    
    spark = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
    )
    #spark.sparkContext.setLogLevel("WARN")
    #print(f"Spark {spark.version} prêt - Spark UI : http://localhost:{spark.sparkContext.uiWebUrl.rsplit(':', 1)[-1]}")
    return spark


def read_table(spark, table: str):
    """Read a SQL Table into Spark DataFrame"""
    return spark.read.jdbc(JDBC_URL, table, properties=JDBC_PROPS)


def write_table(df, table: str, mode: str = "append"):
    """Write a Spark DataFrame into SQL Table (mode 'append' or 'overwrite')."""
    (df.write.mode(mode).option("truncate", "true")
       .jdbc(JDBC_URL, table, properties=JDBC_PROPS))

def stop_all_streams(spark):
    """Stop all streams active from spark connection"""
    for _q in spark.streams.active:
        _q.stop()

def active_streams(spark):
    """Show the active streams with the last progress"""
    for _q in spark.streams.active:
        print('Name : ', _q.name, 'Statut : ', _q.status, 'LastProgress : \n', _q.lastProgress, '\n')

def write_result_into_console(spark_request):
    """Show the first ten results of the request of the stream based on /tmp/checkpoint/ for results"""
    spark_request.writeStream.format('console').outputMode('update')\
    .option("truncate", False)\
    .option("numRows", 10)\
    .option('checkpointLocation', '/tmp/checkpoint').trigger(processingTime="30 seconds").start() 