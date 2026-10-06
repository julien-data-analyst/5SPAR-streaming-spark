#######################################-
# Project : Spark Structured Streaming/Batch utils
# Author : Julien RENOULT
# Date : 05/10/2026
#######################################-

import pyspark
from pyspark.sql import SparkSession
import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")        # depuis le conteneur Jupyter
TOPIC = os.getenv("TOPIC")
DATA_DIR = "/data"
CHECKPOINT_DIR = f"{DATA_DIR}/checkpoints"
MODEL_DIR = f"{DATA_DIR}/models"

JDBC_URL = f"jdbc:postgresql://postgres:5432/{os.getenv("POSTGRES_DB")}"
JDBC_PROPS = {"user": os.getenv("POSTGRES_USER"), "password": os.getenv("POSTGRES_PASSWORD"), "driver": "org.postgresql.Driver"}

def get_spark(
    app_name: str,
    shuffle_partitions: int = 8,
) -> SparkSession:
    """Create or retrieve a SparkSession configured for the project.

    The SparkSession is configured with the application name, the number
    of shuffle partitions, and UTC as the Spark SQL session timezone.

    Args:
        app_name: Name of the Spark application.
        shuffle_partitions: Number of partitions used for Spark SQL
            shuffles. Defaults to 8.

    Returns:
        SparkSession: Configured Spark session.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.sql.shuffle.partitions",
            str(shuffle_partitions),
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    return spark


def read_table(spark: SparkSession, table: str):
    """Read a PostgreSQL table into a Spark DataFrame.

    The connection parameters are retrieved from the PostgreSQL
    environment variables configured for the application.

    Args:
        spark: Active Spark session used to read the table.
        table: Name of the PostgreSQL table to read.

    Returns:
        DataFrame: Spark DataFrame containing the table data.
    """
    return spark.read.jdbc(
        JDBC_URL,
        table,
        properties=JDBC_PROPS,
    )


def write_table(
    df,
    table: str,
    mode: str = "append",
):
    """Write a Spark DataFrame into a PostgreSQL table.

    Args:
        df: Spark DataFrame to write.
        table: Name of the PostgreSQL destination table.
        mode: Save mode used when writing the data. Supported values
            include ``append`` and ``overwrite``. Defaults to ``append``.

    Returns:
        None
    """
    (
        df.write
        .mode(mode)
        .option("truncate", "true")
        .jdbc(
            JDBC_URL,
            table,
            properties=JDBC_PROPS,
        )
    )


def stop_all_streams(spark: SparkSession) -> None:
    """Stop all active Structured Streaming queries.

    Args:
        spark: Active Spark session containing the streaming queries.

    Returns:
        None
    """
    for query in spark.streams.active:
        query.stop()


def active_streams(spark: SparkSession) -> None:
    """Display the status and latest progress of active streams.

    Each active Structured Streaming query is displayed with its name,
    current status, and latest progress information.

    Args:
        spark: Active Spark session containing the streaming queries.

    Returns:
        None
    """
    for query in spark.streams.active:
        print(
            "Name:",
            query.name,
            "Status:",
            query.status,
            "LastProgress:\n",
            query.lastProgress,
            "\n",
        )


def write_result_into_console(spark_request) -> None:
    """Start a streaming query that displays results in the console.

    The query displays up to ten rows for each micro-batch and uses
    update output mode. A checkpoint is stored in ``/tmp/checkpoint``
    to track the progress of the streaming query.

    Args:
        spark_request: Spark DataFrame representing the streaming
            query to display.

    Returns:
        None
    """
    (
        spark_request.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", False)
        .option("numRows", 10)
        .option("checkpointLocation", "/tmp/checkpoint")
        .trigger(processingTime="30 seconds")
        .start()
    )
