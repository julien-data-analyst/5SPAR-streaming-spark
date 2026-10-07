import time
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, TimestampType, LongType, BooleanType, ArrayType)
from scripts.utils.mastodon_utils import (get_spark, read_table, write_table,
                              KAFKA_BOOTSTRAP, TOPIC, CHECKPOINT_DIR, stop_all_streams, JDBC_PROPS, JDBC_URL, active_streams, write_result_into_console)

# Config session spark
spark = get_spark("streaming-mastodon", shuffle_partitions=4)

# Lecture des données en streaming sur le Kafka
raw = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
  .option("subscribe", TOPIC) \
  .option("startingOffsets","earliest") \
  .option("maxOffsetsPerTrigger", 500) \
  .load()

########################################################-
# Parsing JSON + Sélection
########################################################-

# Le schéma d'un JSON tag où on veut juste extraire son nom
tag_schema = StructType([
    StructField("name", StringType(), True),
])

# Le schéma d'un compte utilisateur où on veut extraire son pseudo, id avec quelques informations supplémentaires
account_schema = StructType([
    StructField("id", StringType(), True),
    StructField("username", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("following_count", LongType(), True),
    StructField("followers_count", LongType(), True),
    StructField("statuses_count", LongType(), True),
])

# Le schéma d'un tweet en ajoutant les deux autres schémas pour indiquer les sous-parties (notamment account et tag)
status_schema = StructType([
    StructField("id", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("edited_at", StringType(), True),
    StructField("content", StringType(), True),
    StructField("language", StringType(), True),
    StructField("tags", ArrayType(tag_schema), True),
    StructField("account", account_schema, True),
])

# Le schéma racine du JSON avec le stream tag en plus
schema = StructType([
    StructField("stream_tag", StringType(), True),
    StructField("value", status_schema, True),
])

# Application de l'extraction du json et garder les informations de gestion de kafka avec offset et partition
parsed = (
    raw.withColumn("value", F.col("value").cast("string"))
        .select(F.from_json(F.col("value"), schema).alias("data"),
                F.col("offset").alias("kafka_offset"),                                       F.col("partition").alias("kafka_partition"),
                F.current_timestamp().alias("ingest_time"))
        .select("data.*", "kafka_offset", "kafka_partition", "ingest_time")
)

# Sélection des éléments dans le StructType de value du tweet
parsed_select = parsed.select(
    F.col("value.id").alias("tweet_id"),

    F.col("value.created_at").alias("created_at"),

    F.col("value.edited_at").alias("edited_at"),

    F.col("value.content").alias("content"),

    F.col("value.language").alias("language"),

    F.transform(
        F.col("value.tags"),
        lambda tag: tag["name"]
    ).alias("tags_list"),

    F.col("value.account.id").alias("account_id"),

    F.col("value.account.username").alias("account_username"),

    F.col("value.account.created_at").alias("account_created_at"),

    F.col("value.account.following_count")
        .alias("account_following_count"),

    F.col("value.account.followers_count")
        .alias("account_followers_count"),

    F.col("value.account.statuses_count")
        .alias("account_statuses_count"),
    "kafka_offset",
    "kafka_partition",
    "stream_tag",
    "ingest_time"
)
########################################################-
########################################################-

########################################################-
# Transformations diverses 
########################################################-

# Filtrage des tweets qui sont en anglais uniquement
tweets_en = parsed_select.filter("language = 'en'")

tweets_datetime = tweets_en.withColumns(
    {"created_at" : F.to_timestamp("created_at"),
    "account_created_at" : F.to_timestamp("account_created_at"),
    "edited_at" : F.to_timestamp("edited_at")}
)

tweets_extract_hours_date = tweets_datetime.withColumns(
    {
        "created_at_day" : F.to_date("created_at"),
        "created_at_hour" : F.hour("created_at")
    }
)

# Nettoyage du contenu du tweet
tweets_clean_content = (
    tweets_extract_hours_date

    # Nettoyage du texte HTML
    .withColumn(
        "content_clean", # Suppression des balises HTML
        F.regexp_replace("content", "<[^>]+>", "")
    )
    .withColumn(
        "content_clean",
        F.regexp_replace("content_clean", "&nbsp;", " ")
    )
    .withColumn(
        "content_clean",
        F.regexp_replace("content_clean", "&amp;", "&")
    )
    .withColumn(
        "content_clean",
        F.regexp_replace("content_clean", "&lt;", "<")
    )
    .withColumn(
        "content_clean",
        F.regexp_replace("content_clean", "&gt;", ">")
    )
    .withColumn(
        "content_clean",
        F.trim("content_clean")
    )

    # Nettoyage du texte dans son contenu en enlevant ses URLs et ses tags
    # Suppression des URLs
    .withColumn(
        "content_clean",
        F.regexp_replace(
            "content_clean",
            r"https?://\S+",
            ""
        )
    )
    # Suppression des hashtags
    .withColumn(
        "content_clean",
        F.regexp_replace(
            "content_clean",
            r"#\w+",
            ""
        )
    )

    # Nettoyage final des espaces en trop
    .withColumn(
        "content_clean",
        F.regexp_replace(
            "content_clean",
            r"\s+",
            " "
        )
    )
    .withColumn(
        "content_clean",
        F.trim("content_clean")
    )
)

# Création id concaténé avec le tag du streaming 
tweets_generated_id = tweets_clean_content.withColumn("id_stream_tweet_tag", F.concat("tweet_id", "stream_tag"))

########################################################-
########################################################-

# Définir ici car bug sinon avec spark job !!!
def write_table_def(df, table: str, mode: str = "append"):
    """Write a Spark DataFrame into SQL Table (mode 'append' or 'overwrite')."""
    (df.write.mode(mode).option("truncate", "true")
       .jdbc(JDBC_URL, table, properties=JDBC_PROPS))

##################################-
# Write raw data cleaned
##################################-
# foreachBatch -> table mastodon_stream (append), trigger 10 s, checkpoint dédié
def write_raw(batch_df, batch_id):
    write_table_def(batch_df, "mastodon_stream", mode="append")

def write_windows_nb_tweets(batch_df, batch_id):
    write_table_def(batch_df, "mastodon_stream_windows", mode="append")
##################################-
##################################-

##################################-
# Ecriture du windows par fenêtre d'heure sur le nombre de tweet par heure
##################################-

windowed = (tweets_generated_id
    .withWatermark("created_at", "2 minutes") # Watermark 2 minutes
    .groupBy(
        F.window("created_at", "1 hour"), # Par heure
        F.col("created_at_hour")
    ) # Calcul en group_by par catégorie de l'heure
    .agg(
        F.approx_count_distinct("tweet_id").alias("nb_tweets")
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("created_at_hour"),
        F.col("nb_tweets"),
        F.current_timestamp().alias("ingest_time_windows")
    )) # Sélection des colonnes à la fin pour l'heure et le nombre de tweets distinctes
##################################-
##################################-

#################################################-
# Addded write_global_statistics
#################################################-
def write_global_statistics(batch_df, batch_id):
    write_table_def(batch_df, "mastodon_stream_tags_statistics", mode="append")


hashtag_stats = (
    tweets_generated_id
    
    # Récupération des tags 
    .select("*", F.explode("tags_list").alias("tag")) 
    .filter(F.col("tag") != F.col("stream_tag"))
    
    # Grouper par le stream_tag et le tag afin de faire les couples de tags    
    .groupBy("stream_tag", "tag") 

    .agg(
        # Nombre de tweets différents
        F.approx_count_distinct(F.col("tweet_id")).alias("nb_tweets"), 
        # Nombre d'utilisateurs différents
         F.approx_count_distinct(F.col("account_id")).alias("nb_users"), 
        # Moyenne du nombre de followers sur les comptes qui ont ces deux tags
         F.avg(F.col("account_followers_count")).alias("avg_followers_count")
        )
    .withColumn("ingested_at", F.now())
    )

##################################-
##################################-

##########################################################-
# Configuration des résultats à écrire dans la base de données
###########################################################-
query = (
        hashtag_stats
        .writeStream
        .outputMode("complete")
        .foreachBatch(write_global_statistics)
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_DIR}/hashtag_stats",
        )
        .trigger(processingTime="10 minutes")
    )

    # %%

q_raw = (
        tweets_generated_id.writeStream.foreachBatch(write_raw)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw")
        .trigger(processingTime="10 seconds")
    )

q_win = (
        windowed.writeStream.foreachBatch(write_windows_nb_tweets)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/windows_hour")
    .trigger(processingTime="10 seconds")
    )
######################################################-
######################################################-

# Ecriture des résultats dans la base de données
queries = []
try:
    queries = [query.start(), q_raw.start(), q_win.start()]  

    print("Streaming started.")
    print("Press Ctrl+C to stop.")

    while True:
            time.sleep(60)
    
except KeyboardInterrupt:
        print("\nStopping streaming...")

finally:
        for query in queries:
            if query.isActive:
                query.stop()