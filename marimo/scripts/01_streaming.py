import marimo

__generated_with = "0.25.1"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Dans ce notebook, vous verrez l'application du Spark Structured Streaming dans le streaming des tweets sur le domaine de la data.

    Vous verrez notamment :
    - Lecture des données de streaming
    - Extraction JSON de la valeur
    - Filtrage par le langage "en" pour plus de facilité
    - Nettoyage du texte en HTML
    - Création d'une clé entre l'id du tweet et son tag
    - Calcul d'indicateurs basiques (nombre tweets par minute, moyenne de la longueur du tweet par hashtag, etc)
    """)
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 01 – Stream processing avec Spark Structured Streaming
    Kafka `mastodon_stream` → parsing JSON → filtres / fenêtres / statistiques → PostgreSQL.

    Le producteur doit tourner ('`docker compose up -d producer`' et '`docker compose -f docker-compose-dev.yml exec producer python -m scripts.main --mode stream`').
    """)
    return


@app.cell
def _():
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, TimestampType, LongType, BooleanType, ArrayType)
    from utils.mastodon_utils import (get_spark, read_table, write_table,
                                  KAFKA_BOOTSTRAP, TOPIC, CHECKPOINT_DIR, stop_all_streams, JDBC_PROPS, JDBC_URL, active_streams, write_result_into_console)

    spark = get_spark("streaming-mastodon", shuffle_partitions=4)
    return (
        ArrayType,
        CHECKPOINT_DIR,
        F,
        JDBC_PROPS,
        JDBC_URL,
        KAFKA_BOOTSTRAP,
        LongType,
        StringType,
        StructField,
        StructType,
        TOPIC,
        active_streams,
        spark,
        stop_all_streams,
        write_result_into_console,
    )


@app.cell
def _(spark):
    import sys

    print(sys.version)
    print(sys.executable)
    print(spark.version)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2.1 Lire le topic Kafka
    """)
    return


@app.cell
def _(TOPIC):
    print(TOPIC)
    return


@app.cell
def _(KAFKA_BOOTSTRAP, TOPIC, spark):
    # Lecture des données en streaming sur le Kafka
    raw = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
      .option("subscribe", TOPIC) \
      .option("startingOffsets","earliest") \
      .option("maxOffsetsPerTrigger", 500) \
      .load()
    raw.printSchema()
    return (raw,)


@app.cell
def _(raw, write_result_into_console):
    # Vérification du succès de l'opération
    write_result_into_console(raw)
    return


@app.cell
def _(active_streams, spark):
    active_streams(spark)
    return


@app.cell
def _(spark, stop_all_streams):
    stop_all_streams(spark)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Liste des colonnes :
    - key : clé de la valeur de cette ligne
    - value  : JSON contenant le tag utilisé pour recevoir ce tweet (stream_tag) et le json du tweet (value)
    - topic : le topic d'origine du message
    - paritition ; numéro du partition concerné
    - offset : numéro du message dans le consumer concerné
    - timestamp : Date de création de la ligne
    - timestampType : Type et formattage de la date

    Commentaire :

    Dans le JSON de "value", nous allons extraire les informations suivantes :
    - tweet_id : identifiant du tweet (id)
    - created_at : date de création du tweet
    - edited_at : date de modification du tweet
    - content : contenu du tweet en HTML
    - language : langage du tweet concerné
    - tags_list : liste des tags de ce tweet (à extraire name dans chaque json d'un tag)
    - account_id : identifiant du compte
    - account_username : nom du propriétaire de ce tweet
    - account_created_at : date de création du compte
    - account_following_count : nombre de personnes qui suit
    - account_followers_count : nombre de personnes le suivant
    - account_statuses_count : nombre de tweets effectués pour ce compte
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2.2 Parser le JSON du tweet

    Avant de directement extraire le JSON, il faut définir les différents schémas de ce qu'on veut récupérer notamment :
    - les informations du tweet (value)
    - les tags du tweet (tags)
    - les informations du compte qui a fait ce tweet (account)
    - le stream_tag qui est utilisé pour capturer cette information (stream_tag)
    """)
    return


@app.cell
def _(ArrayType, LongType, StringType, StructField, StructType):
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
    return (schema,)


@app.cell
def _(F, raw, schema):
    # Application de l'extraction du json et garder les informations de gestion de kafka avec offset et partition
    parsed = (
        raw.withColumn("value", F.col("value").cast("string"))
            .select(F.from_json(F.col("value"), schema).alias("data"),
                    F.col("offset").alias("kafka_offset"),                                       F.col("partition").alias("kafka_partition"),
                    F.current_timestamp().alias("ingest_time"))
            .select("data.*", "kafka_offset", "kafka_partition", "ingest_time")
    )


    parsed.printSchema()
    return (parsed,)


@app.cell
def _(parsed, write_result_into_console):
    # Vérification dans la console
    write_result_into_console(parsed)
    return


@app.cell
def _(active_streams, spark):
    active_streams(spark)
    return


@app.cell
def _(spark, stop_all_streams):
    stop_all_streams(spark)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Maintenant que nous avons extraits ces informations, capturons les informations dans des colonnes spécifiques pour retrouver une structure tabulaire.
    """)
    return


@app.cell
def _(F, parsed):
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
    return (parsed_select,)


@app.cell
def _(parsed_select):
    # Vérification de l'opération
    parsed_select.printSchema()
    return


@app.cell
def _(parsed_select, write_result_into_console):
    # Vérification du succès de l'opération
    write_result_into_console(parsed_select)
    return


@app.cell
def _(active_streams, spark):
    active_streams(spark)
    return


@app.cell
def _(spark, stop_all_streams):
    stop_all_streams(spark)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2.3 Transformation 1 – filtrage sur les tweets uniquement anglais
    """)
    return


@app.cell
def _(parsed_select):
    # Filtrage des tweets qui sont en anglais uniquement
    tweets_en = parsed_select.filter("language = 'en'")
    return (tweets_en,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2.4 Transformation 2 - Conversion de type en datetime pour les dates de créations
    """)
    return


@app.cell
def _(F, tweets_en):
    tweets_datetime = tweets_en.withColumns(
        {"created_at" : F.to_timestamp("created_at"),
        "account_created_at" : F.to_timestamp("account_created_at"),
        "edited_at" : F.to_timestamp("edited_at")}
    )
    return (tweets_datetime,)


@app.cell
def _(tweets_datetime):
    tweets_datetime.printSchema()
    return


@app.cell
def _(tweets_datetime, write_result_into_console):
    write_result_into_console(tweets_datetime.select("created_at", "account_created_at"))
    return


@app.cell
def _(active_streams, spark):
    active_streams(spark)
    return


@app.cell
def _(spark, stop_all_streams):
    stop_all_streams(spark)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2.5 Transformation 3 - Extraction de l'heure et du jour du tweet
    """)
    return


@app.cell
def _(F, tweets_datetime):
    tweets_extract_hours_date = tweets_datetime.withColumns(
        {
            "created_at_day" : F.to_date("created_at"),
            "created_at_hour" : F.hour("created_at")
        }
    )
    return (tweets_extract_hours_date,)


@app.cell
def _(tweets_extract_hours_date, write_result_into_console):
    write_result_into_console(tweets_extract_hours_date.select("created_at_day", "created_at_hour"))
    return


@app.cell
def _(active_streams, spark):
    active_streams(spark)
    return


@app.cell
def _(spark, stop_all_streams):
    stop_all_streams(spark)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2.6 Transformation 4 - Nettoyage du texte en enlevant la partie HTML

    Pour le moment, le contenu est brute avec du code HTML qui doit être supprimé pour mieux analyser le contenu du tweet.

    Pour ce faire, nous allons utiliser plusieurs expressions regex pour enlever :
    - les contenus entre les balises
    - les symboles apparaissant dans un script HTML
    """)
    return


@app.cell
def _(F, tweets_extract_hours_date):
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
    return (tweets_clean_content,)


@app.cell
def _(tweets_clean_content, write_result_into_console):
    # Vérification du succès de l'opération dans la console
    write_result_into_console(tweets_clean_content.select("content_clean"))
    return


@app.cell
def _(active_streams, spark):
    active_streams(spark)
    return


@app.cell
def _(spark, stop_all_streams):
    stop_all_streams(spark)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2.7 Transformation 5 : Création identifiant avec id tweets et tag

    L'objectif de créer cet identifiant est de séparer les mêmes tweets pour différents tags.
    """)
    return


@app.cell
def _(F, tweets_clean_content):
    tweets_generated_id = tweets_clean_content.withColumn("id_stream_tweet_tag", F.concat("tweet_id", "stream_tag"))
    return (tweets_generated_id,)


@app.cell
def _(tweets_generated_id):
    tweets_generated_id.printSchema()
    return


@app.cell
def _(tweets_generated_id, write_result_into_console):
    write_result_into_console(tweets_generated_id.select("id_stream_tweet_tag"))
    return


@app.cell
def _(active_streams, spark):
    active_streams(spark)
    return


@app.cell
def _(spark, stop_all_streams):
    stop_all_streams(spark)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2.8 Ecriture des données préparées dans la base de données PostgreSQL

    On va écrire maintenant les données nettoyées dans la base de données brutes pour les traiter en batch dans l'autre partie.

    L'objectif est de séparer la collecte et nettoyages des transformations beaucoup plus applicatives.
    """)
    return


@app.cell
def _(JDBC_PROPS, JDBC_URL):
    def write_table_def(df, table: str, mode: str = "append"):
        """Write a Spark DataFrame into SQL Table (mode 'append' or 'overwrite')."""
        (df.write.mode(mode).option("truncate", "true")
           .jdbc(JDBC_URL, table, properties=JDBC_PROPS))

    return (write_table_def,)


@app.cell
def _(write_table_def):
    # foreachBatch -> table reviews_stream (append), trigger 10 s, checkpoint dédié
    def write_raw(batch_df, batch_id):
        write_table_def(batch_df, "mastodon_stream", mode="append")

    return (write_raw,)


@app.cell
def _(CHECKPOINT_DIR, tweets_generated_id, write_raw):
    q_raw = (
        tweets_generated_id.writeStream.foreachBatch(write_raw)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw")
        .trigger(processingTime="10 seconds").start()
    )
    return (q_raw,)


@app.cell
def _(q_raw):
    q_raw.stop()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2.9 Compter le nombre de tweets window par fenêtre d'heure

    Pour compter le nombre de tweets qu'on capte à l'heure, nous allons utiliser les fonctionnalités de windows pour les calculer à toutes les deux minutes le nombre de tweets par heure.
    """)
    return


@app.cell
def _(write_table_def):
    def write_windows_nb_tweets(batch_df, batch_id):
        write_table_def(batch_df, "mastodon_stream_windows", mode="append")

    return (write_windows_nb_tweets,)


@app.cell
def _(tweets_generated_id):
    tweets_generated_id.printSchema()
    return


@app.cell
def _(CHECKPOINT_DIR, F, tweets_generated_id, write_windows_nb_tweets):
    # Ecriture du windows par fenêtre d'heure
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

    # Résulat à envoyer sur le postgreSQL de windows
    q_win = (
        windowed.writeStream.foreachBatch(write_windows_nb_tweets)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/windows_hour")
    .trigger(processingTime="10 seconds").start()
    )
    return (q_win,)


@app.cell
def _(active_streams, spark):
    active_streams(spark)
    return


@app.cell
def _(q_win):
    q_win.stop()
    return


@app.cell
def _(tweets_generated_id):
    tweets_generated_id.printSchema()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### 2.10 – Calcul d'indicateurs basiques et sauvegarde dans la base de données (time windows de 10 minutes)
    """)
    return


@app.cell
def _(write_table_def):
    def write_global_statistics(batch_df, batch_id):
        write_table_def(batch_df, "mastodon_stream_tags_statistics", mode="append")

    return (write_global_statistics,)


@app.cell
def _(F, tweets_generated_id):
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
    return (hashtag_stats,)


@app.cell
def _(CHECKPOINT_DIR, hashtag_stats, write_global_statistics):
    # Ecriture du résultat dans la base de données
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
        .start()
    )
    return (query,)


@app.cell
def _(hashtag_stats):
    hashtag_stats.printSchema()
    return


@app.cell
def _(query):
    query.stop()
    return


if __name__ == "__main__":
    app.run()
