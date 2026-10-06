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
    - Calcul d'indicateurs basiques (nombre tweets par minute, moyenne de la longueur du tweet par utilisateur et hashtag, )
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
    Le producteur doit tourner (`docker compose up -d producer` et ).
    """)
    return


@app.cell
def _():
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, TimestampType, LongType,
                                   BooleanType, ArrayType)
    from utils.mastodon_utils import (get_spark, read_table, write_table,
                                  KAFKA_BOOTSTRAP, TOPIC, CHECKPOINT_DIR, stop_all_streams, JDBC_PROPS, JDBC_URL, active_streams)

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
        read_table,
        spark,
        stop_all_streams,
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
def _(raw):
    # Vérification du succès de l'opération
    raw.writeStream.format('console').outputMode('update')\
        .option("truncate", True)\
        .option("numRows", 10)\
        .option('checkpointLocation', '/tmp/checkpoint_window_counts_console').trigger(processingTime="30 seconds").start() 
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
def _(parsed):
    # Vérification dans la console
    parsed.writeStream.format('console').outputMode('update')\
        .option("truncate", True)\
        .option("numRows", 10)\
        .option('checkpointLocation', '/tmp/checkpoint_window_counts_console').trigger(processingTime="30 seconds").start() 
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
        "stream_tag"
    )
    return (parsed_select,)


@app.cell
def _(parsed_select):
    # Vérification de l'opération
    parsed_select.printSchema()
    return


@app.cell
def _(parsed_select):
    # Vérification du succès de l'opération
    parsed_select.writeStream.format('console').outputMode('update')\
        .option("truncate", True)\
        .option("numRows", 10)\
        .option('checkpointLocation', '/tmp/checkpoint_window_counts_console').trigger(processingTime="30 seconds").start() 
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
def _(tweets_datetime):
    tweets_datetime.select("created_at", "account_created_at").writeStream.format('console').outputMode('update')\
        .option("truncate", True)\
        .option("numRows", 10)\
        .option('checkpointLocation', '/tmp/checkpoint_window_counts_console').trigger(processingTime="30 seconds").start() 
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
def _(tweets_extract_hours_date):
    tweets_extract_hours_date.select("created_at_day", "created_at_hour").writeStream.format('console').outputMode('update')\
        .option("truncate", True)\
        .option("numRows", 10)\
        .option('checkpointLocation', '/tmp/checkpoint_window_counts_console').trigger(processingTime="30 seconds").start() 
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

    In modern sports engineering, evaluating match progress requires more than looking at static...#python #datascience #sportsanalytics #machinelearning #software #coding #development #engineering #inclusive #communityDesigning a Dynamic Momentum Index for Live Cricket Analytics in Python
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
def _(tweets_clean_content):
    # Vérification du succès de l'opération dans la console
    tweets_clean_content.select("content_clean").writeStream.format('console').outputMode('update')\
        .option("truncate", False)\
        .option("numRows", 10)\
        .option('checkpointLocation', '/tmp/checkpoint_window_counts_console').trigger(processingTime="30 seconds").start() 
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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2.8 Ecriture des données préparées dans la base de données PostgreSQL
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
def _(CHECKPOINT_DIR, tweets_generated_id, write_table_def):
    # foreachBatch -> table reviews_stream (append), trigger 10 s, checkpoint dédié
    def write_raw(batch_df, batch_id):
        write_table_def(batch_df, "mastodon_stream", mode="append")

    q_raw = (tweets_generated_id.writeStream.foreachBatch(write_raw)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw")
    .trigger(processingTime="10 seconds").start())
    return (q_raw,)


@app.cell
def _(q_raw):
    q_raw.stop()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2.9 Compter le nombre de tweets window par fenêtre d'heure
    """)
    return


@app.cell
def _(write_table_def):
    def write_windows_nb_tweets(batch_df, batch_id):
        write_table_def(batch_df, "mastodon_stream", mode="append")

    return


@app.cell
def _(F, tweets_generated_id):
    # Ecriture du windows par fenêtre d'heure
    windowed = (tweets_generated_id
        .withWatermark("event_time", "2 minutes") # Watermark 2 minutes
        .groupBy(
            F.window("event_time", "1 minute"),
            F.col("category")
        ) # Calcul en group_by par catégorie du nombre de review
        .agg(
            F.count_distinct("tweet_id").alias("nb_tweets")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("category"),
            F.col("nb_tweets")
        )) # Sélection des colonnes à la fin pour l'heure et le nombre de tweets distinctes

    # Résulat à 
    q_win = (
        windowed
        .writeStream
        .format("memory") 
        .queryName("stream_window_counts")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoint_window_counts_v2")
        .start()
    )

    #q_win.awaitTermination()
    return q_win, windowed


@app.cell
def _(spark):
    spark.sql("""
        SELECT
            window_start,
            window_end,
            category,
            avg_rating,
            nb_reviews
        FROM stream_window_counts
        ORDER BY window_start, category
    """).show(truncate=False)
    return


@app.cell
def _(q_win):
    q_win.stop()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Q2.10 – Calcul d'indicateurs basic et sauvegarde dans la base de données (time windows de 10 minutes)

    Action 2: Calculate basic statistics, such as the average toot length per user or hashtag.
    """)
    return


@app.cell
def _(windowed):
    # TODO Q2.2 : même agrégation (windowed) vers le sink console en mode update, ~25 s, puis stop()
    # (la sortie console apparaît dans : docker compose logs -f jupyter)
    q_win_1 = windowed.writeStream.format('console').outputMode('update').option('checkpointLocation', '/tmp/checkpoint_window_counts_console').start()  # logs docker
    return (q_win_1,)


@app.cell
def _(q_win_1):
    q_win_1.stop()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    append : ➡️ Spark écrit uniquement les lignes considérées comme définitives et ne les réécrit plus.

    update : ➡️ Spark écrit les lignes dont la valeur a changé depuis le dernier micro-batch.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2.6 Action 2 – statistiques par hashtag (mode complete, table écrasée à chaque micro-batch)
    """)
    return


@app.function
# Fonction utilitaire pour écrire en parquet les résultats (pour chaque batch)
def save_batch(batch_df, batch_id):
    print(f"========== BATCH {batch_id} ==========")

    batch_df.show(truncate=False)

    batch_df.write \
        .mode("overwrite") \
        .saveAsTable("stream_hashtag_stats")

    print("========== TABLE ÉCRITE ==========")


@app.cell
def _(F, parsed):
    # TODO : explode(hashtags) puis par hashtag : nb_reviews, avg_text_length, avg_rating
    hashtag_stats = (
        parsed
        .select("*", F.explode("hashtags").alias("tag")) # Récupération des tags 
        .groupBy("tag") # Grouper par tag
        .agg(F.approx_count_distinct(F.col("review_id")).alias("nb_review"), # Pas count_distinct, pas possible sous streaming !!!
             F.avg(F.col("text_length")).alias("avg_length"),
             F.avg(F.col("rating")).alias("avg_rating")
            )         # Calcul des différents indicateurs
        )
    hashtag_stats.printSchema()
    # TODO : mode complete + foreachBatch qui écrase stream_hashtag_stats
    q_tags = (
            hashtag_stats.writeStream
            .queryName("stream_hashtag_complete")
            .outputMode("complete")
            .foreachBatch(save_batch)
            .option(
                "checkpointLocation",
                "/tmp/checkpoint_hashtag_stats_v3"
            )
            .start()
    )
    return (q_tags,)


@app.cell
def _(spark):
    # En cas où la table existe déjà et qu'il faut la supprimer
    spark.sql("DROP TABLE IF EXISTS stream_hashtag_stats")
    # Si ça ne marche pas, la supprimer depuis le dossier concerné : 
    # docker compose exec jupyter rm -rf /home/jovyan/work/notebooks/spark-warehouse/stream_hashtag_stats
    # docker compose exec jupyter rm -rf /tmp/checkpoint_hashtag_stats_v2
    return


@app.cell
def _(q_tags):
    q_tags.stop()
    return


@app.cell
def _(spark):
    spark.sql("""
        SELECT *
        FROM stream_hashtag_stats
    """).show(truncate=False)
    return


@app.cell
def _(q_tags):
    q_tags.lastProgress
    return


@app.cell
def _(q_tags):
    q_tags.status
    return


@app.cell
def _(q_tags):
    print(q_tags.isActive)
    print(q_tags.exception())
    return


@app.cell
def _(spark):
    # Regarder les tables existantes
    spark.sql("SHOW TABLES").show(truncate=False)
    return


@app.cell
def _(spark):
    spark.catalog.listTables()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 2.7 Supervision
    """)
    return


@app.cell
def _(spark):
    # TODO : pour chaque requête active, afficher name, status et les indicateurs de lastProgress
    for _q in spark.streams.active:
        print('Name : ', _q.name, 'Statut : ', _q.status, 'LastProgress : \n', _q.lastProgress, '\n')
    return


@app.cell
def _(F, read_table, spark):
    for t in ["reviews_stream", "stream_window_counts", "stream_hashtag_stats"]:
        print(t, read_table(spark, t).count())
    read_table(spark, "stream_window_counts").orderBy(F.desc("window_start")).show(8)
    #read_table(spark, "stream_hashtag_stats").orderBy(F.desc("nb_reviews")).show(8)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Reprise sur checkpoint et contrôle des doublons
    """)
    return


@app.cell
def _(spark):
    spark.streams.active
    return


@app.cell
def _(spark):
    # TODO : arrêter q_raw, attendre, la relancer avec LE MÊME checkpoint, puis compter les doublons de review_id
    for _q in spark.streams.active:
        _q.stop()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    | Élément                             | Rôle                                                                             |
    | ----------------------------------- | -------------------------------------------------------------------------------- |
    | **Checkpoint**                      | Mémorise les offsets et l'état de la requête pour permettre la reprise           |
    | **Reprise avec même checkpoint**    | Reprend aux offsets enregistrés                                                  |
    | **Nouveau checkpoint**              | Considéré comme une nouvelle requête → risque de retraiter les données           |
    | **foreachBatch**                    | Permet d'écrire chaque micro-batch dans un système externe                       |
    | **JDBC + foreachBatch**             | Pas exactly-once automatiquement                                                 |
    | **Sémantique pratique**             | Peut être **at-least-once**                                                      |
    | **Pour s'approcher d'exactly-once** | Rendre l'écriture idempotente, utiliser `batch_id`, clés uniques et transactions |
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **Laissez `q_raw` tourner** pendant les parties 3 et 4. En fin de journée :
    ```python
    for q in spark.streams.active: q.stop()
    ```
    """)
    return


@app.cell
def _(spark):
    spark.stop()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Réponses aux questions
    - **Q1.1** : ...
    - **Q1.2** : ...
    - **Q1.3** : ...
    - **Q2.1** : ...
    - **Q2.2** : ...
    - **Q2.3** : ...
    - **Q2.4** : ...
    """)
    return


if __name__ == "__main__":
    app.run()
