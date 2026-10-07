import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Dans ce notebook, vous verrez l'application de batch pour le calcul d'indicateurs sur l'axe des tweets de Mastodon.

    De plus, on créera une nouvell table spécialement sur les offres afin de pouvoir faire des calculs précises sur celles-ci.

    On analysera les tweets en général sur le domaine de la "Data".

    Vous verrez notamment :
    - Le nombre de tweets par jour de la semaine (1 dimanche - 7 samedi)
    - Le nombre de tweets par mois (1 Janvier - 12 Décembre)
    - Le nombre d'utilisateur différents dans le domaine de la data
    - Capturer les offres d'emplois et l'enregistrer dans une table à part
    - Compter le nombre d'offre d'emploi parmi ces tweets
    - Nombre d'offre au cours de l'année (par mois)
    - Journée typique de tweets dans la data (moyenne du nb de tweets par heure de la journée)
      - Moyenne du NB tweets
      - Moyenne du NB tweets offres
      - Q1 du NB de tweets
      - Médiane du NB de tweets
      - Q3 du NB de tweets
      - Q1 du NB de tweets d'offres
      - Médiane du NB de tweets d'offres
      - Q3 du NB de tweets d'offres
    - Nombre de mots clés sur tous les tweets (par nb de mots-clés data, combien de tweets sont concernées)
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Analyse statistique sur les tweets
    """)
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import time
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, TimestampType, LongType,
                                   BooleanType, ArrayType)
    from utils.mastodon_utils import (get_spark, read_table, write_table,
                                  KAFKA_BOOTSTRAP, TOPIC, CHECKPOINT_DIR, stop_all_streams, JDBC_PROPS, JDBC_URL, active_streams, write_result_into_console)

    spark = get_spark("batch-tweets-mastodon", shuffle_partitions=4)
    return F, read_table, spark, write_table


@app.cell
def _(read_table, spark):
    toots_with_stream_tags = (
          read_table(spark, "mastodon_stream")
          .dropDuplicates(["id_stream_tweet_tag"]) # Supprimer les doublons éventuelles
    )

    toots = (
        toots_with_stream_tags
        # On dédoublonne pour n'avoir qu'un seul tweet par ligne vu que la capture se fait part age
        # et qu'on peut capturer le même tweet sur plusieurs stream_tags
        .drop("id_stream_tweet_tag", "stream_tag", "ingest_time", "kafka_offset", "kafka_partition")
        .sort("edited_at", ascending=False)
        .dropDuplicates(["tweet_id"])
    )
    return toots, toots_with_stream_tags


@app.cell
def _(toots):
    toots.printSchema()
    return


@app.cell
def _(toots):
    print("Le nombre de ligne : ", toots.count())
    print("Le nombre de colonnes : ", len(toots.columns))
    return


@app.cell
def _(toots):
    # Extraction des tweets
    toots.show()
    return


@app.cell
def _(toots):
    # Vérification des doublons de tweets
    toots.groupBy("tweet_id").agg({"*" : "count"}).show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.1 Nombre de tweets par jour de la semaine

    Pour calculer le nombre de tweet par jour, nous allons utiliser un group_by sur le jour de la semaine qu'on va ajouter dans notre processus.

    Nous allons ensuite enregistrer les résultats dans une table spécifique PostgreSQL d'analyse.
    """)
    return


@app.cell
def _(F, toots):
    nb_toots_day_of_week = (toots
        .select("tweet_id", "created_at")
        .withColumn("day_of_week", F.dayofweek("created_at"))
        .groupBy("day_of_week")
        .agg(F.count("*").alias("nb_tweets"))
        .withColumn("updated_at", F.now()) # Date de mise à jour
    )
    return (nb_toots_day_of_week,)


@app.cell
def _(nb_toots_day_of_week):
    nb_toots_day_of_week.explain()
    return


@app.cell
def _(nb_toots_day_of_week):
    # Vérification du succès de l'opération
    nb_toots_day_of_week.show()
    return


@app.cell
def _(nb_toots_day_of_week, write_table):
    # Ecrire le résultat dans une table analytique
    write_table(nb_toots_day_of_week, "day_of_week_tweets", "overwrite")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.2 Nombre de tweets par mois

    Pour calculer le nombre de tweet par mois, nous allons utiliser un group_by sur le mois qu'on va ajouter dans notre processus.

    Nous allons ensuite enregistrer les résultats dans une table spécifique PostgreSQL d'analyse.
    """)
    return


@app.cell
def _(F, toots):
    nb_toots_month = (toots
        .select("tweet_id", "created_at")
        .withColumn("month", F.month("created_at"))
        .groupBy("month")
        .agg(F.count("*").alias("nb_tweets"))
        .withColumn("updated_at", F.now()) # Date de mise à jour
    )
    return (nb_toots_month,)


@app.cell
def _(nb_toots_month):
    nb_toots_month.show()
    return


@app.cell
def _(nb_toots_month, write_table):
    # Ecriture du résultat dans une table analytique SQL
    write_table(nb_toots_month, "month_tweets", "overwrite")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.3 Nombre d'utilisateurs dans le domaine de la data
    """)
    return


@app.cell
def _(F, toots):
    distinct_data_account = toots.select(F.countDistinct("account_id").alias("nb_data_accounts"))
    return (distinct_data_account,)


@app.cell
def _(distinct_data_account):
    nb_account = distinct_data_account.collect()[0][0]
    return (nb_account,)


@app.cell(hide_code=True)
def _(mo, nb_account):
    mo.md(f"""
    ### Commentaire : 

    On observe parmi tous nos tweets qu'on a {nb_account} comptes différents
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.4 Capturer les offres d'emplois et écriture dans une base de données spécifiques
    """)
    return


@app.cell
def _(F, toots_with_stream_tags):
    offres_emplois = (
        toots_with_stream_tags
            .select(
                "tweet_id",
                "created_at",
                "content_clean",
                "stream_tag",
                F.explode("tags_list").alias("tag")
            )
            .filter("tag LIKE '%job%' OR tag LIKE '%hiring%'")
            .groupBy("tweet_id", "created_at", "content_clean")
            .agg(
                F.collect_set("stream_tag").alias("stream_tags"),
                F.collect_set("tag").alias("tags")
            )
    )
    return (offres_emplois,)


@app.cell
def _(offres_emplois):
    offres_emplois.show()
    return


@app.cell
def _(offres_emplois, write_table):
    # Ecriture du résultat dans une table analytique SQL
    write_table(offres_emplois, "jobs_tweets_mastodon", "overwrite")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.4 Compter le nombre de tweet concernant une offre d'emplois dans la data
    """)
    return


@app.cell
def _(F, offres_emplois):
    nb_offres_emplois = (
        offres_emplois
            .select(F.count_distinct("tweet_id").alias("effectif"))
    )
    return (nb_offres_emplois,)


@app.cell
def _(nb_offres_emplois):
    nb_offres_emplois.show()
    return


@app.cell
def _(nb_offres_emplois):
    result_offre_emplois = nb_offres_emplois.collect()[0][0]
    return (result_offre_emplois,)


@app.cell(hide_code=True)
def _(mo, result_offre_emplois):
    mo.md(f"""
    ### Commentaire : 

    On observe parmi tous nos tweets qu'on a {result_offre_emplois} offres d'emplois dans la data
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.5 Répartition mensuelle de ces offres d'emplois
    """)
    return


@app.cell
def _(F, offres_emplois):
    nb_offres_emplois_par_mois = (
        offres_emplois
            .withColumn("month", F.month("created_at"))
            .groupBy("month")
            .agg(F.count_distinct("tweet_id").alias("nb_tweets"))
    )
    return (nb_offres_emplois_par_mois,)


@app.cell
def _(nb_offres_emplois_par_mois):
    nb_offres_emplois_par_mois.show()
    return


@app.cell
def _(nb_offres_emplois_par_mois, write_table):
    # Ecriture du résultat dans une table analytique SQL
    write_table(nb_offres_emplois_par_mois, "month_offer", "overwrite")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.6 Journée typique de tweets dans le domaine de la data
    """)
    return


@app.cell
def _(F, toots):
    typic_day_toots = (
        toots
            .select(F.to_date("created_at").alias("date"), "tweet_id", F.explode("tags_list").alias("tag"))
            .select("*", (F.col("tag").like("%job%") | F.col("tag").like("hiring")).alias("job")) 
            #.filter("job = True")
             .groupBy("date")
             .agg(F.count_distinct("tweet_id").alias("nb_tweets"), F.count_if(F.col("job")).alias("nb_tweets_offre"))
    )
    return (typic_day_toots,)


@app.cell
def _(typic_day_toots):
    typic_day_toots.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Comme on peut le voir ci-dessus, nous avons calculé pour chaque jour de l'année le nombre de tweets et le nombre d'offre parmi ces tweets.

    Maintenant passons au calcul d'indicateurs.
    """)
    return


@app.cell
def _(F, typic_day_toots):
    daily_tweets_stats = typic_day_toots.select(
        # Calcul des indicateurs de statistiques pour le nombre de tweets dans une journée
        F.mean("nb_tweets").alias("moyenne_nb_tweets"),
        F.expr("percentile(nb_tweets, 0.25)").alias("Q1_nb_tweets"),
        F.expr("percentile(nb_tweets, 0.50)").alias("mediane_nb_tweets"),
        F.expr("percentile(nb_tweets, 0.75)").alias("Q3_nb_tweets"),
        F.stddev("nb_tweets").alias("ecart_type_nb_tweets"),
    
        # Calcul des indicateurs de statistique pour le nombre d'offre dans une journée
        F.mean("nb_tweets_offre").alias("moyenne_nb_tweets_offre"),
        F.expr("percentile(nb_tweets_offre, 0.25)").alias("Q1_nb_tweets_offre"),
        F.expr("percentile(nb_tweets_offre, 0.50)").alias("mediane_nb_tweets_offre"),
        F.expr("percentile(nb_tweets_offre, 0.75)").alias("Q3_nb_tweets_offre"),
        F.stddev("nb_tweets_offre").alias("ecart_type_nb_tweets_offre")

    )
    return (daily_tweets_stats,)


@app.cell
def _(daily_tweets_stats):
    daily_tweets_stats.show()
    return


@app.cell
def _(daily_tweets_stats, mo):
    stats = daily_tweets_stats.first()

    markdown_stats = f"""
    ### 📊 Statistiques sur le nombre d'offres d'emplois par journée

    - **25 %** des journées comptent au maximum **{stats["Q1_nb_tweets_offre"]:.0f}** offres d'emplois.
    - **50 %** des journées comptent au maximum **{stats["mediane_nb_tweets_offre"]:.0f}** offres d'emplois.
    - **75 %** des journées comptent au maximum **{stats["Q3_nb_tweets_offre"]:.0f}** offres d'emplois.

    La **moyenne** est de **{stats["moyenne_nb_tweets_offre"]:.2f}** offres d'emplois par journée, avec un **écart-type** de **{stats["ecart_type_nb_tweets_offre"]:.2f}**.

    ### 📊 Statistiques sur le nombre de tweets par journée

    - **25 %** des journées comptent au maximum **{stats["Q1_nb_tweets"]:.0f}** de tweets.
    - **50 %** des journées comptent au maximum **{stats["mediane_nb_tweets"]:.0f}** de tweets.
    - **75 %** des journées comptent au maximum **{stats["Q3_nb_tweets"]:.0f}** de tweets.

    La **moyenne** est de **{stats["moyenne_nb_tweets"]:.2f}** tweets par journée, avec un **écart-type** de **{stats["ecart_type_nb_tweets"]:.2f}**.
    """

    mo.md(markdown_stats)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1.7 Le nombre de tweets par le nombre de mots clés data
    """)
    return


@app.cell
def _(F, toots_with_stream_tags):
    stream_tag_effectif = (
        toots_with_stream_tags
        .groupBy("tweet_id")
        .agg(F.count("tweet_id").alias("stream_tag_keys"))
        .groupBy("stream_tag_keys")
        .agg(F.count("tweet_id").alias("nb_tweets"))
    )
    return (stream_tag_effectif,)


@app.cell
def _(stream_tag_effectif):
    stream_tag_effectif.show()
    return


@app.cell
def _(stream_tag_effectif, write_table):
    # Ecriture du résultat dans une table analytique SQL
    write_table(stream_tag_effectif, "stream_tag_nb_tweets", "overwrite")
    return


@app.cell
def _():
    # Arrêt de la session
    #spark.stop()
    return


@app.cell
def _(toots_with_stream_tags):
    toots_with_stream_tags.show()
    return


@app.cell
def _(spark):
    spark.stop()
    return


if __name__ == "__main__":
    app.run()
